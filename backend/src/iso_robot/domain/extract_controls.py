from __future__ import annotations

import asyncio
import io
import logging
import re
import uuid
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings
from iso_robot.domain.heuristics import heuristic_controls_from_text
from iso_robot.domain.indexing_service import build_indexing_service
from iso_robot.domain.llm_service import chat_json_object
from iso_robot.helpers.concurrency import gather_bounded
from iso_robot.helpers.pdf_text import extract_pdf_text, extract_pdf_text_with_page_markers
from iso_robot.helpers.text_chunk import chunk_by_chars
from iso_robot.integrations.document_intelligence import analyze_pdf_bytes_marked
from iso_robot.repositories.control_repository import ControlRepository
from iso_robot.repositories.document_repository import DocumentRepository
from iso_robot.repositories.job_repository import JobRepository

logger = logging.getLogger(__name__)

_PAGE_MARKER_RE = re.compile(r"\[PAGE\s+(\d+)\]", re.I)


def _control_system_prompt(*, has_page_markers: bool) -> str:
    base = (
        "You extract auditable requirements from regulatory and policy text (IMO circulars, ISO annexes, "
        "maritime guidance, internal policies). "
        "A control is one distinct obligation, requirement, recommendation to act, mandatory procedure, "
        "or measurable rule — not document titles, headers, or generic introductions.\n"
        "**Granularity:** Prefer **many** controls: **one JSON object per numbered clause, bullet, "
        "sub-clause, table row, or distinct sentence** that states a duty or recommendation. "
        "Do **not** merge unrelated bullets into one control_text. "
        "A page with 12 bullets should yield about 12 controls unless two are duplicate wording.\n"
        "Return exactly one JSON object with key \"controls\" whose value is an array of objects. "
        'Each object MUST include \"control_text\" (string), confidence(float between 0 and 1) , reasoning(short explanation on confidence score): one requirement, verbatim or lightly edited for clarity. '
        "Optional: section_ref, framework, source_page. "
    )
    page_instr = (
        "Optional key \"source_page\" (integer): **required whenever possible**. "
        "The segment may contain lines `[PAGE N]` from the PDF layout — use the **N** from the **most recent** "
        "`[PAGE N]` line **above** that requirement's text in this segment. "
        "If a requirement spans pages, use the page where it begins.\n"
        if has_page_markers
        else 'Optional \"source_page\" (integer) only if this segment explicitly shows a page number near the clause.\n'
    )
    tail = (
        "Include **every** distinct requirement in the supplied text. Merge exact duplicates only. "
        "Respond with JSON only — no markdown fences, no commentary outside the JSON object."
    )
    return base + page_instr + tail


def _build_control_user_prompt(
    label: str,
    chunk: str,
    chunk_index: int,
    total_chunks: int,
    *,
    retry: bool,
    has_page_markers: bool,
) -> str:
    if total_chunks == 1:
        scope = (
            "SCOPE: FULL DOCUMENT — the text below is the entire PDF as extracted text "
            "(Azure Document Intelligence layout). Extract controls from all parts."
        )
    else:
        scope = (
            f"SCOPE: SEGMENT {chunk_index + 1} OF {total_chunks} — overlapping slices of the same PDF. "
            "Extract every requirement that appears in this segment only."
        )
    retry_note = ""
    if retry:
        retry_note = (
            "\n\nYour previous answer had too few or empty \"control_text\" entries. "
            "Re-read the text: emit **one control per bullet / numbered item / distinct obligation**. "
            "Every item MUST use the key \"control_text\". "
            "Do not return a single merged paragraph unless the source truly has only one requirement.\n"
        )
    markers_note = ""
    if has_page_markers:
        markers_note = (
            "\n\nPAGE NUMBERS: This text contains `[PAGE N]` lines inserted before each layout paragraph. "
            "For **every** control, set `\"source_page\"` to the integer N from the `[PAGE N]` line that applies "
            "(the last `[PAGE N]` still above that requirement).\n"
        )
    return (
        f"Document: {label}\n"
        f"{scope}\n"
        f"{retry_note}"
        f"{markers_note}\n"
        f"--- BEGIN TEXT ---\n{chunk}\n--- END TEXT ---\n\n"
        'Respond with JSON only: {"controls": [{"control_text": "...", "section_ref": null, "framework": null, "source_page": null}]}'
    )


def _chunk_has_page_markers(chunk: str) -> bool:
    return bool(_PAGE_MARKER_RE.search(chunk))


def _annotate_pages_from_chunk_markers(chunk: str, controls: List[Dict[str, Any]]) -> None:
    """Fill missing source_page using `[PAGE N]` positions vs control text location in chunk."""
    markers = [(m.start(), int(m.group(1))) for m in _PAGE_MARKER_RE.finditer(chunk)]
    if not markers:
        return
    for c in controls:
        if c.get("source_page") is not None:
            continue
        ct = (c.get("control_text") or "").strip()
        if not ct:
            continue
        needle = ct[:120]
        pos = chunk.find(needle)
        if pos < 0 and ct.split():
            pos = chunk.find(ct.split()[0][:50])
        if pos < 0:
            pos = 0
        page = markers[0][1]
        for start, pn in markers:
            if start <= pos:
                page = pn
            else:
                break
        c["source_page"] = page


def _control_text_from_llm_item(item: Dict[str, Any]) -> Optional[str]:
    for key in (
        "control_text",
        "text",
        "requirement",
        "statement",
        "description",
        "summary",
        "control",
        "obligation",
        "measure",
    ):
        v = item.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if len(s) >= 12:
            return s
    return None


async def _llm_controls_from_chunk(
    settings: Settings,
    document_label: str,
    chunk: str,
    chunk_index: int,
    total_chunks: int,
    *,
    retry: bool,
) -> List[Dict[str, Any]]:
    has_pm = _chunk_has_page_markers(chunk)
    user = _build_control_user_prompt(
        document_label,
        chunk,
        chunk_index,
        total_chunks,
        retry=retry,
        has_page_markers=has_pm,
    )
    data = await chat_json_object(
        settings,
        system=_control_system_prompt(has_page_markers=has_pm),
        user=user,
        stage="extract_controls",
    )
    raw = data.get("controls")
    if not isinstance(raw, list):
        return []
    out: List[Dict[str, Any]] = []
    for item in raw:
        if isinstance(item, str):
            s = item.strip()
            if len(s) >= 12:
                out.append(
                    {
                        "control_text": s[:2000],
                        "section_ref": None,
                        "framework": None,
                        "source_page": None,
                    }
                )
            continue
        if not isinstance(item, dict):
            continue
        text = _control_text_from_llm_item(item)
        if not text:
            continue
        page = item.get("source_page")
        try:
            page_i = int(page) if page is not None else None
        except (TypeError, ValueError):
            page_i = None
        out.append(
            {
                "control_text": text[:2000],
                "confidence": item.get("confidence"),
                "reasoning":item.get("reasoning"),
                "section_ref": item.get("section_ref"),
                "framework": item.get("framework"),
                "source_page": page_i,
            }
        )
    _annotate_pages_from_chunk_markers(chunk, out)
    return out


async def _llm_try_chunk(
    settings: Settings,
    label: str,
    chunk: str,
    chunk_index: int,
    total_chunks: int,
) -> List[Dict[str, Any]]:
    first = await _llm_controls_from_chunk(
        settings, label, chunk, chunk_index, total_chunks, retry=False
    )
    if first:
        return first
    if len(chunk.strip()) < 400:
        return []
    logger.warning(
        "Chunk %s/%s: LLM returned no controls (%d chars); retrying.",
        chunk_index + 1,
        total_chunks,
        len(chunk.strip()),
    )
    return await _llm_controls_from_chunk(
        settings, label, chunk, chunk_index, total_chunks, retry=True
    )


_MIN_CHUNK_CHARS_FOR_EMPTY_HEURISTIC = 80


async def _controls_from_chunk_with_fallback(
    settings: Settings,
    label: str,
    chunk: str,
    chunk_index: int,
    total_chunks: int,
) -> List[Dict[str, Any]]:
    chunk_stripped = chunk.strip()

    async def _heuristic_salvage() -> List[Dict[str, Any]]:
        if not settings.control_extraction_heuristic_on_empty:
            return []
        if len(chunk_stripped) < _MIN_CHUNK_CHARS_FOR_EMPTY_HEURISTIC:
            return []
        logger.warning(
            "No LLM controls for chunk %s; heuristic salvage (CONTROL_EXTRACTION_HEURISTIC_ON_EMPTY=true).",
            chunk_index,
        )
        rows = heuristic_controls_from_text(chunk, chunk_index)
        _annotate_pages_from_chunk_markers(chunk, rows)
        return rows

    try:
        part = await _llm_try_chunk(settings, label, chunk, chunk_index, total_chunks)
        if part:
            return part
        if settings.control_extraction_heuristic_on_empty and settings.use_llm_fallback:
            return await _heuristic_salvage()
        return []
    except Exception as exc:
        logger.warning("LLM control extraction failed chunk=%s: %s", chunk_index, exc)
        if settings.use_llm_fallback:
            rows = heuristic_controls_from_text(chunk, chunk_index)
            _annotate_pages_from_chunk_markers(chunk, rows)
            return rows
        raise


def _dedupe_key(control_text: str) -> str:
    return " ".join(control_text.lower().split())[:260]


def _di_rejected_as_oversized(exc: Exception) -> bool:
    msg = str(exc)
    return "InvalidContentLength" in msg or "input image is too large" in msg.lower()


def _shift_page_markers(text: str, page_offset: int) -> str:
    if page_offset <= 0:
        return text
    return _PAGE_MARKER_RE.sub(lambda m: f"[PAGE {int(m.group(1)) + page_offset}]", text)


async def _iter_di_page_batch_texts(
    settings: Settings,
    path: Path,
    *,
    pages_per_batch: int,
) -> AsyncIterator[tuple[int, int, str]]:
    """Yield (batch_index, total_batches, marked_text) for each DI page batch.

    The Azure DI analyze calls are pure network I/O (no DB), so they run
    concurrently under ``settings.di_batch_concurrency`` while results are still
    yielded in page order.
    """
    try:
        from pypdf import PdfReader, PdfWriter
    except Exception:
        return
    try:
        reader = await asyncio.to_thread(PdfReader, str(path))
    except Exception:
        return
    total_pages = len(reader.pages)
    if total_pages <= 0:
        return
    step = max(1, pages_per_batch)
    total_batches = (total_pages + step - 1) // step

    # First slice the PDF into per-batch byte blobs (pypdf only — no network/DB).
    batch_blobs: List[tuple[int, int, int, bytes]] = []  # (batch_idx, start, end, bytes)
    for batch_idx, start in enumerate(range(0, total_pages, step)):
        end = min(total_pages, start + step)
        writer = PdfWriter()
        for i in range(start, end):
            writer.add_page(reader.pages[i])
        buf = io.BytesIO()
        try:
            await asyncio.to_thread(writer.write, buf)
        except Exception as seg_exc:
            logger.warning(
                "DI batch slice failed for %s pages %s-%s: %s",
                path.name, start + 1, end, seg_exc,
            )
            continue
        batch_blobs.append((batch_idx, start, end, buf.getvalue()))

    if not batch_blobs:
        return

    # Analyze the batches concurrently; the shared AsyncSession is never touched
    # here so this is safe, and results come back in submission (page) order.
    analyzed = await gather_bounded(
        [
            (lambda b=blob: analyze_pdf_bytes_marked(settings, b))
            for (_bi, _st, _en, blob) in batch_blobs
        ],
        limit=settings.di_batch_concurrency,
        label="di_page_batch",
        return_exceptions=True,
    )

    for (batch_idx, start, end, _blob), seg_text in zip(batch_blobs, analyzed):
        if isinstance(seg_text, BaseException):
            logger.warning(
                "DI batch failed for %s pages %s-%s: %s",
                path.name, start + 1, end, seg_text,
            )
            continue
        seg_text = _shift_page_markers(seg_text or "", page_offset=start).strip()
        if seg_text:
            yield batch_idx, total_batches, seg_text


def _iter_char_chunks(text: str, settings: Settings) -> List[str]:
    full = text.strip()
    if not full:
        return []
    max_single = settings.control_extraction_max_chars_per_call
    if len(full) <= max_single:
        return [full]
    return chunk_by_chars(
        full,
        max_chars=settings.control_extraction_chunk_chars,
        overlap=settings.control_extraction_chunk_overlap,
    )


async def _iter_document_text_segments(
    settings: Settings,
    path: Path,
    pdf_bytes: bytes,
) -> AsyncIterator[tuple[int, int, str, str]]:
    """Yield (segment_index, total_segments, text, source_label) incrementally.

    Large PDFs stream page batches or char chunks so controls can be saved before
    the full document finishes processing.
    """
    local = extract_pdf_text_with_page_markers(path, max_pages=None)
    local_len = len((local or "").strip())
    pages_per_batch = max(1, settings.control_extraction_di_pages_per_batch)

    try:
        di = await analyze_pdf_bytes_marked(settings, pdf_bytes)
        di_len = len((di or "").strip())
        if local_len > max(di_len * 2, di_len + 2000):
            logger.info(
                "Using local PDF text for %s (local=%d chars, di=%d chars); streaming chunks.",
                path.name,
                local_len,
                di_len,
            )
            chunks = _iter_char_chunks(local, settings)
            total = len(chunks)
            for i, ch in enumerate(chunks):
                yield i, total, ch, "local_pdf"
            return
        if di.strip():
            chunks = _iter_char_chunks(di, settings)
            total = len(chunks)
            for i, ch in enumerate(chunks):
                yield i, total, ch, "document_intelligence"
            return
    except Exception as exc:
        if _di_rejected_as_oversized(exc):
            if local_len >= settings.control_extraction_min_local_chars:
                logger.info(
                    "DI rejected full PDF (%s); using local text (%d chars) in streaming chunks.",
                    path.name,
                    local_len,
                )
                chunks = _iter_char_chunks(local, settings)
                total = len(chunks)
                for i, ch in enumerate(chunks):
                    yield i, total, ch, "local_pdf"
                return
            logger.warning(
                "DI rejected full PDF (%s); streaming DI in %s-page batches: %s",
                path.name,
                pages_per_batch,
                exc,
            )
            batch_count = 0
            async for batch_idx, total_batches, seg_text in _iter_di_page_batch_texts(
                settings, path, pages_per_batch=pages_per_batch
            ):
                batch_count += 1
                yield batch_idx, total_batches, seg_text, "document_intelligence_batch"
            if batch_count:
                return
        else:
            logger.warning("Document Intelligence failed (%s); falling back to local text.", exc)

    if local.strip():
        chunks = _iter_char_chunks(local, settings)
        total = len(chunks)
        for i, ch in enumerate(chunks):
            yield i, total, ch, "local_pdf"
        return

    plain = extract_pdf_text(path, max_pages=None)
    if plain.strip():
        chunks = _iter_char_chunks(plain, settings)
        total = len(chunks)
        for i, ch in enumerate(chunks):
            yield i, total, ch, "local_pdf_plain"


async def _persist_controls_from_segment(
    *,
    part: List[Dict[str, Any]],
    document_id: str,
    client_org_id: Optional[str],
    ctrl_repo: ControlRepository,
    seen_keys: set[str],
) -> int:
    """Dedup an already-extracted segment against ``seen_keys`` and persist it.

    The LLM/parse step runs concurrently upstream; this DB-touching pass stays
    sequential so the shared AsyncSession is only ever used by one coroutine.
    Returns the number of rows inserted.
    """
    batch: List[Dict[str, Any]] = []
    for c in part:
        ct = (c.get("control_text") or "").strip()
        if not ct:
            continue
        key = _dedupe_key(ct)
        if key in seen_keys:
            continue
        seen_keys.add(key)
        batch.append(
            {
                "id": str(uuid.uuid4()),
                "document_id": document_id,
                "client_org_id": client_org_id,
                "control_text": ct,
                "condidence":c.get("confidence"),
                "reasoning":c.get("reasoning"),
                "section_ref": str(c["section_ref"]) if c.get("section_ref") else None,
                "framework": str(c["framework"]) if c.get("framework") else None,
                "source_page": c.get("source_page"),
            }
        )
    if batch:
        await ctrl_repo.insert_many(batch)
    return len(batch)


async def _report_extract_progress(
    jobs: Optional[JobRepository],
    job_id: Optional[str],
    *,
    document_id: str,
    document_name: str,
    segment_index: int,
    total_segments: int,
    source: str,
    controls_total: int,
    segment_controls: int,
) -> None:
    if jobs is None or not job_id:
        return
    await jobs.merge_payload(
        job_id,
        {
            "progress": {
                "document_id": document_id,
                "document_name": document_name,
                "segment": segment_index + 1,
                "total_segments": total_segments,
                "source": source,
                "controls_created": controls_total,
                "last_segment_controls": segment_controls,
            }
        },
    )


async def extract_controls_for_document(
    settings: Settings,
    conn: AsyncSession,
    document_id: str,
    client_org_id: Optional[str] = None,
    *,
    job_id: Optional[str] = None,
    jobs: Optional[JobRepository] = None,
) -> List[str]:
    """Re-extract a document's controls; return the ids of the controls created.

    Also incrementally re-indexes ONLY this document's controls in the vector
    store — never an org-wide reindex.
    """
    doc_repo = DocumentRepository(conn)
    ctrl_repo = ControlRepository(conn)
    row = await doc_repo.get_by_id(document_id)
    if row is None:
        raise ValueError(f"Unknown document_id={document_id}")
    path = Path(str(row["path"]))
    if not path.exists():
        raise FileNotFoundError(
            f"Document PDF not found at {path}. "
            "In Docker, set PIPELINE_INGEST_TEMP_DIR under the shared backend/data mount, "
            "or ingest with save_to_storage=true."
        )
    if path.suffix.lower() != ".pdf":
        raise ValueError(f"Only PDF extraction is supported; got {path.suffix}")

    # Remember the controls we are about to replace so their (now-stale) vector
    # chunks can be purged after re-extraction — each run mints fresh UUIDs, so
    # index_controls (which only delete-then-inserts the *new* ids) would leave
    # the old chunks orphaned.
    old_control_ids = [str(c["id"]) for c in await ctrl_repo.get_by_document(document_id)]
    await ctrl_repo.delete_for_document(document_id)

    pdf_bytes = await asyncio.to_thread(path.read_bytes)
    label = f"{row.get('filename') or path.name} ({document_id})"
    seen_keys: set[str] = set()
    running_total = 0

    # Text segmentation (DI / local PDF) touches no DB, so materialize it and run
    # the per-segment LLM extraction concurrently below.
    segments = [seg async for seg in _iter_document_text_segments(settings, path, pdf_bytes)]
    if not segments:
        logger.info("No text extracted for document %s", document_id)
        return []

    # Concurrency rule: ONLY the pure LLM/parse step fans out. The shared
    # AsyncSession is never touched here, so it can't be used by two coroutines
    # at once — every DB write happens in the sequential persist pass afterwards.
    extracted = await gather_bounded(
        [
            (
                lambda text=text, idx=seg_idx, total=total_segs: _controls_from_chunk_with_fallback(
                    settings, label, text, idx, total
                )
            )
            for (seg_idx, total_segs, text, source) in segments
        ],
        limit=settings.extract_segment_concurrency,
        label="extract_controls_segment",
        return_exceptions=True,
    )

    # Sequential persist pass: dedup (seen_keys) + insert per segment, in order,
    # preserving the original first-occurrence-wins dedup semantics.
    for (seg_idx, total_segs, text, source), part in zip(segments, extracted):
        if isinstance(part, BaseException):
            # Error isolation: a single failing segment must not kill the run.
            logger.warning(
                "Controls segment %s/%s (%s) extraction failed: %s — skipping",
                seg_idx + 1,
                total_segs,
                source,
                part,
            )
            continue
        added = await _persist_controls_from_segment(
            part=part,
            document_id=document_id,
            client_org_id=client_org_id,
            ctrl_repo=ctrl_repo,
            seen_keys=seen_keys,
        )
        running_total += added
        logger.info(
            "Controls segment %s/%s (%s) committed %s rows (running total %s) — %s",
            seg_idx + 1,
            total_segs,
            source,
            added,
            running_total,
            path.name,
        )
        await _report_extract_progress(
            jobs,
            job_id,
            document_id=document_id,
            document_name=path.name,
            segment_index=seg_idx,
            total_segments=total_segs,
            source=source,
            controls_total=running_total,
            segment_controls=added,
        )

    logger.info(
        "Controls extraction finished document=%s rows=%s path=%s",
        document_id,
        running_total,
        path.name,
    )

    # Incrementally index ONLY this document's controls — replaces the old
    # per-document org-wide reindex that re-embedded the entire growing corpus.
    controls_for_doc = await ctrl_repo.get_by_document(document_id)
    created_ids = [str(c["id"]) for c in controls_for_doc]
    if client_org_id:
        indexing = build_indexing_service(settings, conn)
        if old_control_ids:
            # Purge chunks for the superseded controls (their UUIDs no longer exist);
            # index_controls only clears the *new* entity ids before upserting.
            await indexing._vectors.delete_by_entity_ids(
                client_org_id=client_org_id,
                entity_type="control",
                entity_ids=old_control_ids,
            )
        await indexing.index_controls(client_org_id, controls_for_doc)

    return created_ids


async def run_extract_controls_job(
    settings: Settings,
    conn: AsyncSession,
    payload: dict[str, Any],
    *,
    job_id: Optional[str] = None,
) -> dict[str, Any]:
    raw_ids = payload.get("document_ids")
    doc_repo = DocumentRepository(conn)
    jobs = JobRepository(conn) if job_id else None
    if isinstance(raw_ids, list) and raw_ids:
        doc_ids = [str(x) for x in raw_ids]
    else:
        rows = await doc_repo.list_all(limit=500, offset=0)
        doc_ids = [str(r["id"]) for r in rows if str(r.get("path", "")).lower().endswith(".pdf")]

    cid = payload.get("client_org_id")
    # Ids of the controls (re)created this run — the Celery layer uses these to
    # drive incremental issue generation. Each document is indexed on its own
    # inside extract_controls_for_document (no org-wide reindex per document).
    control_ids: List[str] = []
    for doc_idx, doc_id in enumerate(doc_ids):
        if jobs and job_id:
            await jobs.merge_payload(
                job_id,
                {
                    "progress": {
                        "documents_total": len(doc_ids),
                        "document_index": doc_idx + 1,
                        "current_document_id": doc_id,
                    }
                },
            )
        try:
            control_ids.extend(
                await extract_controls_for_document(
                    settings,
                    conn,
                    doc_id,
                    client_org_id=cid,
                    job_id=job_id,
                    jobs=jobs,
                )
            )
        except (FileNotFoundError, PermissionError, OSError):
            raise
        except Exception:
            logger.exception("Extract failed for document %s", doc_id)
            if not settings.use_llm_fallback:
                raise

    return {"control_ids": control_ids, "documents": len(doc_ids)}
