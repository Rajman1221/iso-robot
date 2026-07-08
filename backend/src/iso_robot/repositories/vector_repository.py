from __future__ import annotations

import ast
import asyncio
import logging
from typing import Any, Dict, List, Optional, Sequence

from iso_robot.config import Settings

logger = logging.getLogger(__name__)

# Output fields returned by search/query (everything except the raw vector).
_OUTPUT_FIELDS = [
    "client_org_id",
    "stage",
    "entity_type",
    "entity_id",
    "source_table",
    "updated_at",
    "text",
]

_TEXT_MAX_LEN = 65535


def make_chunk_id(entity_type: str, entity_id: str, chunk_index: int) -> str:
    """Stable primary key: ``{entity_type}:{entity_id}:{chunk_index}`` (per the design)."""
    return f"{entity_type}:{entity_id}:{chunk_index}"


def _quote(value: str) -> str:
    """Quote a string literal for a Milvus boolean filter expression."""
    return '"' + str(value).replace("\\", "\\\\").replace('"', '\\"') + '"'


def _coerce_hit(hit: Any) -> Dict[str, Any]:
    """Normalise a pymilvus search hit to a plain dict.

    pymilvus ``Hit`` objects expose ``.get()`` but are not ``isinstance(..., dict)``,
    and ``to_dict()`` may return ``self``. Read id/distance/entity explicitly.
    """
    if isinstance(hit, dict):
        return hit
    if hasattr(hit, "get") and not isinstance(hit, (str, bytes)):
        entity = hit.get("entity")
        return {
            "id": hit.get("id"),
            "distance": hit.get("distance"),
            "score": hit.get("score"),
            "entity": entity if isinstance(entity, dict) else {},
        }
    if isinstance(hit, str):
        try:
            parsed = ast.literal_eval(hit)
            if isinstance(parsed, dict):
                return parsed
        except (ValueError, SyntaxError):
            logger.warning("Could not parse Milvus search hit string: %s", hit[:120])
    return {}


def _entity_from_hit(hit: Dict[str, Any]) -> Dict[str, Any]:
    """Extract scalar metadata from a search hit (nested entity or top-level fields)."""
    nested = hit.get("entity")
    if isinstance(nested, dict) and nested:
        return nested
    return {field: hit[field] for field in _OUTPUT_FIELDS if field in hit}


def _parse_search_hit(hit: Any) -> Dict[str, Any]:
    """Map a raw Milvus search hit to the repository's normalized result shape."""
    row = _coerce_hit(hit)
    if not row:
        return {
            "id": None,
            "score": None,
            "entity_type": None,
            "entity_id": None,
            "stage": None,
            "source_table": None,
            "updated_at": None,
            "text": None,
        }
    entity = _entity_from_hit(row)
    return {
        "id": row.get("id"),
        "score": row.get("distance") if row.get("distance") is not None else row.get("score"),
        "entity_type": entity.get("entity_type"),
        "entity_id": entity.get("entity_id"),
        "stage": entity.get("stage"),
        "source_table": entity.get("source_table"),
        "updated_at": entity.get("updated_at"),
        "text": entity.get("text"),
    }


class VectorRepository:
    """Repository over the single Milvus knowledge collection.

    All rows carry ``client_org_id`` (a partition key) and every search is
    pinned to one org, giving hard tenant isolation. When Milvus is not
    configured/reachable the client is ``None`` and every method is a safe
    no-op so the rest of the app keeps working.
    """

    def __init__(self, client: Optional[Any], settings: Settings) -> None:
        self._client = client
        self._settings = settings
        self._collection = settings.milvus_collection
        self._dim = int(settings.azure_openai_embedding_dim)

    @property
    def enabled(self) -> bool:
        return self._client is not None

    # Schema

    async def ensure_collection(self) -> bool:
        """Create the collection + vector index if missing, then load it. Idempotent."""
        if self._client is None:
            return False
        try:
            return await asyncio.to_thread(self._ensure_collection_sync)
        except Exception:  # noqa: BLE001
            logger.exception("Failed to ensure Milvus collection %s", self._collection)
            return False

    def _ensure_collection_sync(self) -> bool:
        from pymilvus import DataType

        client = self._client
        name = self._collection
        if not client.has_collection(name):
            schema = client.create_schema(auto_id=False, enable_dynamic_field=False)
            schema.add_field("id", DataType.VARCHAR, is_primary=True, max_length=512)
            schema.add_field("vector", DataType.FLOAT_VECTOR, dim=self._dim)
            schema.add_field(
                "client_org_id", DataType.VARCHAR, max_length=64, is_partition_key=True
            )
            schema.add_field("stage", DataType.VARCHAR, max_length=32)
            schema.add_field("entity_type", DataType.VARCHAR, max_length=64)
            schema.add_field("entity_id", DataType.VARCHAR, max_length=128)
            schema.add_field("source_table", DataType.VARCHAR, max_length=64)
            schema.add_field("updated_at", DataType.VARCHAR, max_length=40)
            schema.add_field("text", DataType.VARCHAR, max_length=_TEXT_MAX_LEN)

            index_params = client.prepare_index_params()
            index_params.add_index(
                field_name="vector", index_type="AUTOINDEX", metric_type="COSINE"
            )
            client.create_collection(name, schema=schema, index_params=index_params)
            logger.info("Created Milvus collection %s (dim=%s)", name, self._dim)

        client.load_collection(name)
        return True

    # Writes

    async def upsert(self, records: Sequence[Dict[str, Any]]) -> int:
        """Insert/overwrite chunk records. Each record must include all schema fields."""
        if self._client is None or not records:
            return 0
        rows = [self._normalize_record(r) for r in records]
        await self.ensure_collection()
        try:
            await asyncio.to_thread(self._client.upsert, self._collection, rows)
            return len(rows)
        except Exception:  # noqa: BLE001
            logger.exception("Milvus upsert failed (%d rows)", len(rows))
            return 0

    async def delete_by_entity(
        self, *, entity_type: str, entity_id: str, client_org_id: str
    ) -> bool:
        """Remove all chunks for one entity (used for idempotent delete-then-insert)."""
        if self._client is None:
            return False
        expr = (
            f"client_org_id == {_quote(client_org_id)} "
            f"and entity_type == {_quote(entity_type)} "
            f"and entity_id == {_quote(entity_id)}"
        )
        return await self._delete(expr)

    async def delete_by_org(self, client_org_id: str) -> bool:
        """Remove every chunk for an org (used before a full reindex)."""
        if self._client is None:
            return False
        return await self._delete(f"client_org_id == {_quote(client_org_id)}")

    async def delete_by_entity_type(self, *, client_org_id: str, entity_type: str) -> bool:
        """Remove all chunks of one entity type for an org (e.g. all controls)."""
        if self._client is None:
            return False
        expr = (
            f"client_org_id == {_quote(client_org_id)} "
            f"and entity_type == {_quote(entity_type)}"
        )
        return await self._delete(expr)

    async def delete_by_entity_ids(
        self, *, client_org_id: str, entity_type: str, entity_ids: Sequence[str]
    ) -> bool:
        """Remove chunks for a specific set of entities in one org (chunked ``in`` deletes).

        Replaces N single-entity deletes with a handful of batched expressions,
        so incremental re-indexing of a batch is a couple of round-trips, not one
        per row.
        """
        if self._client is None:
            return False
        ids = [str(e) for e in entity_ids if e]
        if not ids:
            return True
        ok = True
        for start in range(0, len(ids), 500):
            chunk = ids[start : start + 500]
            joined = ", ".join(_quote(e) for e in chunk)
            expr = (
                f"client_org_id == {_quote(client_org_id)} "
                f"and entity_type == {_quote(entity_type)} "
                f"and entity_id in [{joined}]"
            )
            ok = await self._delete(expr) and ok
        return ok

    async def _delete(self, expr: str) -> bool:
        await self.ensure_collection()
        try:
            await asyncio.to_thread(
                lambda: self._client.delete(self._collection, filter=expr)
            )
            return True
        except Exception:  # noqa: BLE001
            logger.exception("Milvus delete failed for expr=%s", expr)
            return False

    # Reads

    async def search(
        self,
        *,
        client_org_id: str,
        query_vector: Sequence[float],
        top_k: int = 8,
        stages: Optional[Sequence[str]] = None,
        entity_types: Optional[Sequence[str]] = None,
    ) -> List[Dict[str, Any]]:
        """Vector search scoped to one org. Returns hits with text + metadata + score."""
        if self._client is None or not query_vector:
            return []

        clauses = [f"client_org_id == {_quote(client_org_id)}"]
        if stages:
            joined = ", ".join(_quote(s) for s in stages)
            clauses.append(f"stage in [{joined}]")
        if entity_types:
            joined = ", ".join(_quote(e) for e in entity_types)
            clauses.append(f"entity_type in [{joined}]")
        expr = " and ".join(clauses)

        await self.ensure_collection()
        try:
            raw = await asyncio.to_thread(
                lambda: self._client.search(
                    collection_name=self._collection,
                    data=[list(query_vector)],
                    filter=expr,
                    limit=int(top_k),
                    output_fields=_OUTPUT_FIELDS,
                    search_params={"metric_type": "COSINE"},
                )
            )
        except Exception:  # noqa: BLE001
            logger.exception("Milvus search failed for org=%s", client_org_id)
            return []

        hits = raw[0] if raw else []
        return [_parse_search_hit(hit) for hit in hits]

    async def count(self, client_org_id: Optional[str] = None) -> int:
        """Count chunks (optionally for a single org)."""
        if self._client is None:
            return 0
        await self.ensure_collection()
        expr = f"client_org_id == {_quote(client_org_id)}" if client_org_id else ""
        try:
            rows = await asyncio.to_thread(
                lambda: self._client.query(
                    collection_name=self._collection,
                    filter=expr,
                    output_fields=["count(*)"],
                )
            )
            if rows:
                first = rows[0]
                return int(first.get("count(*)", 0))
            return 0
        except Exception:  # noqa: BLE001
            logger.exception("Milvus count failed for org=%s", client_org_id)
            return 0

    # Helpers

    def _normalize_record(self, record: Dict[str, Any]) -> Dict[str, Any]:
        text = str(record.get("text") or "")
        if len(text) > _TEXT_MAX_LEN:
            text = text[:_TEXT_MAX_LEN]
        return {
            "id": str(record["id"]),
            "vector": list(record["vector"]),
            "client_org_id": str(record.get("client_org_id") or ""),
            "stage": str(record.get("stage") or ""),
            "entity_type": str(record.get("entity_type") or ""),
            "entity_id": str(record.get("entity_id") or ""),
            "source_table": str(record.get("source_table") or ""),
            "updated_at": str(record.get("updated_at") or ""),
            "text": text,
        }
