from __future__ import annotations

import logging

from sqlalchemy.ext.asyncio import AsyncSession

from iso_robot.config import Settings
from iso_robot.helpers.org_paths import (
    ensure_org_folders_on_disk,
    resolve_file_in_folder,
    stored_path_usable,
)
from iso_robot.repositories.org_repository import (
    ControlDocumentRepository,
    FolderRepository,
    OrgRepository,
)

logger = logging.getLogger(__name__)


async def sync_org_folder_mapping(
    settings: Settings,
    folder_repo: FolderRepository,
    *,
    client_org_id: str,
    org_slug: str,
) -> dict[str, str]:
    """Ensure on-disk folders exist and folder_mapping uses this host's absolute paths."""
    canonical = ensure_org_folders_on_disk(settings, org_slug)
    for folder_type, path in canonical.items():
        await folder_repo.set_folder_path(
            client_org_id=client_org_id,
            folder_type=folder_type,
            folder_path=path,
        )
    return canonical


async def repair_storage_paths(conn: AsyncSession, settings: Settings) -> None:
    """On startup: fix folder_mapping and control_documents paths after DB migration between hosts."""
    org_repo = OrgRepository(conn)
    folder_repo = FolderRepository(conn)
    doc_repo = ControlDocumentRepository(conn)

    orgs = await org_repo.list_all()
    repaired_folders = 0
    repaired_docs = 0

    for org in orgs:
        org_id = str(org["id"])
        slug = str(org["slug"])
        folders = await sync_org_folder_mapping(
            settings,
            folder_repo,
            client_org_id=org_id,
            org_slug=slug,
        )
        repaired_folders += len(folders)

        ctrl_folder = folders["control_documents"]
        for doc in await doc_repo.list_for_org(org_id):
            filename = str(doc.get("filename") or "")
            stored = str(doc.get("document_path") or "")
            if not filename:
                continue
            resolved = resolve_file_in_folder(ctrl_folder, filename, stored)
            new_path = str(resolved)
            if new_path != stored or not stored_path_usable(stored):
                await doc_repo.update_document_path(str(doc["id"]), new_path)
                repaired_docs += 1

    if repaired_folders or repaired_docs:
        logger.info(
            "Storage paths synced to %s (orgs=%s, folder rows=%s, document paths=%s)",
            org_documents_label(settings),
            len(orgs),
            repaired_folders,
            repaired_docs,
        )


def org_documents_label(settings: Settings) -> str:
    from iso_robot.helpers.org_paths import org_documents_root

    return str(org_documents_root(settings).resolve())
