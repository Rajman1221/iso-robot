from functools import lru_cache
from pathlib import Path
from typing import Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


def _backend_root() -> Path:
    """Parent of `src/` — the `backend/` package root in the repo."""
    return Path(__file__).resolve().parents[3]


def _repo_root() -> Path:
    """Monorepo root (parent of `backend/`)."""
    return _backend_root().parent


def _existing_env_files() -> tuple[str, ...]:
    paths = []
    for p in (_backend_root() / ".env", _repo_root() / ".env"):
        if p.is_file():
            paths.append(str(p))
    return tuple(paths)


class Settings(BaseSettings):
    """Application settings from environment and `.env` (backend first, repo root second)."""

    model_config = SettingsConfigDict(
        env_file=_existing_env_files() or None,
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    azure_document_intelligence_endpoint: str = ""
    azure_document_intelligence_key: str = ""
    azure_openai_endpoint: str = ""
    azure_openai_key: str = ""
    azure_openai_deployment: str = ""
    azure_openai_api_version: str = "2024-02-15-preview"
    # If unset, chat requests omit "temperature" and the deployment default applies.
    # Reasoning models (e.g. o4-mini) reject non-default temperature — leave unset.
    azure_openai_temperature: Optional[float] = Field(default=None)
    log_level: str = "INFO"

    milvus_uri: str = Field(
        default="http://localhost:19530",
        description="Milvus gRPC endpoint. In Docker Compose: http://milvus-standalone:19530.",
    )
    milvus_token: str = Field(
        default="",
        description="Auth token for managed/secured Milvus (e.g. Zilliz Cloud). Empty for local standalone.",
    )
    milvus_db_name: str = Field(default="default", description="Milvus database name.")
    milvus_collection: str = Field(
        default="iso_robot_knowledge",
        description="Single collection holding all org knowledge chunks, filtered by client_org_id.",
    )

    # Embeddings (Azure OpenAI)
    azure_openai_embedding_deployment: str = Field(
        default="",
        description="Azure OpenAI embedding deployment name (e.g. text-embedding-3-small).",
    )
    azure_openai_embedding_dim: int = Field(
        default=1536,
        description="Embedding vector dimension; must match the embedding deployment + Milvus collection.",
    )

    # Chatbot retrieval
    chatbot_top_k: int = Field(
        default=8,
        description="Default number of knowledge chunks retrieved per chat question.",
    )
    chatbot_max_context_chars: int = Field(
        default=12_000,
        description="Maximum characters of retrieved context passed to the chat model.",
    )
    
    # ── JWT auth (sliding window) ─────────────────────────────────────────────
    jwt_secret_key: str = Field(
        default="dev-only-change-me",
        description="HMAC secret for signing JWTs. Override in .env.",
    )
    jwt_algorithm: str = Field(default="HS256")
    jwt_idle_minutes: int = Field(
        default=30,
        description="Sliding window: token lifetime per request. Each authenticated "
                    "request issues a fresh token, resetting this idle timeout.",
    )

    # ── External backend verification (ingest / pipeline-status auth) ──────────
    verify_api_url: str = Field(
        default="",
        description="Existing backend endpoint ISO Robot calls to verify a user's "
                    "token. Required for the /ingest and /pipeline/status APIs.",
    )
    verify_api_key: str = Field(
        default="",
        description="Optional API key ISO Robot sends to the verify endpoint "
                    "(header X-Api-Key) to authenticate itself to the external backend.",
    )
    verify_api_timeout_seconds: float = Field(
        default=10.0,
        description="HTTP timeout (seconds) for the external verify call.",
    )
    external_auth_cache_minutes: int = Field(
        default=30,
        description="Sliding TTL (minutes) for a successful external verification. "
                    "Mirrors jwt_idle_minutes — within this window the same token is "
                    "trusted without re-calling the external backend.",
    )
    verify_api_valid_field: str = Field(
        default="",
        description="Optional JSON field in the verify response to assert validity "
                    "(e.g. 'isValid'). Empty means treat any 2xx response as valid.",
    )
    verify_api_valid_value: str = Field(
        default="",
        description="Optional expected value (string-compared) for verify_api_valid_field. "
                    "Empty means the field only needs to be truthy.",
    )
    verify_mock: bool = Field(
        default=False,
        description="When true, skip the external verify HTTP call for /ingest and "
                    "/pipeline/status — trust the supplied X-* headers after the org-id "
                    "match check. Dev/test only; never enable in production.",
    )

    def require_verify_api_url(self) -> str:
        """Return the configured verify endpoint or raise if unset."""
        if not self.verify_api_url:
            raise RuntimeError(
                "VERIFY_API_URL is not configured; the /ingest and /pipeline/status "
                "APIs require it to verify users against the external backend."
            )
        return self.verify_api_url

    database_path: str = Field(
        default_factory=lambda: str(_backend_root() / "data" / "db.sqlite"),
    )
    documents_dir: str = Field(
        default_factory=lambda: str(_repo_root() / "all-docs"),
    )

    # ── DB-agnostic persistence ─────────────────────────────────────────────────
    db_uri: str = Field(
        default="",
        description="SQLAlchemy async URI, e.g. postgresql+asyncpg://user:pass@host/db, "
                    "mysql+aiomysql://user:pass@host/db, mssql+aioodbc://user:pass@host/db, "
                    "or sqlite+aiosqlite:///path/to/file.sqlite. Empty falls back to a "
                    "sqlite URI built from database_path. Change this ONE value to move "
                    "the whole app to any supported database.",
    )
    db_pool_size: int = Field(default=10, description="SQLAlchemy engine pool size (ignored for SQLite).")
    db_pool_max_overflow: int = Field(default=20, description="SQLAlchemy engine max overflow (ignored for SQLite).")
    db_echo_sql: bool = Field(default=False, description="Log every SQL statement (debug only).")

    # ── Automated pipeline (Celery + RabbitMQ + Redis) ──────────────────────────
    celery_broker_url: str = Field(
        default="amqp://guest:guest@localhost:5672//",
        description="RabbitMQ connection URL used as the Celery broker.",
    )
    celery_result_backend: str = Field(
        default="redis://localhost:6379/0",
        description="Redis URL used as the Celery result backend (also backs chord bookkeeping and locks).",
    )
    celery_task_always_eager: bool = Field(
        default=False,
        description="Run Celery tasks synchronously in-process. Used by tests; never true in production.",
    )
    celery_task_soft_time_limit_seconds: int = Field(
        default=7200,
        description="Celery soft time limit (seconds) for pipeline tasks. LLM stages like "
                    "classify_issues can run for hours on large documents.",
    )
    celery_task_time_limit_seconds: int = Field(
        default=7500,
        description="Celery hard time limit (seconds). Must exceed celery_task_soft_time_limit_seconds.",
    )
    pipeline_ingest_temp_dir: str = Field(
        default="",
        description="Directory for ephemeral (save_to_storage=false) ingest uploads before extraction. "
                    "Empty uses the OS temp dir under 'iso-robot-ingest'.",
    )
    pipeline_save_to_storage_default: bool = Field(
        default=False,
        description="Default value of the ingest 'save_to_storage' flag when the caller omits it.",
    )

    def resolved_db_uri(self) -> str:
        """Return the configured DB_URI, or a sqlite+aiosqlite fallback built from database_path.

        This is the single knob for DB portability: point it at Postgres, MySQL,
        MSSQL, or SQLite and every repository works unchanged.
        """
        if self.db_uri:
            return self.db_uri
        return f"sqlite+aiosqlite:///{self.resolved_database_path()}"

    def resolved_pipeline_ingest_temp_dir(self) -> Path:
        if self.pipeline_ingest_temp_dir:
            return Path(self.pipeline_ingest_temp_dir).expanduser().resolve()
        import tempfile

        return Path(tempfile.gettempdir()) / "iso-robot-ingest"
    use_llm_fallback: bool = Field(
        default=True,
        description="Use local PDF text + heuristics when Azure OpenAI or Document Intelligence fail.",
    )
    control_extraction_max_chars_per_call: int = Field(
        default=100_000,
        description="If Document Intelligence text is under this size, send it in ONE LLM call (whole PDF flow).",
    )
    control_extraction_chunk_chars: int = Field(
        default=48_000,
        description="When text exceeds max_chars_per_call, split into chunks of this size.",
    )
    control_extraction_chunk_overlap: int = Field(
        default=1200,
        description="Overlap between chunks to avoid cutting requirements in half.",
    )
    control_extraction_heuristic_on_empty: bool = Field(
        default=False,
        description="If True, run keyword heuristics when the LLM returns no controls. Default False — use LLM + retry only.",
    )
    control_extraction_di_pages_per_batch: int = Field(
        default=2,
        description="When DI rejects a full PDF, analyze this many pages per DI call (streaming mode saves controls after each batch).",
    )
    control_extraction_min_local_chars: int = Field(
        default=5000,
        description="Prefer local PDF text over slow DI page-batching when at least this many characters are extractable locally.",
    )

    def resolved_database_path(self) -> Path:
        return Path(self.database_path).expanduser().resolve()

    def resolved_documents_dir(self) -> Path:
        return Path(self.documents_dir).expanduser().resolve()


@lru_cache
def get_settings() -> Settings:
    return Settings()
