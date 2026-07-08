from __future__ import annotations

import json
import logging
import re
import time
from typing import Any, Dict, Optional

from iso_robot.config import Settings
from iso_robot.helpers.concurrency import RetryExhaustedError, with_retry
from iso_robot.integrations.azure_openai import get_async_azure_openai_client
from iso_robot.observability.context import get_task_name

logger = logging.getLogger(__name__)


class LLMRequestError(RuntimeError):
    """A chat call that failed even after retries (transport) or is misconfigured.

    Lets Celery batch tasks tell a transient-but-exhausted LLM failure (worth a
    task-level retry / DLQ) apart from a business/parse failure (handled inline).
    """


def _transient_openai_errors() -> tuple[type[BaseException], ...]:
    import openai

    return (
        openai.RateLimitError,
        openai.APITimeoutError,
        openai.APIConnectionError,
        openai.InternalServerError,
    )


def _resolve_stage(stage: str) -> str:
    return stage or get_task_name() or "unknown"


def _parse_json_object_text(text: str) -> Dict[str, Any]:
    """Parse model output as JSON; tolerate markdown fences and trailing prose."""
    raw = (text or "").strip()
    if not raw:
        return {}
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        pass
    fence = re.search(r"```(?:json)?\s*([\s\S]*?)```", raw, re.I)
    if fence:
        try:
            return json.loads(fence.group(1).strip())
        except json.JSONDecodeError:
            pass
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            return json.loads(raw[start : end + 1])
        except json.JSONDecodeError:
            pass
    logger.warning("LLM returned non-JSON; raw=%s", raw[:500])
    return {}


def _deployment(settings: Settings) -> str:
    d = (settings.azure_openai_deployment or "").strip()
    if not d:
        raise RuntimeError("AZURE_OPENAI_DEPLOYMENT is not set (chat deployment name).")
    return d


def _emit_llm_usage(deployment: str, stage: str, usage: Any, settings: Settings) -> None:
    """Record token counts (and optional cost) from an OpenAI usage object."""
    from iso_robot.observability.metrics import LLM_COST_USD_TOTAL, LLM_TOKENS_TOTAL

    prompt_tokens = int(getattr(usage, "prompt_tokens", 0) or 0)
    completion_tokens = int(getattr(usage, "completion_tokens", 0) or 0)
    if prompt_tokens:
        LLM_TOKENS_TOTAL.labels(deployment=deployment, stage=stage, kind="prompt").inc(prompt_tokens)
    if completion_tokens:
        LLM_TOKENS_TOTAL.labels(deployment=deployment, stage=stage, kind="completion").inc(completion_tokens)

    prompt_price = settings.llm_price_per_1k_prompt_usd
    completion_price = settings.llm_price_per_1k_completion_usd
    if prompt_price or completion_price:
        cost = prompt_tokens / 1000 * prompt_price + completion_tokens / 1000 * completion_price
        if cost:
            LLM_COST_USD_TOTAL.labels(deployment=deployment, stage=stage).inc(cost)


async def chat_json_object(
    settings: Settings,
    *,
    system: str,
    user: str,
    temperature: Optional[float] = None,
    stage: str = "",
    timeout: Optional[float] = None,
) -> Dict[str, Any]:
    """Call Azure OpenAI chat completions with ``response_format`` JSON object.

    Retries transient transport errors (rate limits, timeouts, 5xx) with jittered
    backoff and records latency/token/cost metrics labelled by ``stage`` (which
    defaults to the current Celery task name). Raises :class:`LLMRequestError`
    when unconfigured or after retries are exhausted.

    If ``temperature`` and ``settings.azure_openai_temperature`` are both unset,
    the parameter is omitted so the model uses its default. Some Azure deployments
    (e.g. o4-mini) only allow the default temperature and return 400 if a value
    like 0.1 is sent.
    """
    from iso_robot.observability.metrics import (
        LLM_REQUEST_DURATION_SECONDS,
        LLM_REQUESTS_TOTAL,
        LLM_RETRIES_TOTAL,
    )

    client = get_async_azure_openai_client(settings)
    if client is None:
        raise LLMRequestError(
            "Azure OpenAI is not configured (AZURE_OPENAI_ENDPOINT / AZURE_OPENAI_KEY)."
        )
    deployment = _deployment(settings)
    eff_stage = _resolve_stage(stage)
    eff_temp = temperature if temperature is not None else settings.azure_openai_temperature
    eff_timeout = timeout if timeout is not None else settings.llm_request_timeout_seconds

    kwargs: Dict[str, Any] = {
        "model": deployment,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "response_format": {"type": "json_object"},
    }
    if eff_temp is not None:
        kwargs["temperature"] = eff_temp

    def _on_retry(_attempt: int, exc: BaseException) -> None:
        LLM_RETRIES_TOTAL.labels(
            deployment=deployment, stage=eff_stage, reason=type(exc).__name__
        ).inc()

    async def _call() -> Dict[str, Any]:
        response = await client.chat.completions.create(**kwargs)
        _emit_llm_usage(deployment, eff_stage, getattr(response, "usage", None), settings)
        text = (response.choices[0].message.content or "").strip()
        return _parse_json_object_text(text)

    start = time.perf_counter()
    try:
        result = await with_retry(
            _call,
            attempts=settings.llm_retry_attempts,
            base_delay=settings.llm_retry_base_delay_seconds,
            timeout=eff_timeout,
            retry_on=_transient_openai_errors(),
            label=f"llm:{eff_stage}",
            on_retry=_on_retry,
        )
    except RetryExhaustedError as exc:
        LLM_REQUESTS_TOTAL.labels(
            deployment=deployment, stage=eff_stage, status="retry_exhausted"
        ).inc()
        raise LLMRequestError(str(exc)) from exc.last_exc
    except Exception:
        LLM_REQUESTS_TOTAL.labels(deployment=deployment, stage=eff_stage, status="error").inc()
        raise
    finally:
        LLM_REQUEST_DURATION_SECONDS.labels(deployment=deployment, stage=eff_stage).observe(
            time.perf_counter() - start
        )

    LLM_REQUESTS_TOTAL.labels(deployment=deployment, stage=eff_stage, status="ok").inc()
    return result


async def generate_structured_stub(
    settings: Settings,
    *,
    prompt: str,
    response_format: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Backward-compatible helper: JSON object chat with a single user prompt."""
    _ = response_format
    return await chat_json_object(
        settings,
        system="You return only valid JSON objects as instructed.",
        user=prompt,
    )
