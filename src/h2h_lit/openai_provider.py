"""Live OpenAI Responses API adapter for the frozen Stage 4 provider boundary."""

from __future__ import annotations

import json
import json as json_module
import os
from dataclasses import dataclass, field
from typing import Any
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from h2h_lit.review import (
    AssistanceMode,
    EligibilityCriterion,
    ExclusionReason,
    TaskCategory,
    VisualizationModality,
)

OPENAI_RESPONSES_URL = "https://api.openai.com/v1/responses"
SUPPORTED_PARAMETERS = {
    "max_output_tokens",
    "reasoning_effort",
    "response_schema_version",
    "store",
    "structured_output",
    "temperature",
    "verbosity",
}


@dataclass(slots=True)
class OpenAIResponsesProvider:
    """Synchronous provider that leaves proposal authority with Stage 4."""

    api_key: str
    endpoint: str = OPENAI_RESPONSES_URL
    timeout_seconds: float = 120.0
    session: Any = field(default_factory=lambda: _UrllibSession())
    response_metadata: dict[tuple[str, int], dict[str, Any]] = field(default_factory=dict)

    @classmethod
    def from_environment(
        cls,
        *,
        variable: str = "OPENAI_API_KEY",
        endpoint: str = OPENAI_RESPONSES_URL,
        timeout_seconds: float = 120.0,
    ) -> OpenAIResponsesProvider:
        api_key = os.environ.get(variable, "").strip()
        if not api_key:
            raise RuntimeError(f"required API credential is absent: {variable}")
        return cls(api_key=api_key, endpoint=endpoint, timeout_seconds=timeout_seconds)

    def generate(
        self,
        *,
        model: str,
        prompt: str,
        input_snapshot: dict[str, Any],
        parameters: dict[str, Any],
        request_id: str,
        attempt_number: int,
    ) -> str:
        unsupported = sorted(set(parameters) - SUPPORTED_PARAMETERS)
        if unsupported:
            raise ValueError(f"unsupported OpenAI parameters: {unsupported}")

        body: dict[str, Any] = {
            "model": model,
            "instructions": prompt,
            "input": json.dumps(
                input_snapshot,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
            ),
            "store": bool(parameters.get("store", False)),
            "text": {
                "format": {
                    "type": "json_schema",
                    "name": str(
                        parameters.get("structured_output", "revised_star_proposal")
                    ),
                    "strict": True,
                    "schema": _response_schema_for_version(
                        str(parameters.get("response_schema_version", "1.0.0"))
                    ),
                },
                "verbosity": str(parameters.get("verbosity", "low")),
            },
        }
        if "max_output_tokens" in parameters:
            body["max_output_tokens"] = int(parameters["max_output_tokens"])
        if "reasoning_effort" in parameters:
            body["reasoning"] = {"effort": str(parameters["reasoning_effort"])}
        if "temperature" in parameters:
            body["temperature"] = float(parameters["temperature"])

        try:
            response = self.session.post(
                self.endpoint,
                headers={
                    "Authorization": f"Bearer {self.api_key}",
                    "Content-Type": "application/json",
                    "X-Client-Request-Id": request_id,
                },
                json=body,
                timeout=self.timeout_seconds,
            )
            response.raise_for_status()
        except Exception as exc:
            self.response_metadata[(request_id, attempt_number)] = {
                "client_request_id": request_id,
                "attempt_number": attempt_number,
                "provider_error_type": type(exc).__name__,
                "provider_error": str(exc),
                "provider_error_status": getattr(exc, "status_code", None),
                "provider_error_response": getattr(exc, "response_body", None),
            }
            raise
        payload = response.json()
        if not isinstance(payload, dict):
            raise TypeError("OpenAI response envelope must be an object")

        output_text = _extract_output_text(payload)
        self.response_metadata[(request_id, attempt_number)] = {
            "provider_response_id": payload.get("id"),
            "provider_status": payload.get("status"),
            "provider_model": payload.get("model"),
            "provider_usage": payload.get("usage"),
            "provider_error": payload.get("error"),
            "client_request_id": request_id,
            "attempt_number": attempt_number,
        }
        return output_text

    def metadata_for(self, request_id: str, attempt_number: int) -> dict[str, Any]:
        return dict(self.response_metadata.get((request_id, attempt_number), {}))


class _UrllibResponse:
    def __init__(self, payload: bytes):
        self._payload = payload

    def raise_for_status(self) -> None:
        return None

    def json(self) -> Any:
        return json.loads(self._payload.decode("utf-8"))


class OpenAIHTTPError(RuntimeError):
    def __init__(self, status_code: int, response_body: str):
        super().__init__(f"OpenAI HTTP request failed with status {status_code}")
        self.status_code = status_code
        self.response_body = response_body


class _UrllibSession:
    def post(
        self,
        url: str,
        *,
        headers: dict[str, str],
        json: dict[str, Any],
        timeout: float,
    ) -> _UrllibResponse:
        request = Request(
            url,
            data=bytes(
                json_module.dumps(
                    json, sort_keys=True, separators=(",", ":"), ensure_ascii=True
                ),
                "utf-8",
            ),
            headers=headers,
            method="POST",
        )
        try:
            with urlopen(request, timeout=timeout) as response:
                return _UrllibResponse(response.read())
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            raise OpenAIHTTPError(exc.code, body) from exc


def _extract_output_text(payload: dict[str, Any]) -> str:
    chunks: list[str] = []
    output = payload.get("output")
    if not isinstance(output, list):
        raise TypeError("OpenAI response has no output array")
    for item in output:
        if not isinstance(item, dict) or item.get("type") != "message":
            continue
        content = item.get("content")
        if not isinstance(content, list):
            continue
        for part in content:
            if not isinstance(part, dict):
                continue
            if part.get("type") == "refusal":
                raise ValueError(f"OpenAI response was refused: {part.get('refusal', '')}")
            if part.get("type") == "output_text" and isinstance(part.get("text"), str):
                chunks.append(part["text"])
    if not chunks:
        raise ValueError("OpenAI response contains no output_text")
    return "".join(chunks)


def revised_star_response_schema() -> dict[str, Any]:
    evidence = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "start": {"type": "integer", "minimum": 0},
            "end": {"type": "integer", "minimum": 1},
            "quote": {"type": "string"},
            "locator": {"type": "string"},
        },
        "required": ["start", "end", "quote", "locator"],
    }
    evidence_array = {"type": "array", "items": evidence}
    criterion = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "decision": {"type": "string", "enum": ["YES", "NO", "UNCERTAIN"]},
            "certainty": {"type": "string", "enum": ["SUPPORTED", "UNCERTAIN"]},
            "evidence": {**evidence_array, "minItems": 1},
            "rationale": {"type": "string", "minLength": 1},
        },
        "required": ["decision", "certainty", "evidence", "rationale"],
    }

    def dimension(labels: list[str]) -> dict[str, Any]:
        item = {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "label": {"type": "string", "enum": labels},
                "state": {
                    "type": "string",
                    "enum": ["PRESENT", "ABSENT", "UNCERTAIN"],
                },
                "certainty": {"type": "string", "enum": ["SUPPORTED", "UNCERTAIN"]},
                "evidence": evidence_array,
                "rationale": {"type": "string", "minLength": 1},
            },
            "required": ["label", "state", "certainty", "evidence", "rationale"],
        }
        return {
            "type": "array",
            "items": item,
            "minItems": len(labels),
            "maxItems": len(labels),
        }

    exclusion_values = [item.value for item in ExclusionReason]
    criteria = {item.value: criterion for item in EligibilityCriterion}
    return {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "criteria": {
                "type": "object",
                "additionalProperties": False,
                "properties": criteria,
                "required": list(criteria),
            },
            "assistance_modes": dimension([item.value for item in AssistanceMode]),
            "visualization_modalities": dimension(
                [item.value for item in VisualizationModality]
            ),
            "tasks": dimension([item.value for item in TaskCategory]),
            "primary_exclusion_reason": {
                "type": ["string", "null"],
                "enum": [*exclusion_values, None],
            },
            "secondary_exclusion_reasons": {
                "type": "array",
                "items": {"type": "string", "enum": exclusion_values},
            },
            "overall_rationale": {"type": "string", "minLength": 1},
        },
        "required": [
            "criteria",
            "assistance_modes",
            "visualization_modalities",
            "tasks",
            "primary_exclusion_reason",
            "secondary_exclusion_reasons",
            "overall_rationale",
        ],
    }


def _response_schema_for_version(version: str) -> dict[str, Any]:
    if version == "1.0.0":
        return revised_star_response_schema()
    if version == "1.1.0":
        from h2h_lit.pilot5b import pilot5b_response_schema

        return pilot5b_response_schema()
    if version == "1.2.0":
        from h2h_lit.pilot5c import pilot5c_response_schema

        return pilot5c_response_schema()
    raise ValueError(f"unsupported response schema version: {version}")
