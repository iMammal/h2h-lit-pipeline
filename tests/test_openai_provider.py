import json

import pytest

from h2h_lit.openai_provider import (
    OpenAIHTTPError,
    OpenAIResponsesProvider,
    revised_star_response_schema,
)
from h2h_lit.pilot5b import pilot5b_response_schema
from h2h_lit.review import AssistanceMode, EligibilityCriterion


class FakeResponse:
    def __init__(self, payload, *, error=None):
        self.payload = payload
        self.error = error

    def raise_for_status(self):
        if self.error:
            raise self.error

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, response):
        self.response = response
        self.calls = []

    def post(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.response


def test_responses_provider_uses_strict_schema_and_retains_usage_metadata():
    output = json.dumps({"proposal": "preserved"})
    session = FakeSession(
        FakeResponse(
            {
                "id": "resp_123",
                "status": "completed",
                "model": "gpt-5-mini-2025-08-07",
                "usage": {"input_tokens": 100, "output_tokens": 20},
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": output}],
                    }
                ],
            }
        )
    )
    provider = OpenAIResponsesProvider(api_key="test-key", session=session)

    result = provider.generate(
        model="gpt-5-mini-2025-08-07",
        prompt="frozen prompt",
        input_snapshot={"text": "record"},
        parameters={
            "max_output_tokens": 6000,
            "reasoning_effort": "low",
            "response_schema_version": "1.0.0",
            "store": False,
            "structured_output": "revised_star_proposal_v1_0_0",
            "verbosity": "low",
        },
        request_id="request:1",
        attempt_number=1,
    )

    assert result == output
    _, kwargs = session.calls[0]
    assert kwargs["headers"]["X-Client-Request-Id"] == "request:1"
    assert kwargs["json"]["store"] is False
    assert kwargs["json"]["reasoning"] == {"effort": "low"}
    assert kwargs["json"]["text"]["format"]["strict"] is True
    assert kwargs["json"]["text"]["format"]["schema"] == revised_star_response_schema()
    metadata = provider.metadata_for("request:1", 1)
    assert metadata["provider_response_id"] == "resp_123"
    assert metadata["provider_usage"]["output_tokens"] == 20


def test_response_schema_is_derived_from_frozen_vocabularies():
    schema = revised_star_response_schema()

    criteria = schema["properties"]["criteria"]
    assert set(criteria["required"]) == {item.value for item in EligibilityCriterion}
    assistance = schema["properties"]["assistance_modes"]
    assert assistance["minItems"] == len(AssistanceMode)
    assert set(assistance["items"]["properties"]["label"]["enum"]) == {
        item.value for item in AssistanceMode
    }


def test_responses_provider_selects_pilot5b_schema_explicitly():
    output = json.dumps({"proposal": "preserved"})
    session = FakeSession(
        FakeResponse(
            {
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "output_text", "text": output}],
                    }
                ]
            }
        )
    )
    provider = OpenAIResponsesProvider(api_key="test-key", session=session)

    provider.generate(
        model="model",
        prompt="prompt",
        input_snapshot={"text": "record"},
        parameters={"response_schema_version": "1.1.0"},
        request_id="request:5b",
        attempt_number=1,
    )

    _, kwargs = session.calls[0]
    assert kwargs["json"]["text"]["format"]["schema"] == pilot5b_response_schema()


def test_provider_rejects_unknown_parameters_before_network_use():
    session = FakeSession(FakeResponse({}))
    provider = OpenAIResponsesProvider(api_key="test-key", session=session)

    with pytest.raises(ValueError, match="unsupported OpenAI parameters"):
        provider.generate(
            model="model",
            prompt="prompt",
            input_snapshot={"text": "record"},
            parameters={"top_p": 0.5},
            request_id="request:1",
            attempt_number=1,
        )

    assert session.calls == []


def test_provider_rejects_refusal_without_coercion():
    session = FakeSession(
        FakeResponse(
            {
                "output": [
                    {
                        "type": "message",
                        "content": [{"type": "refusal", "refusal": "cannot comply"}],
                    }
                ]
            }
        )
    )
    provider = OpenAIResponsesProvider(api_key="test-key", session=session)

    with pytest.raises(ValueError, match="refused"):
        provider.generate(
            model="model",
            prompt="prompt",
            input_snapshot={"text": "record"},
            parameters={},
            request_id="request:1",
            attempt_number=1,
        )


def test_provider_preserves_http_error_response_metadata():
    session = FakeSession(FakeResponse({}, error=OpenAIHTTPError(429, '{"error":"rate"}')))
    provider = OpenAIResponsesProvider(api_key="test-key", session=session)

    with pytest.raises(OpenAIHTTPError):
        provider.generate(
            model="model",
            prompt="prompt",
            input_snapshot={"text": "record"},
            parameters={},
            request_id="request:error",
            attempt_number=1,
        )

    metadata = provider.metadata_for("request:error", 1)
    assert metadata["provider_error_status"] == 429
    assert metadata["provider_error_response"] == '{"error":"rate"}'
