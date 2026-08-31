import json
from pathlib import Path

from h2h_lit.inference import MockInferenceProvider
from h2h_lit.pilot import (
    build_pilot_dataset,
    inference_input_for,
    prepare_pilot,
    run_live_pilot,
)
from h2h_lit.review import (
    AnnotationState,
    AssistanceMode,
    EligibilityCriterion,
    TaskCategory,
    TriState,
    VisualizationModality,
)

NOW = "2026-08-30T12:00:00+00:00"


def _write_config(tmp_path: Path, *, include_bibtex: bool = True) -> Path:
    foundational = tmp_path / "foundational.json"
    foundational.write_text(
        json.dumps(
            {
                "papers": [
                    {
                        "id": "paper-foundation",
                        "title": "Foundational interactive visual analytics",
                        "authors": ["A. Author"],
                        "year": 2024,
                        "metadata": {},
                    }
                ]
            }
        ),
        encoding="utf-8",
    )
    bib_path = tmp_path / "sample.bib"
    bib_path.write_text(
        """@article{eligible,
 title={Interactive biological network visual analytics},
 abstract={Researchers interactively inspect a biological network while an algorithm suggests related pathways.},
 author={A and B}, year={2024}, doi={10.1000/eligible}, note={Source: Local, OpenAccess: True}
}
@article{ineligible,
 title={Static economic forecast table},
 abstract={This economics paper reports a static table without life science applications.},
 author={C}, year={2022}, doi={10.1000/ineligible}, note={Source: Local, OpenAccess: False}
}
""",
        encoding="utf-8",
    )
    config = {
        "config_version": "1.0.0",
        "historical_root_environment": "TEST_HISTORICAL_ROOT_UNUSED",
        "historical_root_default": str(tmp_path),
        "foundational_source": str(foundational),
        "model": {
            "provider": "OpenAI",
            "name": "gpt-5-mini-2025-08-07",
            "parameters": {
                "max_output_tokens": 6000,
                "reasoning_effort": "low",
                "response_schema_version": "1.0.0",
                "store": False,
                "structured_output": "revised_star_proposal_v1_0_0",
                "verbosity": "low",
            },
            "pricing_usd_per_million_tokens": {
                "input": 0.25,
                "cached_input": 0.025,
                "output": 2.0,
            },
        },
        "retry_limit_per_record": 1,
        "foundational_papers": [
            {"paper_id": "paper-foundation", "selection_stratum": "ambiguous"}
        ],
        "bibtex_records": (
            [
                {"path": "sample.bib", "key": "eligible", "selection_stratum": "likely"},
                {
                    "path": "sample.bib",
                    "key": "ineligible",
                    "selection_stratum": "unlikely",
                },
            ]
            if include_bibtex
            else []
        ),
    }
    config_path = tmp_path / "pilot.json"
    config_path.write_text(json.dumps(config), encoding="utf-8")
    return config_path


def _payload(text: str) -> dict[str, object]:
    evidence = [
        {"start": 0, "end": len(text), "quote": text, "locator": "title_abstract"}
    ]
    criteria = {}
    for criterion in EligibilityCriterion:
        decision = (
            TriState.UNCERTAIN
            if criterion is EligibilityCriterion.ADMINISTRATIVE_SCOPE
            else TriState.YES
        )
        criteria[criterion.value] = {
            "decision": decision.value,
            "certainty": "UNCERTAIN" if decision is TriState.UNCERTAIN else "SUPPORTED",
            "evidence": evidence,
            "rationale": "Mocked pilot rationale.",
        }

    def dimension(labels, present):
        return [
            {
                "label": label,
                "state": (
                    AnnotationState.PRESENT.value
                    if label in present
                    else AnnotationState.ABSENT.value
                ),
                "certainty": "SUPPORTED",
                "evidence": evidence if label in present else [],
                "rationale": "Mocked pilot rationale.",
            }
            for label in labels
        ]

    return {
        "criteria": criteria,
        "assistance_modes": dimension(
            [item.value for item in AssistanceMode],
            {AssistanceMode.ALGORITHMIC.value, AssistanceMode.ADAPTIVE.value},
        ),
        "visualization_modalities": dimension(
            [item.value for item in VisualizationModality],
            {VisualizationModality.DESKTOP_2D.value},
        ),
        "tasks": dimension(
            [item.value for item in TaskCategory],
            {TaskCategory.SENSEMAKING_HYPOTHESIS.value},
        ),
        "primary_exclusion_reason": None,
        "secondary_exclusion_reasons": [],
        "overall_rationale": "Mocked pilot proposal only.",
    }


def test_local_pilot_selection_is_deterministic_and_does_not_feed_strata_to_model(tmp_path):
    config_path = _write_config(tmp_path)

    first, first_manifest = build_pilot_dataset(
        config_path=config_path, historical_root=tmp_path, created_at=NOW
    )
    second, second_manifest = build_pilot_dataset(
        config_path=config_path, historical_root=tmp_path, created_at=NOW
    )

    assert first.to_json() == second.to_json()
    assert first_manifest == second_manifest
    assert len(first.occurrences) == 3
    assert len(first.canonical_records) == 3
    assert [item["selection_stratum"] for item in first_manifest["selections"]] == [
        "ambiguous",
        "likely",
        "unlikely",
    ]
    model_input = inference_input_for(first.canonical_records[0])
    assert "selection_stratum" not in model_input.text
    assert "selection_stratum" not in json.dumps(model_input.metadata)
    assert "RETRIEVAL_END_DATE: not yet established" in model_input.text


def test_prepare_pilot_writes_no_call_dataset_and_bounded_preflight(tmp_path):
    config_path = _write_config(tmp_path)
    output = tmp_path / "output"

    dataset, _, preflight, paths = prepare_pilot(
        config_path=config_path,
        historical_root=tmp_path,
        output_dir=output,
        created_at=NOW,
    )

    assert dataset.inference_attempts == []
    assert preflight["live_calls_made"] == 0
    assert preflight["canonical_records"] == 3
    assert preflight["maximum_model_calls"] == 6
    assert len(preflight["prompt"]["sha256"]) == 64
    assert paths.review_dataset.is_file()
    assert paths.selection_manifest.is_file()


def test_mocked_live_pilot_preserves_invalid_retry_and_never_finalizes_membership(tmp_path):
    config_path = _write_config(tmp_path, include_bibtex=False)
    initial, _ = build_pilot_dataset(
        config_path=config_path, historical_root=tmp_path, created_at=NOW
    )
    text = inference_input_for(initial.canonical_records[0]).text
    provider = MockInferenceProvider(["{malformed", json.dumps(_payload(text))])

    dataset, report, paths = run_live_pilot(
        provider=provider,
        config_path=config_path,
        historical_root=tmp_path,
        output_dir=tmp_path / "live",
        created_at=NOW,
    )

    assert len(dataset.inference_attempts) == 2
    assert dataset.inference_attempts[0].validation_status.value == "invalid"
    assert dataset.inference_attempts[1].retry_of_attempt_id == dataset.inference_attempts[0].attempt_id
    assert report["valid_response_rate"] == 1.0
    assert report["retry_rate"] == 1.0
    assert report["full_text_escalation_count"] == 1
    assert report["assistance_mode_overlap"] == {"Adaptive + Algorithmic": 1}
    assert dataset.corpus_memberships == []
    assert paths.report.is_file()
    assert paths.review_table.is_file()
