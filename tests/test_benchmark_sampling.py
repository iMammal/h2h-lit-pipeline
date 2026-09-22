from __future__ import annotations

import json
from pathlib import Path

from h2h_lit.benchmark_sampling import (
    _challenge_stratum,
    _iter_json_array,
    _related_components,
)


def test_streams_named_array_without_loading_other_values(tmp_path: Path) -> None:
    path = tmp_path / "dataset.json"
    path.write_text(
        json.dumps(
            {
                "before": [{"large": "x" * 20}],
                "canonical_records": [{"canonical_id": "canonical:a"}, {"canonical_id": "canonical:b"}],
                "related_version_links": [{"existing_canonical_id": "canonical:a", "snapshot_canonical_id": "canonical:c"}],
            },
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )

    assert [item["canonical_id"] for item in _iter_json_array(path, "canonical_records", chunk_size=11)] == [
        "canonical:a",
        "canonical:b",
    ]
    assert len(list(_iter_json_array(path, "related_version_links", chunk_size=13))) == 1


def test_challenge_overlap_uses_frozen_priority() -> None:
    prior_missing = {
        "source_database": "PriorSurveySeed",
        "source_identifier": "EBK25:x",
        "provenance_source_databases": [],
        "title": "A survey in virtual reality",
        "abstract": "",
    }
    assert _challenge_stratum(prior_missing) == "prior_survey_route"
    assert _challenge_stratum({**prior_missing, "source_database": "PubMed", "source_identifier": ""}) == "missing_abstract"
    assert _challenge_stratum({**prior_missing, "source_database": "PubMed", "source_identifier": "", "abstract": "brief text"}) == "sparse_abstract"
    assert _challenge_stratum({**prior_missing, "source_database": "PubMed", "source_identifier": "", "abstract": "word " * 100}) == "uncommon_modality_or_assistance_cue"


def test_related_components_preserve_bibliographic_ids() -> None:
    mapping, components = _related_components(
        [
            {"existing_canonical_id": "canonical:a", "snapshot_canonical_id": "canonical:b"},
            {"existing_canonical_id": "canonical:b", "snapshot_canonical_id": "canonical:c"},
        ]
    )
    assert components == [["canonical:a", "canonical:b", "canonical:c"]]
    assert mapping["canonical:a"] == mapping["canonical:c"]
