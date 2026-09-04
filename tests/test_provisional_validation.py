from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from h2h_lit import provisional_validation
from h2h_lit.pagination import RateLimiter, RetryPolicy
from h2h_lit.provisional_validation import (
    PREFLIGHT_ARTIFACT_CLASS,
    ProvisionalValidationError,
    build_preflight,
    deterministic_identity_sample,
    execute_pubmed_boundary,
    load_validation_config,
    resolve_output_namespace,
    validate_pubmed_execution_artifacts,
)

ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config/star_provisional_pipeline_validation_v1.json"


def test_config_preserves_nonproduction_boundaries() -> None:
    config = load_validation_config(CONFIG)

    assert config["run_id"].startswith("provisional:")
    assert config["production_import_allowed"] is False
    assert config["output_namespace"].startswith("outputs/provisional/")
    assert config["pubmed"]["complete_identity_enumeration_required"] is True
    assert config["pubmed"]["metadata_sample_size_per_family"] == 100
    assert config["pubmed"]["expected_request_count_without_retries"] == 10
    assert config["jfr25_rediscovery"]["create_seed_occurrences"] is False
    assert all(config["prohibited_effects"].values())


def test_preflight_distinguishes_enumeration_from_metadata_sampling() -> None:
    report = build_preflight(
        root=ROOT,
        config_path=CONFIG,
        verify_acm_artifacts=False,
        generated_at_utc="2026-09-03T12:00:00Z",
    )

    assert report["artifact_class"] == PREFLIGHT_ARTIFACT_CLASS
    assert report["classification"] == {
        "purpose": "PROVISIONAL_NONPRODUCTION_VALIDATION",
        "production_import_allowed": False,
        "production_completion_claimed": False,
        "retrieval_cutoff": None,
        "disposition": "DISCARD_ONLY",
    }
    assert report["pubmed_plan"]["query_count"] == 5
    assert report["pubmed_plan"]["expected_request_count_without_retries"] == 10
    for query in report["pubmed_plan"]["queries"]:
        enumeration = query["complete_identity_enumeration"]
        assert enumeration["semantic_state_when_reconciled"] == (
            "COMPLETE_IDENTITY_ENUMERATION"
        )
        assert enumeration["characterized_as_truncated_due_to_metadata_sampling"] is False
        assert query["metadata_acquisition"]["semantic_state"] == (
            "DETERMINISTIC_SUBSET_PLANNED"
        )
        assert query["expected_requests_without_retries"] == 2

    assert report["acm_plan"]["selected_artifact_accounted_record_count"] == 11664
    assert report["acm_plan"]["selected_artifact_malformed_record_count"] == 3
    assert report["acm_plan"]["provisional_occurrences_created"] == 0
    assert report["jfr25_plan"]["validated_member_count"] == 138
    assert report["jfr25_plan"]["members_with_normalized_doi"] == 128
    assert report["jfr25_plan"]["members_without_normalized_doi"] == 10
    assert report["safeguards"]["network_requests_made"] == 0
    assert report["safeguards"]["acm_provisional_import_performed"] is False
    assert report["screening_plan"]["corpus_memberships_created"] == 0

    material = dict(report)
    claimed_hash = material.pop("preflight_hash")
    encoded = json.dumps(
        material, sort_keys=True, separators=(",", ":"), ensure_ascii=True
    ).encode("utf-8")
    assert claimed_hash == hashlib.sha256(encoded).hexdigest()


def test_output_namespace_guard_fails_closed(tmp_path: Path) -> None:
    expected = resolve_output_namespace(
        tmp_path, "outputs/provisional/star-pipeline-validation-001"
    )
    assert expected == tmp_path / "outputs/provisional/star-pipeline-validation-001"

    with pytest.raises(ProvisionalValidationError, match="beneath outputs/provisional"):
        resolve_output_namespace(tmp_path, "outputs/production")
    with pytest.raises(ProvisionalValidationError, match="repository-relative"):
        resolve_output_namespace(tmp_path, "../outside")


def test_identity_sampling_is_order_independent_and_eligibility_blind() -> None:
    identities = ["pmid:10", "pmid:20", "pmid:30", "pmid:40"]
    forward = deterministic_identity_sample(identities, sample_size=2, salt="fixed")
    reverse = deterministic_identity_sample(
        list(reversed(identities)), sample_size=2, salt="fixed"
    )
    assert forward == reverse
    assert len(forward) == 2

    with pytest.raises(ProvisionalValidationError, match="unique universe"):
        deterministic_identity_sample(
            ["pmid:10", "pmid:10"], sample_size=1, salt="fixed"
        )


class _Response:
    def __init__(self, content: bytes, url: str):
        self.status_code = 200
        self.headers = {"content-type": "application/xml"}
        self.content = content
        self.text = content.decode()
        self.url = url
        self.request_url = url

    def json(self) -> Any:
        raise AssertionError("PubMed XML must not be parsed as JSON")

    def iter_content(self, chunk_size: int = 8192):
        for index in range(0, len(self.content), chunk_size):
            yield self.content[index : index + chunk_size]


class _PubMedClient:
    def __init__(self):
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.enumeration_number = 0
        self.fetch_number = 0

    def post(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        allow_redirects: bool = True,
    ) -> _Response:
        del params, headers, timeout, allow_redirects
        self.enumeration_number += 1
        start = self.enumeration_number * 1000
        pmids = [str(start + offset) for offset in range(120)]
        if self.enumeration_number > 1:
            pmids[0] = "1000"
        self.calls.append(("POST", {"url": url, "data": dict(data or {})}))
        ids = "".join(f"<Id>{pmid}</Id>" for pmid in pmids)
        content = (
            f"<eSearchResult><Count>120</Count><IdList>{ids}</IdList>"
            "<QueryTranslation>frozen</QueryTranslation></eSearchResult>"
        ).encode()
        return _Response(content, url)

    def get(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        stream: bool = False,
        allow_redirects: bool = True,
    ) -> _Response:
        del headers, timeout, stream, allow_redirects
        self.fetch_number += 1
        request = dict(params or {})
        pmids = request["id"].split(",")
        self.calls.append(("GET", {"url": url, "params": request}))
        articles = []
        for index, pmid in enumerate(pmids):
            abstract = "" if index % 10 == 0 else f"<AbstractText>Abstract {pmid}</AbstractText>"
            if self.fetch_number == 3 and index == 0:
                articles.append(
                    "<PubmedBookArticle><BookDocument>"
                    f"<PMID>{pmid}</PMID><ArticleIdList><ArticleId IdType='doi'>"
                    "10.1000/book</ArticleId></ArticleIdList><Book><Publisher>"
                    "<PublisherName>Publisher</PublisherName></Publisher>"
                    "<BookTitle>Book title</BookTitle><PubDate><Year>2025</Year>"
                    "</PubDate><AuthorList><Author><LastName>Author</LastName>"
                    "<Initials>A</Initials></Author></AuthorList></Book>"
                    "<Abstract></Abstract></BookDocument></PubmedBookArticle>"
                )
                continue
            articles.append(
                "<PubmedArticle><MedlineCitation>"
                f"<PMID>{pmid}</PMID><Article><ArticleTitle>Title {pmid}</ArticleTitle>"
                f"<Abstract>{abstract}</Abstract><Journal><Title>Journal</Title>"
                "<JournalIssue><PubDate><Year>2025</Year></PubDate></JournalIssue></Journal>"
                "</Article></MedlineCitation></PubmedArticle>"
            )
        return _Response(
            ("<PubmedArticleSet>" + "".join(articles) + "</PubmedArticleSet>").encode(),
            url,
        )


def test_pubmed_boundary_enumerates_all_before_sampled_metadata_fetch(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "star-pipeline-validation-001"
    monkeypatch.setattr(
        provisional_validation,
        "resolve_output_namespace",
        lambda root, configured: output,
    )
    monkeypatch.setattr(
        provisional_validation,
        "build_preflight",
        lambda **kwargs: {"preflight_hash": "a" * 64},
    )
    client = _PubMedClient()

    execution, path = execute_pubmed_boundary(
        root=ROOT,
        config_path=CONFIG,
        http=client,
        retry_policy=RetryPolicy(max_attempts=1),
        limiter=RateLimiter({"PubMed": 0}),
        retry_sleep=lambda delay: None,
    )

    assert path == output / "pubmed/pubmed_execution.json"
    assert [method for method, _ in client.calls] == ["POST"] * 5 + ["GET"] * 5
    assert execution["request_accounting"]["logical_request_count"] == 10
    assert execution["request_accounting"]["actual_attempt_count"] == 10
    assert execution["request_accounting"]["retry_count"] == 0
    assert execution["classification"]["retrieval_cutoff"] is None
    assert execution["classification"]["production_completion_claimed"] is False
    assert execution["classification"]["production_retrieval_wave_instantiated"] is False
    assert execution["downstream_effects"] == {
        "acm_imported": False,
        "cross_source_deduplication": False,
        "llm_inference": False,
        "screening": False,
        "jfr25_comparison": False,
        "prisma": False,
        "corpus_membership": False,
    }
    assert execution["complete_identity_enumeration_overlap"][
        "summed_family_identity_count"
    ] == 600
    assert execution["complete_identity_enumeration_overlap"][
        "unique_pmid_count_across_families"
    ] == 596
    assert len(execution["families"]) == 5
    for family in execution["families"]:
        assert family["complete_identity_enumeration"]["semantic_state"] == (
            "COMPLETE_IDENTITY_ENUMERATION"
        )
        assert family["metadata_selection"]["selection_count"] == 100
        assert family["metadata_fetch"]["success_count"] == 100
        assert family["metadata_fetch"]["failure_count"] == 0
        assert family["metadata_fetch"]["missing_abstract_count"] == 10
    assert len(list((output / "pubmed/raw_responses").glob("*.json"))) == 10
    verification = validate_pubmed_execution_artifacts(root=ROOT, config_path=CONFIG)
    assert verification["status"] == "VERIFIED_PROVISIONAL_PUBMED_EXECUTION"
    assert verification["family_artifact_count"] == 5
    assert verification["raw_response_artifact_count"] == 10


class _FailAfterEightClient(_PubMedClient):
    def get(self, *args: Any, **kwargs: Any) -> _Response:
        if self.fetch_number == 3:
            raise OSError("simulated interruption")
        return super().get(*args, **kwargs)


def test_pubmed_boundary_resumes_only_verified_partial_artifacts(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    output = tmp_path / "star-pipeline-validation-001"
    monkeypatch.setattr(
        provisional_validation,
        "resolve_output_namespace",
        lambda root, configured: output,
    )
    monkeypatch.setattr(
        provisional_validation,
        "build_preflight",
        lambda **kwargs: {"preflight_hash": "a" * 64},
    )
    interrupted = _FailAfterEightClient()
    with pytest.raises(ProvisionalValidationError, match="attempts exhausted"):
        execute_pubmed_boundary(
            root=ROOT,
            config_path=CONFIG,
            http=interrupted,
            retry_policy=RetryPolicy(max_attempts=1),
            limiter=RateLimiter({"PubMed": 0}),
            retry_sleep=lambda delay: None,
        )
    assert len(interrupted.calls) == 8

    resumed = _PubMedClient()
    execution, _ = execute_pubmed_boundary(
        root=ROOT,
        config_path=CONFIG,
        http=resumed,
        retry_policy=RetryPolicy(max_attempts=1),
        limiter=RateLimiter({"PubMed": 0}),
        retry_sleep=lambda delay: None,
    )
    assert [method for method, _ in resumed.calls] == ["GET", "GET"]
    assert execution["request_accounting"]["logical_request_count"] == 10
    assert execution["request_accounting"]["actual_attempt_count"] == 10


def _occurrence(
    occurrence_id: str,
    *,
    route: str,
    family_code: str,
    doi: str | None = None,
    title: str = "",
) -> dict[str, Any]:
    return {
        "occurrence_id": occurrence_id,
        "route": route,
        "source_database": "PubMed" if route == "PubMed" else "ACMDigitalLibrary",
        "family_id": f"STAR-{family_code}",
        "family_code": family_code,
        "child_query_id": f"query:{family_code}:{route}",
        "field_key": None,
        "artifact_relative_path": f"artifact:{occurrence_id}",
        "artifact_sha256": "a" * 64,
        "artifact_record_ordinal": 1,
        "source_identifier": occurrence_id,
        "raw_entry_sha256": "b" * 64,
        "malformed": False,
        "parse_issue": None,
        "record": {"doi": doi, "title": title},
    }


def test_provisional_canonicalization_uses_doi_then_exact_title() -> None:
    records, decisions, stats = provisional_validation._canonicalize_provisional(
        [
            _occurrence("a", route="ACM", family_code="QF01", doi="10.1000/example"),
            _occurrence("b", route="PubMed", family_code="QF02", doi="10.1000/EXAMPLE"),
            _occurrence("c", route="ACM", family_code="QF01", title="Exact Title"),
            _occurrence("d", route="ACM", family_code="QF03", title="Exact title!"),
            _occurrence("e", route="ACM", family_code="QF04"),
        ]
    )

    assert len(records) == 3
    assert len([item for item in decisions if item["outcome"] == "DUPLICATE"]) == 2
    assert stats["doi_match_duplicate_count"] == 1
    assert stats["title_fallback_duplicate_count"] == 1
    assert stats["unresolved_missing_doi_and_title_count"] == 1
    assert stats["acm_pubmed_exact_identity_overlap"] == 1


def test_validation_cohort_has_independent_strata_and_unique_final_records() -> None:
    canonical = []
    for index in range(800):
        occurrences = [
            {
                "family_code": family,
                "route": route,
                "occurrence_id": f"{index}:{family}:{route}",
            }
            for family in ("QF01", "QF02", "QF03", "QF04", "QF05")
            for route in ("ACM", "PubMed")
        ]
        canonical.append(
            {
                "canonical_id": f"canonical:{index:04d}",
                "representative_record": {
                    "title": f"content must not affect selection {index}",
                    "abstract": "ignored",
                },
                "occurrences": occurrences,
            }
        )

    cohort = provisional_validation._build_validation_cohort(
        canonical_records=canonical,
        config_hash="c" * 64,
        target=750,
    )
    ids = [item["canonical_id"] for item in cohort["records"]]
    assert len(ids) == len(set(ids)) == 750
    assert len(cohort["strata"]) == 10
    assert all(len(item["selected_canonical_ids"]) == 50 for item in cohort["strata"].values())
    assert cohort["content_or_eligibility_inspected_for_selection"] is False
    assert cohort["jfr25_membership_inspected_for_selection"] is False


def test_jfr25_comparison_is_exact_first_and_creates_no_seed_occurrences() -> None:
    canonical = [
        {
            "canonical_id": "doi-record",
            "representative_record": {"doi": "10.1000/shared", "title": "Wrong title"},
            "occurrences": [{"family_code": "QF01", "route": "ACM"}],
        },
        {
            "canonical_id": "title-record",
            "representative_record": {"doi": None, "title": "Unique title"},
            "occurrences": [{"family_code": "QF02", "route": "PubMed"}],
        },
    ]
    matches = provisional_validation._jfr25_matches(
        entries=[
            {
                "entry_id": "jfr-doi",
                "source_member_id": "1",
                "doi": "https://doi.org/10.1000/SHARED",
                "title": "does not matter",
            },
            {
                "entry_id": "jfr-title",
                "source_member_id": "2",
                "doi": None,
                "title": "Unique title!",
            },
        ],
        canonical_records=canonical,
        cohort_ids={"doi-record"},
    )
    assert [item["match_method"] for item in matches] == [
        "exact_normalized_doi",
        "unique_exact_normalized_title",
    ]
    assert [item["in_750_cohort"] for item in matches] == [True, False]
