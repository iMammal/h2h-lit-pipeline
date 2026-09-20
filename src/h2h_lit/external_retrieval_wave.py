"""Deterministic offline planning for the Phase 4A external-retrieval wave."""

from __future__ import annotations

import argparse
import base64
import fcntl
import hashlib
import json
import math
import os
import time
from collections import Counter
from collections.abc import Callable, Mapping
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from h2h_lit.acm_field_execution import load_acm_final_reconciliation_manifest
from h2h_lit.arxiv_snapshot_integration import (
    ArxivSnapshotIntegrationError,
    authorize_snapshot_substitution,
    prepare_integration_dry_run,
    validate_authorized_snapshot_substitution,
)
from h2h_lit.checkpoint import CheckpointStore, atomic_write
from h2h_lit.http import HttpClient, RequestsHttpClient
from h2h_lit.models import ProcessingStatus
from h2h_lit.pagination import PageRequest, RateLimiter, RetryPolicy, native_identifier
from h2h_lit.prior_survey_integration import (
    PriorSurveyIntegrationError,
    authorize_prior_survey_package_locked,
    validate_authorized_prior_survey_imports,
)
from h2h_lit.production_prerequisites import load_prerequisite_package
from h2h_lit.production_query_plan import load_production_query_plan
from h2h_lit.production_wave import (
    EXTERNAL_IDENTIFICATION_SOURCES_V2,
    EXTERNAL_RETRIEVAL_EXECUTION_SCOPE,
    REQUIRED_SUPPORT_SOURCES_V2,
    SOURCE_CONTRACTS,
    ArtifactKind,
    PaginationExpectation,
    ProductionQueryFamily,
    ProductionRetrievalWave,
    ProductionWaveStatus,
    RequiredArtifact,
    ResultWindowStatus,
    compute_query_plan_hash,
    preflight_production_wave,
    save_production_wave,
)
from h2h_lit.query_development import load_semantic_control_set
from h2h_lit.retrieval import (
    PAGINATED_SOURCE_ADAPTERS,
    RetrievalQuerySpec,
    _query_plan_hash,
    execute_paginated_retrieval_run,
    load_review_dataset,
    save_review_dataset,
)
from h2h_lit.review import (
    ActorType,
    DecisionActor,
    DecisionAuthority,
    DecisionProvenance,
    DecisionScope,
    RecordOccurrence,
    RetrievalAttemptStatus,
    RetrievalCompletionStatus,
    canonicalize_occurrences,
)
from h2h_lit.sources.acm_dl import import_acm_selected_reconciliation

PLAN_PATH = "config/star_production_query_plan_v1.json"
PREREQUISITE_PATH = "config/star_retrieval_prerequisites_v1.json"
IEEE_READINESS_PATH = "config/star_retrieval_prerequisites_v1/ieee_readiness.json"
ACM_READINESS_PATH = "config/star_retrieval_prerequisites_v1/acm_operator_spec.json"
SOURCE_WINDOWS_PATH = "config/star_retrieval_prerequisites_v1/source_window_review.json"
ACM_RECONCILIATION_PATH = (
    "provenance/star_acm_field_execution_2026-09-03_final_reconciliation_manifest.json"
)
OUTPUT_ROOT = "outputs/production/star-external-retrieval-wave-001"
WAVE_PATH = f"{OUTPUT_ROOT}/planned_wave.json"
PREFLIGHT_PATH = f"{OUTPUT_ROOT}/preflight.json"
WAVE_ID = "star-external-retrieval-wave-001"
WAVE_VERSION = "1.0.0"
READY_STATUS = "READY_FOR_EXTERNAL_RETRIEVAL_EXECUTION"
EXECUTION_ROOT = f"{OUTPUT_ROOT}/execution"
EXECUTION_STATE_PATH = f"{EXECUTION_ROOT}/execution_state.json"
EXTERNAL_SOURCE_SESSION_LOCK_PATH = (
    f"{EXECUTION_ROOT}/.external-source-session.lock"
)
IEEE_CREDENTIAL_NAME = "IEEE_XPLORE_API_KEY"
IEEE_DAILY_REQUEST_LIMIT = 200
IEEE_TOTAL_DRIFT_RECOVERY_STATUS = "PROVIDER_TOTAL_DRIFT_RECOVERY_READY_TO_RESUME"
IEEE_TOTAL_DRIFT_ERROR_PREFIX = "source exact total changed during pagination: "
IEEE_TOTAL_DRIFT_EXPECTED = (
    (16842, 16841, 6, 1199, 1200),
    (6061, 6060, 2, 399, 400),
    (9787, 9786, 2, 399, 400),
    (8410, 8409, 11, 2199, 2200),
    (4695, 4694, 2, 399, 400),
)
IEEE_TOTAL_DRIFT_EXPECTED_ATTEMPTS = 23
IEEE_REPEATED_WINDOW_RECOVERY_STATUS = (
    "REPEATED_WINDOW_RECOVERY_READY_TO_RESUME"
)
IEEE_REPEATED_WINDOW_EXPECTED = (
    (25, 4993, 4797, 197, 4800),
    (10, 1997, 1799, 199, 1800),
    (11, 2195, 1998, 198, 2000),
    (20, 3997, 3799, 199, 3800),
    (21, 4197, 3999, 199, 4000),
)
IEEE_REPEATED_WINDOW_EXPECTED_TOTAL_HISTORIES = (
    (
        16842, 16842, 16842, 16842, 16842, 16841,
        16845, 16845, 16845, 16845, 16845, 16845, 16845, 16845, 16845,
        16845, 16845, 16845, 16845, 16845, 16845, 16845, 16845, 16842, 16842,
    ),
    (6061, 6060, 6061, 6061, 6061, 6061, 6061, 6061, 6060, 6060),
    (9787, 9786, 9787, 9787, 9787, 9787, 9787, 9787, 9787, 9785, 9785),
    (
        8410, 8410, 8410, 8410, 8410, 8410, 8410, 8410, 8410, 8410,
        8409, 8411, 8411, 8411, 8411, 8411, 8411, 8411, 8410, 8410,
    ),
    (
        4695, 4694, 4696, 4696, 4696, 4696, 4696, 4696, 4696, 4696,
        4696, 4696, 4696, 4696, 4696, 4696, 4696, 4696, 4696, 4695, 4695,
    ),
)
IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS = 87
IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES = 82
IEEE_REPEATED_WINDOW_EXPECTED_REJECTIONS = 5
IEEE_REPEATED_WINDOW_EXPECTED_KNOWN_CALLS = 92
IEEE_VERIFICATION_PATH = (
    "provenance/star_ieee_xplore_verification_2026-09-04_manifest.json"
)
SEMANTIC_CONTROL_PATH = "config/star_query_semantic_controls_v0_3.json"
SEMANTIC_CONTROL_GATE_PATH = (
    f"{EXECUTION_ROOT}/SemanticScholar/control_gate/control_gate.json"
)
SEMANTIC_CONTROL_RECOVERY_GATE_PATH = (
    f"{EXECUTION_ROOT}/SemanticScholar/control_gate/control_gate_recovered.json"
)
SEMANTIC_CONTROL_5XX_RECOVERY_STATUS = (
    "CONTROL_5XX_RECOVERY_READY_TO_RESUME"
)
SEMANTIC_CONTROL_BLOCKED_MANIFEST_RAW_SHA256 = (
    "14871e26bf45540ce9933c1023277f8f3c71e964c40108ad481d17948430fc86"
)
SEMANTIC_CONTROL_BLOCKED_MANIFEST_LOGICAL_HASH = (
    "7f0184a2066abb468092899362929bfd08e835f3e856cc8f92cbbac94a7fcd3d"
)
SEMANTIC_CONTROL_5XX_EXPECTED_SIGNATURE = (
    ("atomic-a", (500, 200), "SUCCEEDED", 943168),
    ("atomic-b", (500, 500, 200), "SUCCEEDED", 955441),
    ("a-and-b", (200,), "SUCCEEDED", 14246),
    ("a-or-b", (500, 500, 500), "UNRESOLVED", None),
)
SEMANTIC_CANDIDATE_5XX_RECOVERY_STATUS = (
    "CANDIDATE_5XX_RECOVERY_READY_TO_RESUME"
)
SEMANTIC_CANDIDATE_5XX_EXPECTED_CHECKPOINT_SHA256 = (
    "d146529f8ec1c9ec221f7d18b48e3d8d3b51927e8b76f9c56102ca1f47228dc0"
)
SEMANTIC_CANDIDATE_5XX_EXPECTED_STALE_STATE_SHA256 = (
    "bb67003b7f474969de4d640b024d4998fe169ae4311fe1aa3f19cacbd94185da"
)
SEMANTIC_CANDIDATE_5XX_EXPECTED_CONTROL_SHA256 = (
    "bab4c59d366d6b1062e5c8004774d5dbba1e7aa8157366e9ae4c4b10a63694c4"
)
SEMANTIC_CANDIDATE_5XX_EXPECTED_CONTROL_LOGICAL_HASH = (
    "0978748cce5a721363fc8f16bee8227e82b7fc0801a2aa07d14e2888ad26cd7d"
)
SEMANTIC_CANDIDATE_5XX_EXPECTED_COUNTS = (
    ("COMPLETE", 17, 16869),
    ("FAILED", 6, 5000),
    ("FAILED", 2, 1000),
    ("COMPLETE", 3, 2954),
    ("COMPLETE", 9, 8905),
)
SEMANTIC_CANDIDATE_5XX_CONTINUATION_TOKENS = (
    None,
    (
        "PCOA3RZ3B2ACAEAFYCVZAV23YAVCAXWFKDWM7RWEILWYY56X5HTIDMHOPYMJMAF7"
        "GACPA5X37F3ULTNEYWC2ZTKJXC4RDR6IKY2I3LESU5ONKGYRUG7R6AG3CS2Q"
    ),
    (
        "PCOA3RZ5BZAEAEAG2CVWZPS2GGDN3YBKUKMN6RVCIAT65TXL3YBRLW7DOAWAK4NB"
        "FOILZY74XOBAMM3KJJKKHWWMFTNMNFE6RETWE44PZSSPBQHWP326UFIG"
    ),
    None,
    None,
)
SEMANTIC_CANDIDATE_5XX_EXPECTED_ATTEMPTS = 59
SEMANTIC_CANDIDATE_5XX_EXPECTED_RESPONSES = 59
SEMANTIC_CANDIDATE_5XX_EXPECTED_SUCCESSFUL_PAGES = 35
SEMANTIC_CANDIDATE_5XX_EXPECTED_TOTAL_PAGES = 37
SEMANTIC_CANDIDATE_5XX_EXPECTED_HTTP_STATUSES = {200: 35, 500: 22, 429: 2}
SEMANTIC_CANDIDATE_5XX_EXPECTED_OCCURRENCES = 34728
SEMANTIC_CANDIDATE_5XX_EXPECTED_CANONICAL_RECORDS = 31532
SEMANTIC_NATIVE_ID_OVERLAP_RECOVERY_STATUS = (
    "NATIVE_ID_OVERLAP_RECOVERY_READY_TO_RESUME"
)
SEMANTIC_NATIVE_ID_OVERLAP_PARENT_CHECKPOINT_SHA256 = (
    "8044d504e7c2c7a885ad2af8dc201c1aab01f03dec2bf7e3ad3dbf06de976f86"
)
SEMANTIC_NATIVE_ID_OVERLAP_PARENT_CHECKPOINT_SIZE = 1_196_606_198
SEMANTIC_NATIVE_ID_OVERLAP_PAPER_ID = (
    "a6a42ae353dba2ac0298dd73047678c91811541f"
)
SEMANTIC_NATIVE_ID_OVERLAP_QUERY_ID = "query:a37bc36618a498762d3cb6a5"
SEMANTIC_NATIVE_ID_OVERLAP_ERROR = (
    "source repeated native identifiers across pages: "
    "['a6a42ae353dba2ac0298dd73047678c91811541f']"
)
SEMANTIC_NATIVE_ID_OVERLAP_OCCURRENCES = (
    {
        "ordinal": 2,
        "page_id": "page:040bf64dc683844d1e4f18b3",
        "occurrence_id": "occurrence:e6d33b87c8510dfe5f56f8c5",
        "raw_payload_hash": (
            "f8ce3541dd1d4c83c3695314f88f64da0dfcf3e39668335be5440d91981b6ed4"
        ),
        "response_path": (
            "responses/32ecc1f19e83a30156b3455e4809a2beb93bcb79f7a4c9e5d9b6d8e9bea3ba47.json"
        ),
        "response_sha256": (
            "568b6d8783137dfc8f7b630941cc2835f1b522e45ce48abef856369dec4c3cb5"
        ),
        "provider_total": 64_570,
        "author_name": "J. Southgate",
    },
    {
        "ordinal": 42,
        "page_id": "page:72a19e2a44086391e70c7707",
        "occurrence_id": "occurrence:538849c8b485810620f05e6c",
        "raw_payload_hash": (
            "9af38240e722e094f65b78f2d3a30efc83776e7c3ccb846279ca34a300bd3012"
        ),
        "response_path": (
            "responses/3a175522f44cd2d6ba7c0da73847fc5589bbf4ac62d87bcb2c926e906da5041a.json"
        ),
        "response_sha256": (
            "10193b78d36ab9cb1cc93387f02b8f03be16b1319c43c3cffbc03d2964f7d149"
        ),
        "provider_total": 64_919,
        "author_name": "Jennifer Southgate",
    },
)
SEMANTIC_NATIVE_ID_OVERLAP_CANONICAL_ID = (
    "canonical:1f288b904d039372f06bc9a0"
)
SEMANTIC_NATIVE_ID_OVERLAP_DEDUPE_KEY = "doi:10.1186/1752-0509-2-102"
SEMANTIC_NATIVE_ID_OVERLAP_QF_COUNTS = (
    ("complete", 17, 16_869),
    ("complete", 11, 10_723),
    ("failed", 43, 42_997),
    ("complete", 3, 2_954),
    ("complete", 9, 8_905),
)
SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_ATTEMPTS = 125
SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_PAGES = 83
SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_OCCURRENCES = 82_448
SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_CANONICAL_RECORDS = 73_912
SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_QF03_DISTINCT_IDS = 42_996
SEMANTIC_NATIVE_ID_OVERLAP_ORDERING_ANOMALIES = (
    {
        "page_ordinal": 2,
        "left_index": 660,
        "left_paper_id": "a6a42ae353dba2ac0298dd73047678c91811541f",
        "right_paper_id": "0a33911e2cf92508f64418592f70e8b7002e326c",
    },
    {
        "page_ordinal": 3,
        "left_index": 180,
        "left_paper_id": "d732e9f34a6bb7ad6f4382acc0cd2661f23d7fd2",
        "right_paper_id": "0c4548946697b1f7300a12adf4e94fa76ce5e928",
    },
)
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PARENT_CHECKPOINT_SHA256 = (
    "9107ea7d8a4c03f9a1909ca07e4610f0abe48c4e73e77285eeb0bec162c58eae"
)
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PARENT_CHECKPOINT_SIZE = 1_380_419_798
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PAPER_ID = (
    "d732e9f34a6bb7ad6f4382acc0cd2661f23d7fd2"
)
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_ERROR = (
    "source repeated native identifiers across pages: "
    "['d732e9f34a6bb7ad6f4382acc0cd2661f23d7fd2']"
)
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_OCCURRENCES = (
    {
        "ordinal": 3,
        "page_id": "page:f6108b0d1487951f0e87120c",
        "occurrence_id": "occurrence:cd35f79b71bcbebed1c2fe78",
        "raw_payload_hash": (
            "1ba07018806a955c0b75c424e8d1c1edbf3c2db367ee0387e130b707a0d0fdfb"
        ),
        "response_path": (
            "responses/7a0110fbcafddba5beef7d86edc10b135bacbeed7a75cde2d2812a8cd0e3e8a1.json"
        ),
        "response_sha256": (
            "fec9075bdab6896a86ec62f7ba2f8eb88442eeb2f0b90c185ba2ea2b1e320011"
        ),
        "provider_total": 64_570,
    },
    {
        "ordinal": 54,
        "page_id": "page:cf6729971b4cb3ea41bfcefb",
        "occurrence_id": "occurrence:11095283d8af9ab1285eaf0a",
        "raw_payload_hash": (
            "1ba07018806a955c0b75c424e8d1c1edbf3c2db367ee0387e130b707a0d0fdfb"
        ),
        "response_path": (
            "responses/69e30b7d452d4d86aa7521fe825fc99cf5bc5b82e021dc2d524535f1204f03d9.json"
        ),
        "response_sha256": (
            "8c891a51226ca2b1a6d204b64a730c011f5814daf301c95c1413653475ccee75"
        ),
        "provider_total": 65_001,
    },
)
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_CANONICAL_ID = (
    "canonical:0bbc19d9cd9342b69734bd66"
)
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_DEDUPE_KEY = (
    "doi:10.1186/1471-2105-8-270"
)
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_QF_COUNTS = (
    ("complete", 17, 16_869),
    ("complete", 11, 10_723),
    ("failed", 55, 54_996),
    ("complete", 3, 2_954),
    ("complete", 9, 8_905),
)
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_ATTEMPTS = 138
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_PAGES = 95
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_OCCURRENCES = 94_447
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_CANONICAL_RECORDS = 85_457
SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_QF03_DISTINCT_IDS = 54_994
PUBMED_TRANSPORT_RETRY_STATUS = "TRANSPORT_RETRY_AUTHORIZED_NOT_STARTED"
PUBMED_PARSER_RECOVERY_STATUS = "PARSER_RECOVERY_COMPLETE_READY_TO_RESUME"
ARXIV_RATE_LIMIT_RECOVERY_STATUS = "RATE_LIMIT_RECOVERY_READY_TO_RESUME"
ARXIV_RATE_LIMIT_EXPECTED_ATTEMPT_KINDS = (
    ("ReadTimeout", "ReadTimeout", "ReadTimeout"),
    ("ReadTimeout", "ReadTimeout", "ReadTimeout"),
    ("ReadTimeout", "ReadTimeout", "HTTP_429"),
    ("HTTP_429", "HTTP_429", "HTTP_429"),
    ("HTTP_429", "HTTP_429", "HTTP_429"),
)
ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS = 15
ARXIV_RATE_LIMIT_EXPECTED_RESPONSES = 7
ARXIV_MIXED_RECOVERY_STATUS = (
    "MIXED_TRANSPORT_RATE_LIMIT_RECOVERY_READY_TO_RESUME"
)
ARXIV_MIXED_EXPECTED_CHECKPOINT_SHA256 = (
    "0fc8478938d772252304e8d0402cc41e125cb7b2e4a993bd4ffc42eb25cda4ae"
)
ARXIV_MIXED_EXPECTED_ATTEMPT_KINDS = (
    ("ReadTimeout", "ReadTimeout", "ReadTimeout"),
    ("ReadTimeout", "ReadTimeout", "ReadTimeout"),
    ("ReadTimeout", "ReadTimeout", "ReadTimeout"),
    ("HTTP_429",),
    (),
)
ARXIV_MIXED_EXPECTED_ATTEMPTS = 10
ARXIV_MIXED_EXPECTED_RESPONSES = 1
ARXIV_EPISODE_3_STALE_CHECKPOINT_SHA256 = (
    "17dcb69270920b860de8ba9ef734d1178812c95cbe2eb009b81c1131ed4886cc"
)
ARXIV_EPISODE_3_STALE_CHECKPOINT_SIZE = 39_017
ARXIV_EPISODE_3_CURRENT_CHECKPOINT_SHA256 = (
    "c32235b0e1b6b4651b45775c0de4edbf8308c7742a9942251252247c6301d6f7"
)
ARXIV_EPISODE_3_CURRENT_CHECKPOINT_SIZE = 46_845
ARXIV_EPISODE_3_EXPECTED_REQUEST_HASH = (
    "5f12d8fe6e41c95dc4d505483933dbef9e53c13b7742f862bab3ed802734df85"
)
ARXIV_TRANSPORT_POLICY_RECOVERY_STATUS = (
    "TRANSPORT_POLICY_RECOVERY_READY_TO_RESUME"
)
ARXIV_RETRYABLE_5XX_RECOVERY_STATUS = (
    "RETRYABLE_5XX_RECOVERY_READY_TO_RESUME"
)
ARXIV_PAGE_SIZE_RECOVERY_STATUS = "PAGE_SIZE_RECOVERY_READY_TO_RESUME"
ARXIV_EPISODE_4_FAILED_CHECKPOINT_SHA256 = (
    "9a2f4888d6b777b7c9eab8094e8837ffeb15333c08acc65362acf999eb1c4f3a"
)
ARXIV_EPISODE_4_FAILED_CHECKPOINT_SIZE = 277_454
ARXIV_EPISODE_4_ATTEMPT_MANIFEST_SHA256 = (
    "ca603bc05360f5ce9875ed84dc3d75c4863b5e2d67139fb2e59960aa2add7ec3"
)
ARXIV_EPISODE_4_FAILURE_SIGNATURES = (
    (
        "production:STAR-QF01-RELATIONAL-VIS:arXiv",
        "query:dc71331332f3131c6ac8b7cd",
        "page:308545d96dfb2269380f8bad",
        "fb3ac42a7aa345b75a36091007082d133f3a96f1176e9f452e8bef366fd470fc",
        500,
    ),
    (
        "production:STAR-QF02-ASSISTED-VIS:arXiv",
        "query:bb3a20413b87a6451cd50381",
        "page:d36c237368975d93e62bc11e",
        "3711800d936e1dd89536b498e4efe6f60fc44b4c027cc12695fb8b1d20886ff9",
        500,
    ),
    (
        "production:STAR-QF03-INTERACTIVE-SYSTEMS:arXiv",
        "query:54277aff164d7db0f6dd7deb",
        "page:b157a552b705303383c0f689",
        "9f03b8756c93fc2bca0cad623bd1acdad7d504359ad6aebe6ceedf4babe95ef8",
        503,
    ),
    (
        "production:STAR-QF04-NONDESKTOP-ENV:arXiv",
        "query:eab7594e261198a0cc02d899",
        "page:c95b851ce236d5e58a6a1b07",
        "6d3fb8e36a1c53bdb4731843803efe51e386b732950fdd9051fb5d5ff71804ef",
        500,
    ),
    (
        "production:STAR-QF05-CONVERSATIONAL:arXiv",
        "query:f2c1cdae2e380638f9873e30",
        "page:8d3266844980afb71151a02d",
        "80a60f727873bd2ec256df0e5a79c62bb009c709d00689cb6534125ebc4d46e7",
        500,
    ),
)
ARXIV_LEGACY_READ_TIMEOUT_SECONDS = 30.0
ARXIV_RECOVERED_READ_TIMEOUT_SECONDS = 120.0
ARXIV_MAX_ATTEMPTS_PER_INVOCATION = 3
ARXIV_LEGACY_PAGE_SIZE = 2_000
ARXIV_RECOVERED_PAGE_SIZE = 100
ARXIV_EPISODE_5_PARENT_CHECKPOINT_SHA256 = (
    "9453b4abbe4710ff7aabeb5d8cf32c5919441d6d32111d975ede379ef29b492f"
)
ARXIV_EPISODE_5_PARENT_CHECKPOINT_SIZE = 90_597
ARXIV_EPISODE_5_ATTEMPT_MANIFEST_SHA256 = (
    "c4c7f84e9a6d2a2522c94034ff9191d86946759c7821802dcf28b7b36677407f"
)
ARXIV_EPISODE_5_RAW_RESPONSE_MANIFEST_SHA256 = (
    "b83d2767a7f248bc095fb5a021b37a795cbb8383c0ccc152f71336b24e469e9d"
)
ARXIV_EPISODE_5_HTTP_STATUSES = (500, 503, 503, 500, 500, 500)
ARXIV_QF01_DIAGNOSTIC_MANIFEST_PATH = (
    "outputs/diagnostics/arxiv/arxiv-qf01-small-page-diagnostic-001/"
    "diagnostic_manifest.json"
)
ARXIV_QF01_DIAGNOSTIC_MANIFEST_SIZE = 17_369
ARXIV_QF01_DIAGNOSTIC_MANIFEST_SHA256 = (
    "562533d6300b7a11469dacb25c10cca08fd6df23b4817ae29d1c6fecb724311d"
)
EUROPE_PMC_TERMINAL_RECOVERY_STATUS = "TERMINAL_SENTINEL_RECOVERY_COMPLETE"
EUROPE_PMC_TERMINAL_ERROR = (
    "PaginationError: Europe PMC returned an empty non-terminal cursor page"
)
EUROPE_PMC_RECOVERY_EXPECTED_COUNTS = (3972, 1500, 3629, 1209, 1717)
EUROPE_PMC_RECOVERY_EXPECTED_ATTEMPTS = 27
PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS = (
    "33085405",
    "39836822",
    "38781405",
    "38484096",
    "37816103",
    "40198015",
)
TRANSPORT_ENVIRONMENT_FAILURE_TYPES = frozenset(
    {"ConnectionError", "ConnectTimeout", "ProxyError", "ReadTimeout", "SSLError", "Timeout"}
)


class ExternalRetrievalWaveError(ValueError):
    """Raised when an external-only production wave cannot pass offline preflight."""


def build_external_retrieval_wave(*, root: str | Path) -> ProductionRetrievalWave:
    """Bind the frozen external queries to a PLANNED schema-1.1 wave."""

    root_path = Path(root).resolve()
    plan_path = root_path / PLAN_PATH
    prerequisite_path = root_path / PREREQUISITE_PATH
    plan = load_production_query_plan(plan_path, root=root_path)
    package = load_prerequisite_package(prerequisite_path, root=root_path)
    phase = package.payload["phase4a_compatibility"]
    external_gate = phase["external_retrieval_execution"]
    closure_gate = phase["identification_set_closure"]
    if external_gate.get("status") != "READY" or external_gate.get("ready") is not True:
        raise ExternalRetrievalWaveError("external retrieval execution gate is not READY")
    if external_gate.get("required_identification_sources") != list(
        EXTERNAL_IDENTIFICATION_SOURCES_V2
    ):
        raise ExternalRetrievalWaveError("external source inventory changed")
    if package.payload["states"].get("ieee") != "VERIFIED_READY_FOR_RETRIEVAL":
        raise ExternalRetrievalWaveError("IEEE is not verified ready for retrieval")
    if package.payload["states"].get("acm") != (
        "RETRIEVAL_EVIDENCE_COMPLETE_NOT_IMPORTED"
    ):
        raise ExternalRetrievalWaveError("ACM retrieval evidence is not complete")
    if closure_gate.get("status") != "BLOCKED_REQUIRED_IDENTIFICATION_INPUT":
        raise ExternalRetrievalWaveError(
            "identification closure must remain blocked until seed completion"
        )
    if closure_gate.get("pending_manifest_seed_set_ids") != ["EBK25", "FP19"]:
        raise ExternalRetrievalWaveError("unexpected prior-survey seed readiness state")

    windows = _load_json(root_path / SOURCE_WINDOWS_PATH)
    window_by_query = {
        (item["family_id"], item["source"]): item for item in windows["items"]
    }
    acm_path = root_path / ACM_RECONCILIATION_PATH
    acm = load_acm_final_reconciliation_manifest(
        acm_path, root=root_path, verify_artifacts=False
    )
    acm_by_query = {item["parent_query_id"]: item for item in acm["families"]}
    roles = {item["source"]: item for item in plan.payload["source_roles"]}

    query_families = []
    for query in plan.payload["source_queries"]:
        source = query["source"]
        if source not in EXTERNAL_IDENTIFICATION_SOURCES_V2:
            continue
        window = window_by_query.get((query["family_id"], source))
        if window is None or not str(window.get("state", "")).startswith("RESOLVED_"):
            raise ExternalRetrievalWaveError(
                f"source window is not resolved for {query['query_id']}"
            )
        if query["query_text_sha256"] != _sha256(query["query_text"].encode("utf-8")):
            raise ExternalRetrievalWaveError(
                f"query-text hash mismatch for {query['query_id']}"
            )
        request_hash = _hash_payload(query["request_specification"])
        if query["request_specification_hash"] != request_hash:
            raise ExternalRetrievalWaveError(
                f"request-specification hash mismatch for {query['query_id']}"
            )
        role = roles.get(source)
        if role is None or role.get("transport") != query["transport"]:
            raise ExternalRetrievalWaveError(
                f"source-role binding mismatch for {query['query_id']}"
            )
        query_families.append(
            _query_family(
                query,
                reported_count=int(window["reported_count"]),
                acm_by_query=acm_by_query,
                acm_manifest_path=acm_path,
            )
        )

    expected_ids = [
        query["query_id"]
        for query in plan.payload["source_queries"]
        if query["source"] in EXTERNAL_IDENTIFICATION_SOURCES_V2
    ]
    if [item.query_family_id for item in query_families] != expected_ids:
        raise ExternalRetrievalWaveError("external query inventory is incomplete or reordered")
    if len(query_families) != 30:
        raise ExternalRetrievalWaveError("external wave must contain exactly 30 queries")

    wave = ProductionRetrievalWave(
        schema_version="1.1.0",
        wave_id=WAVE_ID,
        wave_version=WAVE_VERSION,
        query_plan_version=plan.payload["plan_version"],
        query_plan_hash="",
        required_sources=list(EXTERNAL_IDENTIFICATION_SOURCES_V2),
        support_sources=list(REQUIRED_SUPPORT_SOURCES_V2),
        query_families=query_families,
        status=ProductionWaveStatus.PLANNED,
        retrieval_cutoff_date=None,
        metadata={
            "execution_scope": EXTERNAL_RETRIEVAL_EXECUTION_SCOPE,
            "phase": "Phase 4A external identification retrieval",
            "deferred_identification_sources": ["PriorSurveySeed"],
            "deferred_seed_set_ids": ["EBK25", "JFR25", "FP19"],
            "identification_set_closure_allowed": False,
            "incremental_normalization_allowed": True,
            "final_global_deduplication_allowed": False,
            "retrieval_cutoff_established": False,
            "frozen_source_roles": plan.payload["source_roles"],
            "semantic_scholar_control_gate": {
                "required_gate": plan.payload["semantic_controls"]["required_gate"],
                "gate_behavior": plan.payload["semantic_controls"]["gate_behavior"],
                "control_set": _file_reference(
                    root_path / plan.payload["semantic_controls"]["path"], root_path
                ),
                "must_pass_before_candidate_requests": True,
            },
            "bindings": {
                "production_query_plan": {
                    **_file_reference(plan_path, root_path),
                    "canonical_hash": plan.plan_hash(),
                },
                "retrieval_prerequisites": {
                    **_file_reference(prerequisite_path, root_path),
                    "canonical_hash": package.package_hash(),
                },
                "ieee_readiness": _json_artifact_reference(
                    root_path / IEEE_READINESS_PATH, root_path, "artifact_hash"
                ),
                "acm_readiness": _json_artifact_reference(
                    root_path / ACM_READINESS_PATH, root_path, "artifact_hash"
                ),
                "source_windows": _json_artifact_reference(
                    root_path / SOURCE_WINDOWS_PATH, root_path, "artifact_hash"
                ),
                "acm_final_reconciliation": {
                    **_file_reference(acm_path, root_path),
                    "canonical_hash": acm["manifest_hash"],
                },
            },
            "production_operations": {
                "external_retrieval_executed": False,
                "acm_import_executed": False,
                "seed_import_executed": False,
                "identification_set_closed": False,
                "final_global_deduplication_executed": False,
                "prisma_generated": False,
                "screening_executed": False,
                "corpus_created": False,
            },
        },
    )
    wave.query_plan_hash = compute_query_plan_hash(wave)
    return wave


def preflight_external_retrieval_wave(
    wave: ProductionRetrievalWave, *, root: str | Path
) -> dict[str, Any]:
    """Run the existing offline wave preflight plus phase-specific assertions."""

    root_path = Path(root).resolve()
    _verify_bound_files(wave, root_path)
    package = _load_json(root_path / PREREQUISITE_PATH)
    if package.get("package_hash") != wave.metadata["bindings"][
        "retrieval_prerequisites"
    ]["canonical_hash"]:
        raise ExternalRetrievalWaveError("prerequisite package hash changed")
    external = package.get("phase4a_compatibility", {}).get(
        "external_retrieval_execution", {}
    )
    if external.get("status") != "READY" or external.get("ready") is not True:
        raise ExternalRetrievalWaveError("external retrieval gate is no longer READY")
    report = preflight_production_wave(
        wave,
        manifest_root=root_path,
        configured_credentials={"IEEEXplore": {"api_key"}},
    )
    if not report.ready or report.finalizable or report.execution_complete:
        raise ExternalRetrievalWaveError(
            "external wave did not reach a planned-only ready preflight"
        )
    if wave.retrieval_cutoff_date is not None:
        raise ExternalRetrievalWaveError("external preflight cannot establish a cutoff")
    burdens = _request_burdens(wave)
    acm = load_acm_final_reconciliation_manifest(
        root_path / ACM_RECONCILIATION_PATH,
        root=root_path,
        verify_artifacts=True,
    )
    selected_artifacts = [
        artifact
        for family in acm["families"]
        for child in family["children"]
        for artifact in child["selected_artifacts"]
    ]
    malformed = sum(item["malformed_entry_count"] for item in selected_artifacts)
    raw_occurrences = sum(item["total_accounted_entry_count"] for item in selected_artifacts)
    acm_unique_by_family = {
        item["family_id"]: item["field_union"]["unique_stable_identity_count"]
        for item in acm["families"]
    }
    return {
        "schema_version": "1.0.0",
        "artifact_id": "star-external-retrieval-wave-001-preflight",
        "status": READY_STATUS,
        "wave_id": wave.wave_id,
        "wave_manifest_hash": wave.manifest_hash(),
        "production_query_plan": wave.metadata["bindings"]["production_query_plan"],
        "retrieval_prerequisites": wave.metadata["bindings"][
            "retrieval_prerequisites"
        ],
        "external_gate": package["phase4a_compatibility"][
            "external_retrieval_execution"
        ],
        "identification_closure": package["phase4a_compatibility"][
            "identification_set_closure"
        ],
        "query_inventory": [
            {
                "query_id": item.query_family_id,
                "source": item.source_database,
                "query_sha256": _sha256(item.query_text.encode("utf-8")),
                "query_version": item.query_version,
                "transport_kind": item.transport_kind.value,
                "reported_count_evidence": item.native_parameters[
                    "observed_source_count"
                ],
                "frozen_source_role": next(
                    role["role"]
                    for role in wave.metadata["frozen_source_roles"]
                    if role["source"] == item.source_database
                ),
            }
            for item in wave.query_families
        ],
        "source_query_counts": {
            source: sum(
                item.source_database == source for item in wave.query_families
            )
            for source in wave.required_sources
        },
        "request_burden": burdens,
        "credentials": {
            "required_at_live_execution": ["IEEE_XPLORE_API_KEY"],
            "credential_values_read": False,
            "credential_values_persisted": False,
            "ieee_credential_previously_verified": True,
        },
        "acm_artifact_import": {
            "live_requests": 0,
            "manifest_path": ACM_RECONCILIATION_PATH,
            "manifest_hash": acm["manifest_hash"],
            "selected_artifact_count": len(selected_artifacts),
            "raw_selected_occurrence_count": raw_occurrences,
            "malformed_but_identified_record_count": malformed,
            "unique_identity_count_by_family": acm_unique_by_family,
            "import_executed": False,
            "selected_artifacts_only": True,
            "nonselected_artifacts_preserved_but_excluded": True,
        },
        "semantic_scholar": {
            **wave.metadata["semantic_scholar_control_gate"],
            "control_request_count": 6,
            "candidate_mode": "bulk",
            "completion_is_set_by": "semantic_scholar_bulk_token_exhausted",
        },
        "wave_preflight": report.to_dict(),
        "safeguards": {
            "planned_only": True,
            "network_used": False,
            "production_retrieval_executed": False,
            "production_retrieval_cutoff": None,
            "prior_survey_seed_imported": False,
            "identification_set_closed": False,
            "final_global_deduplication_executed": False,
            "prisma_generated": False,
            "screening_executed": False,
            "corpus_modified": False,
        },
    }


def save_external_retrieval_preflight(*, root: str | Path) -> dict[str, Any]:
    """Persist only deterministic PLANNED wave and offline preflight artifacts."""

    root_path = Path(root).resolve()
    wave = build_external_retrieval_wave(root=root_path)
    preflight = preflight_external_retrieval_wave(wave, root=root_path)
    wave_path = _safe_output_path(root_path, WAVE_PATH)
    preflight_path = _safe_output_path(root_path, PREFLIGHT_PATH)
    wave_file_sha256 = save_production_wave(wave_path, wave)
    preflight["wave_file"] = {
        **_file_reference(wave_path, root_path),
        "sha256_from_save": wave_file_sha256,
    }
    content = _pretty_json(preflight).encode("utf-8")
    atomic_write(preflight_path, content)
    return {
        **preflight,
        "preflight_file": {
            "path": PREFLIGHT_PATH,
            "byte_size": len(content),
            "raw_sha256": _sha256(content),
        },
    }


def _query_family(
    query: Mapping[str, Any],
    *,
    reported_count: int,
    acm_by_query: Mapping[str, dict[str, Any]],
    acm_manifest_path: Path,
) -> ProductionQueryFamily:
    source = str(query["source"])
    contract = SOURCE_CONTRACTS[source]
    completeness = query["completeness"]
    native_parameters = _native_parameters(query, reported_count=reported_count)
    required_artifact = None
    if source == "ACMDigitalLibrary":
        reconciled = acm_by_query.get(str(query["query_id"]))
        if reconciled is None:
            raise ExternalRetrievalWaveError(
                f"ACM reconciliation is missing {query['query_id']}"
            )
        reconciled_count = reconciled["field_union"]["unique_stable_identity_count"]
        if reconciled_count != reported_count:
            raise ExternalRetrievalWaveError(
                f"ACM window count differs from field union for {query['query_id']}"
            )
        required_artifact = RequiredArtifact(
            kind=ArtifactKind.ACM_EXPORT_MANIFEST,
            manifest_path=ACM_RECONCILIATION_PATH,
            manifest_sha256=_sha256(acm_manifest_path.read_bytes()),
            expected_total=reported_count,
            expected_chunks=[],
        )
    return ProductionQueryFamily(
        query_family_id=str(query["query_id"]),
        source_database=source,
        source_role=contract.role,
        identification_route=contract.route,
        transport_kind=contract.transport,
        adapter_id=str(completeness["adapter_id"]),
        adapter_version=str(completeness["adapter_version"]),
        query_version=f"1.0.0:{query['variant_id']}",
        query_text=str(query["query_text"]),
        native_parameters=native_parameters,
        pagination=PaginationExpectation(
            strategy=str(completeness["pagination_strategy"]),
            adapter_version=str(completeness["adapter_version"]),
            completion_proofs=list(completeness["completion_proofs"]),
            exact_total_required=bool(completeness["exact_total_required"]),
            maximum_supported_results=completeness["maximum_supported_results"],
        ),
        required_credentials=["api_key"] if source == "IEEEXplore" else [],
        content_policy=dict(query["content_policy"]),
        result_window_status=ResultWindowStatus.CLEAR,
        required_artifact=required_artifact,
    )


def _native_parameters(
    query: Mapping[str, Any], *, reported_count: int
) -> dict[str, Any]:
    source = query["source"]
    request = query["request_specification"]
    values: dict[str, Any]
    if source == "PubMed":
        values = {**request["form"], "page_size": 200}
    elif source == "EuropePMC":
        values = dict(request["params"])
    elif source == "SemanticScholar":
        values = {**request["params"], "mode": query["mode"]}
    elif source in {"arXiv", "IEEEXplore"}:
        values = dict(request["params"])
    elif source == "ACMDigitalLibrary":
        values = {
            "field_selections": request["fields"],
            "collection_scope": "acm_publications",
            "filters": request["filters"],
            "sort": request["sort"],
            "export_format": request["export_format"],
            "ui_reported_total": reported_count,
        }
    else:  # pragma: no cover - source inventory is validated before construction
        raise ExternalRetrievalWaveError(f"unsupported external source {source}")
    values["observed_source_count"] = reported_count
    values["frozen_request_specification_hash"] = query[
        "request_specification_hash"
    ]
    return values


def _request_burdens(wave: ProductionRetrievalWave) -> dict[str, Any]:
    page_sizes = {
        "PubMed": 200,
        "EuropePMC": 1000,
        "SemanticScholar": 1000,
        "arXiv": 2000,
        "IEEEXplore": 200,
    }
    by_source: dict[str, dict[str, Any]] = {}
    for source, page_size in page_sizes.items():
        families = []
        for item in wave.query_families:
            if item.source_database != source:
                continue
            count = int(item.native_parameters["observed_source_count"])
            pages = math.ceil(count / page_size)
            requests = pages + (1 if source == "PubMed" else 0)
            families.append(
                {
                    "query_id": item.query_family_id,
                    "reported_count_evidence": count,
                    "page_size": page_size,
                    "estimated_candidate_requests": requests,
                    "pubmed_identity_enumeration_requests": (
                        1 if source == "PubMed" else 0
                    ),
                }
            )
        candidate_requests = sum(
            item["estimated_candidate_requests"] for item in families
        )
        control_requests = 6 if source == "SemanticScholar" else 0
        by_source[source] = {
            "families": families,
            "estimated_candidate_requests": candidate_requests,
            "required_control_requests": control_requests,
            "estimated_total_requests": candidate_requests + control_requests,
            "estimate_basis": (
                "prior source-count evidence; bulk token exhaustion is authoritative"
                if source == "SemanticScholar"
                else "prior exact source-count evidence; live counts may change"
            ),
        }
    by_source["ACMDigitalLibrary"] = {
        "families": [],
        "estimated_candidate_requests": 0,
        "required_control_requests": 0,
        "estimated_total_requests": 0,
        "estimate_basis": "offline import from completed bound artifacts",
    }
    return {
        "by_source": by_source,
        "estimated_http_requests": sum(
            item["estimated_total_requests"] for item in by_source.values()
        ),
        "counts_are_planning_estimates_not_completion_proofs": True,
    }


def _safe_output_path(root: Path, relative: str) -> Path:
    path = Path(relative)
    if path.is_absolute() or ".." in path.parts:
        raise ExternalRetrievalWaveError("output path must be traversal-safe and relative")
    resolved = (root / path).resolve()
    expected_root = (root / OUTPUT_ROOT).resolve()
    if not resolved.is_relative_to(expected_root):
        raise ExternalRetrievalWaveError("output escaped the external-wave namespace")
    return resolved


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _file_reference(path: Path, root: Path) -> dict[str, Any]:
    raw = path.read_bytes()
    return {
        "path": path.resolve().relative_to(root.resolve()).as_posix(),
        "byte_size": len(raw),
        "raw_sha256": _sha256(raw),
    }


def _json_artifact_reference(
    path: Path, root: Path, canonical_hash_key: str
) -> dict[str, Any]:
    payload = _load_json(path)
    return {
        **_file_reference(path, root),
        "canonical_hash": payload[canonical_hash_key],
    }


def _verify_bound_files(wave: ProductionRetrievalWave, root: Path) -> None:
    bindings = wave.metadata.get("bindings", {})
    expected_names = {
        "production_query_plan",
        "retrieval_prerequisites",
        "ieee_readiness",
        "acm_readiness",
        "source_windows",
        "acm_final_reconciliation",
    }
    if set(bindings) != expected_names:
        raise ExternalRetrievalWaveError("external wave evidence bindings changed")
    for name, reference in bindings.items():
        value = Path(str(reference.get("path", "")))
        if value.is_absolute() or ".." in value.parts or not value.parts:
            raise ExternalRetrievalWaveError(f"unsafe evidence path for {name}")
        path = (root / value).resolve()
        if not path.is_relative_to(root) or not path.is_file():
            raise ExternalRetrievalWaveError(f"missing bound evidence for {name}")
        raw = path.read_bytes()
        if (
            len(raw) != reference.get("byte_size")
            or _sha256(raw) != reference.get("raw_sha256")
        ):
            raise ExternalRetrievalWaveError(f"bound evidence changed for {name}")


def _hash_payload(payload: Mapping[str, Any]) -> str:
    return _sha256(
        json.dumps(
            payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True
        ).encode("utf-8")
    )


def _sha256(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _pretty_json(payload: Mapping[str, Any]) -> str:
    return json.dumps(payload, sort_keys=True, indent=2, ensure_ascii=True) + "\n"


def utc_now() -> str:
    return datetime.now(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def validate_persisted_external_preflight(
    *, root: str | Path
) -> tuple[ProductionRetrievalWave, dict[str, Any]]:
    """Rebuild all frozen bindings and reject any persisted preflight drift."""

    root_path = Path(root).resolve()
    wave_path = root_path / WAVE_PATH
    preflight_path = root_path / PREFLIGHT_PATH
    expected_wave = build_external_retrieval_wave(root=root_path)
    expected_wave_bytes = (expected_wave.to_json() + "\n").encode("utf-8")
    if wave_path.read_bytes() != expected_wave_bytes:
        raise ExternalRetrievalWaveError("persisted planned wave differs from frozen inputs")
    preflight = _load_json(preflight_path)
    if preflight.get("status") != READY_STATUS:
        raise ExternalRetrievalWaveError("persisted external preflight is not ready")
    if preflight.get("wave_manifest_hash") != expected_wave.manifest_hash():
        raise ExternalRetrievalWaveError("persisted preflight wave hash mismatch")
    wave_ref = preflight.get("wave_file", {})
    if (
        wave_ref.get("path") != WAVE_PATH
        or wave_ref.get("byte_size") != len(expected_wave_bytes)
        or wave_ref.get("raw_sha256") != _sha256(expected_wave_bytes)
    ):
        raise ExternalRetrievalWaveError("persisted preflight wave-file binding mismatch")
    regenerated = preflight_external_retrieval_wave(expected_wave, root=root_path)
    for key in (
        "status",
        "wave_id",
        "wave_manifest_hash",
        "production_query_plan",
        "retrieval_prerequisites",
        "external_gate",
        "identification_closure",
        "query_inventory",
        "source_query_counts",
        "request_burden",
        "credentials",
        "acm_artifact_import",
        "semantic_scholar",
        "safeguards",
    ):
        if preflight.get(key) != regenerated.get(key):
            raise ExternalRetrievalWaveError(
                f"persisted preflight field changed: {key}"
            )
    return expected_wave, preflight


def _semantic_control_request(probe_id: str, expression: str) -> PageRequest:
    return PageRequest(
        "GET",
        "https://api.semanticscholar.org/graph/v1/paper/search/bulk",
        params={
            "query": expression,
            "limit": 1,
            "fields": "paperId",
            "sort": "paperId:asc",
        },
        state={"probe_id": probe_id},
    )


def _validate_blocked_semantic_control_5xx_gate(
    *, root: Path, source_state: Mapping[str, Any]
) -> dict[str, Any]:
    manifest_path = _safe_output_path(root, SEMANTIC_CONTROL_GATE_PATH)
    manifest_bytes = manifest_path.read_bytes()
    if _sha256(manifest_bytes) != SEMANTIC_CONTROL_BLOCKED_MANIFEST_RAW_SHA256:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar blocked control-manifest raw hash changed"
        )
    manifest = json.loads(manifest_bytes)
    _validate_embedded_hash(manifest, "manifest_hash")
    if (
        manifest.get("manifest_hash")
        != SEMANTIC_CONTROL_BLOCKED_MANIFEST_LOGICAL_HASH
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar blocked control-manifest logical hash changed"
        )

    control_path = root / SEMANTIC_CONTROL_PATH
    controls = load_semantic_control_set(control_path)
    control_reference = manifest.get("control_set", {})
    if (
        control_reference.get("path") != SEMANTIC_CONTROL_PATH
        or control_reference.get("raw_sha256")
        != _sha256(control_path.read_bytes())
        or control_reference.get("canonical_hash") != controls.control_set_hash()
        or manifest.get("assertions")
        != [assertion.to_dict() for assertion in controls.assertions]
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar frozen control/query binding changed"
        )
    if (
        manifest.get("status") != "UNRESOLVED"
        or manifest.get("candidate_queries_executed") is not False
        or manifest.get("requests_this_session") != 9
        or manifest.get("unresolved_control_ids") != ["a-or-b"]
        or manifest.get("failed_assertion_ids") != []
        or manifest.get("assertion_results") != []
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar control gate is not the known 5xx-exhausted block"
        )

    probes = {probe.probe_id: probe for probe in controls.probes}
    observations = manifest.get("controls")
    if not isinstance(observations, list) or len(observations) != 4:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar control attempt signature changed"
        )
    response_bindings: list[dict[str, Any]] = []
    observed_response_paths: set[str] = set()
    for observation, expected in zip(
        observations, SEMANTIC_CONTROL_5XX_EXPECTED_SIGNATURE, strict=True
    ):
        probe_id, statuses, final_status, reported_count = expected
        probe = probes[probe_id]
        request = _semantic_control_request(probe_id, probe.expression)
        attempts = observation.get("attempts")
        if (
            observation.get("probe_id") != probe_id
            or observation.get("expression") != probe.expression
            or observation.get("expression_sha256")
            != _sha256(probe.expression.encode("utf-8"))
            or observation.get("request")
            != {
                "method": request.method,
                "url": request.url,
                "params": request.sanitized_params(),
            }
            or observation.get("request_hash") != request.request_hash()
            or observation.get("status") != final_status
            or observation.get("reported_count") != reported_count
            or not isinstance(attempts, list)
            or len(attempts) != len(statuses)
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar control attempt/query signature changed"
            )
        for attempt_number, (attempt, status) in enumerate(
            zip(attempts, statuses, strict=True), start=1
        ):
            response_reference = attempt.get("response")
            expected_attempt_status = "SUCCEEDED" if status == 200 else "FAILED"
            if (
                attempt.get("attempt_number") != attempt_number
                or attempt.get("request_hash") != request.request_hash()
                or attempt.get("status") != expected_attempt_status
                or attempt.get("error")
                != (None if status == 200 else f"HTTP {status}")
                or not isinstance(response_reference, dict)
                or response_reference.get("status") != status
                or response_reference.get("retry_after_header_present") is not False
                or response_reference.get("retry_after") is not None
            ):
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar control attempt signature changed"
                )
            relative_path = response_reference.get("path")
            if not isinstance(relative_path, str) or relative_path in observed_response_paths:
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar control response binding changed"
                )
            response_path = (manifest_path.parent / relative_path).resolve()
            try:
                response_path.relative_to(manifest_path.parent.resolve())
            except ValueError as exc:
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar control response escaped its gate directory"
                ) from exc
            response_bytes = response_path.read_bytes()
            if (
                len(response_bytes) != response_reference.get("byte_size")
                or _sha256(response_bytes) != response_reference.get("sha256")
            ):
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar control response hash/size changed"
                )
            try:
                stored = CheckpointStore(manifest_path.parent).load_response(
                    relative_path, response_reference["sha256"]
                )
                payload = stored.json()
            except (KeyError, OSError, ValueError, json.JSONDecodeError) as exc:
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar control response hash/read failure"
                ) from exc
            if stored.status_code != status:
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar control response status changed"
                )
            if status == 500:
                if payload != {"message": "Internal Server Error"}:
                    raise ExternalRetrievalWaveError(
                        "Semantic Scholar 5xx recovery response signature changed"
                    )
            elif not isinstance(payload, dict) or int(payload.get("total", -1)) != int(
                reported_count
            ):
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar successful control result changed"
                )
            observed_response_paths.add(relative_path)
            response_bindings.append(
                {
                    "probe_id": probe_id,
                    "attempt_number": attempt_number,
                    **dict(response_reference),
                }
            )

    response_dir = manifest_path.parent / "responses"
    persisted_response_paths = {
        path.relative_to(manifest_path.parent).as_posix()
        for path in response_dir.iterdir()
        if path.is_file()
    }
    if persisted_response_paths != observed_response_paths:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar control response inventory changed"
        )
    if [probe.probe_id for probe in controls.probes[4:]] != [
        "grouped-left",
        "grouped-right",
    ]:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar grouped control order changed"
        )
    if (
        source_state.get("status") != "BLOCKED_SEMANTIC_CONTROL_GATE"
        or source_state.get("completed_query_count") != 0
        or source_state.get("total_query_count") != 5
        or source_state.get("candidate_request_count") != 0
        or source_state.get("failure_reason")
        != "Semantic Scholar control gate is UNRESOLVED"
        or source_state.get("pause_reason") is not None
        or source_state.get("semantic_control_gate")
        != {
            "status": "UNRESOLVED",
            "manifest_path": SEMANTIC_CONTROL_GATE_PATH,
            "manifest_hash": SEMANTIC_CONTROL_BLOCKED_MANIFEST_LOGICAL_HASH,
        }
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar source state is not the known blocked control gate"
        )
    candidate_checkpoint = _safe_output_path(
        root, f"{EXECUTION_ROOT}/SemanticScholar/checkpoint"
    )
    if candidate_checkpoint.exists():
        raise ExternalRetrievalWaveError(
            "Semantic Scholar candidate state exists before control recovery"
        )
    return {
        "manifest": manifest,
        "manifest_reference": _file_reference(manifest_path, root),
        "response_bindings": response_bindings,
    }


def _validate_authorized_semantic_control_5xx_recovery(
    *, root: Path, source_state: Mapping[str, Any]
) -> None:
    recovery_state = source_state.get("control_gate_recovery")
    if not isinstance(recovery_state, dict) or not isinstance(
        recovery_state.get("source_state"), dict
    ):
        raise ExternalRetrievalWaveError(
            "authorized Semantic Scholar control recovery lacks source provenance"
        )
    original = _validate_blocked_semantic_control_5xx_gate(
        root=root,
        source_state=recovery_state["source_state"],
    )
    recovered_reference = source_state.get("semantic_control_gate", {})
    if (
        source_state.get("status") != SEMANTIC_CONTROL_5XX_RECOVERY_STATUS
        or recovered_reference.get("manifest_path")
        != SEMANTIC_CONTROL_RECOVERY_GATE_PATH
        or recovered_reference.get("status") != "PAUSED_TRANSIENT_PROVIDER"
        or source_state.get("candidate_request_count") != 0
    ):
        raise ExternalRetrievalWaveError(
            "authorized Semantic Scholar control recovery lineage changed"
        )
    recovered_path = _safe_output_path(
        root, SEMANTIC_CONTROL_RECOVERY_GATE_PATH
    )
    active_reference = recovery_state.get("active_manifest")
    if not isinstance(active_reference, dict):
        raise ExternalRetrievalWaveError(
            "authorized Semantic Scholar recovery lacks an active manifest binding"
        )
    _verify_file_reference(recovered_path, active_reference, root)
    recovered = _load_json(recovered_path)
    _validate_embedded_hash(recovered, "manifest_hash")
    provenance = recovered.get("recovery_provenance", {})
    expected_provenance_hash = _hash_payload(
        {
            "manifest": original["manifest_reference"],
            "responses": original["response_bindings"],
            "source_state": recovery_state["source_state"],
        }
    )
    if (
        recovered_reference.get("manifest_hash") != recovered.get("manifest_hash")
        or recovered.get("status") != "PAUSED_TRANSIENT_PROVIDER"
        or recovered.get("candidate_queries_executed") is not False
        or recovered.get("requests_this_session") != 0
        or provenance.get("source_manifest") != original["manifest_reference"]
        or provenance.get("source_responses") != original["response_bindings"]
        or provenance.get("source_state") != recovery_state["source_state"]
        or provenance.get("source_provenance_hash") != expected_provenance_hash
        or recovery_state.get("source_provenance_hash")
        != expected_provenance_hash
        or recovery_state.get("source_responses")
        != original["response_bindings"]
    ):
        raise ExternalRetrievalWaveError(
            "authorized Semantic Scholar recovered manifest changed"
        )


def authorize_semantic_scholar_control_5xx_recovery(
    *, root: str | Path, timestamp: Callable[[], str] = utc_now
) -> dict[str, Any]:
    """Reopen only the retryable-5xx-exhausted Semantic Scholar control."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["SemanticScholar"]
    if source_state.get("status") == SEMANTIC_CONTROL_5XX_RECOVERY_STATUS:
        _validate_authorized_semantic_control_5xx_recovery(
            root=root_path, source_state=source_state
        )
        return state
    recovered_path = _safe_output_path(
        root_path, SEMANTIC_CONTROL_RECOVERY_GATE_PATH
    )
    if recovered_path.exists():
        raise ExternalRetrievalWaveError(
            "Semantic Scholar recovered control manifest exists without valid lineage"
        )
    if state.get("external_retrieval_cutoff_date") is not None:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar control recovery cannot alter a closed retrieval wave"
        )

    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "SemanticScholar"
    }
    source_state_before = json.loads(json.dumps(source_state, sort_keys=True))
    validated = _validate_blocked_semantic_control_5xx_gate(
        root=root_path, source_state=source_state
    )
    original_path = _safe_output_path(root_path, SEMANTIC_CONTROL_GATE_PATH)
    immutable_files = {
        original_path: original_path.read_bytes(),
        **{
            original_path.parent / binding["path"]: (
                original_path.parent / binding["path"]
            ).read_bytes()
            for binding in validated["response_bindings"]
        },
    }
    recovered_at = timestamp()
    recovered = json.loads(json.dumps(validated["manifest"], sort_keys=True))
    recovered["schema_version"] = "1.2.0"
    recovered["status"] = "PAUSED_TRANSIENT_PROVIDER"
    recovered["pause_state"] = "TRANSIENT_PROVIDER_5XX_EXHAUSTED"
    recovered["pause_reason"] = "RETRYABLE_PROVIDER_5XX_EXHAUSTED"
    recovered["pause_metadata"] = {
        "source_database": "SemanticScholar",
        "probe_id": "a-or-b",
        "http_statuses": [500, 500, 500],
        "retry_after_header_present": False,
        "retry_after": None,
        "attempts_this_invocation": 3,
        "maximum_attempts_per_invocation": 3,
    }
    recovered["requests_this_session"] = 0
    for observation in recovered["controls"]:
        if observation["probe_id"] == "a-or-b":
            observation["status"] = "PAUSED_TRANSIENT_PROVIDER"
    recovered["recovery_provenance"] = {
        "authorization_reason": (
            "OFFLINE_SEMANTIC_SCHOLAR_CONTROL_5XX_EXHAUSTION_RECOVERY"
        ),
        "authorized_at_utc": recovered_at,
        "source_manifest": validated["manifest_reference"],
        "source_manifest_logical_hash": (
            SEMANTIC_CONTROL_BLOCKED_MANIFEST_LOGICAL_HASH
        ),
        "source_state": source_state_before,
        "source_response_count": len(validated["response_bindings"]),
        "source_responses": validated["response_bindings"],
        "source_provenance_hash": _hash_payload(
            {
                "manifest": validated["manifest_reference"],
                "responses": validated["response_bindings"],
                "source_state": source_state_before,
            }
        ),
        "retained_successful_control_ids": [
            "atomic-a",
            "atomic-b",
            "a-and-b",
        ],
        "reopened_control_id": "a-or-b",
        "unattempted_control_ids": ["grouped-left", "grouped-right"],
        "candidate_request_count": 0,
        "network_used": False,
        "immutable_source": True,
    }
    _save_hashed_json(recovered_path, recovered, "manifest_hash")

    source_state.update(
        {
            "status": SEMANTIC_CONTROL_5XX_RECOVERY_STATUS,
            "completed_query_count": 0,
            "total_query_count": 5,
            "candidate_request_count": 0,
            "semantic_control_gate": {
                "status": recovered["status"],
                "manifest_path": SEMANTIC_CONTROL_RECOVERY_GATE_PATH,
                "manifest_hash": recovered["manifest_hash"],
            },
            "control_gate_recovery": {
                **recovered["recovery_provenance"],
                "source_state": source_state_before,
                "active_manifest": _file_reference(recovered_path, root_path),
            },
            "control_requests_this_session": 0,
            "pause_reason": (
                "OFFLINE_CONTROL_5XX_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
            ),
            "pause_metadata": dict(recovered["pause_metadata"]),
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    if {
        key: value
        for key, value in state["sources"].items()
        if key != "SemanticScholar"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar control recovery changed another source"
        )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    for path, expected_bytes in immutable_files.items():
        if path.read_bytes() != expected_bytes:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar immutable control evidence changed during recovery"
            )
    return state


def _validate_passed_semantic_control_gate(
    *, root: Path, source_state: Mapping[str, Any]
) -> dict[str, Any]:
    reference = source_state.get("semantic_control_gate", {})
    if (
        reference.get("status") != "PASSED"
        or reference.get("manifest_path") != SEMANTIC_CONTROL_RECOVERY_GATE_PATH
        or reference.get("manifest_hash")
        != SEMANTIC_CANDIDATE_5XX_EXPECTED_CONTROL_LOGICAL_HASH
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar passing control-gate binding changed"
        )
    path = _safe_output_path(root, SEMANTIC_CONTROL_RECOVERY_GATE_PATH)
    raw = path.read_bytes()
    if _sha256(raw) != SEMANTIC_CANDIDATE_5XX_EXPECTED_CONTROL_SHA256:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar passing control-gate artifact changed"
        )
    manifest = json.loads(raw)
    _validate_embedded_hash(manifest, "manifest_hash")
    controls = manifest.get("controls", [])
    if (
        manifest.get("status") != "PASSED"
        or manifest.get("manifest_hash") != reference["manifest_hash"]
        or len(controls) != 6
        or any(item.get("status") != "SUCCEEDED" for item in controls)
        or manifest.get("failed_assertion_ids") != []
        or manifest.get("unresolved_control_ids") != []
        or not manifest.get("assertion_results")
        or any(
            result.get("passed") is not True
            for result in manifest["assertion_results"]
        )
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar control gate is not the frozen passing gate"
        )
    response_paths: set[str] = set()
    store = CheckpointStore(path.parent)
    for control in controls:
        for attempt in control.get("attempts", []):
            response = attempt.get("response")
            if not isinstance(response, dict):
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar passing control attempt lacks a response"
                )
            relative_path = response.get("path")
            expected_hash = response.get("sha256")
            if (
                not isinstance(relative_path, str)
                or not isinstance(expected_hash, str)
                or relative_path in response_paths
            ):
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar passing control response binding changed"
                )
            stored = store.load_response(relative_path, expected_hash)
            response_path = path.parent / relative_path
            if (
                response_path.stat().st_size != response.get("byte_size")
                or stored.status_code != response.get("status")
            ):
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar passing control response metadata changed"
                )
            response_paths.add(relative_path)
    persisted_paths = {
        item.relative_to(path.parent).as_posix()
        for item in (path.parent / "responses").iterdir()
        if item.is_file()
    }
    if response_paths != persisted_paths:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar passing control response inventory changed"
        )
    return {
        "manifest": _file_reference(path, root),
        "manifest_logical_hash": manifest["manifest_hash"],
        "response_count": len(response_paths),
        "response_manifest_hash": _hash_payload(
            {"responses": sorted(response_paths)}
        ),
    }


def _validate_semantic_candidate_5xx_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    root: Path,
    wave: ProductionRetrievalWave,
) -> dict[str, Any]:
    if len(dataset.retrieval_runs) != 1 or len(dataset.source_queries) != 5:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar candidate checkpoint shape changed"
        )
    run = dataset.retrieval_runs[0]
    specs = _source_query_specs(wave, "SemanticScholar", ieee_credential="")
    if (
        run.run_id != f"{WAVE_ID}:SemanticScholar"
        or run.query_plan_hash != _query_plan_hash(specs)
        or run.query_plan_version != wave.query_plan_hash
        or run.planned_query_ids
        != [query.query_id for query in dataset.source_queries]
        or run.source_query_ids != run.planned_query_ids
        or run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar frozen run/query-plan binding changed"
        )
    if (
        len(dataset.retrieval_attempts)
        != SEMANTIC_CANDIDATE_5XX_EXPECTED_ATTEMPTS
        or len(dataset.occurrences)
        != SEMANTIC_CANDIDATE_5XX_EXPECTED_OCCURRENCES
        or len(dataset.canonical_records)
        != SEMANTIC_CANDIDATE_5XX_EXPECTED_CANONICAL_RECORDS
        or len(dataset.retrieval_pages)
        != SEMANTIC_CANDIDATE_5XX_EXPECTED_TOTAL_PAGES
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar terminal candidate counts changed"
        )

    adapter = PAGINATED_SOURCE_ADAPTERS["SemanticScholar"]
    attempts_by_id = {
        attempt.attempt_id: attempt for attempt in dataset.retrieval_attempts
    }
    response_references: list[tuple[str, str]] = []
    response_bindings: list[dict[str, Any]] = []
    status_counts: dict[int, int] = {}
    failed_query_indexes: list[int] = []
    for index, (query, spec, expected) in enumerate(
        zip(
            dataset.source_queries,
            specs,
            SEMANTIC_CANDIDATE_5XX_EXPECTED_COUNTS,
            strict=True,
        )
    ):
        expected_status, expected_page_count, expected_occurrences = expected
        expected_completion = RetrievalCompletionStatus(expected_status.lower())
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        occurrences = [
            item
            for item in dataset.occurrences
            if item.source_query_id == query.query_id
        ]
        if (
            query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.endpoint != spec.endpoint
            or query.fields != spec.fields
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("pagination_mode") != "bulk"
            or query.completion_status is not expected_completion
            or query.result_count != expected_occurrences
            or len(pages) != expected_page_count
            or len(occurrences) != expected_occurrences
            or query.page_ids != [page.page_id for page in pages]
            or [page.ordinal for page in pages] != list(range(len(pages)))
        ):
            raise ExternalRetrievalWaveError(
                f"Semantic Scholar QF{index + 1:02d} frozen evidence changed"
            )
        identifiers: set[str] = set()
        for page in pages:
            if page.adapter_version != adapter.version or page.strategy != adapter.strategy:
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar candidate adapter lineage changed"
                )
            expected_request = adapter.build_request(spec, page.request_state)
            page_attempts = [attempts_by_id[item] for item in page.attempt_ids]
            if not page_attempts or any(
                attempt.page_id != page.page_id
                or attempt.request_hash != expected_request.request_hash()
                or attempt.request_method != expected_request.method
                or attempt.request_url != expected_request.url
                or attempt.request_params != expected_request.sanitized_params()
                or attempt.request_headers != expected_request.sanitized_headers()
                for attempt in page_attempts
            ):
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar candidate request/hash binding changed"
                )
            for attempt in page_attempts:
                if (
                    attempt.raw_response_path is None
                    or attempt.raw_response_hash is None
                    or attempt.response_status is None
                ):
                    raise ExternalRetrievalWaveError(
                        "Semantic Scholar candidate attempt lacks response evidence"
                    )
                try:
                    stored = CheckpointStore(checkpoint_dir).load_response(
                        attempt.raw_response_path, attempt.raw_response_hash
                    )
                except (OSError, ValueError) as exc:
                    raise ExternalRetrievalWaveError(
                        "Semantic Scholar candidate response hash/read failure"
                    ) from exc
                if stored.status_code != attempt.response_status:
                    raise ExternalRetrievalWaveError(
                        "Semantic Scholar candidate response status changed"
                    )
                if (
                    stored.status_code == 200
                    and attempt.status is not RetrievalAttemptStatus.SUCCEEDED
                ) or (
                    stored.status_code == 500
                    and (
                        attempt.status is not RetrievalAttemptStatus.FAILED
                        or not str(attempt.error or "").startswith("HTTP 500 ")
                    )
                ) or (
                    stored.status_code == 429
                    and (
                        attempt.status is not RetrievalAttemptStatus.FAILED
                        or not str(attempt.error or "").startswith(
                            "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
                        )
                    )
                ):
                    raise ExternalRetrievalWaveError(
                        "Semantic Scholar candidate attempt classification changed"
                    )
                status_counts[stored.status_code] = (
                    status_counts.get(stored.status_code, 0) + 1
                )
                response_references.append(
                    (attempt.raw_response_path, attempt.raw_response_hash)
                )
                response_path = checkpoint_dir / attempt.raw_response_path
                response_bindings.append(
                    {
                        "attempt_id": attempt.attempt_id,
                        "page_id": page.page_id,
                        "path": response_path.relative_to(root).as_posix(),
                        "byte_size": response_path.stat().st_size,
                        "raw_sha256": attempt.raw_response_hash,
                        "http_status": stored.status_code,
                    }
                )
            if page.status is RetrievalCompletionStatus.COMPLETE:
                if page.metadata.get("completion_error") is not None:
                    raise ExternalRetrievalWaveError(
                        "Semantic Scholar retained page has integrity-failure evidence"
                    )
                overlap = identifiers.intersection(page.native_identifiers)
                if overlap or len(page.native_identifiers) != len(
                    set(page.native_identifiers)
                ):
                    raise ExternalRetrievalWaveError(
                        "Semantic Scholar retained candidate identity overlap changed"
                    )
                identifiers.update(page.native_identifiers)
        if any(
            occurrence.metadata.get("source_identifier_missing")
            or occurrence.metadata.get("parser_incomplete")
            for occurrence in occurrences
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar retained candidate record is malformed"
            )

        if expected_status == "FAILED":
            failed_query_indexes.append(index)
            failed_page = pages[-1]
            failed_attempts = [attempts_by_id[item] for item in failed_page.attempt_ids]
            token = SEMANTIC_CANDIDATE_5XX_CONTINUATION_TOKENS[index]
            if (
                query.status is not ProcessingStatus.FAILED
                or len(query.errors) != 1
                or query.errors[0]
                != failed_page.metadata.get("completion_error")
                or failed_page.status is not RetrievalCompletionStatus.FAILED
                or failed_page.request_state != {"mode": "bulk", "token": token}
                or failed_page.returned_item_count != 0
                or failed_page.occurrence_ids
                or len(failed_attempts) != 3
                or any(
                    attempt.status is not RetrievalAttemptStatus.FAILED
                    or attempt.response_status != 500
                    or not str(attempt.error or "").startswith("HTTP 500 ")
                    for attempt in failed_attempts
                )
            ):
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar failed candidate page is not exact 5xx exhaustion"
                )
            for attempt in failed_attempts:
                try:
                    response = CheckpointStore(checkpoint_dir).load_response(
                        attempt.raw_response_path, attempt.raw_response_hash
                    )
                except (OSError, ValueError) as exc:
                    raise ExternalRetrievalWaveError(
                        "Semantic Scholar failed response hash/read failure"
                    ) from exc
                if (
                    response.json() != {"message": "Internal Server Error"}
                    or any(
                        str(key).lower() == "retry-after"
                        for key in response.headers
                    )
                ):
                    raise ExternalRetrievalWaveError(
                        "Semantic Scholar failed candidate response signature changed"
                    )
        elif (
            query.status is not ProcessingStatus.OK
            or query.errors
            or any(
                page.status is not RetrievalCompletionStatus.COMPLETE
                for page in pages
            )
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar completed-family evidence changed"
            )

    response_paths = [item[0] for item in response_references]
    persisted_paths = {
        item.relative_to(checkpoint_dir).as_posix()
        for item in (checkpoint_dir / "responses").iterdir()
        if item.is_file()
    }
    if (
        failed_query_indexes != [1, 2]
        or run.errors
        != [
            f"{dataset.source_queries[index].query_id}: "
            f"{dataset.source_queries[index].errors[0]}"
            for index in failed_query_indexes
        ]
        or len(response_references)
        != SEMANTIC_CANDIDATE_5XX_EXPECTED_RESPONSES
        or len(set(response_paths)) != len(response_paths)
        or set(response_paths) != persisted_paths
        or status_counts != SEMANTIC_CANDIDATE_5XX_EXPECTED_HTTP_STATUSES
        or sum(
            page.status is RetrievalCompletionStatus.COMPLETE
            for page in dataset.retrieval_pages
        )
        != SEMANTIC_CANDIDATE_5XX_EXPECTED_SUCCESSFUL_PAGES
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar candidate response/page inventory changed"
        )
    dataset.validate()
    return {
        "raw_response_references": response_references,
        "raw_response_bindings": response_bindings,
        "failed_query_indexes": failed_query_indexes,
        "attempt_manifest_hash": _hash_payload(
            {
                "attempts": [
                    {
                        "attempt_id": attempt.attempt_id,
                        "request_hash": attempt.request_hash,
                        "response_status": attempt.response_status,
                        "raw_response_hash": attempt.raw_response_hash,
                    }
                    for attempt in dataset.retrieval_attempts
                ]
            }
        ),
    }


def _validate_authorized_semantic_candidate_5xx_recovery(
    *, root: Path, source_state: Mapping[str, Any], wave: ProductionRetrievalWave
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 2:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar candidate recovery episode lineage changed"
        )
    failed, recovered = episodes
    if (
        failed.get("episode_number") != 1
        or failed.get("status") != "FAILED"
        or failed.get("immutable") is not True
        or recovered.get("episode_number") != 2
        or recovered.get("status") != SEMANTIC_CANDIDATE_5XX_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 1
        or recovered.get("network_used") is not False
        or recovered.get("immutable") is not False
        or source_state.get("status") != SEMANTIC_CANDIDATE_5XX_RECOVERY_STATUS
        or source_state.get("active_episode_number") != 2
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
        or source_state.get("checkpoint_dataset")
        != recovered.get("checkpoint_dataset")
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar authorized candidate recovery lineage changed"
        )
    source_checkpoint = _safe_output_path(
        root, failed["checkpoint_dataset"]["path"]
    )
    active_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(source_checkpoint, failed["checkpoint_dataset"], root)
    _verify_file_reference(active_checkpoint, recovered["checkpoint_dataset"], root)
    if len(recovered.get("source_raw_responses", [])) != (
        SEMANTIC_CANDIDATE_5XX_EXPECTED_RESPONSES
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar candidate recovery response manifest changed"
        )
    _verify_recovery_raw_bindings(
        root, recovered.get("source_raw_responses", []), "episode_1_path"
    )
    control = _validate_passed_semantic_control_gate(
        root=root, source_state=source_state
    )
    dataset = load_review_dataset(active_checkpoint)
    if (
        len(dataset.retrieval_attempts)
        != SEMANTIC_CANDIDATE_5XX_EXPECTED_ATTEMPTS
        or len(dataset.occurrences)
        != SEMANTIC_CANDIDATE_5XX_EXPECTED_OCCURRENCES
        or [
            query.completion_status.value for query in dataset.source_queries
        ]
        != ["complete", "running", "running", "complete", "complete"]
        or [
            page.request_state
            for page in dataset.retrieval_pages
            if page.status is RetrievalCompletionStatus.RUNNING
        ]
        != [
            {"mode": "bulk", "token": token}
            for token in SEMANTIC_CANDIDATE_5XX_CONTINUATION_TOKENS[1:3]
        ]
        or recovered.get("frozen_wave_manifest_hash") != wave.manifest_hash()
        or recovered.get("frozen_query_plan_hash") != wave.query_plan_hash
        or recovered.get("control_gate") != control
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar active candidate recovery checkpoint changed"
        )


def authorize_semantic_scholar_candidate_5xx_recovery(
    *, root: str | Path, timestamp: Callable[[], str] = utc_now
) -> dict[str, Any]:
    """Reopen only the two exact retryable-5xx-exhausted candidate pages."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state_bytes = state_path.read_bytes()
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["SemanticScholar"]
    if source_state.get("status") == SEMANTIC_CANDIDATE_5XX_RECOVERY_STATUS:
        _validate_authorized_semantic_candidate_5xx_recovery(
            root=root_path, source_state=source_state, wave=wave
        )
        return state
    if _sha256(state_bytes) != SEMANTIC_CANDIDATE_5XX_EXPECTED_STALE_STATE_SHA256:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar candidate recovery requires the exact stale global state"
        )
    if state.get("external_retrieval_cutoff_date") is not None:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar candidate recovery cannot alter a closed retrieval wave"
        )

    checkpoint_relative = f"{EXECUTION_ROOT}/SemanticScholar/checkpoint"
    checkpoint_path = _safe_output_path(
        root_path, f"{checkpoint_relative}/review_dataset.json"
    )
    checkpoint_raw = checkpoint_path.read_bytes()
    if _sha256(checkpoint_raw) != SEMANTIC_CANDIDATE_5XX_EXPECTED_CHECKPOINT_SHA256:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar terminal candidate checkpoint digest changed"
        )
    try:
        dataset = load_review_dataset(checkpoint_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar terminal candidate checkpoint is invalid"
        ) from exc
    validated = _validate_semantic_candidate_5xx_checkpoint(
        dataset=dataset,
        checkpoint_dir=checkpoint_path.parent,
        root=root_path,
        wave=wave,
    )
    control = _validate_passed_semantic_control_gate(
        root=root_path, source_state=source_state
    )
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "SemanticScholar"
    }
    source_state_before = json.loads(json.dumps(source_state, sort_keys=True))
    recovered_at = timestamp()
    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/SemanticScholar/episodes/episode-002/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "Semantic Scholar candidate recovery checkpoint exists without lineage"
        )
    response_bindings = _copy_recovery_raw_responses(
        root=root_path,
        source_checkpoint_dir=checkpoint_path.parent,
        recovery_checkpoint_dir=recovery_checkpoint_dir,
        raw_response_references=validated["raw_response_references"],
        source_path_key="episode_1_path",
        error_prefix="Semantic Scholar candidate-5xx",
    )

    run = dataset.retrieval_runs[0]
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.errors = [
        "offline Semantic Scholar candidate-5xx recovery complete; live resume pending"
    ]
    run.metadata.pop("pause_state", None)
    run.metadata.pop("pause_reason", None)
    run.metadata.pop("pause_metadata", None)
    run.metadata.pop("session_request_count", None)
    continuation_states = []
    for index in validated["failed_query_indexes"]:
        query = dataset.source_queries[index]
        page = next(
            item
            for item in dataset.retrieval_pages
            if item.page_id == query.page_ids[-1]
        )
        page.status = RetrievalCompletionStatus.RUNNING
        page.metadata.pop("completion_error", None)
        query.status = ProcessingStatus.PARTIAL
        query.completion_status = RetrievalCompletionStatus.RUNNING
        query.errors = []
        query.retrieval_ended_at = recovered_at
        query.metadata.pop("pause_state", None)
        query.metadata.pop("pause_reason", None)
        query.metadata.pop("pause_metadata", None)
        continuation_states.append(
            {
                "query_id": query.query_id,
                "production_query_id": query.metadata["production_query_id"],
                "page_id": page.page_id,
                "request_state": dict(page.request_state),
                "request_hash": dataset.retrieval_attempts[
                    next(
                        position
                        for position, attempt in enumerate(dataset.retrieval_attempts)
                        if attempt.attempt_id == page.attempt_ids[-1]
                    )
                ].request_hash,
            }
        )
    run.metadata["offline_semantic_candidate_5xx_recovery"] = {
        "recovery_episode_number": 2,
        "source_episode_number": 1,
        "source_checkpoint": _file_reference(checkpoint_path, root_path),
        "source_attempt_manifest_hash": validated["attempt_manifest_hash"],
        "source_response_count": len(response_bindings),
        "source_response_manifest_hash": _hash_payload(
            {"responses": response_bindings}
        ),
        "successful_page_count": SEMANTIC_CANDIDATE_5XX_EXPECTED_SUCCESSFUL_PAGES,
        "continuation_states": continuation_states,
        "control_gate": control,
        "network_used": False,
    }
    dataset.validate()
    store = CheckpointStore(recovery_checkpoint_dir)
    checkpoint_hash = store.save_dataset(dataset)
    recovered_checkpoint_reference = _file_reference(store.dataset_path, root_path)
    if checkpoint_hash != recovered_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar candidate recovery checkpoint hash disagreement"
        )
    if (
        checkpoint_path.stat().st_size != len(checkpoint_raw)
        or _sha256(checkpoint_path.read_bytes())
        != SEMANTIC_CANDIDATE_5XX_EXPECTED_CHECKPOINT_SHA256
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar terminal candidate checkpoint changed during recovery"
        )
    _verify_recovery_raw_bindings(root_path, response_bindings, "episode_1_path")

    source_checkpoint_reference = _file_reference(checkpoint_path, root_path)
    episode_1 = {
        "episode_number": 1,
        "episode_id": "SemanticScholar-episode-001",
        "run_id": run.run_id,
        "status": "FAILED",
        "checkpoint_path": checkpoint_relative,
        "checkpoint_dataset": source_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "attempt_count": SEMANTIC_CANDIDATE_5XX_EXPECTED_ATTEMPTS,
        "response_count": SEMANTIC_CANDIDATE_5XX_EXPECTED_RESPONSES,
        "successful_page_count": SEMANTIC_CANDIDATE_5XX_EXPECTED_SUCCESSFUL_PAGES,
        "occurrence_count": SEMANTIC_CANDIDATE_5XX_EXPECTED_OCCURRENCES,
        "completed_query_count": 3,
        "failure_classification": "RETRYABLE_PROVIDER_5XX_EXHAUSTION",
        "source_state_snapshot": source_state_before,
        "immutable": True,
    }
    episode_2 = {
        "episode_number": 2,
        "episode_id": "SemanticScholar-episode-002",
        "run_id": run.run_id,
        "status": SEMANTIC_CANDIDATE_5XX_RECOVERY_STATUS,
        "recovery_of_episode_number": 1,
        "authorization_reason": (
            "OFFLINE_SEMANTIC_SCHOLAR_CANDIDATE_5XX_EXHAUSTION_RECOVERY"
        ),
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovered_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": source_checkpoint_reference,
        "source_attempt_manifest_hash": validated["attempt_manifest_hash"],
        "source_raw_responses": response_bindings,
        "retained_successful_page_count": (
            SEMANTIC_CANDIDATE_5XX_EXPECTED_SUCCESSFUL_PAGES
        ),
        "continuation_states": continuation_states,
        "control_gate": control,
        "network_used": False,
        "immutable": False,
    }
    source_state.update(
        {
            "status": SEMANTIC_CANDIDATE_5XX_RECOVERY_STATUS,
            "execution_episodes": [episode_1, episode_2],
            "active_episode_number": 2,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovered_checkpoint_reference,
            "completed_query_count": 3,
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "preserved_source_attempt_count": (
                SEMANTIC_CANDIDATE_5XX_EXPECTED_ATTEMPTS
            ),
            "preserved_source_response_count": len(response_bindings),
            "requests_this_session": 0,
            "candidate_request_count": 0,
            "pause_reason": (
                "OFFLINE_CANDIDATE_5XX_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
            ),
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    source_state.pop("pause_metadata", None)
    if {
        key: value
        for key, value in state["sources"].items()
        if key != "SemanticScholar"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar candidate recovery changed another source"
        )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def _semantic_native_id_overlap_recovery_active(
    source_state: Mapping[str, Any],
) -> bool:
    active_number = source_state.get("active_episode_number")
    return any(
        episode.get("episode_number") == active_number
        and episode.get("authorization_reason")
        == "OFFLINE_SEMANTIC_SCHOLAR_NATIVE_ID_OVERLAP_RECOVERY"
        for episode in source_state.get("execution_episodes", [])
    )


def _semantic_native_id_overlap_provenance(
    *,
    checkpoint_reference: Mapping[str, Any],
    validated: Mapping[str, Any],
) -> dict[str, Any]:
    provenance = {
        "schema_version": "1.0.0",
        "authorization_reason": (
            "OFFLINE_SEMANTIC_SCHOLAR_NATIVE_ID_OVERLAP_RECOVERY"
        ),
        "parent_episode_number": 2,
        "recovery_episode_number": 3,
        "parent_checkpoint": dict(checkpoint_reference),
        "adjudicated_native_id": SEMANTIC_NATIVE_ID_OVERLAP_PAPER_ID,
        "adjudicated_occurrences": [
            dict(item) for item in SEMANTIC_NATIVE_ID_OVERLAP_OCCURRENCES
        ],
        "canonical_grouping": dict(validated["canonical_grouping"]),
        "qf03_occurrence_count": validated["qf03_occurrence_count"],
        "qf03_distinct_native_id_count": validated[
            "qf03_distinct_native_id_count"
        ],
        "within_page_duplicate_count": 0,
        "cross_page_duplicate_native_ids": [
            SEMANTIC_NATIVE_ID_OVERLAP_PAPER_ID
        ],
        "saved_token_chain_hash": validated["saved_token_chain_hash"],
        "accepted_page_manifest_hash": validated[
            "accepted_page_manifest_hash"
        ],
        "continuation": dict(validated["continuation"]),
        "provider_ordering_policy": "paperId:asc",
        "provider_ordering_anomalies": [
            dict(item) for item in validated["provider_ordering_anomalies"]
        ],
        "cross_page_boundary_ordering_anomalies": [],
        "provider_total_history": list(validated["provider_total_history"]),
        "provider_totals_are_exact": False,
        "provider_completeness": "UNPROVEN",
        "pagination_completion_rule": (
            "continue until the provider omits the next token; termination does not "
            "prove stable-snapshot completeness"
        ),
        "raw_response_count": validated["raw_response_count"],
        "raw_response_manifest_hash": validated[
            "raw_response_manifest_hash"
        ],
        "exception_scope": (
            "exact validated historical occurrence pair only; every new overlap, "
            "including a third occurrence of the same paperId, remains terminal"
        ),
        "generic_duplicate_validation_changed": False,
        "network_used": False,
    }
    provenance["provenance_hash"] = _hash_payload(provenance)
    return provenance


def _semantic_native_id_overlap_adjudications(
    *, include_episode_3_pair: bool
) -> tuple[dict[str, Any], ...]:
    adjudications = [
        {
            "native_id": SEMANTIC_NATIVE_ID_OVERLAP_PAPER_ID,
            "occurrences": SEMANTIC_NATIVE_ID_OVERLAP_OCCURRENCES,
            "canonical_id": SEMANTIC_NATIVE_ID_OVERLAP_CANONICAL_ID,
            "dedupe_key": SEMANTIC_NATIVE_ID_OVERLAP_DEDUPE_KEY,
        }
    ]
    if include_episode_3_pair:
        adjudications.append(
            {
                "native_id": SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PAPER_ID,
                "occurrences": SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_OCCURRENCES,
                "canonical_id": SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_CANONICAL_ID,
                "dedupe_key": SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_DEDUPE_KEY,
            }
        )
    return tuple(adjudications)


def _validate_semantic_native_id_pair_and_canonicalization(
    dataset: Any,
    *,
    require_parent_counts: bool,
    include_episode_3_pair: bool = False,
) -> dict[str, Any]:
    qf03 = next(
        (
            query
            for query in dataset.source_queries
            if query.query_id == SEMANTIC_NATIVE_ID_OVERLAP_QUERY_ID
        ),
        None,
    )
    if qf03 is None:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery QF03 identity changed"
        )
    pages = sorted(
        (
            page
            for page in dataset.retrieval_pages
            if page.source_query_id == qf03.query_id
        ),
        key=lambda item: item.ordinal,
    )
    if not pages or [page.ordinal for page in pages] != list(range(len(pages))):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery page ordinals changed"
        )
    occurrences = [
        occurrence
        for occurrence in dataset.occurrences
        if occurrence.source_query_id == qf03.query_id
    ]
    occurrences_by_page = {
        page.page_id: [
            occurrence
            for occurrence in occurrences
            if occurrence.retrieval_page_id == page.page_id
        ]
        for page in pages
    }
    all_native_ids: list[str] = []
    for page in pages:
        page_occurrences = occurrences_by_page[page.page_id]
        if (
            len(page.native_identifiers) != len(set(page.native_identifiers))
            or page.occurrence_ids
            != [occurrence.occurrence_id for occurrence in page_occurrences]
            or page.native_identifiers
            != [occurrence.source_identifier for occurrence in page_occurrences]
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap recovery contains a within-page or "
                "occurrence-order identity mismatch"
            )
        all_native_ids.extend(page.native_identifiers)
    counts = Counter(all_native_ids)
    adjudications = _semantic_native_id_overlap_adjudications(
        include_episode_3_pair=include_episode_3_pair
    )
    duplicate_ids = {key: value for key, value in counts.items() if value > 1}
    expected_duplicate_ids = {
        adjudication["native_id"]: 2 for adjudication in adjudications
    }
    if duplicate_ids != expected_duplicate_ids:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery contains an unadjudicated native-ID "
            "overlap"
        )
    canonical_groupings = []
    for adjudication in adjudications:
        pair = sorted(
            (
                occurrence
                for occurrence in occurrences
                if occurrence.source_identifier == adjudication["native_id"]
            ),
            key=lambda item: item.page,
        )
        expected_pair = list(adjudication["occurrences"])
        if len(pair) != 2 or any(
            occurrence.page != expected["ordinal"]
            or occurrence.retrieval_page_id != expected["page_id"]
            or occurrence.occurrence_id != expected["occurrence_id"]
            or occurrence.raw_payload_hash != expected["raw_payload_hash"]
            for occurrence, expected in zip(pair, expected_pair, strict=True)
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap recovery occurrence pair changed"
            )
        pair_ids = [item["occurrence_id"] for item in expected_pair]
        canonical = next(
            (
                item
                for item in dataset.canonical_records
                if all(
                    occurrence_id in item.occurrence_ids
                    for occurrence_id in pair_ids
                )
            ),
            None,
        )
        if (
            canonical is None
            or canonical.canonical_id != adjudication["canonical_id"]
            or canonical.survivor_occurrence_id != pair_ids[0]
            or canonical.metadata.get("dedupe_key") != adjudication["dedupe_key"]
            or (require_parent_counts and canonical.occurrence_ids != pair_ids)
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap recovery canonical grouping changed"
            )
        decisions = {
            item.occurrence_id: item
            for item in dataset.duplicate_decisions
            if item.occurrence_id in pair_ids
        }
        if set(decisions) != set(pair_ids) or any(
            decisions[occurrence_id].canonical_record_id != canonical.canonical_id
            or decisions[occurrence_id].survivor_occurrence_id != pair_ids[0]
            or decisions[occurrence_id].match_key != adjudication["dedupe_key"]
            or decisions[occurrence_id].match_rule != "doi_first_title_fallback"
            or decisions[occurrence_id].outcome.value
            != ("unique" if index == 0 else "duplicate")
            for index, occurrence_id in enumerate(pair_ids)
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap recovery duplicate decisions changed"
            )
        canonical_groupings.append(
            {
                "native_id": adjudication["native_id"],
                "canonical_id": canonical.canonical_id,
                "dedupe_key": canonical.metadata["dedupe_key"],
                "survivor_occurrence_id": canonical.survivor_occurrence_id,
                "adjudicated_occurrence_ids": pair_ids,
            }
        )
    return {
        "query": qf03,
        "pages": pages,
        "occurrences": occurrences,
        "qf03_occurrence_count": len(occurrences),
        "qf03_distinct_native_id_count": len(counts),
        "canonical_grouping": {
            key: value
            for key, value in canonical_groupings[0].items()
            if key != "native_id"
        },
        "canonical_groupings": canonical_groupings,
    }


def _validate_semantic_overlap_response_inventory(
    *,
    root: Path,
    checkpoint_path: Path,
    dataset: Any,
    expected_attempt_count: int,
    error_prefix: str,
) -> dict[str, Any]:
    attempts_by_id = {
        attempt.attempt_id: attempt for attempt in dataset.retrieval_attempts
    }
    store = CheckpointStore(checkpoint_path.parent)
    raw_response_references: list[tuple[str, str]] = []
    raw_response_bindings: list[dict[str, Any]] = []
    for attempt in dataset.retrieval_attempts:
        if (
            attempt.raw_response_path is None
            or attempt.raw_response_hash is None
            or attempt.response_status is None
        ):
            raise ExternalRetrievalWaveError(
                f"{error_prefix} attempt lacks response evidence"
            )
        try:
            stored = store.load_response(
                attempt.raw_response_path, attempt.raw_response_hash
            )
        except (OSError, ValueError) as exc:
            raise ExternalRetrievalWaveError(
                f"{error_prefix} response hash/read failure"
            ) from exc
        if stored.status_code != attempt.response_status:
            raise ExternalRetrievalWaveError(
                f"{error_prefix} response status changed"
            )
        raw_response_references.append(
            (attempt.raw_response_path, attempt.raw_response_hash)
        )
        response_path = checkpoint_path.parent / attempt.raw_response_path
        raw_response_bindings.append(
            {
                "attempt_id": attempt.attempt_id,
                "page_id": attempt.page_id,
                "path": response_path.relative_to(root).as_posix(),
                "byte_size": response_path.stat().st_size,
                "raw_sha256": attempt.raw_response_hash,
                "http_status": stored.status_code,
            }
        )
    response_paths = [item[0] for item in raw_response_references]
    persisted_paths = {
        item.relative_to(checkpoint_path.parent).as_posix()
        for item in (checkpoint_path.parent / "responses").iterdir()
        if item.is_file()
    }
    if (
        len(response_paths) != expected_attempt_count
        or len(set(response_paths)) != len(response_paths)
        or set(response_paths) != persisted_paths
    ):
        raise ExternalRetrievalWaveError(
            f"{error_prefix} response inventory changed"
        )
    return {
        "attempts_by_id": attempts_by_id,
        "store": store,
        "raw_response_references": raw_response_references,
        "raw_response_bindings": raw_response_bindings,
    }


def _validate_semantic_qf03_page_chain(
    *,
    pages: list[Any],
    spec: RetrievalQuerySpec,
    attempts_by_id: Mapping[str, Any],
    store: CheckpointStore,
    expected_occurrences: tuple[Mapping[str, Any], ...],
    expected_ordering_anomalies: tuple[Mapping[str, Any], ...],
    error_prefix: str,
) -> dict[str, Any]:
    adapter = PAGINATED_SOURCE_ADAPTERS["SemanticScholar"]
    token_chain: list[dict[str, Any]] = []
    consumed_tokens: list[str] = []
    returned_tokens: list[str] = []
    provider_total_history: list[int | None] = []
    ordering_anomalies: list[dict[str, Any]] = []
    boundary_anomalies: list[dict[str, Any]] = []
    provider_records: dict[tuple[str, int], dict[str, Any]] = {}
    expected_by_ordinal: dict[int, list[Mapping[str, Any]]] = {}
    for expected in expected_occurrences:
        expected_by_ordinal.setdefault(int(expected["ordinal"]), []).append(expected)
    previous_page = None
    for page in pages:
        if page.status is not RetrievalCompletionStatus.COMPLETE:
            raise ExternalRetrievalWaveError(
                f"{error_prefix} accepted-page status changed"
            )
        if previous_page is None:
            if page.request_state != {"mode": "bulk"}:
                raise ExternalRetrievalWaveError(
                    f"{error_prefix} initial state changed"
                )
        elif page.request_state != previous_page.next_state:
            raise ExternalRetrievalWaveError(
                f"{error_prefix} token chain is discontinuous"
            )
        request = adapter.build_request(spec, page.request_state)
        page_attempts = [attempts_by_id[item] for item in page.attempt_ids]
        if not page_attempts or any(
            attempt.page_id != page.page_id
            or attempt.request_hash != request.request_hash()
            or attempt.request_method != request.method
            or attempt.request_url != request.url
            or attempt.request_params != request.sanitized_params()
            or attempt.request_headers != request.sanitized_headers()
            or attempt.request_params.get("sort") != "paperId:asc"
            for attempt in page_attempts
        ):
            raise ExternalRetrievalWaveError(
                f"{error_prefix} request/token binding changed"
            )
        successful = [
            attempt
            for attempt in page_attempts
            if attempt.status is RetrievalAttemptStatus.SUCCEEDED
        ]
        if len(successful) != 1 or successful[0].response_status != 200:
            raise ExternalRetrievalWaveError(
                f"{error_prefix} accepted response changed"
            )
        success = successful[0]
        payload = store.load_response(
            success.raw_response_path, success.raw_response_hash
        ).json()
        data = payload.get("data")
        if not isinstance(data, list):
            raise ExternalRetrievalWaveError(
                f"{error_prefix} response data changed"
            )
        provider_ids = [item.get("paperId") for item in data]
        response_token = payload.get("token")
        expected_next_state = (
            {"mode": "bulk", "token": response_token}
            if response_token is not None
            else None
        )
        if (
            provider_ids != page.native_identifiers
            or len(data) != page.returned_item_count
            or payload.get("total") != page.source_reported_total
            or page.total_is_exact
            or page.next_state != expected_next_state
        ):
            raise ExternalRetrievalWaveError(
                f"{error_prefix} response/page binding changed"
            )
        if page.request_state.get("token") is not None:
            consumed_tokens.append(page.request_state["token"])
        if response_token is not None:
            returned_tokens.append(response_token)
        provider_total_history.append(page.source_reported_total)
        token_chain.append(
            {
                "ordinal": page.ordinal,
                "page_id": page.page_id,
                "request_state": dict(page.request_state),
                "next_state": dict(page.next_state) if page.next_state else None,
                "request_hash": success.request_hash,
                "response_sha256": success.raw_response_hash,
            }
        )
        for index in range(1, len(page.native_identifiers)):
            left = page.native_identifiers[index - 1]
            right = page.native_identifiers[index]
            if left > right:
                ordering_anomalies.append(
                    {
                        "page_ordinal": page.ordinal,
                        "left_index": index - 1,
                        "left_paper_id": left,
                        "right_paper_id": right,
                    }
                )
        if (
            previous_page is not None
            and previous_page.native_identifiers
            and page.native_identifiers
            and previous_page.native_identifiers[-1] > page.native_identifiers[0]
        ):
            boundary_anomalies.append(
                {
                    "left_page_ordinal": previous_page.ordinal,
                    "right_page_ordinal": page.ordinal,
                }
            )
        for expected in expected_by_ordinal.get(page.ordinal, []):
            if (
                success.raw_response_path != expected["response_path"]
                or success.raw_response_hash != expected["response_sha256"]
                or page.source_reported_total != expected["provider_total"]
            ):
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar overlap pair response binding changed"
                )
            matches = [
                item
                for item in data
                if item.get("paperId") == expected["native_id"]
            ]
            if len(matches) != 1:
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar overlap paper is absent from its raw response"
                )
            provider_records[(expected["native_id"], page.ordinal)] = matches[0]
        previous_page = page
    if (
        len(consumed_tokens) != len(pages) - 1
        or len(set(consumed_tokens)) != len(consumed_tokens)
        or len(returned_tokens) != len(pages)
        or len(set(returned_tokens)) != len(returned_tokens)
        or ordering_anomalies != [dict(item) for item in expected_ordering_anomalies]
        or boundary_anomalies
    ):
        raise ExternalRetrievalWaveError(
            f"{error_prefix} continuation/order evidence changed"
        )
    return {
        "provider_records": provider_records,
        "provider_total_history": provider_total_history,
        "provider_ordering_anomalies": ordering_anomalies,
        "token_chain": token_chain,
        "saved_token_chain_hash": _hash_payload({"pages": token_chain}),
        "accepted_page_manifest_hash": _semantic_accepted_page_manifest_hash(pages),
        "continuation": {
            "next_page_ordinal": pages[-1].ordinal + 1,
            "predecessor_page_id": pages[-1].page_id,
            "request_state": dict(pages[-1].next_state),
        },
    }


def _semantic_accepted_page_manifest_hash(pages: list[Any]) -> str:
    return _hash_payload(
        {
            "pages": [
                {
                    "ordinal": page.ordinal,
                    "page_id": page.page_id,
                    "request_state": page.request_state,
                    "next_state": page.next_state,
                    "returned_item_count": page.returned_item_count,
                    "occurrence_ids": page.occurrence_ids,
                    "native_identifiers": page.native_identifiers,
                    "source_reported_total": page.source_reported_total,
                    "total_is_exact": page.total_is_exact,
                }
                for page in pages
            ]
        }
    )


def _validate_semantic_native_id_overlap_parent_checkpoint(
    *,
    root: Path,
    wave: ProductionRetrievalWave,
    checkpoint_reference: Mapping[str, Any],
) -> dict[str, Any]:
    if (
        checkpoint_reference.get("raw_sha256")
        != SEMANTIC_NATIVE_ID_OVERLAP_PARENT_CHECKPOINT_SHA256
        or checkpoint_reference.get("byte_size")
        != SEMANTIC_NATIVE_ID_OVERLAP_PARENT_CHECKPOINT_SIZE
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery requires the exact episode-2 parent "
            "checkpoint"
        )
    checkpoint_path = _safe_output_path(root, checkpoint_reference["path"])
    _verify_file_reference(checkpoint_path, checkpoint_reference, root)
    try:
        dataset = load_review_dataset(checkpoint_path)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap parent checkpoint is invalid"
        ) from exc
    dataset.validate()
    if (
        len(dataset.retrieval_runs) != 1
        or len(dataset.source_queries) != 5
        or len(dataset.retrieval_attempts)
        != SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_ATTEMPTS
        or len(dataset.retrieval_pages) != SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_PAGES
        or len(dataset.occurrences)
        != SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_OCCURRENCES
        or len(dataset.canonical_records)
        != SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_CANONICAL_RECORDS
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap parent counts changed"
        )
    run = dataset.retrieval_runs[0]
    specs = _source_query_specs(wave, "SemanticScholar", ieee_credential="")
    if (
        run.run_id != f"{WAVE_ID}:SemanticScholar"
        or run.query_plan_hash != _query_plan_hash(specs)
        or run.query_plan_version != wave.query_plan_hash
        or run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.errors
        != [f"{SEMANTIC_NATIVE_ID_OVERLAP_QUERY_ID}: {SEMANTIC_NATIVE_ID_OVERLAP_ERROR}"]
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap parent run binding changed"
        )
    for query, spec, expected in zip(
        dataset.source_queries,
        specs,
        SEMANTIC_NATIVE_ID_OVERLAP_QF_COUNTS,
        strict=True,
    ):
        expected_status, expected_pages, expected_occurrences = expected
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        occurrences = [
            item
            for item in dataset.occurrences
            if item.source_query_id == query.query_id
        ]
        if (
            query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.endpoint != spec.endpoint
            or query.fields != spec.fields
            or query.completion_status.value != expected_status
            or query.result_count != expected_occurrences
            or len(pages) != expected_pages
            or len(occurrences) != expected_occurrences
            or query.page_ids != [page.page_id for page in pages]
            or [page.ordinal for page in pages] != list(range(expected_pages))
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap parent query evidence changed"
            )
    pair_evidence = _validate_semantic_native_id_pair_and_canonicalization(
        dataset, require_parent_counts=True
    )
    if (
        pair_evidence["qf03_occurrence_count"]
        != SEMANTIC_NATIVE_ID_OVERLAP_QF_COUNTS[2][2]
        or pair_evidence["qf03_distinct_native_id_count"]
        != SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_QF03_DISTINCT_IDS
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap parent QF03 identity counts changed"
        )
    qf03 = pair_evidence["query"]
    pages = pair_evidence["pages"]
    if (
        qf03.status is not ProcessingStatus.FAILED
        or qf03.errors != [SEMANTIC_NATIVE_ID_OVERLAP_ERROR]
        or pages[-1].status is not RetrievalCompletionStatus.COMPLETE
        or pages[-1].metadata.get("completion_error")
        != SEMANTIC_NATIVE_ID_OVERLAP_ERROR
        or pages[-1].ordinal != 42
        or pages[-1].next_state is None
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap parent failure position changed"
        )

    adapter = PAGINATED_SOURCE_ADAPTERS["SemanticScholar"]
    spec = specs[2]
    attempts_by_id = {
        attempt.attempt_id: attempt for attempt in dataset.retrieval_attempts
    }
    store = CheckpointStore(checkpoint_path.parent)
    raw_response_references: list[tuple[str, str]] = []
    raw_response_bindings: list[dict[str, Any]] = []
    for attempt in dataset.retrieval_attempts:
        if (
            attempt.raw_response_path is None
            or attempt.raw_response_hash is None
            or attempt.response_status is None
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap parent attempt lacks response evidence"
            )
        try:
            stored = store.load_response(
                attempt.raw_response_path, attempt.raw_response_hash
            )
        except (OSError, ValueError) as exc:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap parent response hash/read failure"
            ) from exc
        if stored.status_code != attempt.response_status:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap parent response status changed"
            )
        raw_response_references.append(
            (attempt.raw_response_path, attempt.raw_response_hash)
        )
        response_path = checkpoint_path.parent / attempt.raw_response_path
        raw_response_bindings.append(
            {
                "attempt_id": attempt.attempt_id,
                "page_id": attempt.page_id,
                "path": response_path.relative_to(root).as_posix(),
                "byte_size": response_path.stat().st_size,
                "raw_sha256": attempt.raw_response_hash,
                "http_status": stored.status_code,
            }
        )
    response_paths = [item[0] for item in raw_response_references]
    persisted_paths = {
        item.relative_to(checkpoint_path.parent).as_posix()
        for item in (checkpoint_path.parent / "responses").iterdir()
        if item.is_file()
    }
    if (
        len(response_paths) != SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_ATTEMPTS
        or len(set(response_paths)) != len(response_paths)
        or set(response_paths) != persisted_paths
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap parent response inventory changed"
        )

    token_chain: list[dict[str, Any]] = []
    consumed_tokens: list[str] = []
    returned_tokens: list[str] = []
    provider_total_history: list[int | None] = []
    ordering_anomalies: list[dict[str, Any]] = []
    boundary_anomalies: list[dict[str, Any]] = []
    provider_records: dict[int, dict[str, Any]] = {}
    previous_page = None
    expected_pair_by_ordinal = {
        item["ordinal"]: item for item in SEMANTIC_NATIVE_ID_OVERLAP_OCCURRENCES
    }
    for page in pages:
        if page.status is not RetrievalCompletionStatus.COMPLETE:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap parent accepted-page status changed"
            )
        if previous_page is None:
            if page.request_state != {"mode": "bulk"}:
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar overlap parent initial state changed"
                )
        elif page.request_state != previous_page.next_state:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap parent token chain is discontinuous"
            )
        request = adapter.build_request(spec, page.request_state)
        page_attempts = [attempts_by_id[item] for item in page.attempt_ids]
        if not page_attempts or any(
            attempt.page_id != page.page_id
            or attempt.request_hash != request.request_hash()
            or attempt.request_method != request.method
            or attempt.request_url != request.url
            or attempt.request_params != request.sanitized_params()
            or attempt.request_headers != request.sanitized_headers()
            or attempt.request_params.get("sort") != "paperId:asc"
            for attempt in page_attempts
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap parent request/token binding changed"
            )
        successful = [
            attempt
            for attempt in page_attempts
            if attempt.status is RetrievalAttemptStatus.SUCCEEDED
        ]
        if len(successful) != 1 or successful[0].response_status != 200:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap parent accepted response changed"
            )
        success = successful[0]
        payload = store.load_response(
            success.raw_response_path, success.raw_response_hash
        ).json()
        data = payload.get("data")
        if not isinstance(data, list):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap parent response data changed"
            )
        provider_ids = [item.get("paperId") for item in data]
        response_token = payload.get("token")
        expected_next_state = (
            {"mode": "bulk", "token": response_token}
            if response_token is not None
            else None
        )
        if (
            provider_ids != page.native_identifiers
            or len(data) != page.returned_item_count
            or payload.get("total") != page.source_reported_total
            or page.total_is_exact
            or page.next_state != expected_next_state
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap parent response/page binding changed"
            )
        if page.request_state.get("token") is not None:
            consumed_tokens.append(page.request_state["token"])
        if response_token is not None:
            returned_tokens.append(response_token)
        provider_total_history.append(page.source_reported_total)
        token_chain.append(
            {
                "ordinal": page.ordinal,
                "page_id": page.page_id,
                "request_state": dict(page.request_state),
                "next_state": dict(page.next_state) if page.next_state else None,
                "request_hash": success.request_hash,
                "response_sha256": success.raw_response_hash,
            }
        )
        for index in range(1, len(page.native_identifiers)):
            left = page.native_identifiers[index - 1]
            right = page.native_identifiers[index]
            if left > right:
                ordering_anomalies.append(
                    {
                        "page_ordinal": page.ordinal,
                        "left_index": index - 1,
                        "left_paper_id": left,
                        "right_paper_id": right,
                    }
                )
        if (
            previous_page is not None
            and previous_page.native_identifiers
            and page.native_identifiers
            and previous_page.native_identifiers[-1] > page.native_identifiers[0]
        ):
            boundary_anomalies.append(
                {
                    "left_page_ordinal": previous_page.ordinal,
                    "right_page_ordinal": page.ordinal,
                }
            )
        expected_pair = expected_pair_by_ordinal.get(page.ordinal)
        if expected_pair is not None:
            if (
                success.raw_response_path != expected_pair["response_path"]
                or success.raw_response_hash != expected_pair["response_sha256"]
                or page.source_reported_total != expected_pair["provider_total"]
            ):
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar overlap pair response binding changed"
                )
            matches = [
                item
                for item in data
                if item.get("paperId") == SEMANTIC_NATIVE_ID_OVERLAP_PAPER_ID
            ]
            if len(matches) != 1:
                raise ExternalRetrievalWaveError(
                    "Semantic Scholar overlap paper is absent from its raw response"
                )
            provider_records[page.ordinal] = matches[0]
        previous_page = page
    if (
        len(consumed_tokens) != len(pages) - 1
        or len(set(consumed_tokens)) != len(consumed_tokens)
        or len(returned_tokens) != len(pages)
        or len(set(returned_tokens)) != len(returned_tokens)
        or ordering_anomalies
        != [dict(item) for item in SEMANTIC_NATIVE_ID_OVERLAP_ORDERING_ANOMALIES]
        or boundary_anomalies
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap parent continuation/order evidence changed"
        )
    page_2_record = json.loads(json.dumps(provider_records[2], sort_keys=True))
    page_42_record = json.loads(json.dumps(provider_records[42], sort_keys=True))
    if (
        len(page_2_record.get("authors", [])) != 3
        or len(page_42_record.get("authors", [])) != 3
        or page_2_record["authors"][2].get("name")
        != SEMANTIC_NATIVE_ID_OVERLAP_OCCURRENCES[0]["author_name"]
        or page_42_record["authors"][2].get("name")
        != SEMANTIC_NATIVE_ID_OVERLAP_OCCURRENCES[1]["author_name"]
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap bibliographic difference changed"
        )
    page_42_record["authors"][2]["name"] = page_2_record["authors"][2]["name"]
    if page_2_record != page_42_record:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap records differ beyond the adjudicated author name"
        )
    return {
        "qf03_occurrence_count": pair_evidence["qf03_occurrence_count"],
        "qf03_distinct_native_id_count": pair_evidence[
            "qf03_distinct_native_id_count"
        ],
        "canonical_grouping": pair_evidence["canonical_grouping"],
        "baseline_page_ids": [page.page_id for page in pages],
        "page_42_next_state": dict(pages[42].next_state),
        "raw_response_references": raw_response_references,
        "raw_response_bindings": raw_response_bindings,
        "raw_response_count": len(raw_response_references),
        "raw_response_manifest_hash": _hash_payload(
            {"responses": raw_response_bindings}
        ),
        "saved_token_chain_hash": _hash_payload({"pages": token_chain}),
        "accepted_page_manifest_hash": _hash_payload(
            {
                "pages": [
                    {
                        "ordinal": page.ordinal,
                        "page_id": page.page_id,
                        "request_state": page.request_state,
                        "next_state": page.next_state,
                        "returned_item_count": page.returned_item_count,
                        "occurrence_ids": page.occurrence_ids,
                        "native_identifiers": page.native_identifiers,
                        "source_reported_total": page.source_reported_total,
                        "total_is_exact": page.total_is_exact,
                    }
                    for page in pages
                ]
            }
        ),
        "continuation": {
            "next_page_ordinal": 43,
            "predecessor_page_id": pages[-1].page_id,
            "request_state": dict(pages[-1].next_state),
        },
        "provider_ordering_anomalies": ordering_anomalies,
        "provider_total_history": provider_total_history,
    }


def _semantic_native_id_overlap_episode_4_provenance(
    *,
    checkpoint_reference: Mapping[str, Any],
    validated: Mapping[str, Any],
    prior_provenance: Mapping[str, Any],
) -> dict[str, Any]:
    groupings = {
        item["native_id"]: {
            key: value for key, value in item.items() if key != "native_id"
        }
        for item in validated["canonical_groupings"]
    }
    adjudications = []
    for adjudication in _semantic_native_id_overlap_adjudications(
        include_episode_3_pair=True
    ):
        adjudications.append(
            {
                "native_id": adjudication["native_id"],
                "occurrences": [dict(item) for item in adjudication["occurrences"]],
                "canonical_grouping": groupings[adjudication["native_id"]],
            }
        )
    provenance = {
        "schema_version": "1.1.0",
        "authorization_reason": (
            "OFFLINE_SEMANTIC_SCHOLAR_NATIVE_ID_OVERLAP_RECOVERY"
        ),
        "parent_episode_number": 3,
        "recovery_episode_number": 4,
        "parent_checkpoint": dict(checkpoint_reference),
        "prior_adjudication_provenance_hash": prior_provenance["provenance_hash"],
        "adjudicated_native_ids": [item["native_id"] for item in adjudications],
        "adjudicated_occurrences": [
            occurrence
            for adjudication in adjudications
            for occurrence in adjudication["occurrences"]
        ],
        "adjudications": adjudications,
        "qf03_occurrence_count": validated["qf03_occurrence_count"],
        "qf03_distinct_native_id_count": validated[
            "qf03_distinct_native_id_count"
        ],
        "within_page_duplicate_count": 0,
        "cross_page_duplicate_native_ids": [
            SEMANTIC_NATIVE_ID_OVERLAP_PAPER_ID,
            SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PAPER_ID,
        ],
        "saved_token_chain_hash": validated["saved_token_chain_hash"],
        "accepted_page_manifest_hash": validated[
            "accepted_page_manifest_hash"
        ],
        "continuation": dict(validated["continuation"]),
        "provider_ordering_policy": "paperId:asc",
        "provider_ordering_anomalies": [
            dict(item) for item in validated["provider_ordering_anomalies"]
        ],
        "cross_page_boundary_ordering_anomalies": [],
        "provider_total_history": list(validated["provider_total_history"]),
        "provider_totals_are_exact": False,
        "provider_completeness": "UNPROVEN",
        "pagination_completion_rule": (
            "continue until the provider omits the next token; termination does not "
            "prove stable-snapshot completeness"
        ),
        "raw_response_count": validated["raw_response_count"],
        "raw_response_manifest_hash": validated[
            "raw_response_manifest_hash"
        ],
        "exception_scope": (
            "the two exact validated historical occurrence pairs only; every new "
            "overlap, including a third occurrence of either paperId, remains terminal"
        ),
        "generic_duplicate_validation_changed": False,
        "network_used": False,
    }
    provenance["provenance_hash"] = _hash_payload(provenance)
    return provenance


def _validate_semantic_native_id_overlap_episode_3_parent_checkpoint(
    *,
    root: Path,
    wave: ProductionRetrievalWave,
    episodes: list[Mapping[str, Any]],
) -> dict[str, Any]:
    if len(episodes) < 3:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 recovery parent lineage changed"
        )
    episode_2 = episodes[1]
    episode_3 = episodes[2]
    if (
        episode_2.get("episode_number") != 2
        or episode_2.get("status") != "FAILED"
        or episode_2.get("immutable") is not True
        or episode_3.get("episode_number") != 3
        or episode_3.get("status") != "FAILED"
        or episode_3.get("immutable") is not True
        or episode_3.get("recovery_of_episode_number") != 2
        or episode_3.get("authorization_reason")
        != "OFFLINE_SEMANTIC_SCHOLAR_NATIVE_ID_OVERLAP_RECOVERY"
        or episode_3.get("network_used") is not False
        or episode_3.get("parent_checkpoint_dataset")
        != episode_2.get("checkpoint_dataset")
        or episode_3.get("frozen_wave_manifest_hash") != wave.manifest_hash()
        or episode_3.get("frozen_query_plan_hash") != wave.query_plan_hash
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 recovery parent lineage changed"
        )
    episode_2_validated = _validate_semantic_native_id_overlap_parent_checkpoint(
        root=root,
        wave=wave,
        checkpoint_reference=episode_2.get("checkpoint_dataset", {}),
    )
    prior_provenance = _semantic_native_id_overlap_provenance(
        checkpoint_reference=episode_2["checkpoint_dataset"],
        validated=episode_2_validated,
    )
    if episode_3.get("adjudication_provenance") != prior_provenance:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-3 adjudication provenance changed"
        )
    _verify_recovery_raw_bindings(
        root, episode_3.get("source_raw_responses", []), "episode_2_path"
    )
    checkpoint_reference = episode_3.get("checkpoint_dataset", {})
    if (
        checkpoint_reference.get("raw_sha256")
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PARENT_CHECKPOINT_SHA256
        or checkpoint_reference.get("byte_size")
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PARENT_CHECKPOINT_SIZE
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 recovery requires the exact episode-3 "
            "parent checkpoint"
        )
    checkpoint_path = _safe_output_path(root, checkpoint_reference["path"])
    _verify_file_reference(checkpoint_path, checkpoint_reference, root)
    dataset = load_review_dataset(checkpoint_path)
    dataset.validate()
    if (
        len(dataset.retrieval_runs) != 1
        or len(dataset.source_queries) != 5
        or len(dataset.retrieval_attempts)
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_ATTEMPTS
        or len(dataset.retrieval_pages)
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_PAGES
        or len(dataset.occurrences)
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_OCCURRENCES
        or len(dataset.canonical_records)
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_CANONICAL_RECORDS
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-3 overlap parent counts changed"
        )
    run = dataset.retrieval_runs[0]
    specs = _source_query_specs(wave, "SemanticScholar", ieee_credential="")
    expected_run_error = (
        f"{SEMANTIC_NATIVE_ID_OVERLAP_QUERY_ID}: "
        f"{SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_ERROR}"
    )
    if (
        run.run_id != f"{WAVE_ID}:SemanticScholar"
        or run.query_plan_hash != _query_plan_hash(specs)
        or run.query_plan_version != wave.query_plan_hash
        or run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.errors != [expected_run_error]
        or run.metadata.get("offline_semantic_native_id_overlap_recovery")
        != prior_provenance
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-3 overlap parent run binding changed"
        )
    for query, spec, expected in zip(
        dataset.source_queries,
        specs,
        SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_QF_COUNTS,
        strict=True,
    ):
        expected_status, expected_pages, expected_occurrences = expected
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        occurrences = [
            item
            for item in dataset.occurrences
            if item.source_query_id == query.query_id
        ]
        if (
            query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.endpoint != spec.endpoint
            or query.fields != spec.fields
            or query.completion_status.value != expected_status
            or query.result_count != expected_occurrences
            or len(pages) != expected_pages
            or len(occurrences) != expected_occurrences
            or query.page_ids != [page.page_id for page in pages]
            or [page.ordinal for page in pages] != list(range(expected_pages))
        ):
            raise ExternalRetrievalWaveError(
                "Semantic Scholar episode-3 overlap parent query evidence changed"
            )
    pair_evidence = _validate_semantic_native_id_pair_and_canonicalization(
        dataset,
        require_parent_counts=True,
        include_episode_3_pair=True,
    )
    if (
        pair_evidence["qf03_occurrence_count"]
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_QF_COUNTS[2][2]
        or pair_evidence["qf03_distinct_native_id_count"]
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_QF03_DISTINCT_IDS
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-3 overlap parent identity counts changed"
        )
    qf03 = pair_evidence["query"]
    pages = pair_evidence["pages"]
    if (
        qf03.status is not ProcessingStatus.FAILED
        or qf03.errors != [SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_ERROR]
        or pages[-1].ordinal != 54
        or pages[-1].status is not RetrievalCompletionStatus.COMPLETE
        or pages[-1].metadata.get("completion_error")
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_ERROR
        or pages[-1].next_state is None
        or pages[42].metadata.get("completion_error") is not None
        or pages[43].request_state != episode_2_validated["page_42_next_state"]
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-3 overlap parent failure position changed"
        )
    baseline_attempts = {
        attempt.attempt_id: attempt for attempt in dataset.retrieval_attempts
    }
    baseline_token_chain = []
    for page in pages[:43]:
        successful = [
            baseline_attempts[attempt_id]
            for attempt_id in page.attempt_ids
            if baseline_attempts[attempt_id].status
            is RetrievalAttemptStatus.SUCCEEDED
        ]
        if len(successful) != 1:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar episode-3 baseline attempt history changed"
            )
        baseline_token_chain.append(
            {
                "ordinal": page.ordinal,
                "page_id": page.page_id,
                "request_state": dict(page.request_state),
                "next_state": dict(page.next_state) if page.next_state else None,
                "request_hash": successful[0].request_hash,
                "response_sha256": successful[0].raw_response_hash,
            }
        )
    if (
        [page.page_id for page in pages[:43]]
        != episode_2_validated["baseline_page_ids"]
        or _hash_payload({"pages": baseline_token_chain})
        != episode_2_validated["saved_token_chain_hash"]
        or _semantic_accepted_page_manifest_hash(pages[:43])
        != episode_2_validated["accepted_page_manifest_hash"]
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-3 baseline evidence changed"
        )
    inventory = _validate_semantic_overlap_response_inventory(
        root=root,
        checkpoint_path=checkpoint_path,
        dataset=dataset,
        expected_attempt_count=SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_ATTEMPTS,
        error_prefix="Semantic Scholar episode-3 overlap parent",
    )
    expected_occurrence_bindings = tuple(
        {**dict(item), "native_id": adjudication["native_id"]}
        for adjudication in _semantic_native_id_overlap_adjudications(
            include_episode_3_pair=True
        )
        for item in adjudication["occurrences"]
    )
    chain = _validate_semantic_qf03_page_chain(
        pages=pages,
        spec=specs[2],
        attempts_by_id=inventory["attempts_by_id"],
        store=inventory["store"],
        expected_occurrences=expected_occurrence_bindings,
        expected_ordering_anomalies=SEMANTIC_NATIVE_ID_OVERLAP_ORDERING_ANOMALIES,
        error_prefix="Semantic Scholar episode-3 overlap parent",
    )
    second_records = chain["provider_records"]
    page_3_record = json.loads(
        json.dumps(
            second_records[(SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PAPER_ID, 3)],
            sort_keys=True,
        )
    )
    page_54_record = json.loads(
        json.dumps(
            second_records[(SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PAPER_ID, 54)],
            sort_keys=True,
        )
    )
    if page_3_record != page_54_record:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-3 overlap records changed"
        )
    raw_response_bindings = inventory["raw_response_bindings"]
    return {
        "qf03_occurrence_count": pair_evidence["qf03_occurrence_count"],
        "qf03_distinct_native_id_count": pair_evidence[
            "qf03_distinct_native_id_count"
        ],
        "canonical_groupings": pair_evidence["canonical_groupings"],
        "baseline_page_ids": [page.page_id for page in pages],
        "page_54_next_state": dict(pages[54].next_state),
        "raw_response_references": inventory["raw_response_references"],
        "raw_response_bindings": raw_response_bindings,
        "raw_response_count": len(raw_response_bindings),
        "raw_response_manifest_hash": _hash_payload(
            {"responses": raw_response_bindings}
        ),
        "saved_token_chain_hash": chain["saved_token_chain_hash"],
        "accepted_page_manifest_hash": chain["accepted_page_manifest_hash"],
        "continuation": chain["continuation"],
        "provider_ordering_anomalies": chain[
            "provider_ordering_anomalies"
        ],
        "provider_total_history": chain["provider_total_history"],
        "prior_provenance": prior_provenance,
    }


def _validate_authorized_semantic_native_id_overlap_recovery(
    *,
    root: Path,
    source_state: Mapping[str, Any],
    wave: ProductionRetrievalWave,
) -> None:
    if source_state.get("active_episode_number") == 4:
        _validate_authorized_semantic_native_id_overlap_episode_4(
            root=root, source_state=source_state, wave=wave
        )
        return
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 3:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery episode lineage changed"
        )
    parent = episodes[1]
    active = episodes[2]
    if (
        parent.get("episode_number") != 2
        or parent.get("status") != "FAILED"
        or parent.get("immutable") is not True
        or active.get("episode_number") != 3
        or active.get("recovery_of_episode_number") != 2
        or active.get("authorization_reason")
        != "OFFLINE_SEMANTIC_SCHOLAR_NATIVE_ID_OVERLAP_RECOVERY"
        or active.get("network_used") is not False
        or active.get("parent_checkpoint_dataset")
        != parent.get("checkpoint_dataset")
        or active.get("frozen_wave_manifest_hash") != wave.manifest_hash()
        or active.get("frozen_query_plan_hash") != wave.query_plan_hash
        or active.get("checkpoint_path")
        != f"{EXECUTION_ROOT}/SemanticScholar/episodes/episode-003/checkpoint"
        or source_state.get("active_episode_number") != 3
        or source_state.get("active_run_id") != active.get("run_id")
        or source_state.get("active_checkpoint_path")
        != active.get("checkpoint_path")
        or source_state.get("checkpoint_dataset")
        != active.get("checkpoint_dataset")
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery active lineage changed"
        )
    validated = _validate_semantic_native_id_overlap_parent_checkpoint(
        root=root,
        wave=wave,
        checkpoint_reference=parent.get("checkpoint_dataset", {}),
    )
    expected_provenance = _semantic_native_id_overlap_provenance(
        checkpoint_reference=parent["checkpoint_dataset"], validated=validated
    )
    if active.get("adjudication_provenance") != expected_provenance:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery adjudication provenance changed"
        )
    bindings = active.get("source_raw_responses", [])
    if len(bindings) != validated["raw_response_count"]:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery response manifest changed"
        )
    _verify_recovery_raw_bindings(root, bindings, "episode_2_path")
    active_checkpoint = _safe_output_path(root, active["checkpoint_dataset"]["path"])
    _verify_file_reference(active_checkpoint, active["checkpoint_dataset"], root)
    dataset = load_review_dataset(active_checkpoint)
    dataset.validate()
    run = dataset.retrieval_runs[0]
    if run.metadata.get("offline_semantic_native_id_overlap_recovery") != (
        expected_provenance
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery checkpoint provenance changed"
        )
    if (
        run.run_id != active.get("run_id")
        or run.query_plan_hash
        != _query_plan_hash(
            _source_query_specs(wave, "SemanticScholar", ieee_credential="")
        )
        or run.query_plan_version != wave.query_plan_hash
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery run/query-plan binding changed"
        )
    child = _validate_semantic_native_id_pair_and_canonicalization(
        dataset, require_parent_counts=False
    )
    pages = child["pages"]
    attempts_by_id = {
        attempt.attempt_id: attempt for attempt in dataset.retrieval_attempts
    }
    baseline_token_chain = []
    for page in pages[:43]:
        successful = [
            attempts_by_id[attempt_id]
            for attempt_id in page.attempt_ids
            if attempts_by_id[attempt_id].status
            is RetrievalAttemptStatus.SUCCEEDED
        ]
        if len(successful) != 1:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar overlap recovery baseline attempt history changed"
            )
        success = successful[0]
        baseline_token_chain.append(
            {
                "ordinal": page.ordinal,
                "page_id": page.page_id,
                "request_state": dict(page.request_state),
                "next_state": dict(page.next_state) if page.next_state else None,
                "request_hash": success.request_hash,
                "response_sha256": success.raw_response_hash,
            }
        )
    accepted_page_manifest_hash = _hash_payload(
        {
            "pages": [
                {
                    "ordinal": page.ordinal,
                    "page_id": page.page_id,
                    "request_state": page.request_state,
                    "next_state": page.next_state,
                    "returned_item_count": page.returned_item_count,
                    "occurrence_ids": page.occurrence_ids,
                    "native_identifiers": page.native_identifiers,
                    "source_reported_total": page.source_reported_total,
                    "total_is_exact": page.total_is_exact,
                }
                for page in pages[:43]
            ]
        }
    )
    if (
        len(pages) < 43
        or [page.page_id for page in pages[:43]]
        != validated["baseline_page_ids"]
        or _hash_payload({"pages": baseline_token_chain})
        != validated["saved_token_chain_hash"]
        or accepted_page_manifest_hash
        != validated["accepted_page_manifest_hash"]
        or pages[42].metadata.get("completion_error") is not None
        or pages[42].next_state != validated["page_42_next_state"]
        or (
            len(pages) > 43
            and pages[43].request_state != validated["page_42_next_state"]
        )
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery continuation evidence changed"
        )
    completed_indexes = [0, 1, 3, 4]
    if any(
        dataset.source_queries[index].completion_status
        is not RetrievalCompletionStatus.COMPLETE
        or dataset.source_queries[index].result_count
        != SEMANTIC_NATIVE_ID_OVERLAP_QF_COUNTS[index][2]
        for index in completed_indexes
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery changed a completed query"
        )
    if (
        source_state.get("occurrence_count") != len(dataset.occurrences)
        or source_state.get("attempt_count") != len(dataset.retrieval_attempts)
        or source_state.get("completed_query_count")
        != sum(
            query.completion_status is RetrievalCompletionStatus.COMPLETE
            for query in dataset.source_queries
        )
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery state/checkpoint counts disagree"
        )


def _validate_authorized_semantic_native_id_overlap_episode_4(
    *,
    root: Path,
    source_state: Mapping[str, Any],
    wave: ProductionRetrievalWave,
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 4:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery lineage changed"
        )
    active = episodes[3]
    if (
        active.get("episode_number") != 4
        or active.get("recovery_of_episode_number") != 3
        or active.get("authorization_reason")
        != "OFFLINE_SEMANTIC_SCHOLAR_NATIVE_ID_OVERLAP_RECOVERY"
        or active.get("network_used") is not False
        or active.get("parent_checkpoint_dataset")
        != episodes[2].get("checkpoint_dataset")
        or active.get("frozen_wave_manifest_hash") != wave.manifest_hash()
        or active.get("frozen_query_plan_hash") != wave.query_plan_hash
        or active.get("checkpoint_path")
        != f"{EXECUTION_ROOT}/SemanticScholar/episodes/episode-004/checkpoint"
        or source_state.get("active_episode_number") != 4
        or source_state.get("active_run_id") != active.get("run_id")
        or source_state.get("active_checkpoint_path")
        != active.get("checkpoint_path")
        or source_state.get("checkpoint_dataset")
        != active.get("checkpoint_dataset")
        or active.get("status") != source_state.get("status")
        or active.get("immutable")
        is not (source_state.get("status") in {"COMPLETE", "FAILED"})
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery active lineage changed"
        )
    validated = _validate_semantic_native_id_overlap_episode_3_parent_checkpoint(
        root=root,
        wave=wave,
        episodes=episodes,
    )
    expected_provenance = _semantic_native_id_overlap_episode_4_provenance(
        checkpoint_reference=episodes[2]["checkpoint_dataset"],
        validated=validated,
        prior_provenance=validated["prior_provenance"],
    )
    if active.get("adjudication_provenance") != expected_provenance:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery provenance changed"
        )
    bindings = active.get("source_raw_responses", [])
    if len(bindings) != validated["raw_response_count"]:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery response manifest changed"
        )
    _verify_recovery_raw_bindings(root, bindings, "episode_3_path")
    active_checkpoint = _safe_output_path(root, active["checkpoint_dataset"]["path"])
    _verify_file_reference(active_checkpoint, active["checkpoint_dataset"], root)
    dataset = load_review_dataset(active_checkpoint)
    dataset.validate()
    run = dataset.retrieval_runs[0]
    if (
        run.metadata.get("offline_semantic_native_id_overlap_recovery_episode_4")
        != expected_provenance
        or run.run_id != active.get("run_id")
        or run.query_plan_hash
        != _query_plan_hash(
            _source_query_specs(wave, "SemanticScholar", ieee_credential="")
        )
        or run.query_plan_version != wave.query_plan_hash
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery checkpoint provenance changed"
        )
    child = _validate_semantic_native_id_pair_and_canonicalization(
        dataset,
        require_parent_counts=False,
        include_episode_3_pair=True,
    )
    pages = child["pages"]
    if len(pages) < 55 or [page.page_id for page in pages[:55]] != validated[
        "baseline_page_ids"
    ]:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery baseline pages changed"
        )
    attempts_by_id = {
        attempt.attempt_id: attempt for attempt in dataset.retrieval_attempts
    }
    expected_occurrence_bindings = tuple(
        {**dict(item), "native_id": adjudication["native_id"]}
        for adjudication in _semantic_native_id_overlap_adjudications(
            include_episode_3_pair=True
        )
        for item in adjudication["occurrences"]
    )
    chain = _validate_semantic_qf03_page_chain(
        pages=pages[:55],
        spec=_source_query_specs(
            wave, "SemanticScholar", ieee_credential=""
        )[2],
        attempts_by_id=attempts_by_id,
        store=CheckpointStore(active_checkpoint.parent),
        expected_occurrences=expected_occurrence_bindings,
        expected_ordering_anomalies=SEMANTIC_NATIVE_ID_OVERLAP_ORDERING_ANOMALIES,
        error_prefix="Semantic Scholar episode-4 overlap recovery baseline",
    )
    if (
        chain["saved_token_chain_hash"] != validated["saved_token_chain_hash"]
        or chain["accepted_page_manifest_hash"]
        != validated["accepted_page_manifest_hash"]
        or pages[54].metadata.get("completion_error") is not None
        or pages[54].next_state != validated["page_54_next_state"]
        or (len(pages) > 55 and pages[55].request_state != validated["page_54_next_state"])
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery continuation evidence changed"
        )
    completed_indexes = [0, 1, 3, 4]
    if any(
        dataset.source_queries[index].completion_status
        is not RetrievalCompletionStatus.COMPLETE
        or dataset.source_queries[index].result_count
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_QF_COUNTS[index][2]
        for index in completed_indexes
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery changed a completed query"
        )
    if (
        source_state.get("occurrence_count") != len(dataset.occurrences)
        or source_state.get("attempt_count") != len(dataset.retrieval_attempts)
        or source_state.get("completed_query_count")
        != sum(
            query.completion_status is RetrievalCompletionStatus.COMPLETE
            for query in dataset.source_queries
        )
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery state/checkpoint counts "
            "disagree"
        )


def authorize_semantic_scholar_native_id_overlap_recovery(
    *, root: str | Path, timestamp: Callable[[], str] = utc_now
) -> dict[str, Any]:
    """Create an offline episode for the one exact adjudicated QF03 overlap."""

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        return _authorize_semantic_scholar_native_id_overlap_recovery_locked(
            root=root_path, timestamp=timestamp
        )


def _authorize_semantic_scholar_native_id_overlap_recovery_locked(
    *, root: Path, timestamp: Callable[[], str]
) -> dict[str, Any]:
    wave, preflight = validate_persisted_external_preflight(root=root)
    state_path = _safe_output_path(root, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root, wave, preflight)
    source_state = state["sources"]["SemanticScholar"]
    if _semantic_native_id_overlap_recovery_active(source_state):
        if (
            source_state.get("active_episode_number") == 3
            and source_state.get("status") == "FAILED"
        ):
            return _authorize_semantic_scholar_native_id_overlap_episode_4_locked(
                root=root,
                timestamp=timestamp,
                wave=wave,
                state_path=state_path,
                state=state,
                source_state=source_state,
            )
        _validate_authorized_semantic_native_id_overlap_recovery(
            root=root, source_state=source_state, wave=wave
        )
        return state
    episodes = source_state.get("execution_episodes", [])
    if (
        len(episodes) != 2
        or source_state.get("status") != "FAILED"
        or source_state.get("active_episode_number") != 2
        or source_state.get("active_run_id")
        != f"{WAVE_ID}:SemanticScholar"
        or source_state.get("checkpoint_dataset")
        != episodes[1].get("checkpoint_dataset")
        or source_state.get("active_checkpoint_path")
        != episodes[1].get("checkpoint_path")
        or source_state.get("failure_reason")
        != f"{SEMANTIC_NATIVE_ID_OVERLAP_QUERY_ID}: {SEMANTIC_NATIVE_ID_OVERLAP_ERROR}"
        or source_state.get("completed_query_count") != 4
        or source_state.get("total_query_count") != 5
        or source_state.get("occurrence_count")
        != SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_OCCURRENCES
        or source_state.get("attempt_count")
        != SEMANTIC_NATIVE_ID_OVERLAP_EXPECTED_ATTEMPTS
        or episodes[1].get("episode_number") != 2
        or episodes[1].get("status") != "FAILED"
        or episodes[1].get("immutable") is not True
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery requires the exact failed episode-2 "
            "lineage"
        )
    if state.get("external_retrieval_cutoff_date") is not None:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery cannot alter a closed retrieval wave"
        )
    parent_reference = episodes[1]["checkpoint_dataset"]
    validated = _validate_semantic_native_id_overlap_parent_checkpoint(
        root=root, wave=wave, checkpoint_reference=parent_reference
    )
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "SemanticScholar"
    }
    parent_episodes = json.loads(json.dumps(episodes, sort_keys=True))
    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/SemanticScholar/episodes/episode-003/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(root, recovery_checkpoint_relative)
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery checkpoint exists without valid lineage"
        )
    response_bindings = _copy_recovery_raw_responses(
        root=root,
        source_checkpoint_dir=_safe_output_path(root, episodes[1]["checkpoint_path"]),
        recovery_checkpoint_dir=recovery_checkpoint_dir,
        raw_response_references=validated["raw_response_references"],
        source_path_key="episode_2_path",
        error_prefix="Semantic Scholar native-ID-overlap",
    )
    parent_checkpoint = _safe_output_path(root, parent_reference["path"])
    dataset = load_review_dataset(parent_checkpoint)
    run = dataset.retrieval_runs[0]
    qf03 = next(
        query
        for query in dataset.source_queries
        if query.query_id == SEMANTIC_NATIVE_ID_OVERLAP_QUERY_ID
    )
    page_42 = next(
        page
        for page in dataset.retrieval_pages
        if page.source_query_id == qf03.query_id and page.ordinal == 42
    )
    recovered_at = timestamp()
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.errors = [
        "offline Semantic Scholar native-ID overlap recovery complete; live resume pending"
    ]
    qf03.status = ProcessingStatus.PARTIAL
    qf03.completion_status = RetrievalCompletionStatus.RUNNING
    qf03.errors = []
    qf03.retrieval_ended_at = recovered_at
    page_42.metadata.pop("completion_error", None)
    provenance = _semantic_native_id_overlap_provenance(
        checkpoint_reference=parent_reference, validated=validated
    )
    run.metadata["offline_semantic_native_id_overlap_recovery"] = provenance
    dataset.validate()
    store = CheckpointStore(recovery_checkpoint_dir)
    checkpoint_hash = store.save_dataset(dataset)
    child_reference = _file_reference(store.dataset_path, root)
    if checkpoint_hash != child_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery checkpoint hash disagreement"
        )
    if (
        parent_checkpoint.stat().st_size
        != SEMANTIC_NATIVE_ID_OVERLAP_PARENT_CHECKPOINT_SIZE
        or _sha256(parent_checkpoint.read_bytes())
        != SEMANTIC_NATIVE_ID_OVERLAP_PARENT_CHECKPOINT_SHA256
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-2 parent changed during overlap recovery"
        )
    _verify_recovery_raw_bindings(root, response_bindings, "episode_2_path")
    episode_3 = {
        "episode_number": 3,
        "episode_id": "SemanticScholar-episode-003",
        "run_id": run.run_id,
        "status": SEMANTIC_NATIVE_ID_OVERLAP_RECOVERY_STATUS,
        "recovery_of_episode_number": 2,
        "authorization_reason": (
            "OFFLINE_SEMANTIC_SCHOLAR_NATIVE_ID_OVERLAP_RECOVERY"
        ),
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": child_reference,
        "parent_checkpoint_dataset": dict(parent_reference),
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "adjudication_provenance": provenance,
        "source_raw_responses": response_bindings,
        "retained_successful_page_count": len(dataset.retrieval_pages),
        "continuation_state": dict(validated["continuation"]),
        "network_used": False,
        "immutable": False,
    }
    source_state.update(
        {
            "status": SEMANTIC_NATIVE_ID_OVERLAP_RECOVERY_STATUS,
            "execution_episodes": [*parent_episodes, episode_3],
            "active_episode_number": 3,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": child_reference,
            "completed_query_count": 4,
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "preserved_source_attempt_count": len(dataset.retrieval_attempts),
            "preserved_source_response_count": len(response_bindings),
            "requests_this_session": 0,
            "candidate_request_count": 0,
            "pause_reason": (
                "OFFLINE_NATIVE_ID_OVERLAP_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
            ),
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    source_state.pop("pause_metadata", None)
    if source_state["execution_episodes"][:2] != parent_episodes:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery changed its parent episodes"
        )
    if {
        key: value
        for key, value in state["sources"].items()
        if key != "SemanticScholar"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar overlap recovery changed another source"
        )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    _validate_authorized_semantic_native_id_overlap_recovery(
        root=root, source_state=source_state, wave=wave
    )
    return state


def _authorize_semantic_scholar_native_id_overlap_episode_4_locked(
    *,
    root: Path,
    timestamp: Callable[[], str],
    wave: ProductionRetrievalWave,
    state_path: Path,
    state: dict[str, Any],
    source_state: dict[str, Any],
) -> dict[str, Any]:
    episodes = source_state.get("execution_episodes", [])
    expected_failure = (
        f"{SEMANTIC_NATIVE_ID_OVERLAP_QUERY_ID}: "
        f"{SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_ERROR}"
    )
    if (
        len(episodes) != 3
        or source_state.get("status") != "FAILED"
        or source_state.get("active_episode_number") != 3
        or source_state.get("active_run_id") != episodes[2].get("run_id")
        or source_state.get("checkpoint_dataset")
        != episodes[2].get("checkpoint_dataset")
        or source_state.get("active_checkpoint_path")
        != episodes[2].get("checkpoint_path")
        or source_state.get("failure_reason") != expected_failure
        or source_state.get("completed_query_count") != 4
        or source_state.get("total_query_count") != 5
        or source_state.get("occurrence_count")
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_OCCURRENCES
        or source_state.get("attempt_count")
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_EXPECTED_ATTEMPTS
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery requires the exact failed "
            "episode-3 lineage"
        )
    if state.get("external_retrieval_cutoff_date") is not None:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery cannot alter a closed "
            "retrieval wave"
        )
    validated = _validate_semantic_native_id_overlap_episode_3_parent_checkpoint(
        root=root,
        wave=wave,
        episodes=episodes,
    )
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "SemanticScholar"
    }
    parent_episodes = json.loads(json.dumps(episodes, sort_keys=True))
    parent_reference = episodes[2]["checkpoint_dataset"]
    parent_checkpoint = _safe_output_path(root, parent_reference["path"])
    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/SemanticScholar/episodes/episode-004/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(root, recovery_checkpoint_relative)
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery checkpoint exists without "
            "valid lineage"
        )
    response_bindings = _copy_recovery_raw_responses(
        root=root,
        source_checkpoint_dir=parent_checkpoint.parent,
        recovery_checkpoint_dir=recovery_checkpoint_dir,
        raw_response_references=validated["raw_response_references"],
        source_path_key="episode_3_path",
        error_prefix="Semantic Scholar episode-4 native-ID-overlap",
    )
    dataset = load_review_dataset(parent_checkpoint)
    run = dataset.retrieval_runs[0]
    qf03 = next(
        query
        for query in dataset.source_queries
        if query.query_id == SEMANTIC_NATIVE_ID_OVERLAP_QUERY_ID
    )
    page_54 = next(
        page
        for page in dataset.retrieval_pages
        if page.source_query_id == qf03.query_id and page.ordinal == 54
    )
    recovered_at = timestamp()
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.errors = [
        (
            "offline Semantic Scholar native-ID overlap recovery complete; live "
            "resume pending"
        )
    ]
    qf03.status = ProcessingStatus.PARTIAL
    qf03.completion_status = RetrievalCompletionStatus.RUNNING
    qf03.errors = []
    qf03.retrieval_ended_at = recovered_at
    page_54.metadata.pop("completion_error", None)
    provenance = _semantic_native_id_overlap_episode_4_provenance(
        checkpoint_reference=parent_reference,
        validated=validated,
        prior_provenance=validated["prior_provenance"],
    )
    run.metadata[
        "offline_semantic_native_id_overlap_recovery_episode_4"
    ] = provenance
    dataset.validate()
    store = CheckpointStore(recovery_checkpoint_dir)
    checkpoint_hash = store.save_dataset(dataset)
    child_reference = _file_reference(store.dataset_path, root)
    if checkpoint_hash != child_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery checkpoint hash disagreement"
        )
    if (
        parent_checkpoint.stat().st_size
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PARENT_CHECKPOINT_SIZE
        or _sha256(parent_checkpoint.read_bytes())
        != SEMANTIC_NATIVE_ID_OVERLAP_EPISODE_3_PARENT_CHECKPOINT_SHA256
    ):
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-3 parent changed during episode-4 overlap "
            "recovery"
        )
    _verify_recovery_raw_bindings(root, response_bindings, "episode_3_path")
    episode_4 = {
        "episode_number": 4,
        "episode_id": "SemanticScholar-episode-004",
        "run_id": run.run_id,
        "status": SEMANTIC_NATIVE_ID_OVERLAP_RECOVERY_STATUS,
        "recovery_of_episode_number": 3,
        "authorization_reason": (
            "OFFLINE_SEMANTIC_SCHOLAR_NATIVE_ID_OVERLAP_RECOVERY"
        ),
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": child_reference,
        "parent_checkpoint_dataset": dict(parent_reference),
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "adjudication_provenance": provenance,
        "source_raw_responses": response_bindings,
        "retained_successful_page_count": len(dataset.retrieval_pages),
        "continuation_state": dict(validated["continuation"]),
        "network_used": False,
        "immutable": False,
    }
    source_state.update(
        {
            "status": SEMANTIC_NATIVE_ID_OVERLAP_RECOVERY_STATUS,
            "execution_episodes": [*parent_episodes, episode_4],
            "active_episode_number": 4,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": child_reference,
            "completed_query_count": 4,
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "preserved_source_attempt_count": len(dataset.retrieval_attempts),
            "preserved_source_response_count": len(response_bindings),
            "requests_this_session": 0,
            "candidate_request_count": 0,
            "pause_reason": (
                "OFFLINE_NATIVE_ID_OVERLAP_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
            ),
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    source_state.pop("pause_metadata", None)
    if source_state["execution_episodes"][:3] != parent_episodes:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery changed its parent episodes"
        )
    if {
        key: value
        for key, value in state["sources"].items()
        if key != "SemanticScholar"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar episode-4 overlap recovery changed another source"
        )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    _validate_authorized_semantic_native_id_overlap_recovery(
        root=root,
        source_state=source_state,
        wave=wave,
    )
    return state


def authorize_arxiv_rate_limit_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Authorize a fresh arXiv episode from the immutable timeout/429 failure."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["arXiv"]
    if source_state["status"] == ARXIV_RATE_LIMIT_RECOVERY_STATUS:
        _validate_authorized_arxiv_rate_limit_recovery(source_state, root_path, wave)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "arXiv rate-limit recovery requires the known terminal FAILED component"
        )
    if (
        source_state.get("completed_query_count") != 0
        or source_state.get("total_query_count") != 5
        or source_state.get("occurrence_count") != 0
        or source_state.get("attempt_count") != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or source_state.get("requests_this_session")
        != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or source_state.get("pause_reason") is not None
    ):
        raise ExternalRetrievalWaveError(
            "arXiv source state is not the known timeout/rate-limit failure"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "arXiv rate-limit recovery cannot alter a closed external retrieval wave"
        )
    if source_state.get("execution_episodes"):
        raise ExternalRetrievalWaveError(
            "arXiv rate-limit recovery requires the original failed lineage"
        )

    checkpoint_reference = source_state.get("checkpoint_dataset")
    if not checkpoint_reference:
        raise ExternalRetrievalWaveError("failed arXiv state lacks a checkpoint binding")
    failed_checkpoint = _safe_output_path(
        root_path, str(checkpoint_reference["path"])
    )
    _verify_file_reference(failed_checkpoint, checkpoint_reference, root_path)
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    failed_payload = json.loads(failed_checkpoint_bytes)
    dataset = load_review_dataset(failed_checkpoint)
    recovery = _validate_failed_arxiv_rate_limit_checkpoint(
        dataset=dataset,
        checkpoint_dir=failed_checkpoint.parent,
        root=root_path,
        wave=wave,
    )
    if source_state.get("failure_reason") != "; ".join(
        dataset.retrieval_runs[0].errors
    ):
        raise ExternalRetrievalWaveError("arXiv source/checkpoint failure reason changed")
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "arXiv"
    }
    recovered_at = timestamp()

    episode_1 = {
        "episode_number": 1,
        "episode_id": "arXiv-episode-001",
        "run_id": dataset.retrieval_runs[0].run_id,
        "status": "FAILED",
        "failure_classification": "TRANSPORT_TIMEOUT_AND_PROVIDER_RATE_LIMIT",
        "checkpoint_path": failed_checkpoint.parent.relative_to(root_path).as_posix(),
        "checkpoint_dataset": dict(checkpoint_reference),
        "attempt_count": ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS,
        "transport_timeout_count": 8,
        "http_429_count": ARXIV_RATE_LIMIT_EXPECTED_RESPONSES,
        "successful_http_response_count": 0,
        "occurrence_count": 0,
        "source_attempt_manifest_hash": _hash_payload(
            {"retrieval_attempts": failed_payload.get("retrieval_attempts", [])}
        ),
        "raw_responses": recovery["raw_response_bindings"],
        "query_attempt_signatures": recovery["query_attempt_signatures"],
        "started_at_utc": source_state.get("last_session_started_at_utc"),
        "completed_at_utc": source_state.get("last_session_completed_at_utc"),
        "failure_reason": source_state.get("failure_reason"),
        "immutable": True,
    }

    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/arXiv/episodes/episode-002/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 checkpoint exists without valid state lineage"
        )

    run = dataset.retrieval_runs[0]
    dataset.retrieval_pages = []
    dataset.retrieval_attempts = []
    dataset.occurrences = []
    dataset.canonical_records = []
    dataset.duplicate_decisions = []
    run.retrieval_started_at = recovered_at
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.errors = [
        "offline arXiv rate-limit recovery complete; live resume pending"
    ]
    run.metadata.pop("pause_state", None)
    run.metadata.pop("pause_reason", None)
    run.metadata.pop("pause_metadata", None)
    run.metadata["offline_arxiv_rate_limit_recovery"] = {
        "recovery_episode_number": 2,
        "source_episode_number": 1,
        "source_checkpoint": dict(checkpoint_reference),
        "source_attempt_count": ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS,
        "source_raw_response_count": ARXIV_RATE_LIMIT_EXPECTED_RESPONSES,
        "source_raw_response_manifest_hash": _hash_payload(
            {"responses": recovery["raw_response_bindings"]}
        ),
        "restart_states": recovery["restart_states"],
        "network_used": False,
    }
    for query, restart in zip(
        dataset.source_queries, recovery["restart_states"], strict=True
    ):
        query.retrieval_started_at = recovered_at
        query.retrieval_ended_at = recovered_at
        query.status = ProcessingStatus.PARTIAL
        query.completion_status = RetrievalCompletionStatus.PLANNED
        query.page = None
        query.cursor = None
        query.result_count = 0
        query.errors = []
        query.page_ids = []
        query.source_reported_total = None
        query.total_is_exact = False
        query.completion_proof = None
        query.metadata.pop("pause_state", None)
        query.metadata.pop("pause_reason", None)
        query.metadata.pop("pause_metadata", None)
        query.metadata["offline_arxiv_rate_limit_recovery"] = {
            "source_episode_number": 1,
            "restart_state": dict(restart["request_state"]),
            "request_hash": restart["request_hash"],
        }
    dataset.validate()

    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError("arXiv recovery checkpoint hash disagreement")
    if failed_checkpoint.read_bytes() != failed_checkpoint_bytes:
        raise ExternalRetrievalWaveError(
            "arXiv episode-1 checkpoint changed during recovery"
        )
    _verify_arxiv_raw_response_bindings(
        root_path, recovery["raw_response_bindings"]
    )

    episode_2 = {
        "episode_number": 2,
        "episode_id": "arXiv-episode-002",
        "run_id": run.run_id,
        "status": ARXIV_RATE_LIMIT_RECOVERY_STATUS,
        "recovery_of_episode_number": 1,
        "authorization_reason": "OFFLINE_ARXIV_TIMEOUT_AND_RATE_LIMIT_RECOVERY",
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": episode_1["source_attempt_manifest_hash"],
        "source_attempt_count": ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS,
        "source_raw_responses": recovery["raw_response_bindings"],
        "restart_states": recovery["restart_states"],
        "network_used": False,
        "immutable": False,
    }
    source_state["execution_episodes"] = [episode_1, episode_2]
    source_state.update(
        {
            "status": ARXIV_RATE_LIMIT_RECOVERY_STATUS,
            "active_episode_number": 2,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": 0,
            "total_query_count": 5,
            "occurrence_count": 0,
            "attempt_count": 0,
            "preserved_source_attempt_count": ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS,
            "preserved_source_raw_response_count": (
                ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
            ),
            "requests_this_session": 0,
            "pause_reason": (
                "OFFLINE_RATE_LIMIT_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
            ),
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    if {
        key: value for key, value in state["sources"].items() if key != "arXiv"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "arXiv rate-limit recovery changed another source component"
        )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def authorize_arxiv_mixed_state_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Authorize episode 3 from the exact response-free-timeout/429 pause."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["arXiv"]
    if source_state["status"] == ARXIV_MIXED_RECOVERY_STATUS:
        _validate_authorized_arxiv_mixed_state_recovery(
            source_state, root_path, wave
        )
        return state
    if (
        source_state.get("status") != "PAUSED_PROVIDER_RATE_LIMIT"
        or source_state.get("active_episode_number") != 2
        or source_state.get("completed_query_count") != 0
        or source_state.get("total_query_count") != 5
        or source_state.get("occurrence_count") != 0
        or source_state.get("attempt_count") != ARXIV_MIXED_EXPECTED_ATTEMPTS
        or source_state.get("requests_this_session")
        != ARXIV_MIXED_EXPECTED_ATTEMPTS
        or source_state.get("pause_reason")
        != "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
        or source_state.get("failure_reason") is not None
        or source_state.get("pause_metadata")
        != {
            "source_database": "arXiv",
            "http_status": 429,
            "retry_after_header_present": False,
            "retry_after": None,
        }
    ):
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery requires the exact episode-2 timeout/429 pause"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery cannot alter a closed external retrieval wave"
        )

    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 2:
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery requires exactly two prior episodes"
        )
    episode_1, episode_2 = episodes
    _validate_arxiv_episode_1_provenance(
        episode_1=episode_1,
        episode_2=episode_2,
        root=root_path,
        wave=wave,
    )
    if (
        episode_2.get("episode_number") != 2
        or episode_2.get("status") != "PAUSED_PROVIDER_RATE_LIMIT"
        or episode_2.get("recovery_of_episode_number") != 1
        or episode_2.get("authorization_reason")
        != "OFFLINE_ARXIV_TIMEOUT_AND_RATE_LIMIT_RECOVERY"
        or episode_2.get("frozen_wave_manifest_hash") != wave.manifest_hash()
        or episode_2.get("frozen_query_plan_hash") != wave.query_plan_hash
        or episode_2.get("immutable") is not False
        or episode_2.get("network_used") is not False
        or episode_2.get("attempt_count") != ARXIV_MIXED_EXPECTED_ATTEMPTS
        or episode_2.get("completed_query_count") != 0
        or episode_2.get("occurrence_count") != 0
        or episode_2.get("pause_reason")
        != "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
        or episode_2.get("failure_reason") is not None
        or source_state.get("active_checkpoint_path")
        != episode_2.get("checkpoint_path")
        or source_state.get("checkpoint_dataset")
        != episode_2.get("checkpoint_dataset")
        or source_state.get("active_run_id") != episode_2.get("run_id")
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 execution lineage changed"
        )

    checkpoint_reference = episode_2.get("checkpoint_dataset")
    if not checkpoint_reference:
        raise ExternalRetrievalWaveError("arXiv episode 2 lacks a checkpoint binding")
    if (
        checkpoint_reference.get("raw_sha256")
        != ARXIV_MIXED_EXPECTED_CHECKPOINT_SHA256
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 checkpoint does not match the authorized production digest"
        )
    checkpoint_path = _safe_output_path(
        root_path, str(checkpoint_reference["path"])
    )
    _verify_file_reference(checkpoint_path, checkpoint_reference, root_path)
    checkpoint_bytes = checkpoint_path.read_bytes()
    checkpoint_payload = json.loads(checkpoint_bytes)
    dataset = load_review_dataset(checkpoint_path)
    recovery = _validate_arxiv_mixed_state_checkpoint(
        dataset=dataset,
        checkpoint_dir=checkpoint_path.parent,
        root=root_path,
        wave=wave,
    )
    if (
        dataset.retrieval_runs[0].run_id != episode_2.get("run_id")
        or recovery["restart_states"] != episode_2.get("restart_states")
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 frozen run/request identity changed"
        )
    episode_2_attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": checkpoint_payload.get("retrieval_attempts", [])}
    )
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "arXiv"
    }
    recovered_at = timestamp()

    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/arXiv/episodes/episode-003/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "arXiv episode-3 checkpoint exists without valid state lineage"
        )

    run = dataset.retrieval_runs[0]
    dataset.retrieval_pages = []
    dataset.retrieval_attempts = []
    dataset.occurrences = []
    dataset.canonical_records = []
    dataset.duplicate_decisions = []
    run.retrieval_started_at = recovered_at
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.errors = [
        "offline arXiv mixed transport/rate-limit recovery complete; live resume pending"
    ]
    run.metadata.pop("pause_state", None)
    run.metadata.pop("pause_reason", None)
    run.metadata.pop("pause_metadata", None)
    run.metadata.pop("session_request_count", None)
    provenance_bindings = [
        {
            "episode_number": 1,
            "checkpoint_dataset": dict(episode_1["checkpoint_dataset"]),
            "attempt_manifest_hash": episode_1["source_attempt_manifest_hash"],
            "raw_response_manifest_hash": _hash_payload(
                {"responses": episode_1["raw_responses"]}
            ),
        },
        {
            "episode_number": 2,
            "checkpoint_dataset": dict(checkpoint_reference),
            "attempt_manifest_hash": episode_2_attempt_manifest_hash,
            "raw_response_manifest_hash": _hash_payload(
                {"responses": recovery["raw_response_bindings"]}
            ),
        },
    ]
    run.metadata["offline_arxiv_mixed_state_recovery"] = {
        "recovery_episode_number": 3,
        "source_episode_number": 2,
        "source_episodes": provenance_bindings,
        "restart_states": recovery["restart_states"],
        "network_used": False,
    }
    for query, restart in zip(
        dataset.source_queries, recovery["restart_states"], strict=True
    ):
        query.retrieval_started_at = recovered_at
        query.retrieval_ended_at = recovered_at
        query.status = ProcessingStatus.PARTIAL
        query.completion_status = RetrievalCompletionStatus.PLANNED
        query.page = None
        query.cursor = None
        query.result_count = 0
        query.errors = []
        query.page_ids = []
        query.source_reported_total = None
        query.total_is_exact = False
        query.completion_proof = None
        query.metadata.pop("pause_state", None)
        query.metadata.pop("pause_reason", None)
        query.metadata.pop("pause_metadata", None)
        query.metadata["offline_arxiv_mixed_state_recovery"] = {
            "source_episode_number": 2,
            "restart_state": dict(restart["request_state"]),
            "request_hash": restart["request_hash"],
        }
    dataset.validate()

    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery checkpoint hash disagreement"
        )
    if checkpoint_path.read_bytes() != checkpoint_bytes:
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 checkpoint changed during recovery"
        )
    _verify_arxiv_raw_response_bindings(
        root_path, episode_1["raw_responses"]
    )
    _verify_arxiv_mixed_raw_response_bindings(
        root_path, recovery["raw_response_bindings"]
    )

    episode_2["immutable"] = True
    episode_3 = {
        "episode_number": 3,
        "episode_id": "arXiv-episode-003",
        "run_id": run.run_id,
        "status": ARXIV_MIXED_RECOVERY_STATUS,
        "recovery_of_episode_number": 2,
        "authorization_reason": (
            "OFFLINE_ARXIV_MIXED_TRANSPORT_AND_RATE_LIMIT_RECOVERY"
        ),
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episodes": provenance_bindings,
        "source_attempt_count": (
            ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
            + ARXIV_MIXED_EXPECTED_ATTEMPTS
        ),
        "source_raw_response_count": (
            ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
            + ARXIV_MIXED_EXPECTED_RESPONSES
        ),
        "episode_2_attempt_manifest_hash": episode_2_attempt_manifest_hash,
        "episode_2_raw_responses": recovery["raw_response_bindings"],
        "episode_2_query_attempt_signatures": recovery[
            "query_attempt_signatures"
        ],
        "restart_states": recovery["restart_states"],
        "network_used": False,
        "immutable": False,
    }
    source_state["execution_episodes"].append(episode_3)
    source_state.update(
        {
            "status": ARXIV_MIXED_RECOVERY_STATUS,
            "active_episode_number": 3,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": 0,
            "total_query_count": 5,
            "occurrence_count": 0,
            "attempt_count": 0,
            "preserved_source_attempt_count": (
                ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
                + ARXIV_MIXED_EXPECTED_ATTEMPTS
            ),
            "preserved_source_raw_response_count": (
                ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
                + ARXIV_MIXED_EXPECTED_RESPONSES
            ),
            "requests_this_session": 0,
            "pause_reason": (
                "OFFLINE_MIXED_STATE_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
            ),
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    source_state.pop("pause_metadata", None)
    if {
        key: value for key, value in state["sources"].items() if key != "arXiv"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery changed another source component"
        )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def authorize_arxiv_episode_3_state_reconciliation(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Refresh only the stale global reference to the valid arXiv episode 3."""

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        wave, preflight = validate_persisted_external_preflight(root=root_path)
        state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
        if not state_path.is_file():
            raise ExternalRetrievalWaveError("external execution state does not exist")
        state = _load_execution_state(state_path, root_path, wave, preflight)
        if any(
            item.get("status") == "RUNNING" for item in state["sources"].values()
        ):
            raise ExternalRetrievalWaveError(
                "cannot reconcile while an external-source session is marked RUNNING"
            )
        if state.get("external_retrieval_cutoff_date") is not None:
            raise ExternalRetrievalWaveError(
                "arXiv state reconciliation cannot alter a closed retrieval wave"
            )

        source_state = state["sources"]["arXiv"]
        episodes = source_state.get("execution_episodes", [])
        if (
            source_state.get("status") != "PAUSED_TRANSIENT_TRANSPORT"
            or source_state.get("active_episode_number") != 3
            or source_state.get("active_run_id") != f"{WAVE_ID}:arXiv"
            or source_state.get("completed_query_count") != 0
            or source_state.get("total_query_count") != 5
            or source_state.get("occurrence_count") != 0
            or source_state.get("pause_reason")
            != "TRANSIENT_TRANSPORT_EXHAUSTED_NO_RESPONSE"
            or source_state.get("failure_reason") is not None
            or len(episodes) != 3
        ):
            raise ExternalRetrievalWaveError(
                "arXiv source is not the identified episode-3 transport pause"
            )
        episode = episodes[2]
        checkpoint_relative = (
            f"{EXECUTION_ROOT}/arXiv/episodes/episode-003/checkpoint"
        )
        checkpoint_file_relative = f"{checkpoint_relative}/review_dataset.json"
        if (
            episode.get("episode_number") != 3
            or episode.get("episode_id") != "arXiv-episode-003"
            or episode.get("run_id") != source_state["active_run_id"]
            or episode.get("recovery_of_episode_number") != 2
            or episode.get("authorization_reason")
            != "OFFLINE_ARXIV_MIXED_TRANSPORT_AND_RATE_LIMIT_RECOVERY"
            or episode.get("frozen_wave_manifest_hash") != wave.manifest_hash()
            or episode.get("frozen_query_plan_hash") != wave.query_plan_hash
            or episode.get("network_used") is not False
            or episode.get("immutable") is not False
            or source_state.get("active_checkpoint_path") != checkpoint_relative
            or source_state.get("checkpoint_path") != checkpoint_relative
            or episode.get("checkpoint_path") != checkpoint_relative
            or source_state.get("checkpoint_dataset")
            != episode.get("checkpoint_dataset")
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 recovery lineage changed"
            )

        checkpoint_path = _safe_output_path(root_path, checkpoint_file_relative)
        checkpoint_bytes = checkpoint_path.read_bytes()
        actual_reference = _file_reference(checkpoint_path, root_path)
        if actual_reference != {
            "path": checkpoint_file_relative,
            "byte_size": ARXIV_EPISODE_3_CURRENT_CHECKPOINT_SIZE,
            "raw_sha256": ARXIV_EPISODE_3_CURRENT_CHECKPOINT_SHA256,
        }:
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 current checkpoint digest is not authorized"
            )

        checkpoint_payload = json.loads(checkpoint_bytes)
        dataset = load_review_dataset(checkpoint_path)
        validation = _validate_arxiv_episode_3_reconciliation_checkpoint(
            dataset=dataset,
            checkpoint_dir=checkpoint_path.parent,
            checkpoint_payload=checkpoint_payload,
            wave=wave,
        )
        prior_reference = source_state.get("checkpoint_dataset")
        stale_reference = {
            "path": checkpoint_file_relative,
            "byte_size": ARXIV_EPISODE_3_STALE_CHECKPOINT_SIZE,
            "raw_sha256": ARXIV_EPISODE_3_STALE_CHECKPOINT_SHA256,
        }
        already_reconciled = prior_reference == actual_reference
        if already_reconciled:
            evidence = source_state.get("state_reconciliation")
            if (
                not isinstance(evidence, dict)
                or evidence.get("classification")
                != "CONCURRENT_EXECUTION_STATE_LOST_UPDATE"
                or evidence.get("prior_checkpoint_dataset") != stale_reference
                or evidence.get("reconciled_checkpoint_dataset")
                != actual_reference
                or evidence.get("appended_attempt_numbers") != [5, 6, 7]
                or evidence.get("attempt_manifest_hash")
                != validation["attempt_manifest_hash"]
                or evidence.get("persisted_response_hashes")
                != validation["persisted_response_hashes"]
                or evidence.get("network_used") is not False
                or evidence.get("checkpoint_modified") is not False
                or episode.get("state_reconciliation") != evidence
                or source_state.get("attempt_count") != 7
                or episode.get("attempt_count") != 7
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv episode-3 reconciled state evidence changed"
                )
            return state
        if (
            prior_reference != stale_reference
            or source_state.get("attempt_count") != 4
            or episode.get("attempt_count") != 4
            or source_state.get("requests_this_session") != 3
            or source_state.get("last_session_completed_at_utc")
            != episode.get("completed_at_utc")
            or source_state.get("last_session_started_at_utc")
            != episode.get("started_at_utc")
            or str(source_state.get("last_session_started_at_utc"))
            > validation["prior_session_started_at_utc"]
            or str(source_state.get("last_session_completed_at_utc"))
            < validation["prior_session_completed_at_utc"]
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 stale execution-state signature changed"
            )

        other_sources_before = {
            key: json.loads(json.dumps(value, sort_keys=True))
            for key, value in state["sources"].items()
            if key != "arXiv"
        }
        reconciled_at = timestamp()
        evidence = {
            "classification": "CONCURRENT_EXECUTION_STATE_LOST_UPDATE",
            "reconciled_at_utc": reconciled_at,
            "prior_checkpoint_dataset": stale_reference,
            "reconciled_checkpoint_dataset": actual_reference,
            "prior_attempt_count": 4,
            "reconciled_attempt_count": 7,
            "appended_attempt_numbers": [5, 6, 7],
            "attempt_manifest_hash": validation["attempt_manifest_hash"],
            "persisted_response_hashes": validation[
                "persisted_response_hashes"
            ],
            "network_used": False,
            "checkpoint_modified": False,
        }
        source_state.update(
            {
                "checkpoint_dataset": actual_reference,
                "attempt_count": 7,
                "requests_this_session": 3,
                "last_session_started_at_utc": validation[
                    "current_session_started_at_utc"
                ],
                "last_session_completed_at_utc": validation[
                    "current_session_completed_at_utc"
                ],
                "pause_metadata": validation["pause_metadata"],
                "state_reconciliation": evidence,
            }
        )
        _sync_active_retry_episode(source_state)
        episode["state_reconciliation"] = evidence
        if {
            key: value for key, value in state["sources"].items() if key != "arXiv"
        } != other_sources_before:
            raise ExternalRetrievalWaveError(
                "arXiv state reconciliation changed another source component"
            )
        _save_execution_state(state_path, state)
        if checkpoint_path.read_bytes() != checkpoint_bytes:
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 checkpoint changed during state reconciliation"
            )
        return state


def _validate_arxiv_episode_3_reconciliation_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    checkpoint_payload: Mapping[str, Any],
    wave: ProductionRetrievalWave,
) -> dict[str, Any]:
    """Validate the exact append-only episode-3 evidence before state repair."""

    dataset.validate()
    specs = _source_query_specs(wave, "arXiv", ieee_credential="")
    if len(dataset.retrieval_runs) != 1 or len(specs) != 5:
        raise ExternalRetrievalWaveError("arXiv episode-3 run/query count changed")
    run = dataset.retrieval_runs[0]
    queries = dataset.source_queries
    if (
        run.run_id != f"{WAVE_ID}:arXiv"
        or run.query_plan_version != wave.query_plan_hash
        or run.query_plan_hash != _query_plan_hash(specs)
        or run.planned_query_ids != [item.query_id for item in queries]
        or run.source_query_ids != run.planned_query_ids
        or run.status is not ProcessingStatus.PARTIAL
        or run.completion_status is not RetrievalCompletionStatus.RUNNING
        or run.retrieval_cutoff_date is not None
        or run.errors != ["TRANSIENT_TRANSPORT_EXHAUSTED_NO_RESPONSE"]
        or run.metadata.get("pause_state") != "TRANSIENT_TRANSPORT_EXHAUSTED"
        or run.metadata.get("pause_reason")
        != "TRANSIENT_TRANSPORT_EXHAUSTED_NO_RESPONSE"
        or run.metadata.get("session_request_count") != 3
        or len(queries) != 5
    ):
        raise ExternalRetrievalWaveError("arXiv episode-3 run identity/state changed")

    adapter = PAGINATED_SOURCE_ADAPTERS["arXiv"]
    restart_states = run.metadata.get("offline_arxiv_mixed_state_recovery", {}).get(
        "restart_states"
    )
    if not isinstance(restart_states, list) or len(restart_states) != 5:
        raise ExternalRetrievalWaveError("arXiv episode-3 restart lineage changed")
    for index, (query, spec, restart) in enumerate(
        zip(queries, specs, restart_states, strict=True)
    ):
        expected_request = adapter.build_request(spec, {"start": 0})
        if (
            query.query_id != run.planned_query_ids[index]
            or query.run_id != run.run_id
            or query.source_database != "arXiv"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.endpoint != spec.endpoint
            or query.filters != {"page_size": 2000}
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
            or restart
            != {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "request_state": {"start": 0},
                "max_results": 2000,
                "request_hash": expected_request.request_hash(),
            }
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 frozen query/request identity changed"
            )
    if restart_states[0]["request_hash"] != ARXIV_EPISODE_3_EXPECTED_REQUEST_HASH:
        raise ExternalRetrievalWaveError("arXiv episode-3 QF01 request hash changed")

    pages = dataset.retrieval_pages
    attempts = dataset.retrieval_attempts
    if (
        len(pages) != 1
        or len(attempts) != 7
        or dataset.occurrences
        or dataset.canonical_records
        or dataset.duplicate_decisions
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-3 accepted-page/attempt evidence changed"
        )
    page = pages[0]
    if (
        page.source_query_id != queries[0].query_id
        or page.ordinal != 0
        or page.strategy != adapter.strategy
        or page.adapter_version != adapter.version
        or page.request_state != {"start": 0}
        or page.next_state is not None
        or page.status is not RetrievalCompletionStatus.RUNNING
        or page.returned_item_count != 0
        or page.occurrence_ids
        or page.native_identifiers
        or page.attempt_ids != [item.attempt_id for item in attempts]
        or queries[0].completion_status is not RetrievalCompletionStatus.RUNNING
        or queries[0].page_ids != [page.page_id]
        or any(
            query.completion_status is not RetrievalCompletionStatus.PLANNED
            or query.page_ids
            for query in queries[1:]
        )
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-3 page/continuation lineage changed"
        )

    expected_retry_delays = [None, 1.0, 2.0, None, 1.0, 2.0, None]
    for index, attempt in enumerate(attempts):
        if (
            attempt.attempt_number != index + 1
            or attempt.page_id != page.page_id
            or attempt.request_method != "GET"
            or attempt.request_params.get("start") != 0
            or attempt.request_params.get("max_results") != 2000
            or attempt.request_hash != ARXIV_EPISODE_3_EXPECTED_REQUEST_HASH
            or attempt.retry_delay_seconds != expected_retry_delays[index]
            or attempt.status is not RetrievalAttemptStatus.FAILED
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 append-only attempt lineage changed"
            )
    rate_limit_attempt = attempts[0]
    if (
        rate_limit_attempt.response_status != 429
        or rate_limit_attempt.error != "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
        or not rate_limit_attempt.raw_response_path
        or not rate_limit_attempt.raw_response_hash
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-3 persisted rate-limit response changed"
        )
    response = CheckpointStore(checkpoint_dir).load_response(
        rate_limit_attempt.raw_response_path,
        rate_limit_attempt.raw_response_hash,
    )
    response_files = sorted(
        path.relative_to(checkpoint_dir).as_posix()
        for path in (checkpoint_dir / "responses").glob("*.json")
    )
    if response.status_code != 429 or response_files != [
        rate_limit_attempt.raw_response_path
    ]:
        raise ExternalRetrievalWaveError(
            "arXiv episode-3 persisted response set changed"
        )
    for attempt in attempts[1:]:
        if (
            not str(attempt.error or "").startswith("ReadTimeout: ")
            or attempt.response_status is not None
            or attempt.raw_response_path is not None
            or attempt.raw_response_hash is not None
            or attempt.response_url is not None
            or attempt.response_headers
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 timeout signature changed"
            )
    pause_metadata = {
        "source_database": "arXiv",
        "response_received": False,
        "attempts_this_invocation": 3,
        "maximum_attempts_per_invocation": 3,
        "failure_types": ["ReadTimeout", "ReadTimeout", "ReadTimeout"],
    }
    if run.metadata.get("pause_metadata") != pause_metadata:
        raise ExternalRetrievalWaveError("arXiv episode-3 pause metadata changed")
    return {
        "prior_session_started_at_utc": attempts[1].started_at,
        "prior_session_completed_at_utc": attempts[3].ended_at,
        "current_session_started_at_utc": attempts[4].started_at,
        "current_session_completed_at_utc": attempts[6].ended_at,
        "pause_metadata": pause_metadata,
        "attempt_manifest_hash": _hash_payload(
            {"retrieval_attempts": checkpoint_payload.get("retrieval_attempts", [])}
        ),
        "persisted_response_hashes": [rate_limit_attempt.raw_response_hash],
    }


def authorize_arxiv_transport_policy_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Create episode 4 with a provenance-bound 120-second arXiv timeout."""

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        wave, preflight = validate_persisted_external_preflight(root=root_path)
        state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
        if not state_path.is_file():
            raise ExternalRetrievalWaveError(
                "external execution state does not exist"
            )
        state = _load_execution_state(state_path, root_path, wave, preflight)
        if any(
            item.get("status") == "RUNNING"
            for item in state["sources"].values()
        ):
            raise ExternalRetrievalWaveError(
                "cannot recover while an external-source session is marked RUNNING"
            )
        if state.get("external_retrieval_cutoff_date") is not None:
            raise ExternalRetrievalWaveError(
                "arXiv transport-policy recovery cannot alter a closed wave"
            )

        source_state = state["sources"]["arXiv"]
        episodes = source_state.get("execution_episodes", [])
        if (
            source_state.get("status") != "PAUSED_TRANSIENT_TRANSPORT"
            or source_state.get("active_episode_number") != 3
            or source_state.get("active_run_id") != f"{WAVE_ID}:arXiv"
            or source_state.get("completed_query_count") != 0
            or source_state.get("total_query_count") != 5
            or source_state.get("occurrence_count") != 0
            or source_state.get("attempt_count") != 7
            or source_state.get("requests_this_session") != 3
            or source_state.get("pause_reason")
            != "TRANSIENT_TRANSPORT_EXHAUSTED_NO_RESPONSE"
            or source_state.get("failure_reason") is not None
            or len(episodes) != 3
        ):
            raise ExternalRetrievalWaveError(
                "arXiv transport-policy recovery requires the exact "
                "reconciled episode-3 pause"
            )
        episode_1, episode_2, episode_3 = episodes
        _validate_arxiv_episode_1_provenance(
            episode_1=episode_1,
            episode_2=episode_2,
            root=root_path,
            wave=wave,
        )
        episode_2_reference = episode_2.get("checkpoint_dataset")
        if (
            not isinstance(episode_2_reference, dict)
            or episode_2_reference.get("raw_sha256")
            != ARXIV_MIXED_EXPECTED_CHECKPOINT_SHA256
            or episode_2.get("status") != "PAUSED_PROVIDER_RATE_LIMIT"
            or episode_2.get("attempt_count")
            != ARXIV_MIXED_EXPECTED_ATTEMPTS
            or episode_2.get("completed_query_count") != 0
            or episode_2.get("occurrence_count") != 0
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-2 provenance changed"
            )
        episode_2_checkpoint = _safe_output_path(
            root_path, str(episode_2_reference["path"])
        )
        _verify_file_reference(
            episode_2_checkpoint, episode_2_reference, root_path
        )
        episode_2_payload = _load_json(episode_2_checkpoint)
        episode_2_attempt_manifest_hash = _hash_payload(
            {
                "retrieval_attempts": episode_2_payload.get(
                    "retrieval_attempts", []
                )
            }
        )
        episode_2_validation = _validate_arxiv_mixed_state_checkpoint(
            dataset=load_review_dataset(episode_2_checkpoint),
            checkpoint_dir=episode_2_checkpoint.parent,
            root=root_path,
            wave=wave,
        )
        prior_source_episodes = [
            {
                "episode_number": 1,
                "checkpoint_dataset": dict(episode_1["checkpoint_dataset"]),
                "attempt_manifest_hash": episode_1[
                    "source_attempt_manifest_hash"
                ],
                "raw_response_manifest_hash": _hash_payload(
                    {"responses": episode_1["raw_responses"]}
                ),
            },
            {
                "episode_number": 2,
                "checkpoint_dataset": dict(episode_2_reference),
                "attempt_manifest_hash": episode_2_attempt_manifest_hash,
                "raw_response_manifest_hash": _hash_payload(
                    {
                        "responses": episode_2_validation[
                            "raw_response_bindings"
                        ]
                    }
                ),
            },
        ]
        if (
            episode_3.get("source_episodes") != prior_source_episodes
            or episode_3.get("episode_2_attempt_manifest_hash")
            != episode_2_attempt_manifest_hash
            or episode_3.get("episode_2_raw_responses")
            != episode_2_validation["raw_response_bindings"]
            or episode_3.get("episode_2_query_attempt_signatures")
            != episode_2_validation["query_attempt_signatures"]
            or episode_3.get("restart_states")
            != episode_2_validation["restart_states"]
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 source-episode provenance changed"
            )
        _verify_arxiv_mixed_raw_response_bindings(
            root_path, episode_2_validation["raw_response_bindings"]
        )
        checkpoint_relative = (
            f"{EXECUTION_ROOT}/arXiv/episodes/episode-003/checkpoint"
        )
        checkpoint_file_relative = (
            f"{checkpoint_relative}/review_dataset.json"
        )
        expected_parent_reference = {
            "path": checkpoint_file_relative,
            "byte_size": ARXIV_EPISODE_3_CURRENT_CHECKPOINT_SIZE,
            "raw_sha256": ARXIV_EPISODE_3_CURRENT_CHECKPOINT_SHA256,
        }
        if (
            episode_2.get("episode_number") != 2
            or episode_2.get("immutable") is not True
            or episode_3.get("episode_number") != 3
            or episode_3.get("episode_id") != "arXiv-episode-003"
            or episode_3.get("run_id") != source_state["active_run_id"]
            or episode_3.get("recovery_of_episode_number") != 2
            or episode_3.get("authorization_reason")
            != "OFFLINE_ARXIV_MIXED_TRANSPORT_AND_RATE_LIMIT_RECOVERY"
            or episode_3.get("frozen_wave_manifest_hash")
            != wave.manifest_hash()
            or episode_3.get("frozen_query_plan_hash")
            != wave.query_plan_hash
            or episode_3.get("network_used") is not False
            or episode_3.get("immutable") is not False
            or episode_3.get("checkpoint_path") != checkpoint_relative
            or episode_3.get("checkpoint_dataset")
            != expected_parent_reference
            or source_state.get("active_checkpoint_path")
            != checkpoint_relative
            or source_state.get("checkpoint_path") != checkpoint_relative
            or source_state.get("checkpoint_dataset")
            != expected_parent_reference
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 recovery lineage changed"
            )

        parent_checkpoint = _safe_output_path(
            root_path, checkpoint_file_relative
        )
        _verify_file_reference(
            parent_checkpoint, expected_parent_reference, root_path
        )
        parent_bytes = parent_checkpoint.read_bytes()
        parent_payload = json.loads(parent_bytes)
        parent_dataset = load_review_dataset(parent_checkpoint)
        parent_validation = _validate_arxiv_episode_3_reconciliation_checkpoint(
            dataset=parent_dataset,
            checkpoint_dir=parent_checkpoint.parent,
            checkpoint_payload=parent_payload,
            wave=wave,
        )
        parent_recovery = parent_dataset.retrieval_runs[0].metadata.get(
            "offline_arxiv_mixed_state_recovery", {}
        )
        if (
            parent_recovery.get("source_episodes") != prior_source_episodes
            or parent_recovery.get("restart_states")
            != episode_2_validation["restart_states"]
            or parent_recovery.get("network_used") is not False
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 checkpoint recovery provenance changed"
            )
        stale_reference = {
            "path": checkpoint_file_relative,
            "byte_size": ARXIV_EPISODE_3_STALE_CHECKPOINT_SIZE,
            "raw_sha256": ARXIV_EPISODE_3_STALE_CHECKPOINT_SHA256,
        }
        reconciliation = source_state.get("state_reconciliation")
        if (
            not isinstance(reconciliation, dict)
            or reconciliation.get("classification")
            != "CONCURRENT_EXECUTION_STATE_LOST_UPDATE"
            or reconciliation.get("prior_checkpoint_dataset")
            != stale_reference
            or reconciliation.get("reconciled_checkpoint_dataset")
            != expected_parent_reference
            or reconciliation.get("prior_attempt_count") != 4
            or reconciliation.get("reconciled_attempt_count") != 7
            or reconciliation.get("appended_attempt_numbers") != [5, 6, 7]
            or reconciliation.get("attempt_manifest_hash")
            != parent_validation["attempt_manifest_hash"]
            or reconciliation.get("persisted_response_hashes")
            != parent_validation["persisted_response_hashes"]
            or reconciliation.get("network_used") is not False
            or reconciliation.get("checkpoint_modified") is not False
            or episode_3.get("state_reconciliation") != reconciliation
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-3 reconciliation evidence changed"
            )

        old_specs = _source_query_specs(
            wave, "arXiv", ieee_credential=""
        )
        new_specs = _source_query_specs(
            wave,
            "arXiv",
            ieee_credential="",
            arxiv_read_timeout_seconds=(
                ARXIV_RECOVERED_READ_TIMEOUT_SECONDS
            ),
        )
        adapter = PAGINATED_SOURCE_ADAPTERS["arXiv"]
        request_identity_changes = []
        for query, old_spec, new_spec in zip(
            parent_dataset.source_queries,
            old_specs,
            new_specs,
            strict=True,
        ):
            state_zero = {"start": 0}
            old_request = adapter.build_request(old_spec, state_zero)
            new_request = adapter.build_request(new_spec, state_zero)
            if (
                query.query_text != old_spec.query_text
                or query.query_version != old_spec.query_version
                or query.metadata.get("production_query_id")
                != old_spec.metadata["production_query_id"]
                or query.metadata.get("frozen_request_specification_hash")
                != old_spec.metadata["frozen_request_specification_hash"]
                or old_request.timeout
                != ARXIV_LEGACY_READ_TIMEOUT_SECONDS
                or new_request.timeout
                != ARXIV_RECOVERED_READ_TIMEOUT_SECONDS
                or old_request.params != new_request.params
                or old_request.url != new_request.url
                or old_request.state != new_request.state
                or old_request.request_hash() == new_request.request_hash()
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv timeout recovery request identity is not a "
                    "timeout-only change"
                )
            request_identity_changes.append(
                {
                    "production_query_id": old_spec.metadata[
                        "production_query_id"
                    ],
                    "query_id": query.query_id,
                    "request_state": state_zero,
                    "max_results": 2000,
                    "old_request_hash": old_request.request_hash(),
                    "new_request_hash": new_request.request_hash(),
                    "old_timeout_seconds": old_request.timeout,
                    "new_timeout_seconds": new_request.timeout,
                }
            )

        historical_files: dict[Path, bytes] = {}
        for episode in episodes:
            reference = episode.get("checkpoint_dataset")
            if not isinstance(reference, dict):
                raise ExternalRetrievalWaveError(
                    "arXiv historical episode lacks checkpoint provenance"
                )
            checkpoint = _safe_output_path(
                root_path, str(reference.get("path"))
            )
            _verify_file_reference(checkpoint, reference, root_path)
            for artifact in checkpoint.parent.rglob("*"):
                if artifact.is_file():
                    historical_files.setdefault(artifact, artifact.read_bytes())

        recovery_checkpoint_relative = (
            f"{EXECUTION_ROOT}/arXiv/episodes/episode-004/checkpoint"
        )
        recovery_checkpoint_dir = _safe_output_path(
            root_path, recovery_checkpoint_relative
        )
        if recovery_checkpoint_dir.exists():
            raise ExternalRetrievalWaveError(
                "arXiv episode-4 checkpoint exists without valid state lineage"
            )
        other_sources_before = {
            key: json.loads(json.dumps(value, sort_keys=True))
            for key, value in state["sources"].items()
            if key != "arXiv"
        }
        recovered_at = timestamp()
        run = parent_dataset.retrieval_runs[0]
        parent_dataset.retrieval_pages = []
        parent_dataset.retrieval_attempts = []
        parent_dataset.occurrences = []
        parent_dataset.canonical_records = []
        parent_dataset.duplicate_decisions = []
        run.query_plan_hash = _query_plan_hash(new_specs)
        run.retrieval_started_at = recovered_at
        run.retrieval_completed_at = recovered_at
        run.retrieval_cutoff_date = None
        run.status = ProcessingStatus.PARTIAL
        run.completion_status = RetrievalCompletionStatus.RUNNING
        run.errors = [
            "offline arXiv transport-policy recovery complete; live resume pending"
        ]
        run.metadata.pop("pause_state", None)
        run.metadata.pop("pause_reason", None)
        run.metadata.pop("pause_metadata", None)
        run.metadata.pop("session_request_count", None)
        transport_policy = {
            "read_timeout_seconds": ARXIV_RECOVERED_READ_TIMEOUT_SECONDS,
            "maximum_attempts_per_invocation": (
                ARXIV_MAX_ATTEMPTS_PER_INVOCATION
            ),
        }
        source_episode_bindings = [
            *prior_source_episodes,
            {
                "episode_number": 3,
                "checkpoint_dataset": dict(expected_parent_reference),
                "attempt_manifest_hash": parent_validation[
                    "attempt_manifest_hash"
                ],
                "raw_response_manifest_hash": _hash_payload(
                    {
                        "responses": parent_validation[
                            "persisted_response_hashes"
                        ]
                    }
                ),
            },
        ]
        provenance = {
            "recovery_episode_number": 4,
            "parent_episode_number": 3,
            "parent_checkpoint_dataset": dict(expected_parent_reference),
            "parent_attempt_manifest_hash": parent_validation[
                "attempt_manifest_hash"
            ],
            "parent_response_hashes": parent_validation[
                "persisted_response_hashes"
            ],
            "source_episodes": source_episode_bindings,
            "old_transport_policy": {
                "read_timeout_seconds": ARXIV_LEGACY_READ_TIMEOUT_SECONDS,
                "maximum_attempts_per_invocation": (
                    ARXIV_MAX_ATTEMPTS_PER_INVOCATION
                ),
            },
            "new_transport_policy": transport_policy,
            "parent_query_plan_hash": _query_plan_hash(old_specs),
            "new_query_plan_hash": _query_plan_hash(new_specs),
            "request_identity_changes": request_identity_changes,
            "network_used": False,
        }
        run.metadata["offline_arxiv_transport_policy_recovery"] = provenance
        for query, new_spec, identity in zip(
            parent_dataset.source_queries,
            new_specs,
            request_identity_changes,
            strict=True,
        ):
            query.retrieval_started_at = recovered_at
            query.retrieval_ended_at = recovered_at
            query.status = ProcessingStatus.PARTIAL
            query.completion_status = RetrievalCompletionStatus.PLANNED
            query.page = None
            query.cursor = None
            query.result_count = 0
            query.errors = []
            query.page_ids = []
            query.source_reported_total = None
            query.total_is_exact = False
            query.completion_proof = None
            query.metadata.pop("pause_state", None)
            query.metadata.pop("pause_reason", None)
            query.metadata.pop("pause_metadata", None)
            query.metadata["request_timeout_seconds"] = (
                new_spec.metadata["request_timeout_seconds"]
            )
            query.metadata["offline_arxiv_transport_policy_recovery"] = {
                "parent_episode_number": 3,
                "request_state": dict(identity["request_state"]),
                "old_request_hash": identity["old_request_hash"],
                "new_request_hash": identity["new_request_hash"],
            }
        parent_dataset.validate()

        recovery_store = CheckpointStore(recovery_checkpoint_dir)
        checkpoint_hash = recovery_store.save_dataset(parent_dataset)
        recovery_reference = _file_reference(
            recovery_store.dataset_path, root_path
        )
        if checkpoint_hash != recovery_reference["raw_sha256"]:
            raise ExternalRetrievalWaveError(
                "arXiv transport-policy recovery checkpoint hash disagreement"
            )
        if any(path.read_bytes() != content for path, content in historical_files.items()):
            raise ExternalRetrievalWaveError(
                "arXiv historical evidence changed during transport-policy recovery"
            )

        episode_3["immutable"] = True
        episode_4 = {
            "episode_number": 4,
            "episode_id": "arXiv-episode-004",
            "run_id": run.run_id,
            "status": ARXIV_TRANSPORT_POLICY_RECOVERY_STATUS,
            "recovery_of_episode_number": 3,
            "authorization_reason": "OFFLINE_ARXIV_TRANSPORT_POLICY_RECOVERY",
            "authorized_at_utc": recovered_at,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_reference,
            "frozen_wave_manifest_hash": wave.manifest_hash(),
            "frozen_query_plan_hash": wave.query_plan_hash,
            "parent_checkpoint_dataset": dict(expected_parent_reference),
            "parent_attempt_manifest_hash": parent_validation[
                "attempt_manifest_hash"
            ],
            "parent_response_hashes": parent_validation[
                "persisted_response_hashes"
            ],
            "parent_state_reconciliation": dict(reconciliation),
            "source_episodes": source_episode_bindings,
            "old_transport_policy": provenance["old_transport_policy"],
            "transport_policy": transport_policy,
            "parent_query_plan_hash": provenance["parent_query_plan_hash"],
            "query_plan_hash": provenance["new_query_plan_hash"],
            "request_identity_changes": request_identity_changes,
            "restart_states": [
                {
                    "production_query_id": item["production_query_id"],
                    "query_id": item["query_id"],
                    "request_state": dict(item["request_state"]),
                    "max_results": item["max_results"],
                    "request_hash": item["new_request_hash"],
                }
                for item in request_identity_changes
            ],
            "network_used": False,
            "immutable": False,
        }
        source_state["execution_episodes"].append(episode_4)
        source_state.update(
            {
                "status": ARXIV_TRANSPORT_POLICY_RECOVERY_STATUS,
                "active_episode_number": 4,
                "active_run_id": run.run_id,
                "active_checkpoint_path": recovery_checkpoint_relative,
                "checkpoint_path": recovery_checkpoint_relative,
                "checkpoint_dataset": recovery_reference,
                "completed_query_count": 0,
                "total_query_count": 5,
                "occurrence_count": 0,
                "attempt_count": 0,
                "preserved_source_attempt_count": 32,
                "preserved_source_raw_response_count": 9,
                "requests_this_session": 0,
                "pause_reason": (
                    "OFFLINE_TRANSPORT_POLICY_RECOVERY_COMPLETE; "
                    "LIVE_RESUME_REQUIRED"
                ),
                "failure_reason": None,
                "last_session_started_at_utc": recovered_at,
                "last_session_completed_at_utc": recovered_at,
                "transport_policy": transport_policy,
            }
        )
        source_state.pop("pause_metadata", None)
        if {
            key: value
            for key, value in state["sources"].items()
            if key != "arXiv"
        } != other_sources_before:
            raise ExternalRetrievalWaveError(
                "arXiv transport-policy recovery changed another source"
            )
        state["status"] = "RUNNING"
        state["external_retrieval_completed_at_utc"] = None
        state["external_retrieval_cutoff_date"] = None
        _save_execution_state(state_path, state)
        if any(path.read_bytes() != content for path, content in historical_files.items()):
            raise ExternalRetrievalWaveError(
                "arXiv historical evidence changed after state persistence"
            )
        return state


def _validate_failed_arxiv_retryable_5xx_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    wave: ProductionRetrievalWave,
) -> dict[str, Any]:
    """Validate the exact response-backed episode-4 arXiv failure."""

    dataset.validate()
    specs = _source_query_specs(
        wave,
        "arXiv",
        ieee_credential="",
        arxiv_read_timeout_seconds=ARXIV_RECOVERED_READ_TIMEOUT_SECONDS,
    )
    signatures = ARXIV_EPISODE_4_FAILURE_SIGNATURES
    run = dataset.retrieval_runs[0] if len(dataset.retrieval_runs) == 1 else None
    if (
        run is None
        or run.run_id != f"{WAVE_ID}:arXiv"
        or run.status is not ProcessingStatus.FAILED
        or run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.query_plan_hash != _query_plan_hash(specs)
        or len(dataset.source_queries) != 5
        or len(dataset.retrieval_pages) != 5
        or len(dataset.retrieval_attempts) != 19
        or dataset.occurrences
        or dataset.canonical_records
        or dataset.duplicate_decisions
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-4 retryable-5xx failure shape changed"
        )
    attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": dataset.to_dict()["retrieval_attempts"]}
    )
    if attempt_manifest_hash != ARXIV_EPISODE_4_ATTEMPT_MANIFEST_SHA256:
        raise ExternalRetrievalWaveError(
            "arXiv episode-4 19-attempt manifest changed"
        )

    pages = {item.source_query_id: item for item in dataset.retrieval_pages}
    attempts = {item.attempt_id: item for item in dataset.retrieval_attempts}
    adapter = PAGINATED_SOURCE_ADAPTERS["arXiv"]
    response_statuses: list[int] = []
    response_bindings: list[dict[str, Any]] = []
    restart_states: list[dict[str, Any]] = []
    terminal_statuses: list[int] = []
    store = CheckpointStore(checkpoint_dir)
    for query, spec, signature in zip(
        dataset.source_queries, specs, signatures, strict=True
    ):
        production_id, query_id, page_id, request_hash, terminal_status = signature
        page = pages.get(query_id)
        request = adapter.build_request(spec, {"start": 0})
        if (
            query.query_id != query_id
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id") != production_id
            or query.filters != {"page_size": 2000}
            or query.metadata.get("request_timeout_seconds")
            != ARXIV_RECOVERED_READ_TIMEOUT_SECONDS
            or query.status is not ProcessingStatus.FAILED
            or query.completion_status is not RetrievalCompletionStatus.FAILED
            or query.result_count != 0
            or query.page_ids != [page_id]
            or query.source_reported_total is not None
            or page is None
            or page.page_id != page_id
            or page.ordinal != 0
            or page.request_state != {"start": 0}
            or page.status is not RetrievalCompletionStatus.FAILED
            or page.returned_item_count != 0
            or page.occurrence_ids
            or page.native_identifiers
            or page.next_state is not None
            or page.terminal
            or request.timeout != ARXIV_RECOVERED_READ_TIMEOUT_SECONDS
            or request.params.get("start") != 0
            or request.params.get("max_results") != 2000
            or request.request_hash() != request_hash
            or not page.attempt_ids
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-4 query/page/request identity changed"
            )
        page_attempts = [attempts[item] for item in page.attempt_ids]
        if any(
            attempt.status is not RetrievalAttemptStatus.FAILED
            or attempt.request_hash != request_hash
            or attempt.request_params != request.params
            or attempt.request_method != request.method
            or attempt.request_url != request.url
            or attempt.raw_response_path is None
            or attempt.raw_response_hash is None
            or attempt.response_status not in {429, 500, 503}
            for attempt in page_attempts
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-4 request or response evidence changed"
            )
        for attempt in page_attempts:
            response = store.load_response(
                attempt.raw_response_path, attempt.raw_response_hash
            )
            if response.status_code != attempt.response_status:
                raise ExternalRetrievalWaveError(
                    "arXiv episode-4 persisted response status changed"
                )
            response_statuses.append(response.status_code)
            response_bindings.append(
                {
                    "attempt_id": attempt.attempt_id,
                    "path": attempt.raw_response_path,
                    "raw_sha256": attempt.raw_response_hash,
                    "http_status": response.status_code,
                }
            )
        final_status = page_attempts[-1].response_status
        terminal_statuses.append(final_status)
        if final_status != terminal_status:
            raise ExternalRetrievalWaveError(
                "arXiv episode-4 terminal HTTP statuses changed"
            )
        restart_states.append(
            {
                "production_query_id": production_id,
                "query_id": query_id,
                "page_id": page_id,
                "request_state": {"start": 0},
                "max_results": 2000,
                "request_hash": request_hash,
                "terminal_http_status": terminal_status,
            }
        )
    if (
        Counter(response_statuses) != Counter({429: 4, 500: 12, 503: 3})
        or tuple(terminal_statuses) != (500, 500, 503, 500, 500)
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-4 response-status manifest changed"
        )
    return {
        "attempt_manifest_hash": attempt_manifest_hash,
        "raw_response_bindings": response_bindings,
        "raw_response_manifest_hash": _hash_payload(
            {"responses": response_bindings}
        ),
        "restart_states": restart_states,
        "terminal_http_statuses": terminal_statuses,
    }


def _arxiv_retryable_5xx_recovery_active(
    source_state: Mapping[str, Any],
) -> bool:
    return (
        source_state.get("active_episode_number") == 5
        and any(
            item.get("episode_number") == 5
            and item.get("authorization_reason")
            == "OFFLINE_ARXIV_RETRYABLE_5XX_RECOVERY"
            for item in source_state.get("execution_episodes", [])
        )
    )


def _validate_authorized_arxiv_retryable_5xx_recovery(
    *,
    root: Path,
    source_state: Mapping[str, Any],
    wave: ProductionRetrievalWave,
    allow_descendant: bool = False,
) -> dict[str, Any]:
    """Revalidate episode-5 provenance before authorization reuse or resume."""

    all_episodes = source_state.get("execution_episodes", [])
    episodes = all_episodes[:5]
    if (
        (len(all_episodes) != 6 if allow_descendant else len(all_episodes) != 5)
        or [item.get("episode_number") for item in episodes] != [
        1,
        2,
        3,
        4,
        5,
        ]
    ):
        raise ExternalRetrievalWaveError("arXiv episode-5 lineage changed")
    for historical in episodes[:4]:
        reference = historical.get("checkpoint_dataset")
        if not isinstance(reference, dict) or historical.get("immutable") is not True:
            raise ExternalRetrievalWaveError(
                "arXiv historical episode provenance changed"
            )
        checkpoint = _safe_output_path(root, str(reference.get("path")))
        _verify_file_reference(checkpoint, reference, root)
    episode_4 = episodes[3]
    expected_parent_reference = {
        "path": (
            f"{EXECUTION_ROOT}/arXiv/episodes/episode-004/checkpoint/"
            "review_dataset.json"
        ),
        "byte_size": ARXIV_EPISODE_4_FAILED_CHECKPOINT_SIZE,
        "raw_sha256": ARXIV_EPISODE_4_FAILED_CHECKPOINT_SHA256,
    }
    if (
        episode_4.get("episode_id") != "arXiv-episode-004"
        or episode_4.get("status") != "FAILED"
        or episode_4.get("authorization_reason")
        != "OFFLINE_ARXIV_TRANSPORT_POLICY_RECOVERY"
        or episode_4.get("checkpoint_dataset") != expected_parent_reference
        or episode_4.get("attempt_count") != 19
        or episode_4.get("occurrence_count") != 0
        or episode_4.get("completed_query_count") != 0
    ):
        raise ExternalRetrievalWaveError("arXiv failed episode-4 lineage changed")
    parent_checkpoint = _safe_output_path(root, expected_parent_reference["path"])
    parent_validation = _validate_failed_arxiv_retryable_5xx_checkpoint(
        dataset=load_review_dataset(parent_checkpoint),
        checkpoint_dir=parent_checkpoint.parent,
        wave=wave,
    )
    parent_episode_manifest_hash = _hash_payload(
        {"execution_episodes": episodes[:4]}
    )
    episode_5 = episodes[4]
    transport_policy = {
        "read_timeout_seconds": ARXIV_RECOVERED_READ_TIMEOUT_SECONDS,
        "maximum_attempts_per_invocation": ARXIV_MAX_ATTEMPTS_PER_INVOCATION,
    }
    expected_provenance = {
        "recovery_episode_number": 5,
        "parent_episode_number": 4,
        "parent_checkpoint_dataset": expected_parent_reference,
        "parent_episode_manifest_hash": parent_episode_manifest_hash,
        "parent_attempt_manifest_hash": parent_validation[
            "attempt_manifest_hash"
        ],
        "parent_raw_response_manifest_hash": parent_validation[
            "raw_response_manifest_hash"
        ],
        "parent_raw_responses": parent_validation["raw_response_bindings"],
        "terminal_http_statuses": parent_validation[
            "terminal_http_statuses"
        ],
        "restart_states": parent_validation["restart_states"],
        "historical_lineage_counts": {
            "preserved_before_episode_4_attempts": 32,
            "preserved_before_episode_4_raw_responses": 9,
            "episode_4_attempts": 19,
            "episode_4_raw_responses": 19,
            "preserved_total_attempts": 51,
            "preserved_total_raw_responses": 28,
            "accepted_occurrences": 0,
        },
        "transport_policy": transport_policy,
        "network_used": False,
    }
    checkpoint_reference = episode_5.get("checkpoint_dataset")
    if not isinstance(checkpoint_reference, dict):
        raise ExternalRetrievalWaveError("arXiv episode-5 checkpoint binding is absent")
    checkpoint = _safe_output_path(root, str(checkpoint_reference.get("path")))
    _verify_file_reference(checkpoint, checkpoint_reference, root)
    dataset = load_review_dataset(checkpoint)
    dataset.validate()
    specs = _source_query_specs(
        wave,
        "arXiv",
        ieee_credential="",
        arxiv_read_timeout_seconds=ARXIV_RECOVERED_READ_TIMEOUT_SECONDS,
    )
    run = dataset.retrieval_runs[0] if len(dataset.retrieval_runs) == 1 else None
    if (
        episode_5.get("episode_id") != "arXiv-episode-005"
        or episode_5.get("authorization_reason")
        != "OFFLINE_ARXIV_RETRYABLE_5XX_RECOVERY"
        or episode_5.get("recovery_of_episode_number") != 4
        or episode_5.get("parent_checkpoint_dataset")
        != expected_parent_reference
        or episode_5.get("recovery_provenance") != expected_provenance
        or episode_5.get("transport_policy") != transport_policy
        or episode_5.get("network_used") is not False
        or (
            not allow_descendant
            and (
                source_state.get("active_episode_number") != 5
                or source_state.get("active_run_id") != f"{WAVE_ID}:arXiv"
                or source_state.get("active_checkpoint_path")
                != episode_5.get("checkpoint_path")
                or source_state.get("checkpoint_path")
                != episode_5.get("checkpoint_path")
                or source_state.get("checkpoint_dataset") != checkpoint_reference
                or source_state.get("transport_policy") != transport_policy
                or source_state.get("preserved_source_attempt_count") != 51
                or source_state.get("preserved_source_raw_response_count") != 28
                or source_state.get("attempt_count")
                != len(dataset.retrieval_attempts)
                or source_state.get("occurrence_count") != len(dataset.occurrences)
                or source_state.get("completed_query_count")
                != sum(
                    item.completion_status is RetrievalCompletionStatus.COMPLETE
                    for item in dataset.source_queries
                )
            )
        )
        or run is None
        or run.run_id != f"{WAVE_ID}:arXiv"
        or run.query_plan_hash != _query_plan_hash(specs)
        or run.metadata.get("offline_arxiv_retryable_5xx_recovery")
        != expected_provenance
        or len(dataset.source_queries) != 5
    ):
        raise ExternalRetrievalWaveError(
            "authorized arXiv episode-5 recovery provenance changed"
        )
    for query, spec, restart in zip(
        dataset.source_queries,
        specs,
        parent_validation["restart_states"],
        strict=True,
    ):
        request = PAGINATED_SOURCE_ADAPTERS["arXiv"].build_request(
            spec, {"start": 0}
        )
        if (
            query.query_id != restart["query_id"]
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != restart["production_query_id"]
            or query.filters != {"page_size": 2000}
            or query.metadata.get("request_timeout_seconds")
            != ARXIV_RECOVERED_READ_TIMEOUT_SECONDS
            or request.request_hash() != restart["request_hash"]
            or query.metadata.get("offline_arxiv_retryable_5xx_recovery")
            != {
                "parent_episode_number": 4,
                "request_state": {"start": 0},
                "request_hash": restart["request_hash"],
            }
        ):
            raise ExternalRetrievalWaveError(
                "authorized arXiv episode-5 query provenance changed"
            )
    return {
        "dataset": dataset,
        "checkpoint": checkpoint,
        "checkpoint_reference": checkpoint_reference,
        "episode": episode_5,
        "specs": specs,
        "transport_policy": transport_policy,
        "recovery_provenance": expected_provenance,
    }


def authorize_arxiv_retryable_5xx_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Create an opt-in episode 5 for the verified retryable-5xx failure."""

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        wave, preflight = validate_persisted_external_preflight(root=root_path)
        state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
        if not state_path.is_file():
            raise ExternalRetrievalWaveError("external execution state does not exist")
        state = _load_execution_state(state_path, root_path, wave, preflight)
        if any(item.get("status") == "RUNNING" for item in state["sources"].values()):
            raise ExternalRetrievalWaveError(
                "cannot recover while an external-source session is marked RUNNING"
            )
        if state.get("external_retrieval_cutoff_date") is not None:
            raise ExternalRetrievalWaveError(
                "arXiv retryable-5xx recovery cannot alter a closed wave"
            )
        source_state = state["sources"]["arXiv"]
        if _arxiv_retryable_5xx_recovery_active(source_state):
            _validate_authorized_arxiv_retryable_5xx_recovery(
                root=root_path, source_state=source_state, wave=wave
            )
            return state
        episodes = source_state.get("execution_episodes", [])
        expected_parent_reference = {
            "path": (
                f"{EXECUTION_ROOT}/arXiv/episodes/episode-004/checkpoint/"
                "review_dataset.json"
            ),
            "byte_size": ARXIV_EPISODE_4_FAILED_CHECKPOINT_SIZE,
            "raw_sha256": ARXIV_EPISODE_4_FAILED_CHECKPOINT_SHA256,
        }
        if (
            source_state.get("status") != "FAILED"
            or source_state.get("active_episode_number") != 4
            or source_state.get("active_run_id") != f"{WAVE_ID}:arXiv"
            or source_state.get("checkpoint_dataset") != expected_parent_reference
            or source_state.get("attempt_count") != 19
            or source_state.get("occurrence_count") != 0
            or source_state.get("completed_query_count") != 0
            or source_state.get("total_query_count") != 5
            or source_state.get("preserved_source_attempt_count") != 32
            or source_state.get("preserved_source_raw_response_count") != 9
            or len(episodes) != 4
        ):
            raise ExternalRetrievalWaveError(
                "arXiv retryable-5xx recovery requires the exact failed episode 4"
            )
        episode_4 = episodes[3]
        if (
            episode_4.get("episode_number") != 4
            or episode_4.get("episode_id") != "arXiv-episode-004"
            or episode_4.get("status") != "FAILED"
            or episode_4.get("immutable") is not True
            or episode_4.get("authorization_reason")
            != "OFFLINE_ARXIV_TRANSPORT_POLICY_RECOVERY"
            or episode_4.get("checkpoint_dataset") != expected_parent_reference
            or episode_4.get("transport_policy")
            != {
                "read_timeout_seconds": ARXIV_RECOVERED_READ_TIMEOUT_SECONDS,
                "maximum_attempts_per_invocation": (
                    ARXIV_MAX_ATTEMPTS_PER_INVOCATION
                ),
            }
        ):
            raise ExternalRetrievalWaveError("arXiv episode-4 provenance changed")
        parent_checkpoint = _safe_output_path(
            root_path, expected_parent_reference["path"]
        )
        _verify_file_reference(parent_checkpoint, expected_parent_reference, root_path)
        parent_dataset = load_review_dataset(parent_checkpoint)
        parent_validation = _validate_failed_arxiv_retryable_5xx_checkpoint(
            dataset=parent_dataset,
            checkpoint_dir=parent_checkpoint.parent,
            wave=wave,
        )
        transport_recovery = parent_dataset.retrieval_runs[0].metadata.get(
            "offline_arxiv_transport_policy_recovery", {}
        )
        if (
            transport_recovery.get("recovery_episode_number") != 4
            or transport_recovery.get("network_used") is not False
            or transport_recovery.get("new_transport_policy")
            != episode_4["transport_policy"]
            or [
                item.get("new_request_hash")
                for item in transport_recovery.get("request_identity_changes", [])
            ]
            != [item[3] for item in ARXIV_EPISODE_4_FAILURE_SIGNATURES]
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-4 transport-policy provenance changed"
            )

        historical_files: dict[Path, bytes] = {}
        for episode in episodes:
            reference = episode.get("checkpoint_dataset")
            if not isinstance(reference, dict):
                raise ExternalRetrievalWaveError(
                    "arXiv historical episode lacks checkpoint provenance"
                )
            checkpoint = _safe_output_path(root_path, str(reference.get("path")))
            _verify_file_reference(checkpoint, reference, root_path)
            for artifact in checkpoint.parent.rglob("*"):
                if artifact.is_file():
                    historical_files.setdefault(artifact, artifact.read_bytes())
        parent_episode_manifest_hash = _hash_payload(
            {"execution_episodes": episodes}
        )
        recovery_checkpoint_relative = (
            f"{EXECUTION_ROOT}/arXiv/episodes/episode-005/checkpoint"
        )
        recovery_checkpoint_dir = _safe_output_path(
            root_path, recovery_checkpoint_relative
        )
        if recovery_checkpoint_dir.exists():
            raise ExternalRetrievalWaveError(
                "arXiv episode-5 checkpoint exists without valid state lineage"
            )
        other_sources_before = {
            key: json.loads(json.dumps(value, sort_keys=True))
            for key, value in state["sources"].items()
            if key != "arXiv"
        }
        recovered_at = timestamp()
        run = parent_dataset.retrieval_runs[0]
        parent_dataset.retrieval_pages = []
        parent_dataset.retrieval_attempts = []
        parent_dataset.occurrences = []
        parent_dataset.canonical_records = []
        parent_dataset.duplicate_decisions = []
        run.retrieval_started_at = recovered_at
        run.retrieval_completed_at = recovered_at
        run.retrieval_cutoff_date = None
        run.status = ProcessingStatus.PARTIAL
        run.completion_status = RetrievalCompletionStatus.RUNNING
        run.errors = [
            "offline arXiv retryable-5xx recovery complete; live resume pending"
        ]
        for key in (
            "pause_state",
            "pause_reason",
            "pause_metadata",
            "session_request_count",
        ):
            run.metadata.pop(key, None)
        transport_policy = dict(episode_4["transport_policy"])
        provenance = {
            "recovery_episode_number": 5,
            "parent_episode_number": 4,
            "parent_checkpoint_dataset": expected_parent_reference,
            "parent_episode_manifest_hash": parent_episode_manifest_hash,
            "parent_attempt_manifest_hash": parent_validation[
                "attempt_manifest_hash"
            ],
            "parent_raw_response_manifest_hash": parent_validation[
                "raw_response_manifest_hash"
            ],
            "parent_raw_responses": parent_validation["raw_response_bindings"],
            "terminal_http_statuses": parent_validation[
                "terminal_http_statuses"
            ],
            "restart_states": parent_validation["restart_states"],
            "historical_lineage_counts": {
                "preserved_before_episode_4_attempts": 32,
                "preserved_before_episode_4_raw_responses": 9,
                "episode_4_attempts": 19,
                "episode_4_raw_responses": 19,
                "preserved_total_attempts": 51,
                "preserved_total_raw_responses": 28,
                "accepted_occurrences": 0,
            },
            "transport_policy": transport_policy,
            "network_used": False,
        }
        run.metadata["offline_arxiv_retryable_5xx_recovery"] = provenance
        for query, restart in zip(
            parent_dataset.source_queries,
            parent_validation["restart_states"],
            strict=True,
        ):
            query.retrieval_started_at = recovered_at
            query.retrieval_ended_at = recovered_at
            query.status = ProcessingStatus.PARTIAL
            query.completion_status = RetrievalCompletionStatus.PLANNED
            query.page = None
            query.cursor = None
            query.result_count = 0
            query.errors = []
            query.page_ids = []
            query.source_reported_total = None
            query.total_is_exact = False
            query.completion_proof = None
            for key in ("pause_state", "pause_reason", "pause_metadata"):
                query.metadata.pop(key, None)
            query.metadata["offline_arxiv_retryable_5xx_recovery"] = {
                "parent_episode_number": 4,
                "request_state": {"start": 0},
                "request_hash": restart["request_hash"],
            }
        parent_dataset.validate()
        recovery_store = CheckpointStore(recovery_checkpoint_dir)
        checkpoint_hash = recovery_store.save_dataset(parent_dataset)
        recovery_reference = _file_reference(
            recovery_store.dataset_path, root_path
        )
        if checkpoint_hash != recovery_reference["raw_sha256"]:
            raise ExternalRetrievalWaveError(
                "arXiv retryable-5xx recovery checkpoint hash disagreement"
            )
        if any(
            path.read_bytes() != content
            for path, content in historical_files.items()
        ):
            raise ExternalRetrievalWaveError(
                "arXiv historical evidence changed during retryable-5xx recovery"
            )
        episode_5 = {
            "episode_number": 5,
            "episode_id": "arXiv-episode-005",
            "run_id": run.run_id,
            "status": ARXIV_RETRYABLE_5XX_RECOVERY_STATUS,
            "recovery_of_episode_number": 4,
            "authorization_reason": "OFFLINE_ARXIV_RETRYABLE_5XX_RECOVERY",
            "authorized_at_utc": recovered_at,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_reference,
            "frozen_wave_manifest_hash": wave.manifest_hash(),
            "frozen_query_plan_hash": wave.query_plan_hash,
            "parent_checkpoint_dataset": expected_parent_reference,
            "transport_policy": transport_policy,
            "recovery_provenance": provenance,
            "network_used": False,
            "immutable": False,
        }
        source_state["execution_episodes"].append(episode_5)
        source_state.update(
            {
                "status": ARXIV_RETRYABLE_5XX_RECOVERY_STATUS,
                "active_episode_number": 5,
                "active_run_id": run.run_id,
                "active_checkpoint_path": recovery_checkpoint_relative,
                "checkpoint_path": recovery_checkpoint_relative,
                "checkpoint_dataset": recovery_reference,
                "completed_query_count": 0,
                "total_query_count": 5,
                "occurrence_count": 0,
                "attempt_count": 0,
                "preserved_source_attempt_count": 51,
                "preserved_source_raw_response_count": 28,
                "requests_this_session": 0,
                "pause_reason": (
                    "OFFLINE_RETRYABLE_5XX_RECOVERY_COMPLETE; "
                    "LIVE_RESUME_REQUIRED"
                ),
                "failure_reason": None,
                "last_session_started_at_utc": recovered_at,
                "last_session_completed_at_utc": recovered_at,
                "transport_policy": transport_policy,
            }
        )
        source_state.pop("pause_metadata", None)
        if {
            key: value
            for key, value in state["sources"].items()
            if key != "arXiv"
        } != other_sources_before:
            raise ExternalRetrievalWaveError(
                "arXiv retryable-5xx recovery changed another source"
            )
        state["status"] = "RUNNING"
        state["external_retrieval_completed_at_utc"] = None
        state["external_retrieval_cutoff_date"] = None
        _save_execution_state(state_path, state)
        if any(
            path.read_bytes() != content
            for path, content in historical_files.items()
        ):
            raise ExternalRetrievalWaveError(
                "arXiv historical evidence changed after state persistence"
            )
        _validate_authorized_arxiv_retryable_5xx_recovery(
            root=root_path, source_state=source_state, wave=wave
        )
        return state


def _arxiv_page_size_recovery_active(source_state: Mapping[str, Any]) -> bool:
    return (
        source_state.get("active_episode_number") == 6
        and any(
            item.get("episode_number") == 6
            and item.get("authorization_reason")
            == "OFFLINE_ARXIV_PAGE_SIZE_RECOVERY"
            for item in source_state.get("execution_episodes", [])
        )
    )


def _verify_fixed_repository_file(
    *,
    root: Path,
    relative_path: str,
    byte_size: int,
    raw_sha256: str,
) -> tuple[Path, dict[str, Any]]:
    path = (root / relative_path).resolve()
    try:
        relative = path.relative_to(root).as_posix()
    except ValueError as exc:
        raise ExternalRetrievalWaveError(
            "bound repository evidence escaped the repository root"
        ) from exc
    reference = {
        "path": relative,
        "byte_size": byte_size,
        "raw_sha256": raw_sha256,
    }
    if relative != relative_path or not path.is_file():
        raise ExternalRetrievalWaveError("bound repository evidence is absent")
    raw = path.read_bytes()
    if len(raw) != byte_size or _sha256(raw) != raw_sha256:
        raise ExternalRetrievalWaveError("bound repository evidence changed")
    return path, reference


def _validate_arxiv_qf01_page_size_diagnostic(
    *,
    root: Path,
    wave: ProductionRetrievalWave,
    parent_reference: Mapping[str, Any],
    qf01_query_id: str,
) -> dict[str, Any]:
    manifest_path, manifest_reference = _verify_fixed_repository_file(
        root=root,
        relative_path=ARXIV_QF01_DIAGNOSTIC_MANIFEST_PATH,
        byte_size=ARXIV_QF01_DIAGNOSTIC_MANIFEST_SIZE,
        raw_sha256=ARXIV_QF01_DIAGNOSTIC_MANIFEST_SHA256,
    )
    manifest = _load_json(manifest_path)
    attempts = manifest.get("attempts")
    attempt = attempts[0] if isinstance(attempts, list) and len(attempts) == 1 else None
    old_spec = _source_query_specs(
        wave,
        "arXiv",
        ieee_credential="",
        arxiv_read_timeout_seconds=ARXIV_RECOVERED_READ_TIMEOUT_SECONDS,
    )[0]
    diagnostic_spec = RetrievalQuerySpec(
        source_database=old_spec.source_database,
        query_text=old_spec.query_text,
        query_version=old_spec.query_version,
        limit=1,
        endpoint=old_spec.endpoint,
        fields=list(old_spec.fields),
        filters=dict(old_spec.filters),
        metadata=dict(old_spec.metadata),
        pagination_mode=old_spec.pagination_mode,
    )
    request = PAGINATED_SOURCE_ADAPTERS["arXiv"].build_request(
        diagnostic_spec, {"start": 0}
    )
    expected_request = {
        "method": request.method,
        "endpoint": request.url,
        "params": request.params,
        "headers": request.headers,
        "timeout_seconds": request.timeout,
        "request_hash": request.request_hash(),
        "source_query_id": qf01_query_id,
    }
    production_binding = manifest.get("production_binding")
    integrity = manifest.get("production_integrity")
    if (
        manifest.get("diagnostic_mode") != "single-qf01"
        or manifest.get("status") != "COMPLETE"
        or manifest.get("request_budget") != 1
        or manifest.get("requests_made") != 1
        or manifest.get("automatic_retries") is not False
        or manifest.get("production_eligible") is not False
        or manifest.get("production_records_created") is not False
        or manifest.get("prisma_counted") is not False
        or not isinstance(attempt, dict)
        or attempt.get("probe_id")
        != "checkpointed-production-qf01-small-page"
        or attempt.get("http_status") != 200
        or attempt.get("transport_exception") is not None
        or attempt.get("request") != expected_request
        or production_binding
        != {
            "source_checkpoint": dict(parent_reference),
            "production_run_id": f"{WAVE_ID}:arXiv",
            "production_query_id": old_spec.metadata["production_query_id"],
            "source_query_id": qf01_query_id,
        }
        or not isinstance(integrity, dict)
        or integrity.get("unchanged") is not True
        or integrity.get("changed_paths") != []
        or integrity.get("before") != integrity.get("after")
    ):
        raise ExternalRetrievalWaveError(
            "supporting arXiv QF01 diagnostic provenance changed"
        )
    raw_reference = attempt.get("raw_response")
    if not isinstance(raw_reference, dict):
        raise ExternalRetrievalWaveError(
            "supporting arXiv QF01 diagnostic response binding is absent"
        )
    response_path = (manifest_path.parent / str(raw_reference.get("path"))).resolve()
    try:
        response_path.relative_to(manifest_path.parent)
    except ValueError as exc:
        raise ExternalRetrievalWaveError(
            "supporting arXiv QF01 diagnostic response escaped its namespace"
        ) from exc
    raw_response = response_path.read_bytes() if response_path.is_file() else b""
    if _sha256(raw_response) != raw_reference.get("raw_sha256"):
        raise ExternalRetrievalWaveError(
            "supporting arXiv QF01 diagnostic response hash changed"
        )
    response_payload = json.loads(raw_response)
    try:
        content = base64.b64decode(response_payload["content_base64"], validate=True)
    except (KeyError, TypeError, ValueError) as exc:
        raise ExternalRetrievalWaveError(
            "supporting arXiv QF01 diagnostic response encoding changed"
        ) from exc
    parsed = PAGINATED_SOURCE_ADAPTERS["arXiv"].parse_response(
        diagnostic_spec,
        {"start": 0},
        SimpleNamespace(content=content),
    )
    if (
        response_payload.get("status_code") != 200
        or _sha256(content) != attempt.get("response_body_sha256")
        or parsed.raw_item_count != 1
        or len(parsed.records) != 1
        or parsed.metadata.get("items_per_page") != 1
        or parsed.source_reported_total is None
    ):
        raise ExternalRetrievalWaveError(
            "supporting arXiv QF01 diagnostic success evidence changed"
        )
    return {
        "manifest": manifest_reference,
        "raw_response": {
            "path": response_path.relative_to(root).as_posix(),
            "raw_sha256": raw_reference["raw_sha256"],
        },
        "request_hash": request.request_hash(),
        "http_status": 200,
        "valid_entry_count": 1,
        "source_reported_total": parsed.source_reported_total,
        "items_per_page": 1,
        "production_eligible": False,
        "prisma_counted": False,
        "page_size_causality": "UNPROVEN",
    }


def _validate_arxiv_episode_5_page_size_parent(
    *,
    root: Path,
    source_state: Mapping[str, Any],
    wave: ProductionRetrievalWave,
    allow_descendant: bool = False,
) -> dict[str, Any]:
    lineage = _validate_authorized_arxiv_retryable_5xx_recovery(
        root=root,
        source_state=source_state,
        wave=wave,
        allow_descendant=allow_descendant,
    )
    reference = {
        "path": (
            f"{EXECUTION_ROOT}/arXiv/episodes/episode-005/checkpoint/"
            "review_dataset.json"
        ),
        "byte_size": ARXIV_EPISODE_5_PARENT_CHECKPOINT_SIZE,
        "raw_sha256": ARXIV_EPISODE_5_PARENT_CHECKPOINT_SHA256,
    }
    episode = lineage["episode"]
    dataset = lineage["dataset"]
    run = dataset.retrieval_runs[0]
    page = dataset.retrieval_pages[0] if len(dataset.retrieval_pages) == 1 else None
    qf01 = dataset.source_queries[0]
    old_request = PAGINATED_SOURCE_ADAPTERS["arXiv"].build_request(
        lineage["specs"][0], {"start": 0}
    )
    if (
        episode.get("checkpoint_dataset") != reference
        or episode.get("status") != "PAUSED_TRANSIENT_PROVIDER"
        or episode.get("attempt_count") != 6
        or episode.get("occurrence_count") != 0
        or episode.get("completed_query_count") != 0
        or run.status is not ProcessingStatus.PARTIAL
        or run.completion_status is not RetrievalCompletionStatus.RUNNING
        or run.errors != ["RETRYABLE_PROVIDER_5XX_EXHAUSTED"]
        or len(dataset.retrieval_attempts) != 6
        or dataset.occurrences
        or dataset.canonical_records
        or dataset.duplicate_decisions
        or page is None
        or page.source_query_id != qf01.query_id
        or page.ordinal != 0
        or page.request_state != {"start": 0}
        or page.status is not RetrievalCompletionStatus.RUNNING
        or page.returned_item_count != 0
        or page.occurrence_ids
        or page.native_identifiers
        or page.next_state is not None
        or page.terminal
        or page.attempt_ids
        != [attempt.attempt_id for attempt in dataset.retrieval_attempts]
        or qf01.result_count != 0
        or qf01.page_ids != [page.page_id]
        or qf01.source_reported_total is not None
        or any(query.result_count != 0 for query in dataset.source_queries)
        or old_request.params.get("max_results") != ARXIV_LEGACY_PAGE_SIZE
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-5 page-size-recovery parent shape changed"
        )
    attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": dataset.to_dict()["retrieval_attempts"]}
    )
    if attempt_manifest_hash != ARXIV_EPISODE_5_ATTEMPT_MANIFEST_SHA256:
        raise ExternalRetrievalWaveError(
            "arXiv episode-5 six-attempt manifest changed"
        )
    attempts = {item.attempt_id: item for item in dataset.retrieval_attempts}
    page_attempts = [attempts[item] for item in page.attempt_ids]
    response_bindings = []
    response_statuses = []
    store = CheckpointStore(lineage["checkpoint"].parent)
    for attempt in page_attempts:
        if (
            attempt.status is not RetrievalAttemptStatus.FAILED
            or attempt.request_hash != old_request.request_hash()
            or attempt.request_method != old_request.method
            or attempt.request_url != old_request.url
            or attempt.request_params != old_request.params
            or attempt.request_headers != old_request.headers
            or attempt.raw_response_path is None
            or attempt.raw_response_hash is None
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-5 request or response evidence changed"
            )
        response = store.load_response(
            attempt.raw_response_path, attempt.raw_response_hash
        )
        if response.status_code != attempt.response_status:
            raise ExternalRetrievalWaveError(
                "arXiv episode-5 persisted response status changed"
            )
        response_statuses.append(response.status_code)
        response_bindings.append(
            {
                "attempt_id": attempt.attempt_id,
                "path": attempt.raw_response_path,
                "raw_sha256": attempt.raw_response_hash,
                "http_status": response.status_code,
            }
        )
    if tuple(response_statuses) != ARXIV_EPISODE_5_HTTP_STATUSES:
        raise ExternalRetrievalWaveError(
            "arXiv episode-5 HTTP status sequence changed"
        )
    raw_response_manifest_hash = _hash_payload({"responses": response_bindings})
    if raw_response_manifest_hash != ARXIV_EPISODE_5_RAW_RESPONSE_MANIFEST_SHA256:
        raise ExternalRetrievalWaveError(
            "arXiv episode-5 raw-response manifest changed"
        )
    if not allow_descendant and (
        source_state.get("status") != "PAUSED_TRANSIENT_PROVIDER"
        or source_state.get("active_episode_number") != 5
        or source_state.get("checkpoint_dataset") != reference
        or source_state.get("attempt_count") != 6
        or source_state.get("occurrence_count") != 0
        or source_state.get("completed_query_count") != 0
        or source_state.get("preserved_source_attempt_count") != 51
        or source_state.get("preserved_source_raw_response_count") != 28
        or source_state.get("requests_this_session") != 3
        or source_state.get("pause_reason")
        != "RETRYABLE_PROVIDER_5XX_EXHAUSTED"
        or source_state.get("failure_reason") is not None
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-5 execution-state parent binding changed"
        )
    diagnostic = _validate_arxiv_qf01_page_size_diagnostic(
        root=root,
        wave=wave,
        parent_reference=reference,
        qf01_query_id=qf01.query_id,
    )
    return {
        **lineage,
        "checkpoint_reference": reference,
        "attempt_manifest_hash": attempt_manifest_hash,
        "raw_response_bindings": response_bindings,
        "raw_response_manifest_hash": raw_response_manifest_hash,
        "http_statuses": response_statuses,
        "diagnostic": diagnostic,
    }


def _arxiv_page_size_recovery_provenance(
    *,
    source_state: Mapping[str, Any],
    parent_validation: Mapping[str, Any],
    wave: ProductionRetrievalWave,
) -> tuple[dict[str, Any], list[RetrievalQuerySpec]]:
    old_specs = parent_validation["specs"]
    new_specs = _source_query_specs(
        wave,
        "arXiv",
        ieee_credential="",
        arxiv_read_timeout_seconds=ARXIV_RECOVERED_READ_TIMEOUT_SECONDS,
        arxiv_page_size=ARXIV_RECOVERED_PAGE_SIZE,
    )
    identities = []
    for query, old_spec, new_spec in zip(
        parent_validation["dataset"].source_queries,
        old_specs,
        new_specs,
        strict=True,
    ):
        state = {"start": 0}
        old_request = PAGINATED_SOURCE_ADAPTERS["arXiv"].build_request(
            old_spec, state
        )
        new_request = PAGINATED_SOURCE_ADAPTERS["arXiv"].build_request(
            new_spec, state
        )
        old_params = dict(old_request.params)
        new_params = dict(new_request.params)
        old_max_results = old_params.pop("max_results", None)
        new_max_results = new_params.pop("max_results", None)
        if (
            old_spec.query_text != new_spec.query_text
            or old_spec.query_version != new_spec.query_version
            or old_spec.endpoint != new_spec.endpoint
            or old_spec.metadata != new_spec.metadata
            or old_spec.filters != new_spec.filters
            or old_max_results != ARXIV_LEGACY_PAGE_SIZE
            or new_max_results != ARXIV_RECOVERED_PAGE_SIZE
            or old_params != new_params
            or old_request.method != new_request.method
            or old_request.url != new_request.url
            or old_request.headers != new_request.headers
            or old_request.timeout != new_request.timeout
            or old_request.state != new_request.state
            or old_request.request_hash() == new_request.request_hash()
            or query.metadata.get("frozen_request_specification_hash")
            != old_spec.metadata["frozen_request_specification_hash"]
        ):
            raise ExternalRetrievalWaveError(
                "arXiv page-size recovery request identity is not a page-size-only change"
            )
        identities.append(
            {
                "production_query_id": old_spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "frozen_request_specification_hash": old_spec.metadata[
                    "frozen_request_specification_hash"
                ],
                "request_state": state,
                "old_page_size": old_max_results,
                "new_page_size": new_max_results,
                "old_request_hash": old_request.request_hash(),
                "new_request_hash": new_request.request_hash(),
            }
        )
    provenance = {
        "recovery_episode_number": 6,
        "parent_episode_number": 5,
        "parent_checkpoint_dataset": parent_validation["checkpoint_reference"],
        "parent_episode_manifest_hash": _hash_payload(
            {"execution_episodes": source_state["execution_episodes"][:5]}
        ),
        "parent_attempt_manifest_hash": parent_validation[
            "attempt_manifest_hash"
        ],
        "parent_raw_response_manifest_hash": parent_validation[
            "raw_response_manifest_hash"
        ],
        "parent_raw_responses": parent_validation["raw_response_bindings"],
        "parent_http_statuses": parent_validation["http_statuses"],
        "historical_lineage_counts": {
            "preserved_before_episode_5_attempts": 51,
            "preserved_before_episode_5_raw_responses": 28,
            "episode_5_attempts": 6,
            "episode_5_raw_responses": 6,
            "preserved_total_attempts": 57,
            "preserved_total_raw_responses": 34,
            "accepted_pages": 0,
            "accepted_occurrences": 0,
        },
        "old_page_size": ARXIV_LEGACY_PAGE_SIZE,
        "new_page_size": ARXIV_RECOVERED_PAGE_SIZE,
        "parent_query_plan_hash": _query_plan_hash(old_specs),
        "new_query_plan_hash": _query_plan_hash(new_specs),
        "request_identity_changes": identities,
        "supporting_diagnostic": parent_validation["diagnostic"],
        "page_size_causality": "UNPROVEN",
        "transport_policy": parent_validation["transport_policy"],
        "network_used": False,
    }
    return provenance, new_specs


def _validate_authorized_arxiv_page_size_recovery(
    *,
    root: Path,
    source_state: Mapping[str, Any],
    wave: ProductionRetrievalWave,
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 6 or [item.get("episode_number") for item in episodes] != [
        1,
        2,
        3,
        4,
        5,
        6,
    ]:
        raise ExternalRetrievalWaveError("arXiv episode-6 lineage changed")
    if any(item.get("immutable") is not True for item in episodes[:5]):
        raise ExternalRetrievalWaveError(
            "arXiv episode-6 historical episode mutability changed"
        )
    parent_validation = _validate_arxiv_episode_5_page_size_parent(
        root=root,
        source_state=source_state,
        wave=wave,
        allow_descendant=True,
    )
    provenance, specs = _arxiv_page_size_recovery_provenance(
        source_state=source_state,
        parent_validation=parent_validation,
        wave=wave,
    )
    episode = episodes[5]
    checkpoint_reference = episode.get("checkpoint_dataset")
    if not isinstance(checkpoint_reference, dict):
        raise ExternalRetrievalWaveError("arXiv episode-6 checkpoint binding is absent")
    checkpoint = _safe_output_path(root, str(checkpoint_reference.get("path")))
    _verify_file_reference(checkpoint, checkpoint_reference, root)
    dataset = load_review_dataset(checkpoint)
    dataset.validate()
    run = dataset.retrieval_runs[0] if len(dataset.retrieval_runs) == 1 else None
    page_size_policy = {
        "old_page_size": ARXIV_LEGACY_PAGE_SIZE,
        "page_size": ARXIV_RECOVERED_PAGE_SIZE,
    }
    if (
        episode.get("episode_id") != "arXiv-episode-006"
        or episode.get("authorization_reason")
        != "OFFLINE_ARXIV_PAGE_SIZE_RECOVERY"
        or episode.get("recovery_of_episode_number") != 5
        or episode.get("parent_checkpoint_dataset")
        != parent_validation["checkpoint_reference"]
        or episode.get("recovery_provenance") != provenance
        or episode.get("transport_policy") != parent_validation["transport_policy"]
        or episode.get("page_size_policy") != page_size_policy
        or episode.get("network_used") is not False
        or source_state.get("active_episode_number") != 6
        or source_state.get("active_run_id") != f"{WAVE_ID}:arXiv"
        or source_state.get("active_checkpoint_path")
        != episode.get("checkpoint_path")
        or source_state.get("checkpoint_path") != episode.get("checkpoint_path")
        or source_state.get("checkpoint_dataset") != checkpoint_reference
        or source_state.get("transport_policy")
        != parent_validation["transport_policy"]
        or source_state.get("page_size_policy") != page_size_policy
        or source_state.get("preserved_source_attempt_count") != 57
        or source_state.get("preserved_source_raw_response_count") != 34
        or source_state.get("attempt_count") != len(dataset.retrieval_attempts)
        or source_state.get("occurrence_count") != len(dataset.occurrences)
        or source_state.get("completed_query_count")
        != sum(
            item.completion_status is RetrievalCompletionStatus.COMPLETE
            for item in dataset.source_queries
        )
        or run is None
        or run.run_id != f"{WAVE_ID}:arXiv"
        or run.query_plan_hash != _query_plan_hash(specs)
        or run.metadata.get("offline_arxiv_page_size_recovery") != provenance
        or len(dataset.source_queries) != 5
    ):
        raise ExternalRetrievalWaveError(
            "authorized arXiv episode-6 recovery provenance changed"
        )
    for query, spec, identity in zip(
        dataset.source_queries,
        specs,
        provenance["request_identity_changes"],
        strict=True,
    ):
        if (
            query.query_id != identity["query_id"]
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.endpoint != spec.endpoint
            or query.filters != {"page_size": ARXIV_RECOVERED_PAGE_SIZE}
            or query.metadata.get("production_query_id")
            != identity["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != identity["frozen_request_specification_hash"]
            or query.metadata.get("request_timeout_seconds")
            != ARXIV_RECOVERED_READ_TIMEOUT_SECONDS
            or query.metadata.get("offline_arxiv_page_size_recovery")
            != {
                "parent_episode_number": 5,
                "request_state": {"start": 0},
                "old_request_hash": identity["old_request_hash"],
                "new_request_hash": identity["new_request_hash"],
                "old_page_size": ARXIV_LEGACY_PAGE_SIZE,
                "new_page_size": ARXIV_RECOVERED_PAGE_SIZE,
            }
        ):
            raise ExternalRetrievalWaveError(
                "authorized arXiv episode-6 query provenance changed"
            )


def authorize_arxiv_page_size_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Create an opt-in episode 6 with an arXiv page size of 100."""

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        wave, preflight = validate_persisted_external_preflight(root=root_path)
        state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
        if not state_path.is_file():
            raise ExternalRetrievalWaveError("external execution state does not exist")
        state = _load_execution_state(state_path, root_path, wave, preflight)
        if any(item.get("status") == "RUNNING" for item in state["sources"].values()):
            raise ExternalRetrievalWaveError(
                "cannot recover while an external-source session is marked RUNNING"
            )
        if state.get("external_retrieval_cutoff_date") is not None:
            raise ExternalRetrievalWaveError(
                "arXiv page-size recovery cannot alter a closed wave"
            )
        source_state = state["sources"]["arXiv"]
        if _arxiv_page_size_recovery_active(source_state):
            _validate_authorized_arxiv_page_size_recovery(
                root=root_path, source_state=source_state, wave=wave
            )
            return state
        parent_validation = _validate_arxiv_episode_5_page_size_parent(
            root=root_path,
            source_state=source_state,
            wave=wave,
        )
        episodes = source_state["execution_episodes"]
        if len(episodes) != 5 or episodes[4].get("immutable") is not False:
            raise ExternalRetrievalWaveError(
                "arXiv page-size recovery requires the exact mutable episode 5"
            )
        historical_references = []
        for episode in episodes:
            reference = episode.get("checkpoint_dataset")
            if not isinstance(reference, dict):
                raise ExternalRetrievalWaveError(
                    "arXiv historical episode lacks checkpoint provenance"
                )
            checkpoint = _safe_output_path(root_path, str(reference.get("path")))
            _verify_file_reference(checkpoint, reference, root_path)
            for artifact in sorted(
                (item for item in checkpoint.parent.rglob("*") if item.is_file()),
                key=lambda item: item.as_posix(),
            ):
                historical_references.append(_file_reference(artifact, root_path))
        recovery_checkpoint_relative = (
            f"{EXECUTION_ROOT}/arXiv/episodes/episode-006/checkpoint"
        )
        recovery_checkpoint_dir = _safe_output_path(
            root_path, recovery_checkpoint_relative
        )
        if recovery_checkpoint_dir.exists():
            raise ExternalRetrievalWaveError(
                "arXiv episode-6 checkpoint exists without valid state lineage"
            )
        other_sources_before = {
            key: json.loads(json.dumps(value, sort_keys=True))
            for key, value in state["sources"].items()
            if key != "arXiv"
        }
        episodes[4]["immutable"] = True
        provenance, new_specs = _arxiv_page_size_recovery_provenance(
            source_state=source_state,
            parent_validation=parent_validation,
            wave=wave,
        )
        recovered_at = timestamp()
        dataset = parent_validation["dataset"]
        dataset.retrieval_pages = []
        dataset.retrieval_attempts = []
        dataset.occurrences = []
        dataset.canonical_records = []
        dataset.duplicate_decisions = []
        run = dataset.retrieval_runs[0]
        run.query_plan_hash = provenance["new_query_plan_hash"]
        run.retrieval_started_at = recovered_at
        run.retrieval_completed_at = recovered_at
        run.retrieval_cutoff_date = None
        run.status = ProcessingStatus.PARTIAL
        run.completion_status = RetrievalCompletionStatus.RUNNING
        run.errors = [
            "offline arXiv page-size recovery complete; live resume pending"
        ]
        for key in (
            "pause_state",
            "pause_reason",
            "pause_metadata",
            "session_request_count",
        ):
            run.metadata.pop(key, None)
        run.metadata["offline_arxiv_page_size_recovery"] = provenance
        for query, spec, identity in zip(
            dataset.source_queries,
            new_specs,
            provenance["request_identity_changes"],
            strict=True,
        ):
            query.retrieval_started_at = recovered_at
            query.retrieval_ended_at = recovered_at
            query.status = ProcessingStatus.PARTIAL
            query.completion_status = RetrievalCompletionStatus.PLANNED
            query.page = None
            query.cursor = None
            query.result_count = 0
            query.errors = []
            query.page_ids = []
            query.source_reported_total = None
            query.total_is_exact = False
            query.completion_proof = None
            query.filters = {"page_size": spec.limit}
            for key in ("pause_state", "pause_reason", "pause_metadata"):
                query.metadata.pop(key, None)
            query.metadata["offline_arxiv_page_size_recovery"] = {
                "parent_episode_number": 5,
                "request_state": {"start": 0},
                "old_request_hash": identity["old_request_hash"],
                "new_request_hash": identity["new_request_hash"],
                "old_page_size": ARXIV_LEGACY_PAGE_SIZE,
                "new_page_size": ARXIV_RECOVERED_PAGE_SIZE,
            }
        dataset.validate()
        recovery_store = CheckpointStore(recovery_checkpoint_dir)
        checkpoint_hash = recovery_store.save_dataset(dataset)
        recovery_reference = _file_reference(recovery_store.dataset_path, root_path)
        if checkpoint_hash != recovery_reference["raw_sha256"]:
            raise ExternalRetrievalWaveError(
                "arXiv page-size recovery checkpoint hash disagreement"
            )
        page_size_policy = {
            "old_page_size": ARXIV_LEGACY_PAGE_SIZE,
            "page_size": ARXIV_RECOVERED_PAGE_SIZE,
        }
        episode_6 = {
            "episode_number": 6,
            "episode_id": "arXiv-episode-006",
            "run_id": run.run_id,
            "status": ARXIV_PAGE_SIZE_RECOVERY_STATUS,
            "recovery_of_episode_number": 5,
            "authorization_reason": "OFFLINE_ARXIV_PAGE_SIZE_RECOVERY",
            "authorized_at_utc": recovered_at,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_reference,
            "frozen_wave_manifest_hash": wave.manifest_hash(),
            "frozen_query_plan_hash": wave.query_plan_hash,
            "parent_checkpoint_dataset": parent_validation[
                "checkpoint_reference"
            ],
            "transport_policy": parent_validation["transport_policy"],
            "page_size_policy": page_size_policy,
            "recovery_provenance": provenance,
            "network_used": False,
            "immutable": False,
        }
        episodes.append(episode_6)
        source_state.update(
            {
                "status": ARXIV_PAGE_SIZE_RECOVERY_STATUS,
                "active_episode_number": 6,
                "active_run_id": run.run_id,
                "active_checkpoint_path": recovery_checkpoint_relative,
                "checkpoint_path": recovery_checkpoint_relative,
                "checkpoint_dataset": recovery_reference,
                "completed_query_count": 0,
                "total_query_count": 5,
                "occurrence_count": 0,
                "attempt_count": 0,
                "preserved_source_attempt_count": 57,
                "preserved_source_raw_response_count": 34,
                "requests_this_session": 0,
                "pause_reason": (
                    "OFFLINE_PAGE_SIZE_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
                ),
                "failure_reason": None,
                "last_session_started_at_utc": recovered_at,
                "last_session_completed_at_utc": recovered_at,
                "transport_policy": parent_validation["transport_policy"],
                "page_size_policy": page_size_policy,
            }
        )
        source_state.pop("pause_metadata", None)
        if {
            key: value for key, value in state["sources"].items() if key != "arXiv"
        } != other_sources_before:
            raise ExternalRetrievalWaveError(
                "arXiv page-size recovery changed another source"
            )
        state["status"] = "RUNNING"
        state["external_retrieval_completed_at_utc"] = None
        state["external_retrieval_cutoff_date"] = None
        _save_execution_state(state_path, state)
        for reference in historical_references:
            artifact = (root_path / reference["path"]).resolve()
            raw = artifact.read_bytes() if artifact.is_file() else b""
            if (
                len(raw) != reference["byte_size"]
                or _sha256(raw) != reference["raw_sha256"]
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv historical evidence changed during page-size recovery"
                )
        _validate_authorized_arxiv_page_size_recovery(
            root=root_path, source_state=source_state, wave=wave
        )
        return state


def authorize_pubmed_transport_retry(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Authorize a new PubMed episode after a response-free transport failure."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["PubMed"]
    if source_state["status"] == PUBMED_TRANSPORT_RETRY_STATUS:
        _validate_authorized_pubmed_retry(source_state, root_path)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "PubMed transport retry requires a terminal FAILED component"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "PubMed transport retry cannot alter a closed external retrieval wave"
        )

    checkpoint_path = source_state.get("checkpoint_dataset", {}).get("path")
    if not checkpoint_path:
        raise ExternalRetrievalWaveError("failed PubMed state lacks a checkpoint binding")
    checkpoint = _safe_output_path(root_path, checkpoint_path)
    _verify_file_reference(checkpoint, source_state["checkpoint_dataset"], root_path)
    dataset = load_review_dataset(checkpoint)
    _validate_response_free_pubmed_transport_failure(
        dataset=dataset,
        checkpoint_dir=checkpoint.parent,
        wave=wave,
    )

    episodes = source_state.setdefault("execution_episodes", [])
    if episodes:
        active_number = int(source_state.get("active_episode_number", len(episodes)))
        active = next(
            (item for item in episodes if item["episode_number"] == active_number), None
        )
        if active is None or active.get("status") != "FAILED":
            raise ExternalRetrievalWaveError(
                "PubMed retry episode lineage does not match the failed component"
            )
        active["checkpoint_dataset"] = dict(source_state["checkpoint_dataset"])
        active["immutable"] = True
    else:
        active_number = 1
        run = dataset.retrieval_runs[0]
        episodes.append(
            {
                "episode_number": active_number,
                "episode_id": "PubMed-episode-001",
                "run_id": run.run_id,
                "status": "FAILED",
                "failure_classification": (
                    "TRANSPORT_ENVIRONMENT_FAILURE_BEFORE_PROVIDER_RESPONSE"
                ),
                "checkpoint_path": checkpoint.parent.relative_to(root_path).as_posix(),
                "checkpoint_dataset": dict(source_state["checkpoint_dataset"]),
                "attempt_count": len(dataset.retrieval_attempts),
                "successful_http_response_count": 0,
                "raw_provider_response_count": 0,
                "occurrence_count": 0,
                "canonical_record_count": 0,
                "request_hashes": sorted(
                    {item.request_hash for item in dataset.retrieval_attempts}
                ),
                "started_at_utc": source_state.get("last_session_started_at_utc"),
                "completed_at_utc": source_state.get("last_session_completed_at_utc"),
                "immutable": True,
            }
        )

    next_number = active_number + 1
    retry_checkpoint_path = (
        f"{EXECUTION_ROOT}/PubMed/episodes/episode-{next_number:03d}/checkpoint"
    )
    authorized_at = timestamp()
    retry_episode = {
        "episode_number": next_number,
        "episode_id": f"PubMed-episode-{next_number:03d}",
        "run_id": f"{WAVE_ID}:PubMed:episode-{next_number:03d}",
        "status": PUBMED_TRANSPORT_RETRY_STATUS,
        "retry_of_episode_number": active_number,
        "authorization_reason": (
            "TRANSPORT_ENVIRONMENT_FAILURE_BEFORE_PROVIDER_RESPONSE"
        ),
        "authorized_at_utc": authorized_at,
        "checkpoint_path": retry_checkpoint_path,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "prior_episode_checkpoint_sha256": source_state["checkpoint_dataset"][
            "raw_sha256"
        ],
        "immutable": False,
    }
    episodes.append(retry_episode)
    source_state.update(
        {
            "status": PUBMED_TRANSPORT_RETRY_STATUS,
            "active_episode_number": next_number,
            "active_run_id": retry_episode["run_id"],
            "active_checkpoint_path": retry_checkpoint_path,
            "checkpoint_path": retry_checkpoint_path,
            "checkpoint_dataset": None,
            "completed_query_count": 0,
            "total_query_count": 5,
            "occurrence_count": 0,
            "attempt_count": 0,
            "requests_this_session": 0,
            "pause_reason": None,
            "failure_reason": None,
            "last_session_started_at_utc": None,
            "last_session_completed_at_utc": None,
        }
    )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def authorize_pubmed_parser_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Reparse immutable PubMed episode-2 responses into a resumable checkpoint."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["PubMed"]
    if source_state["status"] == PUBMED_PARSER_RECOVERY_STATUS:
        _validate_authorized_pubmed_parser_recovery(source_state, root_path)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery requires a terminal FAILED component"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery cannot alter a closed external retrieval wave"
        )

    episodes = source_state.get("execution_episodes", [])
    if source_state.get("active_episode_number") != 2 or len(episodes) != 2:
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery is restricted to failed execution episode 2"
        )
    failed_episode = next(
        (item for item in episodes if item.get("episode_number") == 2), None
    )
    if (
        failed_episode is None
        or failed_episode.get("status") != "FAILED"
        or not failed_episode.get("immutable")
    ):
        raise ExternalRetrievalWaveError(
            "PubMed episode 2 is not preserved as an immutable failed episode"
        )
    checkpoint_reference = source_state.get("checkpoint_dataset")
    if not checkpoint_reference or failed_episode.get("checkpoint_dataset") != checkpoint_reference:
        raise ExternalRetrievalWaveError("PubMed episode-2 checkpoint lineage changed")
    failed_checkpoint = _safe_output_path(
        root_path, str(checkpoint_reference["path"])
    )
    _verify_file_reference(failed_checkpoint, checkpoint_reference, root_path)
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    failed_checkpoint_dir = failed_checkpoint.parent
    dataset = load_review_dataset(failed_checkpoint)
    recovered_at = timestamp()
    recovery = _reparse_failed_pubmed_checkpoint(
        dataset=dataset,
        checkpoint_dir=failed_checkpoint_dir,
        wave=wave,
        recovered_at=recovered_at,
    )

    next_number = 3
    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/PubMed/episodes/episode-{next_number:03d}/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "PubMed parser-recovery checkpoint already exists without valid state lineage"
        )
    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    raw_bindings = []
    for relative_path, expected_hash in recovery["raw_response_references"]:
        response_path = Path(relative_path)
        if (
            response_path.is_absolute()
            or ".." in response_path.parts
            or not response_path.parts
            or response_path.parts[0] != "responses"
        ):
            raise ExternalRetrievalWaveError(
                "PubMed episode-2 raw response path is unsafe"
            )
        source = failed_checkpoint_dir / response_path
        raw = source.read_bytes()
        if _sha256(raw) != expected_hash:
            raise ExternalRetrievalWaveError(
                f"PubMed episode-2 raw response hash mismatch: {relative_path}"
            )
        destination = recovery_checkpoint_dir / response_path
        atomic_write(destination, raw)
        if destination.read_bytes() != raw:
            raise ExternalRetrievalWaveError(
                f"PubMed recovery raw-response copy mismatch: {relative_path}"
            )
        raw_bindings.append(
            {
                "episode_2_path": source.relative_to(root_path).as_posix(),
                "recovery_copy_path": destination.relative_to(root_path).as_posix(),
                "byte_size": len(raw),
                "raw_sha256": expected_hash,
            }
        )

    run = dataset.retrieval_runs[0]
    run.metadata["offline_parser_recovery"] = {
        "recovery_episode_number": next_number,
        "source_episode_number": 2,
        "source_checkpoint": dict(checkpoint_reference),
        "source_raw_response_count": len(raw_bindings),
        "source_raw_response_manifest_hash": _hash_payload(
            {"responses": raw_bindings}
        ),
        "recovered_pmids": list(recovery["recovered_pmids"]),
        "remaining_efetch_request_count": recovery[
            "remaining_efetch_request_count"
        ],
        "network_used": False,
    }
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError("PubMed recovery checkpoint hash disagreement")
    if failed_checkpoint.read_bytes() != failed_checkpoint_bytes:
        raise ExternalRetrievalWaveError("PubMed episode-2 checkpoint changed during recovery")
    for binding in raw_bindings:
        source = root_path / binding["episode_2_path"]
        raw = source.read_bytes()
        if len(raw) != binding["byte_size"] or _sha256(raw) != binding["raw_sha256"]:
            raise ExternalRetrievalWaveError(
                "PubMed episode-2 raw response changed during recovery"
            )

    recovery_episode = {
        "episode_number": next_number,
        "episode_id": f"PubMed-episode-{next_number:03d}",
        "run_id": run.run_id,
        "status": PUBMED_PARSER_RECOVERY_STATUS,
        "recovery_of_episode_number": 2,
        "authorization_reason": "OFFLINE_PUBMED_XML_PARSER_CORRECTION",
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": dict(checkpoint_reference),
        "source_raw_responses": raw_bindings,
        "recovered_pmids": list(recovery["recovered_pmids"]),
        "already_fetched_occurrence_count": len(dataset.occurrences),
        "remaining_efetch_request_count": recovery[
            "remaining_efetch_request_count"
        ],
        "remaining_efetch_batches": recovery["remaining_efetch_batches"],
        "network_used": False,
        "immutable": False,
    }
    episodes.append(recovery_episode)
    source_state.update(
        {
            "status": PUBMED_PARSER_RECOVERY_STATUS,
            "active_episode_number": next_number,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": sum(
                query.completion_status is RetrievalCompletionStatus.COMPLETE
                for query in dataset.source_queries
            ),
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "requests_this_session": 0,
            "pause_reason": "OFFLINE_PARSER_RECOVERY_COMPLETE; LIVE_EFETCH_RESUME_REQUIRED",
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def authorize_europe_pmc_terminal_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Reconcile Europe PMC terminal sentinels from immutable stored responses."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["EuropePMC"]
    if source_state["status"] == "COMPLETE":
        _validate_authorized_europe_pmc_terminal_recovery(source_state, root_path)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "Europe PMC terminal recovery requires a terminal FAILED component"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery cannot alter a closed external retrieval wave"
        )
    if source_state.get("execution_episodes"):
        raise ExternalRetrievalWaveError(
            "Europe PMC terminal recovery requires the original failed lineage"
        )

    checkpoint_reference = source_state.get("checkpoint_dataset")
    if not checkpoint_reference:
        raise ExternalRetrievalWaveError("Europe PMC failed checkpoint is absent")
    failed_checkpoint = _safe_output_path(
        root_path, str(checkpoint_reference["path"])
    )
    _verify_file_reference(failed_checkpoint, checkpoint_reference, root_path)
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    failed_payload = json.loads(failed_checkpoint_bytes)
    source_attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": failed_payload.get("retrieval_attempts", [])}
    )
    dataset = load_review_dataset(failed_checkpoint)
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "EuropePMC"
    }
    recovered_at = timestamp()
    recovery = _reparse_failed_europe_pmc_checkpoint(
        dataset=dataset,
        checkpoint_dir=failed_checkpoint.parent,
        wave=wave,
        recovered_at=recovered_at,
    )

    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/EuropePMC/episodes/episode-002/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery checkpoint exists without valid state lineage"
        )
    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    raw_bindings = []
    for relative_path, expected_hash in recovery["raw_response_references"]:
        response_path = Path(relative_path)
        if (
            response_path.is_absolute()
            or ".." in response_path.parts
            or not response_path.parts
            or response_path.parts[0] != "responses"
        ):
            raise ExternalRetrievalWaveError(
                "Europe PMC raw response path is unsafe"
            )
        source = failed_checkpoint.parent / response_path
        raw = source.read_bytes()
        if _sha256(raw) != expected_hash:
            raise ExternalRetrievalWaveError(
                f"Europe PMC raw response hash mismatch: {relative_path}"
            )
        destination = recovery_checkpoint_dir / response_path
        atomic_write(destination, raw)
        if destination.read_bytes() != raw:
            raise ExternalRetrievalWaveError(
                f"Europe PMC recovery response copy mismatch: {relative_path}"
            )
        raw_bindings.append(
            {
                "failed_episode_path": source.relative_to(root_path).as_posix(),
                "recovery_copy_path": destination.relative_to(root_path).as_posix(),
                "byte_size": len(raw),
                "raw_sha256": expected_hash,
            }
        )

    run = dataset.retrieval_runs[0]
    run.metadata["offline_terminal_sentinel_recovery"] = {
        "recovery_episode_number": 2,
        "source_episode_number": 1,
        "source_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_raw_response_count": len(raw_bindings),
        "source_raw_response_manifest_hash": _hash_payload(
            {"responses": raw_bindings}
        ),
        "query_occurrence_counts": recovery["query_occurrence_counts"],
        "network_used": False,
    }
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery checkpoint hash disagreement"
        )
    if failed_checkpoint.read_bytes() != failed_checkpoint_bytes:
        raise ExternalRetrievalWaveError(
            "Europe PMC failed checkpoint changed during recovery"
        )
    for binding in raw_bindings:
        source = root_path / binding["failed_episode_path"]
        raw = source.read_bytes()
        if len(raw) != binding["byte_size"] or _sha256(raw) != binding["raw_sha256"]:
            raise ExternalRetrievalWaveError(
                "Europe PMC failed raw response changed during recovery"
            )

    failed_episode = {
        "episode_number": 1,
        "episode_id": "EuropePMC-episode-001",
        "run_id": run.run_id,
        "status": "FAILED",
        "failure_classification": (
            "TERMINAL_SENTINEL_PARSER_REJECTION_AFTER_EXACT_COUNT"
        ),
        "started_at_utc": source_state.get("last_session_started_at_utc"),
        "completed_at_utc": source_state.get("last_session_completed_at_utc"),
        "checkpoint_path": source_state["checkpoint_path"],
        "checkpoint_dataset": dict(checkpoint_reference),
        "attempt_count": source_state["attempt_count"],
        "occurrence_count": source_state["occurrence_count"],
        "completed_query_count": source_state["completed_query_count"],
        "failure_reason": source_state["failure_reason"],
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "immutable": True,
    }
    recovery_episode = {
        "episode_number": 2,
        "episode_id": "EuropePMC-episode-002",
        "run_id": run.run_id,
        "status": EUROPE_PMC_TERMINAL_RECOVERY_STATUS,
        "recovery_of_episode_number": 1,
        "authorization_reason": "OFFLINE_REPEATED_CURSOR_TERMINAL_RECONCILIATION",
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_raw_responses": raw_bindings,
        "query_occurrence_counts": recovery["query_occurrence_counts"],
        "occurrence_count": len(dataset.occurrences),
        "attempt_count": len(dataset.retrieval_attempts),
        "completed_query_count": 5,
        "network_used": False,
        "immutable": True,
    }
    source_state.update(
        {
            "status": "COMPLETE",
            "execution_episodes": [failed_episode, recovery_episode],
            "active_episode_number": 2,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": 5,
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "requests_this_session": 0,
            "pause_reason": None,
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    if {
        key: value for key, value in state["sources"].items() if key != "EuropePMC"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery changed another source component"
        )
    _finalize_execution_state(state, recovered_at)
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery unexpectedly closed the external retrieval wave"
        )
    _save_execution_state(state_path, state)
    return state


def authorize_ieee_total_drift_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Rebuild failed IEEE pagination as a mutable-total resumable lineage."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["IEEEXplore"]
    if source_state["status"] == IEEE_TOTAL_DRIFT_RECOVERY_STATUS:
        _validate_authorized_ieee_total_drift_recovery(source_state, root_path)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "IEEE total-drift recovery requires a terminal FAILED component"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "IEEE total-drift recovery cannot alter a closed external retrieval wave"
        )
    if source_state.get("execution_episodes"):
        raise ExternalRetrievalWaveError(
            "IEEE total-drift recovery requires the original failed lineage"
        )

    checkpoint_reference = source_state.get("checkpoint_dataset")
    if not checkpoint_reference:
        raise ExternalRetrievalWaveError("IEEE failed checkpoint is absent")
    failed_checkpoint = _safe_output_path(
        root_path, str(checkpoint_reference["path"])
    )
    _verify_file_reference(failed_checkpoint, checkpoint_reference, root_path)
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    failed_payload = json.loads(failed_checkpoint_bytes)
    source_attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": failed_payload.get("retrieval_attempts", [])}
    )
    dataset = load_review_dataset(failed_checkpoint)
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "IEEEXplore"
    }
    quota_before = json.loads(
        json.dumps(source_state.get("ieee_quota"), sort_keys=True)
    )
    recovered_at = timestamp()
    recovery = _reparse_failed_ieee_total_drift_checkpoint(
        dataset=dataset,
        checkpoint_dir=failed_checkpoint.parent,
        wave=wave,
        recovered_at=recovered_at,
    )

    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/IEEEXplore/episodes/episode-002/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "IEEE recovery checkpoint exists without valid state lineage"
        )
    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    raw_bindings = _copy_recovery_raw_responses(
        root=root_path,
        source_checkpoint_dir=failed_checkpoint.parent,
        recovery_checkpoint_dir=recovery_checkpoint_dir,
        raw_response_references=recovery["raw_response_references"],
        source_path_key="failed_episode_path",
        error_prefix="IEEE",
    )

    run = dataset.retrieval_runs[0]
    run.metadata["offline_provider_total_drift_recovery"] = {
        "recovery_episode_number": 2,
        "source_episode_number": 1,
        "source_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_raw_response_count": len(raw_bindings),
        "source_raw_response_manifest_hash": _hash_payload(
            {"responses": raw_bindings}
        ),
        "continuation_plan": recovery["continuation_plan"],
        "provider_total_histories": recovery["provider_total_histories"],
        "network_used": False,
    }
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError("IEEE recovery checkpoint hash disagreement")
    if failed_checkpoint.read_bytes() != failed_checkpoint_bytes:
        raise ExternalRetrievalWaveError(
            "IEEE failed checkpoint changed during recovery"
        )
    _verify_recovery_raw_bindings(root_path, raw_bindings, "failed_episode_path")

    failed_episode = {
        "episode_number": 1,
        "episode_id": "IEEEXplore-episode-001",
        "run_id": run.run_id,
        "status": "FAILED",
        "failure_classification": "MUTABLE_PROVIDER_TOTAL_REJECTED_AS_INCONSISTENT",
        "started_at_utc": source_state.get("last_session_started_at_utc"),
        "completed_at_utc": source_state.get("last_session_completed_at_utc"),
        "checkpoint_path": source_state["checkpoint_path"],
        "checkpoint_dataset": dict(checkpoint_reference),
        "attempt_count": source_state["attempt_count"],
        "occurrence_count": source_state["occurrence_count"],
        "completed_query_count": source_state["completed_query_count"],
        "failure_reason": source_state["failure_reason"],
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "immutable": True,
    }
    recovery_episode = {
        "episode_number": 2,
        "episode_id": "IEEEXplore-episode-002",
        "run_id": run.run_id,
        "status": IEEE_TOTAL_DRIFT_RECOVERY_STATUS,
        "recovery_of_episode_number": 1,
        "authorization_reason": "OFFLINE_MUTABLE_PROVIDER_TOTAL_RECONCILIATION",
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_raw_responses": raw_bindings,
        "continuation_plan": recovery["continuation_plan"],
        "provider_total_histories": recovery["provider_total_histories"],
        "known_daily_calls_preserved": quota_before["known_calls_after_session"],
        "network_used": False,
        "immutable": False,
    }
    source_state.update(
        {
            "status": IEEE_TOTAL_DRIFT_RECOVERY_STATUS,
            "execution_episodes": [failed_episode, recovery_episode],
            "active_episode_number": 2,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": 0,
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "requests_this_session": 0,
            "pause_reason": "OFFLINE_TOTAL_DRIFT_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED",
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    if source_state.get("ieee_quota") != quota_before:
        raise ExternalRetrievalWaveError("IEEE recovery changed the daily quota ledger")
    if {
        key: value for key, value in state["sources"].items() if key != "IEEEXplore"
    } != other_sources_before:
        raise ExternalRetrievalWaveError("IEEE recovery changed another source component")
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


def authorize_ieee_repeated_window_recovery(
    *,
    root: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Exclude proven repeated IEEE windows in a new offline episode."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    if not state_path.is_file():
        raise ExternalRetrievalWaveError("external execution state does not exist")
    state = _load_execution_state(state_path, root_path, wave, preflight)
    source_state = state["sources"]["IEEEXplore"]
    if source_state["status"] == IEEE_REPEATED_WINDOW_RECOVERY_STATUS:
        _validate_authorized_ieee_repeated_window_recovery(source_state, root_path)
        return state
    if source_state["status"] != "FAILED":
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery requires a terminal FAILED component"
        )
    if state["external_retrieval_cutoff_date"] is not None:
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery cannot alter a closed retrieval wave"
        )

    episodes = source_state.get("execution_episodes", [])
    if (
        len(episodes) != 2
        or source_state.get("active_episode_number") != 2
        or any(not episode.get("immutable") for episode in episodes)
        or episodes[0].get("episode_number") != 1
        or episodes[1].get("episode_number") != 2
        or episodes[1].get("status") != "FAILED"
    ):
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery requires immutable failed episodes 1 and 2"
        )
    for episode in episodes:
        reference = episode.get("checkpoint_dataset")
        if not reference:
            raise ExternalRetrievalWaveError("IEEE episode checkpoint is absent")
        _verify_file_reference(
            _safe_output_path(root_path, reference["path"]), reference, root_path
        )

    quota_before = json.loads(
        json.dumps(source_state.get("ieee_quota"), sort_keys=True)
    )
    if (
        not quota_before
        or quota_before.get("daily_limit") != IEEE_DAILY_REQUEST_LIMIT
        or quota_before.get("known_calls_after_session")
        != IEEE_REPEATED_WINDOW_EXPECTED_KNOWN_CALLS
    ):
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery quota evidence changed"
        )
    checkpoint_reference = source_state.get("checkpoint_dataset")
    if checkpoint_reference != episodes[1].get("checkpoint_dataset"):
        raise ExternalRetrievalWaveError("IEEE active episode-2 checkpoint changed")
    failed_checkpoint = _safe_output_path(root_path, checkpoint_reference["path"])
    failed_checkpoint_bytes = failed_checkpoint.read_bytes()
    failed_payload = json.loads(failed_checkpoint_bytes)
    source_attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": failed_payload.get("retrieval_attempts", [])}
    )
    dataset = load_review_dataset(failed_checkpoint)
    other_sources_before = {
        key: json.loads(json.dumps(value, sort_keys=True))
        for key, value in state["sources"].items()
        if key != "IEEEXplore"
    }
    recovered_at = timestamp()
    recovery = _reparse_failed_ieee_repeated_window_checkpoint(
        dataset=dataset,
        checkpoint_dir=failed_checkpoint.parent,
        root=root_path,
        wave=wave,
        recovered_at=recovered_at,
    )

    recovery_checkpoint_relative = (
        f"{EXECUTION_ROOT}/IEEEXplore/episodes/episode-003/checkpoint"
    )
    recovery_checkpoint_dir = _safe_output_path(
        root_path, recovery_checkpoint_relative
    )
    if recovery_checkpoint_dir.exists():
        raise ExternalRetrievalWaveError(
            "IEEE episode-3 checkpoint exists without valid state lineage"
        )
    recovery_store = CheckpointStore(recovery_checkpoint_dir)
    raw_bindings = _copy_recovery_raw_responses(
        root=root_path,
        source_checkpoint_dir=failed_checkpoint.parent,
        recovery_checkpoint_dir=recovery_checkpoint_dir,
        raw_response_references=recovery["raw_response_references"],
        source_path_key="episode_2_path",
        error_prefix="IEEE repeated-window",
    )
    binding_by_source = {
        binding["episode_2_path"]: binding for binding in raw_bindings
    }
    rejected_bindings = [
        binding_by_source[item["episode_2_response_path"]]
        for item in recovery["rejection_evidence"]
    ]

    run = dataset.retrieval_runs[0]
    run.metadata["offline_repeated_window_recovery"] = {
        "recovery_episode_number": 3,
        "source_episode_number": 2,
        "source_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_attempt_count": IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS,
        "retained_page_count": recovery["retained_page_count"],
        "rejected_page_count": len(recovery["rejection_evidence"]),
        "quota_only_rejected_attempt_count": len(recovery["rejection_evidence"]),
        "source_raw_response_count": len(raw_bindings),
        "source_raw_response_manifest_hash": _hash_payload(
            {"responses": raw_bindings}
        ),
        "rejected_response_manifest_hash": _hash_payload(
            {"responses": rejected_bindings}
        ),
        "continuation_plan": recovery["continuation_plan"],
        "provider_total_histories": recovery["provider_total_histories"],
        "rejection_evidence": recovery["rejection_evidence"],
        "known_daily_calls_preserved": quota_before["known_calls_after_session"],
        "quota_day_utc": quota_before["quota_day_utc"],
        "network_used": False,
    }
    checkpoint_hash = recovery_store.save_dataset(dataset)
    recovery_checkpoint_reference = _file_reference(
        recovery_store.dataset_path, root_path
    )
    if checkpoint_hash != recovery_checkpoint_reference["raw_sha256"]:
        raise ExternalRetrievalWaveError(
            "IEEE episode-3 recovery checkpoint hash disagreement"
        )
    if failed_checkpoint.read_bytes() != failed_checkpoint_bytes:
        raise ExternalRetrievalWaveError(
            "IEEE episode-2 checkpoint changed during recovery"
        )
    _verify_recovery_raw_bindings(root_path, raw_bindings, "episode_2_path")
    for episode in episodes:
        reference = episode["checkpoint_dataset"]
        _verify_file_reference(
            _safe_output_path(root_path, reference["path"]), reference, root_path
        )

    recovery_episode = {
        "episode_number": 3,
        "episode_id": "IEEEXplore-episode-003",
        "run_id": run.run_id,
        "status": IEEE_REPEATED_WINDOW_RECOVERY_STATUS,
        "recovery_of_episode_number": 2,
        "authorization_reason": "OFFLINE_REPEATED_PROVIDER_WINDOW_RECONCILIATION",
        "authorized_at_utc": recovered_at,
        "checkpoint_path": recovery_checkpoint_relative,
        "checkpoint_dataset": recovery_checkpoint_reference,
        "frozen_wave_manifest_hash": wave.manifest_hash(),
        "frozen_query_plan_hash": wave.query_plan_hash,
        "source_episode_checkpoint": dict(checkpoint_reference),
        "source_attempt_manifest_hash": source_attempt_manifest_hash,
        "source_attempt_count": IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS,
        "source_raw_responses": raw_bindings,
        "rejected_raw_responses": rejected_bindings,
        "rejection_evidence": recovery["rejection_evidence"],
        "retained_page_count": recovery["retained_page_count"],
        "continuation_plan": recovery["continuation_plan"],
        "provider_total_histories": recovery["provider_total_histories"],
        "known_daily_calls_preserved": quota_before["known_calls_after_session"],
        "remaining_daily_calls": (
            IEEE_DAILY_REQUEST_LIMIT - quota_before["known_calls_after_session"]
        ),
        "network_used": False,
        "immutable": False,
    }
    source_state.update(
        {
            "status": IEEE_REPEATED_WINDOW_RECOVERY_STATUS,
            "execution_episodes": [*episodes, recovery_episode],
            "active_episode_number": 3,
            "active_run_id": run.run_id,
            "active_checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_path": recovery_checkpoint_relative,
            "checkpoint_dataset": recovery_checkpoint_reference,
            "completed_query_count": 0,
            "total_query_count": 5,
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "preserved_source_attempt_count": IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS,
            "rejected_page_count": len(recovery["rejection_evidence"]),
            "requests_this_session": 0,
            "pause_reason": (
                "OFFLINE_REPEATED_WINDOW_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
            ),
            "failure_reason": None,
            "last_session_started_at_utc": recovered_at,
            "last_session_completed_at_utc": recovered_at,
        }
    )
    if source_state.get("ieee_quota") != quota_before:
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery changed the daily quota ledger"
        )
    if {
        key: value for key, value in state["sources"].items() if key != "IEEEXplore"
    } != other_sources_before:
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery changed another source component"
        )
    state["status"] = "RUNNING"
    state["external_retrieval_completed_at_utc"] = None
    state["external_retrieval_cutoff_date"] = None
    _save_execution_state(state_path, state)
    return state


@contextmanager
def _exclusive_external_source_session(root: Path):
    """Hold the one cross-process lock covering a complete source session."""

    lock_path = _safe_output_path(root, EXTERNAL_SOURCE_SESSION_LOCK_PATH)
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+b") as lock_handle:
        try:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise ExternalRetrievalWaveError(
                "another external-source session is already active"
            ) from exc
        try:
            yield
        finally:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)


def prepare_arxiv_snapshot_integration(
    *,
    root: str | Path,
    package_dir: str | Path,
    output_dir: str | Path,
) -> dict[str, Any]:
    """Validate and stage the snapshot integration without production mutation."""

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        wave, preflight = validate_persisted_external_preflight(root=root_path)
        state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
        if not state_path.is_file():
            raise ExternalRetrievalWaveError("external execution state does not exist")
        state_before = state_path.read_bytes()
        state = _load_execution_state(state_path, root_path, wave, preflight)
        result = prepare_integration_dry_run(
            root=root_path,
            state=state,
            package_dir=package_dir,
            output_dir=output_dir,
        )
        if state_path.read_bytes() != state_before:
            raise ExternalRetrievalWaveError(
                "snapshot integration dry run changed production state"
            )
        return result


def authorize_arxiv_snapshot_integration(
    *,
    root: str | Path,
    package_dir: str | Path,
    amendment_v2_path: str | Path,
    timestamp: Callable[[], str] = utc_now,
) -> dict[str, Any]:
    """Authorize the v303 replacement route while preserving API episode state."""

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        wave, preflight = validate_persisted_external_preflight(root=root_path)
        state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
        if not state_path.is_file():
            raise ExternalRetrievalWaveError("external execution state does not exist")
        state = _load_execution_state(state_path, root_path, wave, preflight)
        source = state["sources"]["arXiv"]
        if source.get("approved_snapshot_substitution") is not None:
            validate_authorized_snapshot_substitution(root=root_path, state=state)
            return state
        other_sources_before = {
            key: json.loads(json.dumps(value, sort_keys=True))
            for key, value in state["sources"].items()
            if key != "arXiv"
        }
        api_state_before = json.loads(json.dumps(source, sort_keys=True))
        authorized_at = timestamp()
        try:
            authorize_snapshot_substitution(
                root=root_path,
                state=state,
                package_dir=package_dir,
                amendment_v2_path=amendment_v2_path,
                timestamp=lambda: authorized_at,
            )
        except ArxivSnapshotIntegrationError as exc:
            raise ExternalRetrievalWaveError(str(exc)) from exc
        substitution = source.pop("approved_snapshot_substitution")
        if source != api_state_before:
            raise ExternalRetrievalWaveError(
                "snapshot integration changed preserved arXiv API state"
            )
        source["approved_snapshot_substitution"] = substitution
        if {
            key: value for key, value in state["sources"].items() if key != "arXiv"
        } != other_sources_before:
            raise ExternalRetrievalWaveError(
                "snapshot integration changed another source component"
            )
        _finalize_execution_state(state, authorized_at)
        if (
            state.get("identification_set_closed") is not False
            or state.get("prior_survey_seed_imported") is not False
            or state.get("final_global_deduplication_executed") is not False
            or state.get("prisma_generated") is not False
            or state.get("screening_executed") is not False
            or state.get("corpus_modified") is not False
        ):
            raise ExternalRetrievalWaveError(
                "snapshot source substitution bypassed an identification closure gate"
            )
        _save_execution_state(state_path, state)
        validate_authorized_snapshot_substitution(root=root_path, state=state)
        return state


def authorize_prior_survey_import(
    *,
    root: str | Path,
    package_dir: str | Path,
    expected_package_manifest_sha256: str,
    timestamp: Callable[[], str] = utc_now,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Authorize one qualified seed package without closing identification."""

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        wave, preflight = validate_persisted_external_preflight(root=root_path)
        state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
        if not state_path.is_file():
            raise ExternalRetrievalWaveError("external execution state does not exist")
        state = _load_execution_state(state_path, root_path, wave, preflight)
        sources_before = json.loads(json.dumps(state["sources"], sort_keys=True))
        closure_before = {
            key: state.get(key)
            for key in (
                "prior_survey_seed_imported",
                "identification_set_closed",
                "final_global_deduplication_executed",
                "screening_executed",
                "prisma_generated",
                "corpus_modified",
            )
        }
        try:
            state, registration = authorize_prior_survey_package_locked(
                root=root_path,
                state=state,
                package_dir=package_dir,
                expected_package_manifest_sha256=expected_package_manifest_sha256,
                authorized_at=timestamp(),
            )
        except PriorSurveyIntegrationError as exc:
            raise ExternalRetrievalWaveError(str(exc)) from exc
        if state["sources"] != sources_before:
            raise ExternalRetrievalWaveError(
                "prior-survey authorization changed an external source component"
            )
        if any(state.get(key) != value for key, value in closure_before.items()):
            raise ExternalRetrievalWaveError(
                "prior-survey authorization changed a closure or screening gate"
            )
        _save_execution_state(state_path, state)
        try:
            validate_authorized_prior_survey_imports(root=root_path, state=state)
        except PriorSurveyIntegrationError as exc:
            raise ExternalRetrievalWaveError(str(exc)) from exc
        return state, registration


def _active_arxiv_transport_policy(
    source_state: Mapping[str, Any],
) -> dict[str, Any] | None:
    """Return the immutable policy bound to an authorized arXiv episode."""

    active_number = source_state.get("active_episode_number")
    if active_number is None:
        return None
    active = next(
        (
            item
            for item in source_state.get("execution_episodes", [])
            if item.get("episode_number") == active_number
        ),
        None,
    )
    if active is None:
        raise ExternalRetrievalWaveError("active arXiv episode is missing")
    policy = active.get("transport_policy")
    if policy is None:
        return None
    expected = {
        "read_timeout_seconds": ARXIV_RECOVERED_READ_TIMEOUT_SECONDS,
        "maximum_attempts_per_invocation": (
            ARXIV_MAX_ATTEMPTS_PER_INVOCATION
        ),
    }
    expected_reason = {
        4: "OFFLINE_ARXIV_TRANSPORT_POLICY_RECOVERY",
        5: "OFFLINE_ARXIV_RETRYABLE_5XX_RECOVERY",
        6: "OFFLINE_ARXIV_PAGE_SIZE_RECOVERY",
    }.get(active_number)
    if (
        expected_reason is None
        or active.get("authorization_reason") != expected_reason
        or policy != expected
        or source_state.get("transport_policy") != expected
    ):
        raise ExternalRetrievalWaveError(
            "active arXiv transport policy lineage changed"
        )
    return dict(policy)


def _active_arxiv_page_size(source_state: Mapping[str, Any]) -> int | None:
    if source_state.get("active_episode_number") != 6:
        return None
    active = next(
        (
            item
            for item in source_state.get("execution_episodes", [])
            if item.get("episode_number") == 6
        ),
        None,
    )
    expected = {
        "old_page_size": ARXIV_LEGACY_PAGE_SIZE,
        "page_size": ARXIV_RECOVERED_PAGE_SIZE,
    }
    if (
        active is None
        or active.get("authorization_reason")
        != "OFFLINE_ARXIV_PAGE_SIZE_RECOVERY"
        or active.get("page_size_policy") != expected
        or source_state.get("page_size_policy") != expected
    ):
        raise ExternalRetrievalWaveError(
            "active arXiv page-size policy lineage changed"
        )
    return ARXIV_RECOVERED_PAGE_SIZE


def execute_external_source_session(
    *,
    root: str | Path,
    source: str,
    http: HttpClient | None,
    resume: bool,
    ieee_credential: str = "",
    quota_day_utc: str | None = None,
    timestamp: Callable[[], str] = utc_now,
    retry_policy: RetryPolicy | None = None,
    rate_limiter: RateLimiter | None = None,
    retry_sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Execute or resume exactly one authorized external source component."""

    root_path = Path(root).resolve()
    with _exclusive_external_source_session(root_path):
        return _execute_external_source_session_locked(
            root=root_path,
            source=source,
            http=http,
            resume=resume,
            ieee_credential=ieee_credential,
            quota_day_utc=quota_day_utc,
            timestamp=timestamp,
            retry_policy=retry_policy,
            rate_limiter=rate_limiter,
            retry_sleep=retry_sleep,
        )


def _execute_external_source_session_locked(
    *,
    root: str | Path,
    source: str,
    http: HttpClient | None,
    resume: bool,
    ieee_credential: str = "",
    quota_day_utc: str | None = None,
    timestamp: Callable[[], str] = utc_now,
    retry_policy: RetryPolicy | None = None,
    rate_limiter: RateLimiter | None = None,
    retry_sleep: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    """Execute one source while the caller holds the global session lock."""

    root_path = Path(root).resolve()
    wave, preflight = validate_persisted_external_preflight(root=root_path)
    if source not in wave.required_sources:
        raise ExternalRetrievalWaveError(f"source is not in the external wave: {source}")
    state_path = _safe_output_path(root_path, EXECUTION_STATE_PATH)
    state = (
        _load_execution_state(state_path, root_path, wave, preflight)
        if state_path.exists()
        else _initial_execution_state(root_path, wave, preflight, timestamp())
    )
    source_state = state["sources"][source]
    if source == "arXiv" and source_state.get("approved_snapshot_substitution"):
        raise ExternalRetrievalWaveError(
            "arXiv API retrieval is superseded for this wave by the approved "
            "snapshot route"
        )
    if source == "SemanticScholar" and _semantic_native_id_overlap_recovery_active(
        source_state
    ):
        _validate_authorized_semantic_native_id_overlap_recovery(
            root=root_path, source_state=source_state, wave=wave
        )
    if source == "arXiv" and _arxiv_retryable_5xx_recovery_active(source_state):
        _validate_authorized_arxiv_retryable_5xx_recovery(
            root=root_path, source_state=source_state, wave=wave
        )
    if source == "arXiv" and _arxiv_page_size_recovery_active(source_state):
        _validate_authorized_arxiv_page_size_recovery(
            root=root_path, source_state=source_state, wave=wave
        )
    if source_state["status"] == "COMPLETE":
        return state
    arxiv_transport_policy = (
        _active_arxiv_transport_policy(source_state)
        if source == "arXiv"
        else None
    )
    arxiv_page_size = (
        _active_arxiv_page_size(source_state) if source == "arXiv" else None
    )
    effective_retry_policy = retry_policy or RetryPolicy()
    if (
        arxiv_transport_policy is not None
        and effective_retry_policy.max_attempts
        != arxiv_transport_policy["maximum_attempts_per_invocation"]
    ):
        raise ExternalRetrievalWaveError(
            "arXiv active episode requires exactly three attempts per invocation"
        )

    started_at = timestamp()
    source_state.update(
        {
            "status": "RUNNING",
            "last_session_started_at_utc": started_at,
            "last_session_completed_at_utc": None,
            "pause_reason": None,
            "failure_reason": None,
        }
    )
    source_state.pop("pause_metadata", None)
    _sync_active_retry_episode(source_state)
    _save_execution_state(state_path, state)
    if source == "ACMDigitalLibrary":
        _execute_acm_import(root_path, wave, state, source_state, timestamp)
        _finalize_execution_state(state, timestamp())
        _save_execution_state(state_path, state)
        return state
    if http is None:
        raise ExternalRetrievalWaveError(f"HTTP client is required for {source}")
    if source == "IEEEXplore" and not ieee_credential:
        source_state["status"] = "BLOCKED_CREDENTIAL"
        source_state["failure_reason"] = f"{IEEE_CREDENTIAL_NAME} is absent"
        _save_execution_state(state_path, state)
        raise ExternalRetrievalWaveError(f"{IEEE_CREDENTIAL_NAME} is required")

    if source == "SemanticScholar":
        semantic_manifest_path = source_state.get(
            "semantic_control_gate", {}
        ).get("manifest_path", SEMANTIC_CONTROL_GATE_PATH)
        gate = _execute_semantic_control_gate(
            root=root_path,
            http=http,
            resume=resume,
            timestamp=timestamp,
            retry_policy=retry_policy or RetryPolicy(),
            rate_limiter=rate_limiter,
            retry_sleep=retry_sleep,
            manifest_relative_path=str(semantic_manifest_path),
        )
        source_state["semantic_control_gate"] = {
            "status": gate["status"],
            "manifest_path": semantic_manifest_path,
            "manifest_hash": gate["manifest_hash"],
        }
        if gate["status"] in {
            "PAUSED_PROVIDER_RATE_LIMIT",
            "PAUSED_TRANSIENT_PROVIDER",
            "PAUSED_TRANSIENT_TRANSPORT",
        }:
            source_state["status"] = gate["status"]
            source_state["failure_reason"] = None
            source_state["pause_reason"] = gate.get("pause_reason")
            if gate.get("pause_metadata") is not None:
                source_state["pause_metadata"] = dict(gate["pause_metadata"])
            source_state["candidate_request_count"] = 0
            source_state["control_requests_this_session"] = gate.get(
                "requests_this_session", 0
            )
            source_state["last_session_completed_at_utc"] = timestamp()
            _save_execution_state(state_path, state)
            return state
        if gate["status"] != "PASSED":
            source_state["status"] = "BLOCKED_SEMANTIC_CONTROL_GATE"
            source_state["failure_reason"] = (
                f"Semantic Scholar control gate is {gate['status']}"
            )
            source_state["candidate_request_count"] = 0
            source_state["last_session_completed_at_utc"] = timestamp()
            _save_execution_state(state_path, state)
            return state

    checkpoint_relative = source_state.get(
        "active_checkpoint_path", f"{EXECUTION_ROOT}/{source}/checkpoint"
    )
    if not str(checkpoint_relative).startswith(f"{EXECUTION_ROOT}/{source}/"):
        raise ExternalRetrievalWaveError("source checkpoint escaped its component namespace")
    checkpoint_dir = _safe_output_path(root_path, str(checkpoint_relative))
    checkpoint_exists = (checkpoint_dir / "review_dataset.json").exists()
    if checkpoint_exists and not resume:
        raise ExternalRetrievalWaveError(
            f"{source} checkpoint exists; pass --resume to continue"
        )
    if resume and not checkpoint_exists and source != "SemanticScholar":
        raise ExternalRetrievalWaveError(f"{source} has no checkpoint to resume")
    retrieval_resume = resume and checkpoint_exists
    ieee_mutable_total_mode = (
        source == "IEEEXplore"
        and _ieee_total_drift_recovery_active(source_state)
    )
    specs = _source_query_specs(
        wave,
        source,
        ieee_credential=ieee_credential,
        ieee_mutable_total_mode=ieee_mutable_total_mode,
        arxiv_read_timeout_seconds=(
            arxiv_transport_policy["read_timeout_seconds"]
            if arxiv_transport_policy is not None
            else None
        ),
        arxiv_page_size=arxiv_page_size,
    )
    request_budget = None
    quota = None
    if source == "IEEEXplore":
        quota_day = quota_day_utc or datetime.now(UTC).date().isoformat()
        used = _ieee_calls_on_day(root_path, checkpoint_dir, quota_day)
        request_budget = max(0, IEEE_DAILY_REQUEST_LIMIT - used)
        quota = {
            "quota_day_utc": quota_day,
            "daily_limit": IEEE_DAILY_REQUEST_LIMIT,
            "known_calls_before_session": used,
            "session_request_budget": request_budget,
        }

    before_attempts = _checkpoint_attempt_count(checkpoint_dir)
    dataset = execute_paginated_retrieval_run(
        run_id=source_state.get("active_run_id", f"{WAVE_ID}:{source}"),
        queries=specs,
        http_clients={source: http},
        checkpoint_dir=checkpoint_dir,
        resume=retrieval_resume,
        timestamp=timestamp,
        software_version=WAVE_VERSION,
        query_plan_version=wave.query_plan_hash,
        retry_policy=effective_retry_policy,
        rate_limiter=rate_limiter,
        retry_sleep=retry_sleep,
        request_budget=request_budget,
        pause_status_codes=(
            frozenset({429})
            if source in {"arXiv", "IEEEXplore", "SemanticScholar"}
            else frozenset()
        ),
        resumable_transport_exhaustion_sources=(
            frozenset({source})
            if source in {"arXiv", "SemanticScholar"}
            else frozenset()
        ),
        resumable_provider_5xx_exhaustion_sources=(
            frozenset({source})
            if source in {"arXiv", "SemanticScholar"}
            else frozenset()
        ),
    )
    after_attempts = len(dataset.retrieval_attempts)
    requests_this_session = after_attempts - before_attempts
    run = dataset.retrieval_runs[0]
    ieee_terminal_reconciliation = None
    if (
        source == "IEEEXplore"
        and ieee_mutable_total_mode
        and run.completion_status is RetrievalCompletionStatus.COMPLETE
    ):
        ieee_terminal_reconciliation = _ieee_terminal_reconciliation(dataset)
        run.metadata["ieee_terminal_reconciliation"] = ieee_terminal_reconciliation
        CheckpointStore(checkpoint_dir).save_dataset(dataset)
    pause_state = run.metadata.get("pause_state")
    if run.completion_status is RetrievalCompletionStatus.COMPLETE:
        status = "COMPLETE"
        failure_reason = None
        pause_reason = None
    elif pause_state == "REQUEST_BUDGET_EXHAUSTED":
        status = "PAUSED_DAILY_QUOTA" if source == "IEEEXplore" else "PAUSED"
        failure_reason = None
        pause_reason = run.metadata.get("pause_reason")
    elif pause_state == "PROVIDER_QUOTA_EXHAUSTED":
        status = "PAUSED_PROVIDER_QUOTA"
        failure_reason = None
        pause_reason = run.metadata.get("pause_reason")
    elif pause_state == "PROVIDER_RATE_LIMIT":
        status = "PAUSED_PROVIDER_RATE_LIMIT"
        failure_reason = None
        pause_reason = run.metadata.get("pause_reason")
    elif pause_state == "TRANSIENT_TRANSPORT_EXHAUSTED":
        status = "PAUSED_TRANSIENT_TRANSPORT"
        failure_reason = None
        pause_reason = run.metadata.get("pause_reason")
    elif pause_state == "TRANSIENT_PROVIDER_5XX_EXHAUSTED":
        status = "PAUSED_TRANSIENT_PROVIDER"
        failure_reason = None
        pause_reason = run.metadata.get("pause_reason")
    else:
        status = "FAILED"
        failure_reason = "; ".join(run.errors)
        pause_reason = None
    source_state.update(
        {
            "status": status,
            "checkpoint_path": checkpoint_dir.relative_to(root_path).as_posix(),
            "checkpoint_dataset": _file_reference(
                checkpoint_dir / "review_dataset.json", root_path
            ),
            "completed_query_count": sum(
                item.completion_status is RetrievalCompletionStatus.COMPLETE
                for item in dataset.source_queries
            ),
            "total_query_count": len(dataset.source_queries),
            "occurrence_count": len(dataset.occurrences),
            "attempt_count": len(dataset.retrieval_attempts),
            "requests_this_session": requests_this_session,
            "pause_reason": pause_reason,
            "failure_reason": failure_reason,
            "last_session_completed_at_utc": timestamp(),
        }
    )
    if run.metadata.get("pause_metadata") is not None:
        source_state["pause_metadata"] = dict(run.metadata["pause_metadata"])
    else:
        source_state.pop("pause_metadata", None)
    if quota is not None:
        source_state["ieee_quota"] = {
            **quota,
            "requests_this_session": requests_this_session,
            "known_calls_after_session": quota["known_calls_before_session"]
            + requests_this_session,
        }
    if ieee_terminal_reconciliation is not None:
        source_state["terminal_reconciliation"] = ieee_terminal_reconciliation
    _sync_active_retry_episode(source_state)
    _finalize_execution_state(state, timestamp())
    _save_execution_state(state_path, state)
    return state


def _source_query_specs(
    wave: ProductionRetrievalWave,
    source: str,
    *,
    ieee_credential: str,
    ieee_mutable_total_mode: bool = False,
    arxiv_read_timeout_seconds: float | None = None,
    arxiv_page_size: int | None = None,
) -> list[RetrievalQuerySpec]:
    specs = []
    for family in wave.query_families:
        if family.source_database != source:
            continue
        parameters = family.native_parameters
        limit = int(
            parameters.get("page_size")
            or parameters.get("pageSize")
            or parameters.get("limit")
            or parameters.get("max_results")
            or parameters.get("max_records")
        )
        if source == "arXiv" and arxiv_page_size is not None:
            if arxiv_page_size < 1 or arxiv_page_size > ARXIV_LEGACY_PAGE_SIZE:
                raise ExternalRetrievalWaveError(
                    "arXiv episode page size is outside the supported range"
                )
            limit = arxiv_page_size
        metadata = {
            "production_query_id": family.query_family_id,
            "frozen_request_specification_hash": parameters[
                "frozen_request_specification_hash"
            ],
            "content_policy": family.content_policy,
        }
        fields = []
        endpoint = None
        if source == "EuropePMC":
            metadata["result_type"] = parameters["resultType"]
            endpoint = "https://www.ebi.ac.uk/europepmc/webservices/rest/search"
        elif source == "SemanticScholar":
            fields = str(parameters["fields"]).split(",")
            metadata["sort"] = parameters["sort"]
            endpoint = "https://api.semanticscholar.org/graph/v1/paper/search/bulk"
        elif source == "arXiv":
            if arxiv_read_timeout_seconds is not None:
                metadata["request_timeout_seconds"] = (
                    arxiv_read_timeout_seconds
                )
            endpoint = "http://export.arxiv.org/api/query"
        elif source == "IEEEXplore":
            metadata.update(
                {
                    "query_parameter": parameters["query_parameter"],
                    "sort_field": parameters["sort_field"],
                    "sort_order": parameters["sort_order"],
                    "mutable_provider_totals": ieee_mutable_total_mode,
                }
            )
            endpoint = "https://ieeexploreapi.ieee.org/api/v1/search/articles"
        specs.append(
            RetrievalQuerySpec(
                source_database=source,
                query_text=family.query_text,
                query_version=family.query_version,
                limit=limit,
                endpoint=endpoint,
                fields=fields,
                metadata=metadata,
                pagination_mode="bulk" if source == "SemanticScholar" else None,
                credentials={"api_key": ieee_credential}
                if source == "IEEEXplore"
                else {},
            )
        )
    if len(specs) != 5:
        raise ExternalRetrievalWaveError(f"{source} must have exactly five queries")
    return specs


def _execute_acm_import(
    root: Path,
    wave: ProductionRetrievalWave,
    state: dict[str, Any],
    source_state: dict[str, Any],
    timestamp: Callable[[], str],
) -> None:
    query_texts = {
        item.query_family_id: item.query_text
        for item in wave.query_families
        if item.source_database == "ACMDigitalLibrary"
    }
    datasets = import_acm_selected_reconciliation(
        root / ACM_RECONCILIATION_PATH,
        root=root,
        query_text_by_parent=query_texts,
        query_version=wave.query_plan_version,
        run_id_prefix=f"{WAVE_ID}:ACMDigitalLibrary",
        software_version=WAVE_VERSION,
    )
    family_files = []
    occurrence_count = 0
    malformed_count = 0
    for family_id, dataset in datasets.items():
        suffix = family_id.removeprefix("STAR-").split("-", 1)[0]
        path = _safe_output_path(
            root, f"{EXECUTION_ROOT}/ACMDigitalLibrary/{suffix}_review_dataset.json"
        )
        save_review_dataset(path, dataset)
        occurrence_count += len(dataset.occurrences)
        malformed_count += sum(
            bool(item.metadata.get("malformed_but_identified"))
            for item in dataset.occurrences
        )
        family_files.append(
            {
                "family_id": family_id,
                "dataset": _file_reference(path, root),
                "occurrence_count": len(dataset.occurrences),
                "canonical_identity_count": len(dataset.canonical_records),
            }
        )
    if occurrence_count != 11664 or malformed_count != 3:
        raise ExternalRetrievalWaveError("ACM production import accounting changed")
    source_state.update(
        {
            "status": "COMPLETE",
            "completed_query_count": 5,
            "total_query_count": 5,
            "occurrence_count": occurrence_count,
            "malformed_but_identified_count": malformed_count,
            "family_datasets": family_files,
            "network_request_count": 0,
            "last_session_completed_at_utc": timestamp(),
        }
    )
    state["acm_live_search_performed"] = False


def _execute_semantic_control_gate(
    *,
    root: Path,
    http: HttpClient,
    resume: bool,
    timestamp: Callable[[], str],
    retry_policy: RetryPolicy,
    rate_limiter: RateLimiter | None,
    retry_sleep: Callable[[float], None],
    manifest_relative_path: str = SEMANTIC_CONTROL_GATE_PATH,
) -> dict[str, Any]:
    if manifest_relative_path not in {
        SEMANTIC_CONTROL_GATE_PATH,
        SEMANTIC_CONTROL_RECOVERY_GATE_PATH,
    }:
        raise ExternalRetrievalWaveError(
            "Semantic Scholar control manifest path is not authorized"
        )
    path = _safe_output_path(root, manifest_relative_path)
    control_path = root / SEMANTIC_CONTROL_PATH
    controls = load_semantic_control_set(control_path)
    if path.exists():
        manifest = _load_json(path)
        _validate_embedded_hash(manifest, "manifest_hash")
        if manifest["control_set"]["raw_sha256"] != _sha256(
            control_path.read_bytes()
        ):
            raise ExternalRetrievalWaveError("Semantic control binding changed")
        if manifest["status"] in {"PASSED", "FAILED", "UNRESOLVED"}:
            return manifest
        if not resume:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar control checkpoint exists; pass --resume"
            )
    else:
        if resume:
            raise ExternalRetrievalWaveError(
                "Semantic Scholar control gate has no checkpoint to resume"
            )
        manifest = {
            "schema_version": "1.1.0",
            "gate": "bulk_boolean_semantics",
            "status": "RUNNING",
            "control_set": {
                **_file_reference(control_path, root),
                "canonical_hash": controls.control_set_hash(),
            },
            "controls": [],
            "assertions": [assertion.to_dict() for assertion in controls.assertions],
            "candidate_queries_executed": False,
            "manifest_hash": None,
        }
    manifest.pop("pause_state", None)
    manifest.pop("pause_reason", None)
    manifest.pop("pause_metadata", None)
    manifest["requests_this_session"] = 0
    store = CheckpointStore(path.parent)
    limiter = rate_limiter or RateLimiter()
    observations = {item["probe_id"]: item for item in manifest["controls"]}
    terminal_unresolved = False
    for probe in controls.probes:
        observation = observations.get(probe.probe_id)
        if observation and observation["status"] == "SUCCEEDED":
            continue
        if observation is None:
            request = _semantic_control_request(probe.probe_id, probe.expression)
            observation = {
                "probe_id": probe.probe_id,
                "expression": probe.expression,
                "expression_sha256": _sha256(probe.expression.encode("utf-8")),
                "request": {
                    "method": request.method,
                    "url": request.url,
                    "params": request.sanitized_params(),
                },
                "request_hash": request.request_hash(),
                "status": "RUNNING",
                "reported_count": None,
                "attempts": [],
            }
            manifest["controls"].append(observation)
            observations[probe.probe_id] = observation
            _save_hashed_json(path, manifest, "manifest_hash")
        request = PageRequest(
            observation["request"]["method"],
            observation["request"]["url"],
            params=dict(observation["request"]["params"]),
            state={"probe_id": probe.probe_id},
        )
        if (
            observation["expression"] != probe.expression
            or observation["expression_sha256"]
            != _sha256(probe.expression.encode("utf-8"))
            or observation["request_hash"] != request.request_hash()
        ):
            raise ExternalRetrievalWaveError(
                "Semantic control frozen query/request binding changed"
            )
        observation["status"] = "RUNNING"
        attempts_before_invocation = len(observation["attempts"])
        while (
            len(observation["attempts"]) - attempts_before_invocation
            < retry_policy.max_attempts
        ):
            attempt_number = len(observation["attempts"]) + 1
            invocation_attempt_number = (
                len(observation["attempts"]) - attempts_before_invocation + 1
            )
            attempt = {
                "attempt_number": attempt_number,
                "started_at_utc": timestamp(),
                "status": "STARTED",
                "request_hash": request.request_hash(),
                "response": None,
                "error": None,
            }
            observation["attempts"].append(attempt)
            _save_hashed_json(path, manifest, "manifest_hash")
            try:
                delay = limiter.wait("SemanticScholar")
                manifest["requests_this_session"] += 1
                response = http.get(
                    request.url,
                    params=request.params,
                    timeout=request.timeout,
                )
            except Exception as exc:  # noqa: BLE001 - persist every control failure
                attempt["status"] = "FAILED"
                attempt["completed_at_utc"] = timestamp()
                attempt["error"] = f"{type(exc).__name__}: {exc}"
                transient_transport = (
                    type(exc).__name__ in TRANSPORT_ENVIRONMENT_FAILURE_TYPES
                )
                if (
                    transient_transport
                    and invocation_attempt_number < retry_policy.max_attempts
                ):
                    delay = retry_policy.delay(invocation_attempt_number)
                    attempt["retry_delay_seconds"] = delay
                    _save_hashed_json(path, manifest, "manifest_hash")
                    retry_sleep(delay)
                    continue
                if transient_transport:
                    pause_metadata = {
                        "source_database": "SemanticScholar",
                        "probe_id": probe.probe_id,
                        "response_received": False,
                        "attempts_this_invocation": (
                            len(observation["attempts"])
                            - attempts_before_invocation
                        ),
                        "maximum_attempts_per_invocation": retry_policy.max_attempts,
                        "failure_types": [
                            str(item.get("error") or "").partition(":")[0]
                            for item in observation["attempts"][
                                attempts_before_invocation:
                            ]
                        ],
                    }
                    observation["status"] = "PAUSED_TRANSIENT_TRANSPORT"
                    manifest["status"] = "PAUSED_TRANSIENT_TRANSPORT"
                    manifest["pause_state"] = "TRANSIENT_TRANSPORT_EXHAUSTED"
                    manifest["pause_reason"] = (
                        "TRANSIENT_TRANSPORT_EXHAUSTED_NO_RESPONSE"
                    )
                    manifest["pause_metadata"] = pause_metadata
                    manifest["candidate_queries_executed"] = False
                    _save_hashed_json(path, manifest, "manifest_hash")
                    return manifest
                observation["status"] = "UNRESOLVED"
                terminal_unresolved = True
                _save_hashed_json(path, manifest, "manifest_hash")
                break

            try:
                attempt_id = (
                    f"semantic-control-{probe.probe_id}-attempt-{attempt_number:03d}"
                )
                response_path, response_hash = store.save_response(attempt_id, response)
                attempt["response"] = {
                    "status": response.status_code,
                    "path": response_path,
                    "sha256": response_hash,
                    "byte_size": (path.parent / response_path).stat().st_size,
                }
                attempt["rate_limit_delay_seconds"] = delay
                retry_after = next(
                    (
                        str(value)
                        for key, value in (response.headers or {}).items()
                        if str(key).lower() == "retry-after"
                    ),
                    None,
                )
                attempt["response"]["retry_after_header_present"] = (
                    retry_after is not None
                )
                attempt["response"]["retry_after"] = retry_after
                if response.status_code == 429:
                    pause_metadata = {
                        "source_database": "SemanticScholar",
                        "probe_id": probe.probe_id,
                        "http_status": 429,
                        "retry_after_header_present": retry_after is not None,
                        "retry_after": retry_after,
                    }
                    attempt["status"] = "FAILED"
                    attempt["completed_at_utc"] = timestamp()
                    attempt["error"] = "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
                    attempt["provider_pause"] = dict(pause_metadata)
                    observation["status"] = "PAUSED_PROVIDER_RATE_LIMIT"
                    manifest["status"] = "PAUSED_PROVIDER_RATE_LIMIT"
                    manifest["pause_state"] = "PROVIDER_RATE_LIMIT"
                    manifest["pause_reason"] = attempt["error"]
                    manifest["pause_metadata"] = pause_metadata
                    manifest["candidate_queries_executed"] = False
                    _save_hashed_json(path, manifest, "manifest_hash")
                    return manifest
                if not 200 <= response.status_code < 300:
                    attempt["status"] = "FAILED"
                    attempt["completed_at_utc"] = timestamp()
                    attempt["error"] = f"HTTP {response.status_code}"
                    if (
                        response.status_code in retry_policy.retry_statuses
                        and invocation_attempt_number < retry_policy.max_attempts
                    ):
                        delay = retry_policy.delay(
                            invocation_attempt_number, retry_after
                        )
                        attempt["retry_delay_seconds"] = delay
                        _save_hashed_json(path, manifest, "manifest_hash")
                        retry_sleep(delay)
                        continue
                    if (
                        500 <= response.status_code < 600
                        and response.status_code in retry_policy.retry_statuses
                    ):
                        invocation_attempts = observation["attempts"][
                            attempts_before_invocation:
                        ]
                        pause_metadata = {
                            "source_database": "SemanticScholar",
                            "probe_id": probe.probe_id,
                            "http_statuses": [
                                item["response"]["status"]
                                for item in invocation_attempts
                            ],
                            "retry_after_header_present": retry_after is not None,
                            "retry_after": retry_after,
                            "attempts_this_invocation": len(invocation_attempts),
                            "maximum_attempts_per_invocation": (
                                retry_policy.max_attempts
                            ),
                        }
                        attempt["provider_pause"] = dict(pause_metadata)
                        observation["status"] = "PAUSED_TRANSIENT_PROVIDER"
                        manifest["status"] = "PAUSED_TRANSIENT_PROVIDER"
                        manifest["pause_state"] = (
                            "TRANSIENT_PROVIDER_5XX_EXHAUSTED"
                        )
                        manifest["pause_reason"] = (
                            "RETRYABLE_PROVIDER_5XX_EXHAUSTED"
                        )
                        manifest["pause_metadata"] = pause_metadata
                        manifest["candidate_queries_executed"] = False
                        _save_hashed_json(path, manifest, "manifest_hash")
                        return manifest
                    observation["status"] = "UNRESOLVED"
                    terminal_unresolved = True
                    _save_hashed_json(path, manifest, "manifest_hash")
                    break
                payload = response.json()
                if not isinstance(payload, dict) or "total" not in payload:
                    raise ExternalRetrievalWaveError(
                        "Semantic control response omitted total"
                    )
                count = int(payload["total"])
                if count < 0 or payload.get("error") or payload.get("errors"):
                    raise ExternalRetrievalWaveError(
                        "Semantic control response is invalid"
                    )
                observation["reported_count"] = count
                observation["status"] = "SUCCEEDED"
                attempt["status"] = "SUCCEEDED"
                attempt["completed_at_utc"] = timestamp()
                _save_hashed_json(path, manifest, "manifest_hash")
                break
            except Exception as exc:  # noqa: BLE001 - persist invalid response evidence
                attempt["status"] = "FAILED"
                attempt["completed_at_utc"] = timestamp()
                attempt["error"] = f"{type(exc).__name__}: {exc}"
                observation["status"] = "UNRESOLVED"
                terminal_unresolved = True
                _save_hashed_json(path, manifest, "manifest_hash")
                break
        if terminal_unresolved:
            break

    unresolved = [
        item["probe_id"]
        for item in manifest["controls"]
        if item["status"] != "SUCCEEDED"
    ]
    assertion_results = []
    failed = []
    counts = {item["probe_id"]: item["reported_count"] for item in manifest["controls"]}
    if not unresolved:
        for assertion in manifest["assertions"]:
            left = int(counts[assertion["left_probe_id"]])
            right = int(counts[assertion["right_probe_id"]])
            relation = assertion["relation"]
            passed = {
                "less_than_or_equal": left <= right,
                "greater_than_or_equal": left >= right,
                "equal": left == right,
            }[relation]
            result = {
                **assertion,
                "left_count": left,
                "right_count": right,
                "passed": passed,
            }
            assertion_results.append(result)
            if not passed:
                failed.append(assertion["assertion_id"])
    manifest["assertion_results"] = assertion_results
    manifest["unresolved_control_ids"] = unresolved
    manifest["failed_assertion_ids"] = failed
    manifest["status"] = "UNRESOLVED" if unresolved else "FAILED" if failed else "PASSED"
    manifest["candidate_queries_executed"] = False
    _save_hashed_json(path, manifest, "manifest_hash")
    return manifest


def _initial_execution_state(
    root: Path,
    wave: ProductionRetrievalWave,
    preflight: dict[str, Any],
    created_at: str,
) -> dict[str, Any]:
    return {
        "schema_version": "1.0.0",
        "execution_id": WAVE_ID,
        "status": "RUNNING",
        "created_at_utc": created_at,
        "updated_at_utc": created_at,
        "wave_manifest_hash": wave.manifest_hash(),
        "planned_wave_raw_sha256": _sha256(
            _safe_output_path(root, WAVE_PATH).read_bytes()
        ),
        "preflight_raw_sha256": _sha256(
            _safe_output_path(root, PREFLIGHT_PATH).read_bytes()
        ),
        "sources": {
            source: {
                "status": "NOT_STARTED",
                "completed_query_count": 0,
                "total_query_count": 5,
            }
            for source in wave.required_sources
        },
        "external_retrieval_completed_at_utc": None,
        "external_retrieval_cutoff_date": None,
        "acm_live_search_performed": False,
        "prior_survey_seed_imported": False,
        "identification_set_closed": False,
        "final_global_deduplication_executed": False,
        "prisma_generated": False,
        "screening_executed": False,
        "corpus_modified": False,
        "state_hash": None,
    }


def _load_execution_state(
    path: Path,
    root: Path,
    wave: ProductionRetrievalWave,
    preflight: dict[str, Any],
) -> dict[str, Any]:
    state = _load_json(path)
    _validate_embedded_hash(state, "state_hash")
    if (
        state.get("wave_manifest_hash") != wave.manifest_hash()
        or state.get("planned_wave_raw_sha256")
        != _sha256(_safe_output_path(root, WAVE_PATH).read_bytes())
        or state.get("preflight_raw_sha256")
        != _sha256(_safe_output_path(root, PREFLIGHT_PATH).read_bytes())
    ):
        raise ExternalRetrievalWaveError("execution checkpoint frozen-wave hash mismatch")
    try:
        validate_authorized_snapshot_substitution(root=root, state=state)
    except ArxivSnapshotIntegrationError as exc:
        raise ExternalRetrievalWaveError(str(exc)) from exc
    try:
        validate_authorized_prior_survey_imports(root=root, state=state)
    except PriorSurveyIntegrationError as exc:
        raise ExternalRetrievalWaveError(str(exc)) from exc
    return state


def _reparse_failed_ieee_total_drift_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    wave: ProductionRetrievalWave,
    recovered_at: str,
) -> dict[str, Any]:
    dataset.validate()
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError(
            "IEEE total-drift recovery requires exactly one run"
        )
    run = dataset.retrieval_runs[0]
    if (
        run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.query_plan_version != wave.query_plan_hash
    ):
        raise ExternalRetrievalWaveError(
            "IEEE checkpoint is not an eligible total-drift failure"
        )
    if len(dataset.retrieval_attempts) != IEEE_TOTAL_DRIFT_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError("IEEE failed attempt count changed")
    expected_occurrences = sum(item[3] for item in IEEE_TOTAL_DRIFT_EXPECTED)
    if len(dataset.occurrences) != expected_occurrences:
        raise ExternalRetrievalWaveError("IEEE failed occurrence count changed")

    specs = _source_query_specs(
        wave,
        "IEEEXplore",
        ieee_credential="offline-recovery-redacted",
        ieee_mutable_total_mode=True,
    )
    failed_query_plan_hash = run.query_plan_hash
    recovery_query_plan_hash = _query_plan_hash(specs)
    if len(dataset.source_queries) != len(specs):
        raise ExternalRetrievalWaveError("IEEE query count changed")
    adapter = PAGINATED_SOURCE_ADAPTERS["IEEEXplore"]
    response_store = CheckpointStore(checkpoint_dir)
    raw_response_references: list[tuple[str, str]] = []
    seen_response_paths: set[str] = set()
    continuation_plan = []
    total_histories: dict[str, list[int]] = {}

    for query, spec, expected in zip(
        dataset.source_queries, specs, IEEE_TOTAL_DRIFT_EXPECTED, strict=True
    ):
        initial_total, final_total, page_count, occurrence_count, next_start = expected
        expected_error = (
            f"{IEEE_TOTAL_DRIFT_ERROR_PREFIX}[{final_total}, {initial_total}]"
        )
        if (
            query.source_database != "IEEEXplore"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
            or query.completion_status is not RetrievalCompletionStatus.FAILED
            or query.status is not ProcessingStatus.FAILED
            or query.errors != [expected_error]
        ):
            raise ExternalRetrievalWaveError(
                "IEEE recovery refused changed query/failure provenance"
            )
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        if (
            len(pages) != page_count
            or [page.ordinal for page in pages] != list(range(page_count))
        ):
            raise ExternalRetrievalWaveError(
                "IEEE persisted page lineage is incomplete or unordered"
            )

        expected_start = 1
        query_identifiers: set[str] = set()
        observed_totals = []
        for page in pages:
            if (
                page.strategy != adapter.strategy
                or page.adapter_version != adapter.version
                or page.request_state != {"start_record": expected_start}
                or page.status is not RetrievalCompletionStatus.COMPLETE
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE persisted start_record/page lineage changed"
                )
            request = adapter.build_request(spec, page.request_state)
            attempts = sorted(
                (
                    attempt
                    for attempt in dataset.retrieval_attempts
                    if attempt.page_id == page.page_id
                ),
                key=lambda item: item.attempt_number,
            )
            if (
                len(attempts) != 1
                or page.attempt_ids != [attempts[0].attempt_id]
                or attempts[0].attempt_number != 1
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE recovery requires one zero-retry attempt per page"
                )
            attempt = attempts[0]
            if (
                attempt.status is not RetrievalAttemptStatus.SUCCEEDED
                or attempt.response_status != 200
                or attempt.error is not None
                or not attempt.raw_response_path
                or not attempt.raw_response_hash
                or attempt.request_method != request.method
                or attempt.request_url != request.url
                or attempt.request_params != request.sanitized_params()
                or attempt.request_headers != request.sanitized_headers()
                or attempt.request_hash != request.request_hash()
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE recovery requires exact successful request lineage"
                )
            if attempt.raw_response_path in seen_response_paths:
                raise ExternalRetrievalWaveError("IEEE raw response path is reused")
            try:
                response = response_store.load_response(
                    attempt.raw_response_path, attempt.raw_response_hash
                )
            except (OSError, ValueError) as exc:
                raise ExternalRetrievalWaveError(
                    f"IEEE raw response hash/read failure: {exc}"
                ) from exc
            seen_response_paths.add(attempt.raw_response_path)
            raw_response_references.append(
                (attempt.raw_response_path, attempt.raw_response_hash)
            )
            try:
                parsed = adapter.parse_response(spec, page.request_state, response)
            except Exception as exc:
                raise ExternalRetrievalWaveError(
                    f"IEEE offline total-drift parse failed: {type(exc).__name__}: {exc}"
                ) from exc
            if (
                parsed.incomplete_reason
                or parsed.raw_item_count != len(parsed.records)
                or parsed.raw_item_count < 1
                or parsed.raw_item_count > 200
                or parsed.total_is_exact
                or parsed.terminal
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE persisted page is not a valid nonterminal recovery page"
                )
            page_occurrences = sorted(
                (
                    occurrence
                    for occurrence in dataset.occurrences
                    if occurrence.retrieval_page_id == page.page_id
                ),
                key=lambda item: item.source_rank or 0,
            )
            if (
                len(page_occurrences) != parsed.raw_item_count
                or [item.source_identifier for item in page_occurrences]
                != parsed.native_identifiers
                or page.returned_item_count != parsed.raw_item_count
                or page.native_identifiers != parsed.native_identifiers
                or len(set(parsed.native_identifiers)) != len(parsed.native_identifiers)
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE persisted record/page accounting changed"
                )
            overlap = query_identifiers.intersection(parsed.native_identifiers)
            if overlap:
                raise ExternalRetrievalWaveError(
                    "IEEE stable identity overlaps across persisted pages"
                )
            query_identifiers.update(parsed.native_identifiers)
            observed_totals.append(int(parsed.source_reported_total))
            persisted_next_start = int(page.next_state["start_record"])
            if persisted_next_start != expected_start + parsed.raw_item_count:
                raise ExternalRetrievalWaveError(
                    "IEEE response created a pagination gap or overlap"
                )
            expected_start = persisted_next_start

            prior_error = page.metadata.pop("completion_error", None)
            if prior_error:
                page.metadata["prior_completion_error"] = prior_error
            page.total_is_exact = False
            page.metadata.update(parsed.metadata)
            page.metadata["offline_provider_total_drift_recovery"] = True
            page.metadata["raw_response_reused_without_network"] = True

        if (
            expected_start != next_start
            or len(query_identifiers) != occurrence_count
            or query.result_count != occurrence_count
            or observed_totals[0] != initial_total
            or observed_totals[-1] != final_total
            or any(value != initial_total for value in observed_totals[:-1])
        ):
            raise ExternalRetrievalWaveError(
                "IEEE observed total/count/continuation evidence changed"
            )
        query.status = ProcessingStatus.PARTIAL
        query.completion_status = RetrievalCompletionStatus.RUNNING
        query.completion_proof = None
        query.source_reported_total = final_total
        query.total_is_exact = False
        query.errors = []
        query.retrieval_ended_at = recovered_at
        query.metadata["provider_total_observations"] = observed_totals
        query.metadata["provider_total_observation_range"] = [
            min(observed_totals),
            max(observed_totals),
        ]
        query.metadata["next_start_record"] = next_start
        query.metadata["offline_provider_total_drift_recovery"] = True
        query.metadata["mutable_provider_totals"] = True
        total_histories[spec.metadata["production_query_id"]] = observed_totals
        continuation_plan.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "next_start_record": next_start,
                "persisted_page_count": page_count,
                "persisted_occurrence_count": occurrence_count,
                "observed_provider_totals": observed_totals,
            }
        )

    if len(raw_response_references) != IEEE_TOTAL_DRIFT_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError("IEEE raw-response accounting changed")
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.query_plan_hash = recovery_query_plan_hash
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.errors = [
        "offline IEEE provider-total-drift recovery complete; live pagination resume pending"
    ]
    run.metadata["provider_total_semantics"] = "MUTABLE_PAGINATION_OBSERVATION"
    run.metadata["network_requests_during_recovery"] = 0
    run.metadata["source_raw_response_count"] = len(raw_response_references)
    run.metadata["failed_episode_query_plan_hash"] = failed_query_plan_hash
    run.metadata["recovery_query_plan_hash"] = recovery_query_plan_hash
    dataset.validate()
    return {
        "raw_response_references": raw_response_references,
        "continuation_plan": continuation_plan,
        "provider_total_histories": total_histories,
    }


def _reparse_failed_ieee_repeated_window_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    root: Path,
    wave: ProductionRetrievalWave,
    recovered_at: str,
) -> dict[str, Any]:
    dataset.validate()
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery requires exactly one run"
        )
    run = dataset.retrieval_runs[0]
    specs = _source_query_specs(
        wave,
        "IEEEXplore",
        ieee_credential="offline-recovery-redacted",
        ieee_mutable_total_mode=True,
    )
    if (
        run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.query_plan_hash != _query_plan_hash(specs)
        or len(dataset.source_queries) != len(specs)
    ):
        raise ExternalRetrievalWaveError(
            "IEEE checkpoint is not an eligible repeated-window failure"
        )
    if len(dataset.retrieval_attempts) != IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError("IEEE episode-2 attempt count changed")
    if len(IEEE_REPEATED_WINDOW_EXPECTED) != len(specs) or len(
        IEEE_REPEATED_WINDOW_EXPECTED_TOTAL_HISTORIES
    ) != len(specs):
        raise ExternalRetrievalWaveError("IEEE repeated-window signature is incomplete")

    adapter = PAGINATED_SOURCE_ADAPTERS["IEEEXplore"]
    response_store = CheckpointStore(checkpoint_dir)
    raw_response_references: list[tuple[str, str]] = []
    seen_response_paths: set[str] = set()
    rejected_page_ids: set[str] = set()
    rejected_attempt_ids: set[str] = set()
    rejected_occurrence_ids: set[str] = set()
    rejection_evidence = []
    continuation_plan = []
    total_histories: dict[str, list[int]] = {}
    retained_page_count = 0

    for query, spec, expected, expected_history in zip(
        dataset.source_queries,
        specs,
        IEEE_REPEATED_WINDOW_EXPECTED,
        IEEE_REPEATED_WINDOW_EXPECTED_TOTAL_HISTORIES,
        strict=True,
    ):
        page_count, occurrence_count, rejected_start, rejected_count, next_start = expected
        if (
            query.source_database != "IEEEXplore"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
            or query.completion_status is not RetrievalCompletionStatus.FAILED
            or query.status is not ProcessingStatus.FAILED
            or query.result_count != occurrence_count
        ):
            raise ExternalRetrievalWaveError(
                "IEEE repeated-window recovery refused changed query provenance"
            )
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        if (
            len(pages) != page_count
            or len(pages) < 2
            or [page.ordinal for page in pages] != list(range(page_count))
        ):
            raise ExternalRetrievalWaveError(
                "IEEE episode-2 page lineage is incomplete or unordered"
            )

        parsed_pages = []
        observed_totals = []
        query_identifiers: set[str] = set()
        for page_index, page in enumerate(pages):
            if (
                page.strategy != adapter.strategy
                or page.adapter_version != adapter.version
                or page.status is not RetrievalCompletionStatus.COMPLETE
                or (
                    page_index
                    and page.request_state != pages[page_index - 1].next_state
                )
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE episode-2 pagination ordering or request state changed"
                )
            request = adapter.build_request(spec, page.request_state)
            attempts = sorted(
                (
                    attempt
                    for attempt in dataset.retrieval_attempts
                    if attempt.page_id == page.page_id
                ),
                key=lambda item: item.attempt_number,
            )
            if (
                len(attempts) != 1
                or page.attempt_ids != [attempts[0].attempt_id]
                or attempts[0].attempt_number != 1
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE repeated-window recovery requires one attempt per page"
                )
            attempt = attempts[0]
            if (
                attempt.status is not RetrievalAttemptStatus.SUCCEEDED
                or attempt.response_status != 200
                or attempt.error is not None
                or not attempt.raw_response_path
                or not attempt.raw_response_hash
                or attempt.request_method != request.method
                or attempt.request_url != request.url
                or attempt.request_params != request.sanitized_params()
                or attempt.request_headers != request.sanitized_headers()
                or attempt.request_hash != request.request_hash()
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE repeated-window recovery requires exact request identity"
                )
            if attempt.raw_response_path in seen_response_paths:
                raise ExternalRetrievalWaveError("IEEE raw response path is reused")
            try:
                response = response_store.load_response(
                    attempt.raw_response_path, attempt.raw_response_hash
                )
            except (OSError, ValueError) as exc:
                raise ExternalRetrievalWaveError(
                    f"IEEE repeated-window raw response hash/read failure: {exc}"
                ) from exc
            seen_response_paths.add(attempt.raw_response_path)
            raw_response_references.append(
                (attempt.raw_response_path, attempt.raw_response_hash)
            )
            try:
                parsed = adapter.parse_response(spec, page.request_state, response)
            except Exception as exc:
                raise ExternalRetrievalWaveError(
                    f"IEEE repeated-window offline parse failed: {type(exc).__name__}: {exc}"
                ) from exc
            page_occurrences = sorted(
                (
                    occurrence
                    for occurrence in dataset.occurrences
                    if occurrence.retrieval_page_id == page.page_id
                ),
                key=lambda item: item.source_rank or 0,
            )
            if (
                parsed.incomplete_reason
                or parsed.raw_item_count != len(parsed.records)
                or parsed.raw_item_count < 1
                or parsed.raw_item_count > spec.limit
                or parsed.total_is_exact
                or len(page_occurrences) != parsed.raw_item_count
                or [item.source_identifier for item in page_occurrences]
                != parsed.native_identifiers
                or page.returned_item_count != parsed.raw_item_count
                or page.native_identifiers != parsed.native_identifiers
                or len(set(parsed.native_identifiers)) != len(parsed.native_identifiers)
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE repeated-window page or occurrence accounting changed"
                )
            parsed_pages.append((page, attempt, parsed, page_occurrences))
            observed_totals.append(int(parsed.source_reported_total))
            if page_index:
                prior_page, _, prior_parsed, _ = parsed_pages[-2]
                expected_legacy_start = (
                    int(prior_page.request_state["start_record"])
                    + prior_parsed.raw_item_count
                )
                if int(page.request_state["start_record"]) != expected_legacy_start:
                    raise ExternalRetrievalWaveError(
                        "IEEE episode-2 lineage contains an unexplained pagination gap"
                    )

        previous_page, _, previous_parsed, _ = parsed_pages[-2]
        rejected_page, rejected_attempt, rejected_parsed, rejected_occurrences = (
            parsed_pages[-1]
        )
        previous_start = int(previous_page.request_state["start_record"])
        actual_rejected_start = int(rejected_page.request_state["start_record"])
        calculated_boundary = previous_start + int(spec.limit)
        expected_error = (
            "source repeated native identifiers across pages: "
            f"{sorted(rejected_parsed.native_identifiers)!r}"
        )
        if (
            query.errors != [expected_error]
            or rejected_page.metadata.get("completion_error") != expected_error
            or previous_parsed.raw_item_count != rejected_count
            or rejected_parsed.raw_item_count != rejected_count
            or actual_rejected_start != rejected_start
            or actual_rejected_start
            != previous_start + previous_parsed.raw_item_count
            or calculated_boundary != next_start
            or actual_rejected_start >= calculated_boundary
            or previous_start // int(spec.limit)
            != actual_rejected_start // int(spec.limit)
            or previous_parsed.native_identifiers
            != rejected_parsed.native_identifiers
            or previous_parsed.source_reported_total
            != rejected_parsed.source_reported_total
            or [record.original_metadata for record in previous_parsed.records]
            != [record.original_metadata for record in rejected_parsed.records]
            or previous_parsed.terminal
            or rejected_parsed.terminal
        ):
            raise ExternalRetrievalWaveError(
                "IEEE malformed repeated-window rejection signature"
            )
        if observed_totals != list(expected_history):
            raise ExternalRetrievalWaveError(
                "IEEE complete provider-total history changed"
            )

        for page, _, parsed, _ in parsed_pages[:-1]:
            overlap = query_identifiers.intersection(parsed.native_identifiers)
            if overlap:
                raise ExternalRetrievalWaveError(
                    "IEEE retained pages contain native-identifier overlap"
                )
            query_identifiers.update(parsed.native_identifiers)
        if len(query_identifiers) != occurrence_count - rejected_count:
            raise ExternalRetrievalWaveError(
                "IEEE retained occurrence accounting changed"
            )

        rejected_page_ids.add(rejected_page.page_id)
        rejected_attempt_ids.add(rejected_attempt.attempt_id)
        rejected_occurrence_ids.update(
            occurrence.occurrence_id for occurrence in rejected_occurrences
        )
        previous_page.next_state = {"start_record": calculated_boundary}
        previous_page.metadata["recovered_next_start_record"] = calculated_boundary
        rejection_evidence.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "preceding_page_id": previous_page.page_id,
                "rejected_page_id": rejected_page.page_id,
                "rejected_attempt_id": rejected_attempt.attempt_id,
                "rejected_request_hash": rejected_attempt.request_hash,
                "episode_2_response_path": (
                    checkpoint_dir / rejected_attempt.raw_response_path
                ).relative_to(root).as_posix(),
                "raw_response_hash": rejected_attempt.raw_response_hash,
                "preceding_start_record": previous_start,
                "rejected_start_record": actual_rejected_start,
                "returned_item_count": rejected_count,
                "corrected_next_start_record": calculated_boundary,
                "native_identifier_manifest_hash": _hash_payload(
                    {"native_identifiers": rejected_parsed.native_identifiers}
                ),
                "source_reported_total": rejected_parsed.source_reported_total,
                "terminal_error": expected_error,
                "classification": "REJECTED_WHOLE_PROVIDER_WINDOW_REPETITION",
            }
        )
        retained_page_count += len(pages) - 1
        query.page_ids = [page.page_id for page in pages[:-1]]
        query.result_count = occurrence_count - rejected_count
        query.status = ProcessingStatus.PARTIAL
        query.completion_status = RetrievalCompletionStatus.RUNNING
        query.completion_proof = None
        query.errors = []
        query.retrieval_ended_at = recovered_at
        query.total_is_exact = False
        query.metadata["provider_total_observations"] = observed_totals
        query.metadata["provider_total_observation_range"] = [
            min(observed_totals),
            max(observed_totals),
        ]
        query.metadata["recovery_retained_page_count"] = len(pages) - 1
        query.metadata["next_start_record"] = calculated_boundary
        query.metadata["offline_repeated_window_recovery"] = True
        query.metadata["mutable_provider_totals"] = True
        total_histories[spec.metadata["production_query_id"]] = observed_totals
        continuation_plan.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "next_start_record": calculated_boundary,
                "retained_page_count": len(pages) - 1,
                "retained_occurrence_count": occurrence_count - rejected_count,
                "rejected_page_count": 1,
                "observed_provider_totals": observed_totals,
            }
        )

    if (
        len(raw_response_references) != IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS
        or len(rejection_evidence) != IEEE_REPEATED_WINDOW_EXPECTED_REJECTIONS
        or retained_page_count != IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES
    ):
        raise ExternalRetrievalWaveError(
            "IEEE repeated-window recovery aggregate accounting changed"
        )

    dataset.retrieval_pages = [
        page for page in dataset.retrieval_pages if page.page_id not in rejected_page_ids
    ]
    dataset.retrieval_attempts = [
        attempt
        for attempt in dataset.retrieval_attempts
        if attempt.attempt_id not in rejected_attempt_ids
    ]
    dataset.occurrences = [
        occurrence
        for occurrence in dataset.occurrences
        if occurrence.occurrence_id not in rejected_occurrence_ids
    ]
    dataset.duplicate_decisions = [
        decision
        for decision in dataset.duplicate_decisions
        if decision.occurrence_id not in rejected_occurrence_ids
    ]
    for canonical in dataset.canonical_records:
        canonical.occurrence_ids = [
            occurrence_id
            for occurrence_id in canonical.occurrence_ids
            if occurrence_id not in rejected_occurrence_ids
        ]
    dataset.canonical_records = [
        canonical for canonical in dataset.canonical_records if canonical.occurrence_ids
    ]
    if any(
        canonical.survivor_occurrence_id in rejected_occurrence_ids
        for canonical in dataset.canonical_records
    ):
        raise ExternalRetrievalWaveError(
            "IEEE rejected occurrence was a canonical survivor"
        )

    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = None
    run.errors = [
        "offline IEEE repeated-window recovery complete; live pagination resume pending"
    ]
    run.metadata.pop("pause_state", None)
    run.metadata.pop("pause_reason", None)
    run.metadata["provider_total_semantics"] = "MUTABLE_PAGINATION_OBSERVATION"
    run.metadata["network_requests_during_recovery"] = 0
    dataset.validate()
    return {
        "raw_response_references": raw_response_references,
        "continuation_plan": continuation_plan,
        "provider_total_histories": total_histories,
        "rejection_evidence": rejection_evidence,
        "retained_page_count": retained_page_count,
    }


def _copy_recovery_raw_responses(
    *,
    root: Path,
    source_checkpoint_dir: Path,
    recovery_checkpoint_dir: Path,
    raw_response_references: list[tuple[str, str]],
    source_path_key: str,
    error_prefix: str,
) -> list[dict[str, Any]]:
    bindings = []
    for relative_path, expected_hash in raw_response_references:
        response_path = Path(relative_path)
        if (
            response_path.is_absolute()
            or ".." in response_path.parts
            or not response_path.parts
            or response_path.parts[0] != "responses"
        ):
            raise ExternalRetrievalWaveError(
                f"{error_prefix} raw response path is unsafe"
            )
        source = source_checkpoint_dir / response_path
        raw = source.read_bytes()
        if _sha256(raw) != expected_hash:
            raise ExternalRetrievalWaveError(
                f"{error_prefix} raw response hash mismatch: {relative_path}"
            )
        destination = recovery_checkpoint_dir / response_path
        atomic_write(destination, raw)
        if destination.read_bytes() != raw:
            raise ExternalRetrievalWaveError(
                f"{error_prefix} recovery response copy mismatch: {relative_path}"
            )
        bindings.append(
            {
                source_path_key: source.relative_to(root).as_posix(),
                "recovery_copy_path": destination.relative_to(root).as_posix(),
                "byte_size": len(raw),
                "raw_sha256": expected_hash,
            }
        )
    return bindings


def _verify_recovery_raw_bindings(
    root: Path, bindings: list[dict[str, Any]], source_path_key: str
) -> None:
    for binding in bindings:
        for key in (source_path_key, "recovery_copy_path"):
            path = _safe_output_path(root, binding[key])
            raw = path.read_bytes()
            if len(raw) != binding["byte_size"] or _sha256(raw) != binding["raw_sha256"]:
                raise ExternalRetrievalWaveError(
                    "recovery raw-response binding changed"
                )


def _validate_authorized_ieee_total_drift_recovery(
    source_state: dict[str, Any], root: Path
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 2:
        raise ExternalRetrievalWaveError("IEEE recovery episode lineage changed")
    failed, recovered = episodes
    if (
        failed.get("episode_number") != 1
        or failed.get("status") != "FAILED"
        or not failed.get("immutable")
        or recovered.get("episode_number") != 2
        or recovered.get("status") != IEEE_TOTAL_DRIFT_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 1
        or recovered.get("network_used") is not False
        or recovered.get("immutable") is not False
        or source_state.get("active_episode_number") != 2
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
    ):
        raise ExternalRetrievalWaveError(
            "authorized IEEE total-drift recovery lineage changed"
        )
    source_checkpoint = _safe_output_path(
        root, recovered["source_episode_checkpoint"]["path"]
    )
    _verify_file_reference(
        source_checkpoint, recovered["source_episode_checkpoint"], root
    )
    recovery_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(recovery_checkpoint, recovered["checkpoint_dataset"], root)
    bindings = recovered.get("source_raw_responses", [])
    if len(bindings) != IEEE_TOTAL_DRIFT_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError("IEEE recovery raw-response manifest changed")
    _verify_recovery_raw_bindings(root, bindings, "failed_episode_path")


def _validate_authorized_ieee_repeated_window_recovery(
    source_state: dict[str, Any], root: Path
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 3:
        raise ExternalRetrievalWaveError("IEEE episode-3 recovery lineage changed")
    first, second, recovered = episodes
    if (
        first.get("episode_number") != 1
        or second.get("episode_number") != 2
        or not first.get("immutable")
        or not second.get("immutable")
        or second.get("status") != "FAILED"
        or recovered.get("episode_number") != 3
        or recovered.get("status") != IEEE_REPEATED_WINDOW_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 2
        or recovered.get("network_used") is not False
        or recovered.get("immutable") is not False
        or recovered.get("retained_page_count")
        != IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES
        or len(recovered.get("rejection_evidence", []))
        != IEEE_REPEATED_WINDOW_EXPECTED_REJECTIONS
        or recovered.get("known_daily_calls_preserved")
        != IEEE_REPEATED_WINDOW_EXPECTED_KNOWN_CALLS
        or source_state.get("ieee_quota", {}).get("known_calls_after_session")
        != IEEE_REPEATED_WINDOW_EXPECTED_KNOWN_CALLS
        or source_state.get("active_episode_number") != 3
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
    ):
        raise ExternalRetrievalWaveError(
            "authorized IEEE episode-3 recovery lineage changed"
        )
    for episode in (first, second):
        checkpoint = _safe_output_path(
            root, episode["checkpoint_dataset"]["path"]
        )
        _verify_file_reference(checkpoint, episode["checkpoint_dataset"], root)
    source_checkpoint = _safe_output_path(
        root, recovered["source_episode_checkpoint"]["path"]
    )
    _verify_file_reference(
        source_checkpoint, recovered["source_episode_checkpoint"], root
    )
    recovery_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(recovery_checkpoint, recovered["checkpoint_dataset"], root)
    bindings = recovered.get("source_raw_responses", [])
    if (
        len(bindings) != IEEE_REPEATED_WINDOW_EXPECTED_ATTEMPTS
        or len(recovered.get("rejected_raw_responses", []))
        != IEEE_REPEATED_WINDOW_EXPECTED_REJECTIONS
    ):
        raise ExternalRetrievalWaveError(
            "IEEE episode-3 raw-response manifest changed"
        )
    _verify_recovery_raw_bindings(root, bindings, "episode_2_path")
    dataset = load_review_dataset(recovery_checkpoint)
    if (
        len(dataset.retrieval_pages) != IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES
        or len(dataset.retrieval_attempts)
        != IEEE_REPEATED_WINDOW_EXPECTED_VALID_PAGES
        or [
            query.metadata.get("next_start_record")
            for query in dataset.source_queries
        ]
        != [item[4] for item in IEEE_REPEATED_WINDOW_EXPECTED]
    ):
        raise ExternalRetrievalWaveError(
            "IEEE episode-3 continuation checkpoint changed"
        )


def _ieee_total_drift_recovery_active(source_state: Mapping[str, Any]) -> bool:
    active_number = source_state.get("active_episode_number")
    active = next(
        (
            item
            for item in source_state.get("execution_episodes", [])
            if item.get("episode_number") == active_number
        ),
        None,
    )
    return bool(
        active
        and (
            (
                active.get("episode_number") == 2
                and active.get("recovery_of_episode_number") == 1
                and active.get("authorization_reason")
                == "OFFLINE_MUTABLE_PROVIDER_TOTAL_RECONCILIATION"
            )
            or (
                active.get("episode_number") == 3
                and active.get("recovery_of_episode_number") == 2
                and active.get("authorization_reason")
                == "OFFLINE_REPEATED_PROVIDER_WINDOW_RECONCILIATION"
            )
        )
    )


def _ieee_terminal_reconciliation(dataset: Any) -> dict[str, Any]:
    families = []
    for query in dataset.source_queries:
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        identifiers = [
            identifier for page in pages for identifier in page.native_identifiers
        ]
        duplicate_count = len(identifiers) - len(set(identifiers))
        if (
            query.completion_status is not RetrievalCompletionStatus.COMPLETE
            or not pages
            or not pages[-1].terminal
            or duplicate_count
        ):
            raise ExternalRetrievalWaveError(
                "IEEE terminal reconciliation requires complete overlap-free pagination"
            )
        current_observations = [
            int(page.metadata["provider_total_observation"]) for page in pages
        ]
        retained_page_count = query.metadata.get("recovery_retained_page_count")
        if retained_page_count is None:
            observations = current_observations
        else:
            preserved_observations = list(
                query.metadata.get("provider_total_observations", [])
            )
            if (
                not isinstance(retained_page_count, int)
                or retained_page_count < 1
                or retained_page_count > len(current_observations)
                or len(preserved_observations) < retained_page_count
            ):
                raise ExternalRetrievalWaveError(
                    "IEEE recovered provider-total history is malformed"
                )
            observations = [
                *preserved_observations,
                *current_observations[retained_page_count:],
            ]
        query.metadata["provider_total_observations"] = observations
        query.metadata["provider_total_observation_range"] = [
            min(observations),
            max(observations),
        ]
        query.metadata.pop("next_start_record", None)
        final_total = observations[-1]
        unique_count = len(set(identifiers))
        difference = unique_count - final_total
        drift_span = max(observations) - min(observations)
        families.append(
            {
                "production_query_id": query.metadata["production_query_id"],
                "unique_retrieved_identities": unique_count,
                "page_count": len(pages),
                "observed_provider_totals": observations,
                "provider_total_range": [min(observations), max(observations)],
                "duplicate_identities_across_pages": duplicate_count,
                "final_provider_total": final_total,
                "retrieved_minus_final_provider_total": difference,
                "discrepancy_explainable_by_observed_index_drift": (
                    abs(difference) <= drift_span
                ),
                "snapshot_equivalent_completeness_claimed": False,
                "completion_basis": (
                    "OFFSET_PAGINATION_EXHAUSTED_AGAINST_CURRENT_PAGE_TOTAL"
                ),
                "limitation": (
                    "mutable provider totals prevent a snapshot-equivalent completeness claim"
                ),
            }
        )
    return {
        "status": "TERMINAL_OFFSET_EXHAUSTION_RECONCILED_WITH_INDEX_DRIFT_LIMITATION",
        "families": families,
        "snapshot_equivalent_completeness_claimed": False,
    }


def _reparse_failed_europe_pmc_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    wave: ProductionRetrievalWave,
    recovered_at: str,
) -> dict[str, Any]:
    dataset.validate()
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError(
            "Europe PMC terminal recovery requires exactly one run"
        )
    run = dataset.retrieval_runs[0]
    if (
        run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.query_plan_version != wave.query_plan_hash
    ):
        raise ExternalRetrievalWaveError(
            "Europe PMC checkpoint is not an eligible terminal-sentinel failure"
        )
    if len(dataset.retrieval_attempts) != EUROPE_PMC_RECOVERY_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError(
            "Europe PMC failed attempt count changed"
        )
    if len(dataset.occurrences) != sum(EUROPE_PMC_RECOVERY_EXPECTED_COUNTS):
        raise ExternalRetrievalWaveError(
            "Europe PMC failed occurrence count changed"
        )

    specs = _source_query_specs(wave, "EuropePMC", ieee_credential="")
    if len(dataset.source_queries) != len(specs):
        raise ExternalRetrievalWaveError("Europe PMC query count changed")
    adapter = PAGINATED_SOURCE_ADAPTERS["EuropePMC"]
    response_store = CheckpointStore(checkpoint_dir)
    raw_response_references: list[tuple[str, str]] = []
    seen_response_paths: set[str] = set()
    recovered_counts: dict[str, int] = {}
    failed_query_count = 0

    for query_index, (query, spec) in enumerate(
        zip(dataset.source_queries, specs, strict=True)
    ):
        expected_count = EUROPE_PMC_RECOVERY_EXPECTED_COUNTS[query_index]
        if (
            query.source_database != "EuropePMC"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
        ):
            raise ExternalRetrievalWaveError(
                "Europe PMC frozen query/request binding changed"
            )
        was_failed = query.completion_status is RetrievalCompletionStatus.FAILED
        if was_failed:
            failed_query_count += 1
            if query.errors != [EUROPE_PMC_TERMINAL_ERROR]:
                raise ExternalRetrievalWaveError(
                    "Europe PMC recovery refused a non-terminal-sentinel failure"
                )
        elif (
            query.completion_status is not RetrievalCompletionStatus.COMPLETE
            or query.status is not ProcessingStatus.OK
            or query.errors
        ):
            raise ExternalRetrievalWaveError(
                "Europe PMC recovery found an unsupported query state"
            )

        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        if not pages or [page.ordinal for page in pages] != list(range(len(pages))):
            raise ExternalRetrievalWaveError(
                "Europe PMC page lineage is incomplete or unordered"
            )
        expected_cursor = "*"
        cumulative_count = 0
        exact_hit_count: int | None = None
        for page in pages:
            if (
                page.strategy != adapter.strategy
                or page.adapter_version != adapter.version
                or page.request_state != {"cursor_mark": expected_cursor}
            ):
                raise ExternalRetrievalWaveError(
                    "Europe PMC persisted pagination/request lineage changed"
                )
            request = adapter.build_request(spec, page.request_state)
            attempts = sorted(
                (
                    attempt
                    for attempt in dataset.retrieval_attempts
                    if attempt.page_id == page.page_id
                ),
                key=lambda item: item.attempt_number,
            )
            if (
                not attempts
                or [attempt.attempt_id for attempt in attempts] != page.attempt_ids
                or [attempt.attempt_number for attempt in attempts]
                != list(range(1, len(attempts) + 1))
            ):
                raise ExternalRetrievalWaveError(
                    "Europe PMC attempt lineage is incomplete or unordered"
                )

            parsed_attempts = []
            parse_state = {
                "cursor_mark": expected_cursor,
                "retrieved_count": cumulative_count,
                "expected_hit_count": exact_hit_count,
            }
            for attempt in attempts:
                if (
                    attempt.response_status is None
                    or not 200 <= attempt.response_status < 300
                    or not attempt.raw_response_path
                    or not attempt.raw_response_hash
                    or attempt.request_method != request.method
                    or attempt.request_url != request.url
                    or attempt.request_params != request.sanitized_params()
                    or attempt.request_headers != request.sanitized_headers()
                    or attempt.request_hash != request.request_hash()
                ):
                    raise ExternalRetrievalWaveError(
                        "Europe PMC recovery requires exact successful HTTP request lineage"
                    )
                if attempt.status is RetrievalAttemptStatus.FAILED:
                    if attempt.error != EUROPE_PMC_TERMINAL_ERROR:
                        raise ExternalRetrievalWaveError(
                            "Europe PMC recovery refused a non-parser attempt failure"
                        )
                elif attempt.status is not RetrievalAttemptStatus.SUCCEEDED:
                    raise ExternalRetrievalWaveError(
                        "Europe PMC recovery found an unsupported attempt state"
                    )
                if attempt.raw_response_path in seen_response_paths:
                    raise ExternalRetrievalWaveError(
                        "Europe PMC raw response path is reused"
                    )
                try:
                    response = response_store.load_response(
                        attempt.raw_response_path, attempt.raw_response_hash
                    )
                except (OSError, ValueError) as exc:
                    raise ExternalRetrievalWaveError(
                        f"Europe PMC raw response hash/read failure: {exc}"
                    ) from exc
                seen_response_paths.add(attempt.raw_response_path)
                raw_response_references.append(
                    (attempt.raw_response_path, attempt.raw_response_hash)
                )
                try:
                    parsed = adapter.parse_response(spec, parse_state, response)
                except Exception as exc:
                    raise ExternalRetrievalWaveError(
                        "Europe PMC offline terminal recovery parse failed: "
                        f"{type(exc).__name__}: {exc}"
                    ) from exc
                parsed_attempts.append(parsed)

            parsed = parsed_attempts[-1]
            signatures = {
                _hash_payload(
                    {
                        "raw_item_count": item.raw_item_count,
                        "native_identifiers": item.native_identifiers,
                        "next_state": item.next_state,
                        "terminal": item.terminal,
                        "source_reported_total": item.source_reported_total,
                    }
                )
                for item in parsed_attempts
            }
            if len(signatures) != 1:
                raise ExternalRetrievalWaveError(
                    "Europe PMC retry responses disagree for one page"
                )
            page_occurrences = sorted(
                (
                    occurrence
                    for occurrence in dataset.occurrences
                    if occurrence.retrieval_page_id == page.page_id
                ),
                key=lambda item: item.source_rank or 0,
            )
            if (
                len(page_occurrences) != parsed.raw_item_count
                or [item.source_identifier for item in page_occurrences]
                != parsed.native_identifiers
                or page.returned_item_count != parsed.raw_item_count
                or page.native_identifiers != parsed.native_identifiers
            ):
                raise ExternalRetrievalWaveError(
                    "Europe PMC persisted record/page accounting changed"
                )
            if parsed.source_reported_total != expected_count or not parsed.total_is_exact:
                raise ExternalRetrievalWaveError(
                    "Europe PMC exact provider hitCount changed"
                )

            parsed_legacy_next_state = (
                {"cursor_mark": parsed.next_state["cursor_mark"]}
                if parsed.next_state is not None
                else None
            )
            final_attempt = attempts[-1]
            if page.status is RetrievalCompletionStatus.COMPLETE:
                if (
                    page.next_state != parsed_legacy_next_state
                    or page.source_reported_total != parsed.source_reported_total
                    or page.total_is_exact != parsed.total_is_exact
                    or page.terminal != parsed.terminal
                    or page.completion_proof != parsed.completion_proof
                    or final_attempt.status is not RetrievalAttemptStatus.SUCCEEDED
                ):
                    raise ExternalRetrievalWaveError(
                        "Europe PMC completed-page evidence changed"
                    )
            else:
                if (
                    not was_failed
                    or page is not pages[-1]
                    or page.status is not RetrievalCompletionStatus.FAILED
                    or not parsed.terminal
                    or not parsed.metadata.get("repeated_cursor_terminal_sentinel")
                ):
                    raise ExternalRetrievalWaveError(
                        "Europe PMC recovery found a non-terminal failed page"
                    )
                prior_completion_error = page.metadata.get("completion_error")
                page.status = RetrievalCompletionStatus.COMPLETE
                page.source_reported_total = parsed.source_reported_total
                page.total_is_exact = parsed.total_is_exact
                page.terminal = parsed.terminal
                page.completion_proof = parsed.completion_proof
                page.next_state = parsed_legacy_next_state
                page.metadata = {
                    **parsed.metadata,
                    "offline_terminal_sentinel_recovery": True,
                    "raw_response_reused_without_network": True,
                    "prior_completion_error": prior_completion_error,
                }
                final_attempt.metadata = {
                    **final_attempt.metadata,
                    "offline_terminal_sentinel_recovery": True,
                    "prior_error": final_attempt.error,
                    "raw_response_reused_without_network": True,
                }
                final_attempt.status = RetrievalAttemptStatus.SUCCEEDED
                final_attempt.error = None
            cumulative_count += parsed.raw_item_count
            exact_hit_count = parsed.source_reported_total
            if parsed.next_state is not None:
                expected_cursor = str(parsed.next_state["cursor_mark"])
            elif page is not pages[-1]:
                raise ExternalRetrievalWaveError(
                    "Europe PMC terminal page is not last in its query lineage"
                )

        if not pages[-1].terminal or pages[-1].completion_proof != (
            "europe_pmc_cursor_exhausted"
        ):
            raise ExternalRetrievalWaveError(
                "Europe PMC query lacks a verified terminal page"
            )
        if cumulative_count != expected_count:
            raise ExternalRetrievalWaveError(
                "Europe PMC cumulative count does not match exact hitCount"
            )
        if was_failed:
            query.status = ProcessingStatus.OK
            query.completion_status = RetrievalCompletionStatus.COMPLETE
            query.completion_proof = pages[-1].completion_proof
            query.result_count = cumulative_count
            query.source_reported_total = expected_count
            query.total_is_exact = True
            query.errors = []
            query.retrieval_ended_at = recovered_at
            query.metadata["offline_terminal_sentinel_recovery"] = True
        elif (
            query.result_count != cumulative_count
            or query.source_reported_total != expected_count
            or not query.total_is_exact
            or query.completion_proof != pages[-1].completion_proof
        ):
            raise ExternalRetrievalWaveError(
                "Europe PMC completed QF02 accounting changed"
            )
        recovered_counts[spec.metadata["production_query_id"]] = cumulative_count

    if failed_query_count != 4:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery expected exactly four sentinel-rejected queries"
        )
    if len(raw_response_references) != EUROPE_PMC_RECOVERY_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError(
            "Europe PMC raw-response accounting changed"
        )
    run.status = ProcessingStatus.OK
    run.completion_status = RetrievalCompletionStatus.COMPLETE
    run.retrieval_completed_at = recovered_at
    run.retrieval_cutoff_date = recovered_at[:10]
    run.errors = []
    run.metadata["offline_terminal_sentinel_recovery"] = True
    run.metadata["network_requests_during_recovery"] = 0
    run.metadata["source_raw_response_count"] = len(raw_response_references)
    dataset.validate()
    return {
        "raw_response_references": raw_response_references,
        "query_occurrence_counts": recovered_counts,
    }


def _validate_authorized_europe_pmc_terminal_recovery(
    source_state: dict[str, Any], root: Path
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 2:
        raise ExternalRetrievalWaveError(
            "Europe PMC recovery episode lineage changed"
        )
    failed, recovered = episodes
    if (
        failed.get("episode_number") != 1
        or failed.get("status") != "FAILED"
        or not failed.get("immutable")
        or recovered.get("episode_number") != 2
        or recovered.get("status") != EUROPE_PMC_TERMINAL_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 1
        or not recovered.get("immutable")
        or recovered.get("network_used") is not False
        or source_state.get("active_episode_number") != 2
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
    ):
        raise ExternalRetrievalWaveError(
            "authorized Europe PMC terminal-recovery lineage changed"
        )
    source_checkpoint = _safe_output_path(
        root, recovered["source_episode_checkpoint"]["path"]
    )
    _verify_file_reference(
        source_checkpoint, recovered["source_episode_checkpoint"], root
    )
    recovery_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(recovery_checkpoint, recovered["checkpoint_dataset"], root)
    for binding in recovered.get("source_raw_responses", []):
        for key in ("failed_episode_path", "recovery_copy_path"):
            path = _safe_output_path(root, binding[key])
            raw = path.read_bytes()
            if len(raw) != binding["byte_size"] or _sha256(raw) != binding["raw_sha256"]:
                raise ExternalRetrievalWaveError(
                    "authorized Europe PMC recovery raw-response binding changed"
                )


def _reparse_failed_pubmed_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    wave: ProductionRetrievalWave,
    recovered_at: str,
) -> dict[str, Any]:
    dataset.validate()
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError("PubMed parser recovery requires exactly one run")
    run = dataset.retrieval_runs[0]
    if (
        run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.query_plan_version != wave.query_plan_hash
    ):
        raise ExternalRetrievalWaveError(
            "PubMed episode-2 run is not an eligible parser-only failure"
        )
    if not dataset.occurrences or not dataset.retrieval_attempts:
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery requires persisted fetched records and responses"
        )
    if any(
        item.status is not ProcessingStatus.FAILED
        or not item.errors
        or any("PubMed EFetch PMID sequence" not in error for error in item.errors)
        for item in dataset.source_queries
    ):
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery refused for a non-sequence parser failure"
        )

    specs = _source_query_specs(wave, "PubMed", ieee_credential="")
    if len(dataset.source_queries) != len(specs):
        raise ExternalRetrievalWaveError("PubMed episode-2 query count changed")
    adapter = PAGINATED_SOURCE_ADAPTERS["PubMed"]
    response_store = CheckpointStore(checkpoint_dir)
    old_identifiers = {item.source_identifier for item in dataset.occurrences}
    raw_response_references: list[tuple[str, str]] = []
    seen_response_paths: set[str] = set()
    reconstructed_occurrences: list[RecordOccurrence] = []
    remaining_batches: list[dict[str, Any]] = []
    historical_request_hashes = {
        attempt.request_hash for attempt in dataset.retrieval_attempts
    }

    for query, spec in zip(dataset.source_queries, specs, strict=True):
        if (
            query.source_database != "PubMed"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
        ):
            raise ExternalRetrievalWaveError(
                "PubMed episode-2 frozen query/request binding changed"
            )
        pages = sorted(
            (
                page
                for page in dataset.retrieval_pages
                if page.source_query_id == query.query_id
            ),
            key=lambda item: item.ordinal,
        )
        if not pages or pages[0].request_state != adapter.initial_state(spec):
            raise ExternalRetrievalWaveError(
                "PubMed episode-2 pagination does not start with frozen ESearch"
            )
        expected_state = adapter.initial_state(spec)
        source_rank = 0
        for page in pages:
            if page.request_state != expected_state:
                raise ExternalRetrievalWaveError(
                    "PubMed episode-2 persisted pagination state sequence changed"
                )
            if page.adapter_version not in {"2.0.0", adapter.version}:
                raise ExternalRetrievalWaveError(
                    "PubMed episode-2 adapter version is not eligible for parser recovery"
                )
            request = adapter.build_request(spec, page.request_state)
            attempts = [
                attempt
                for attempt in dataset.retrieval_attempts
                if attempt.page_id == page.page_id
            ]
            if not attempts:
                raise ExternalRetrievalWaveError(
                    "PubMed episode-2 page lacks persisted attempt provenance"
                )
            for attempt in attempts:
                if (
                    attempt.status is not RetrievalAttemptStatus.SUCCEEDED
                    or attempt.response_status is None
                    or not 200 <= attempt.response_status < 300
                    or not attempt.raw_response_path
                    or not attempt.raw_response_hash
                    or attempt.request_method != request.method
                    or attempt.request_hash != request.request_hash()
                ):
                    raise ExternalRetrievalWaveError(
                        "PubMed parser recovery requires successful hash-bound responses"
                    )
                if attempt.raw_response_path in seen_response_paths:
                    raise ExternalRetrievalWaveError(
                        "PubMed episode-2 raw response path is reused"
                    )
                _load_pubmed_recovery_response(
                    response_store,
                    attempt.raw_response_path,
                    attempt.raw_response_hash,
                )
                seen_response_paths.add(attempt.raw_response_path)
                raw_response_references.append(
                    (attempt.raw_response_path, attempt.raw_response_hash)
                )
            attempt = attempts[-1]
            response = _load_pubmed_recovery_response(
                response_store,
                str(attempt.raw_response_path),
                str(attempt.raw_response_hash),
            )
            try:
                parsed = adapter.parse_response(spec, request.state, response)
            except Exception as exc:
                raise ExternalRetrievalWaveError(
                    f"PubMed episode-2 offline parse failed: {type(exc).__name__}: {exc}"
                ) from exc
            if parsed.incomplete_reason or parsed.raw_item_count != len(parsed.records):
                raise ExternalRetrievalWaveError(
                    parsed.incomplete_reason
                    or "PubMed recovery parser record accounting mismatch"
                )
            expected_pmids = list(request.state.get("batch_pmids") or [])
            returned_pmids = [record.pmid for record in parsed.records]
            if expected_pmids and returned_pmids != expected_pmids:
                raise ExternalRetrievalWaveError(
                    "PubMed recovery returned a missing, unexpected, or reordered PMID"
                )

            prior_adapter_version = page.adapter_version
            page.adapter_version = adapter.version
            page.returned_item_count = parsed.raw_item_count
            page.native_identifiers = list(parsed.native_identifiers)
            page.next_state = parsed.next_state
            page.source_reported_total = parsed.source_reported_total
            page.total_is_exact = parsed.total_is_exact
            page.terminal = parsed.terminal
            page.completion_proof = parsed.completion_proof
            page.truncated = parsed.truncated
            page.truncation_reason = parsed.truncation_reason
            page.status = RetrievalCompletionStatus.COMPLETE
            page.metadata = {
                **parsed.metadata,
                "offline_parser_recovery": True,
                "recovered_from_adapter_version": prior_adapter_version,
                "raw_response_reused_without_network": True,
            }
            page.occurrence_ids = []
            for rank, record in enumerate(parsed.records, start=1):
                source_rank += 1
                source_identifier = native_identifier(record, source_rank)
                occurrence = RecordOccurrence(
                    occurrence_id=_retrieval_stable_id(
                        "occurrence", page.page_id, str(rank), source_identifier
                    ),
                    source_query_id=query.query_id,
                    source_identifier=source_identifier,
                    retrieved_at=attempt.ended_at or recovered_at,
                    record=record,
                    source_rank=source_rank,
                    page=page.ordinal,
                    cursor=_pubmed_state_cursor(page.request_state),
                    raw_payload_hash=_hash_payload(record.original_metadata),
                    metadata={
                        "source_identifier_missing": record.source_identifier is None,
                        "parser_incomplete": bool(
                            record.original_metadata.get("parser_incomplete")
                        ),
                        "offline_parser_recovery": True,
                    },
                    retrieval_page_id=page.page_id,
                )
                reconstructed_occurrences.append(occurrence)
                page.occurrence_ids.append(occurrence.occurrence_id)
            expected_state = parsed.next_state

        search_page = pages[0]
        pmids = list(search_page.next_state.get("pmids") or []) if search_page.next_state else []
        if expected_state is None:
            query.status = ProcessingStatus.OK
            query.completion_status = RetrievalCompletionStatus.COMPLETE
            query.completion_proof = pages[-1].completion_proof
        else:
            if expected_state.get("phase") != "fetch":
                raise ExternalRetrievalWaveError(
                    "PubMed episode-2 remaining state is not EFetch"
                )
            next_index = int(expected_state.get("index", -1))
            if next_index < 0 or next_index >= len(pmids):
                raise ExternalRetrievalWaveError(
                    "PubMed episode-2 remaining EFetch index is invalid"
                )
            for start in range(next_index, len(pmids), spec.limit):
                batch = pmids[start : start + spec.limit]
                batch_state = {"phase": "fetch", "pmids": pmids, "index": start}
                batch_request = adapter.build_request(spec, batch_state)
                if batch_request.request_hash() in historical_request_hashes:
                    raise ExternalRetrievalWaveError(
                        "PubMed recovery planned a remaining batch that was already requested"
                    )
                remaining_batches.append(
                    {
                        "production_query_id": spec.metadata["production_query_id"],
                        "start_index": start,
                        "end_index_exclusive": start + len(batch),
                        "pmids": batch,
                        "pmid_manifest_sha256": _hash_payload({"pmids": batch}),
                        "request_hash": batch_request.request_hash(),
                    }
                )
            query.status = ProcessingStatus.PARTIAL
            query.completion_status = RetrievalCompletionStatus.RUNNING
            query.completion_proof = None
        query.errors = []
        query.result_count = source_rank
        query.source_reported_total = len(pmids)
        query.total_is_exact = True
        query.retrieval_ended_at = recovered_at

    reconstructed_identifiers = {
        item.source_identifier for item in reconstructed_occurrences
    }
    recovered_pmids = tuple(
        pmid
        for pmid in PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS
        if pmid in reconstructed_identifiers and pmid not in old_identifiers
    )
    if recovered_pmids != PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS or (
        reconstructed_identifiers - old_identifiers
    ) != set(PUBMED_EPISODE_2_OMITTED_BOOK_PMIDS):
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery did not recover exactly the six known book PMIDs"
        )
    record_by_pmid = {
        occurrence.record.pmid: occurrence.record for occurrence in reconstructed_occurrences
    }
    if any(
        record_by_pmid[pmid].original_metadata.get("pubmed_record_type")
        != "PubmedBookArticle"
        for pmid in recovered_pmids
    ):
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery PMID is not a PubmedBookArticle"
        )

    dataset.occurrences = reconstructed_occurrences
    if not remaining_batches:
        raise ExternalRetrievalWaveError(
            "PubMed parser recovery found no remaining live EFetch work"
        )
    provenance = DecisionProvenance(
        actor=DecisionActor(
            actor_id="h2h_lit.external_retrieval_wave.pubmed_parser_recovery",
            actor_type=ActorType.SOFTWARE,
            metadata={"software_version": WAVE_VERSION},
        ),
        authority=DecisionAuthority.DETERMINISTIC,
        scope=DecisionScope.PROSPECTIVE,
        protocol_version="1.0.0",
        rubric_version="1.0.0",
        created_at=run.retrieval_started_at,
        metadata={
            "run_id": run.run_id,
            "rule": "doi_first_title_fallback",
            "offline_parser_recovery": True,
        },
    )
    dataset.canonical_records, dataset.duplicate_decisions = canonicalize_occurrences(
        dataset.occurrences, provenance=provenance
    )
    run.status = ProcessingStatus.PARTIAL
    run.completion_status = RetrievalCompletionStatus.RUNNING
    run.retrieval_cutoff_date = None
    run.retrieval_completed_at = recovered_at
    run.errors = ["offline parser recovery complete; remaining PubMed EFetch batches pending"]
    run.metadata.pop("pause_state", None)
    run.metadata.pop("pause_reason", None)
    run.metadata["parser_recovery_source_response_count"] = len(
        raw_response_references
    )
    run.metadata["remaining_efetch_request_count"] = len(remaining_batches)
    dataset.validate()
    return {
        "recovered_pmids": recovered_pmids,
        "raw_response_references": raw_response_references,
        "remaining_efetch_request_count": len(remaining_batches),
        "remaining_efetch_batches": remaining_batches,
    }


def _validate_authorized_pubmed_parser_recovery(
    source_state: dict[str, Any], root: Path
) -> None:
    episodes = source_state.get("execution_episodes", [])
    active = next(
        (
            item
            for item in episodes
            if item.get("episode_number") == source_state.get("active_episode_number")
        ),
        None,
    )
    if (
        active is None
        or active.get("episode_number") != 3
        or active.get("status") != PUBMED_PARSER_RECOVERY_STATUS
        or active.get("recovery_of_episode_number") != 2
        or source_state.get("active_checkpoint_path") != active.get("checkpoint_path")
    ):
        raise ExternalRetrievalWaveError("authorized PubMed parser recovery lineage changed")
    prior = next(
        (item for item in episodes if item.get("episode_number") == 2), None
    )
    if prior is None or not prior.get("immutable") or prior.get("status") != "FAILED":
        raise ExternalRetrievalWaveError("PubMed episode 2 is not preserved")
    prior_checkpoint = _safe_output_path(
        root, active["source_episode_checkpoint"]["path"]
    )
    _verify_file_reference(prior_checkpoint, active["source_episode_checkpoint"], root)
    recovery_checkpoint = _safe_output_path(
        root, active["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(recovery_checkpoint, active["checkpoint_dataset"], root)
    for binding in active.get("source_raw_responses", []):
        for key in ("episode_2_path", "recovery_copy_path"):
            path = _safe_output_path(root, binding[key])
            raw = path.read_bytes()
            if len(raw) != binding["byte_size"] or _sha256(raw) != binding["raw_sha256"]:
                raise ExternalRetrievalWaveError(
                    "authorized PubMed parser recovery raw-response binding changed"
                )


def _load_pubmed_recovery_response(
    store: CheckpointStore, relative_path: str, expected_hash: str
) -> Any:
    try:
        return store.load_response(relative_path, expected_hash)
    except (OSError, ValueError) as exc:
        raise ExternalRetrievalWaveError(
            f"PubMed episode-2 raw response hash/read failure: {exc}"
        ) from exc


def _retrieval_stable_id(prefix: str, *parts: str) -> str:
    digest = hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()[:24]
    return f"{prefix}:{digest}"


def _pubmed_state_cursor(state: Mapping[str, Any]) -> str | None:
    return str(state["index"]) if state.get("index") is not None else None


def _validate_response_free_pubmed_transport_failure(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    wave: ProductionRetrievalWave,
) -> None:
    if dataset.occurrences or dataset.canonical_records or dataset.duplicate_decisions:
        raise ExternalRetrievalWaveError(
            "PubMed transport retry refused because records were already imported"
        )
    dataset.validate()
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError("PubMed failure checkpoint must contain one run")
    run = dataset.retrieval_runs[0]
    if run.completion_status is not RetrievalCompletionStatus.FAILED:
        raise ExternalRetrievalWaveError("PubMed checkpoint is not terminal FAILED")
    if run.query_plan_version != wave.query_plan_hash:
        raise ExternalRetrievalWaveError("PubMed failed episode query-plan binding changed")
    if not dataset.retrieval_attempts:
        raise ExternalRetrievalWaveError("PubMed failed episode contains no attempts")
    if any(
        attempt.status is not RetrievalAttemptStatus.FAILED
        or attempt.response_status is not None
        or attempt.raw_response_path is not None
        or attempt.raw_response_hash is not None
        or attempt.response_url is not None
        for attempt in dataset.retrieval_attempts
    ):
        raise ExternalRetrievalWaveError(
            "PubMed transport retry refused because an HTTP response exists"
        )
    response_dir = checkpoint_dir / "responses"
    if response_dir.exists() and any(response_dir.iterdir()):
        raise ExternalRetrievalWaveError(
            "PubMed transport retry refused because raw provider responses exist"
        )
    for attempt in dataset.retrieval_attempts:
        error_type = str(attempt.error or "").partition(":")[0]
        if error_type not in TRANSPORT_ENVIRONMENT_FAILURE_TYPES:
            raise ExternalRetrievalWaveError(
                "PubMed transport retry refused for a non-transport failure"
            )

    specs = _source_query_specs(wave, "PubMed", ieee_credential="")
    if len(dataset.source_queries) != len(specs):
        raise ExternalRetrievalWaveError("PubMed failed episode query count changed")
    adapter = PAGINATED_SOURCE_ADAPTERS["PubMed"]
    for query, spec in zip(dataset.source_queries, specs, strict=True):
        if (
            query.source_database != "PubMed"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
        ):
            raise ExternalRetrievalWaveError(
                "PubMed failed episode frozen query/request binding changed"
            )
        pages = [
            page for page in dataset.retrieval_pages if page.source_query_id == query.query_id
        ]
        if len(pages) != 1 or pages[0].request_state != adapter.initial_state(spec):
            raise ExternalRetrievalWaveError(
                "PubMed failed episode does not contain only its initial ESearch page"
            )
        expected_request = adapter.build_request(spec, pages[0].request_state)
        attempts = [
            attempt
            for attempt in dataset.retrieval_attempts
            if attempt.page_id == pages[0].page_id
        ]
        if not attempts or any(
            attempt.request_method != "POST"
            or attempt.request_hash != expected_request.request_hash()
            for attempt in attempts
        ):
            raise ExternalRetrievalWaveError(
                "PubMed failed episode ESearch request hash/method changed"
            )


def _validate_arxiv_episode_1_provenance(
    *,
    episode_1: Mapping[str, Any],
    episode_2: Mapping[str, Any],
    root: Path,
    wave: ProductionRetrievalWave,
) -> None:
    if (
        episode_1.get("episode_number") != 1
        or episode_1.get("status") != "FAILED"
        or episode_1.get("immutable") is not True
        or episode_1.get("attempt_count") != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or episode_1.get("transport_timeout_count") != 8
        or episode_1.get("http_429_count") != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        or episode_1.get("successful_http_response_count") != 0
        or episode_1.get("occurrence_count") != 0
        or episode_2.get("source_episode_checkpoint")
        != episode_1.get("checkpoint_dataset")
        or episode_2.get("source_attempt_manifest_hash")
        != episode_1.get("source_attempt_manifest_hash")
        or episode_2.get("source_attempt_count")
        != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or episode_2.get("source_raw_responses")
        != episode_1.get("raw_responses")
        or episode_2.get("restart_states") is None
    ):
        raise ExternalRetrievalWaveError("arXiv episode-1 provenance changed")
    checkpoint_reference = episode_1.get("checkpoint_dataset")
    if not checkpoint_reference:
        raise ExternalRetrievalWaveError("arXiv episode 1 lacks a checkpoint binding")
    checkpoint = _safe_output_path(root, str(checkpoint_reference["path"]))
    _verify_file_reference(checkpoint, checkpoint_reference, root)
    payload = _load_json(checkpoint)
    if episode_1.get("source_attempt_manifest_hash") != _hash_payload(
        {"retrieval_attempts": payload.get("retrieval_attempts", [])}
    ):
        raise ExternalRetrievalWaveError("arXiv episode-1 attempt manifest changed")
    validated = _validate_failed_arxiv_rate_limit_checkpoint(
        dataset=load_review_dataset(checkpoint),
        checkpoint_dir=checkpoint.parent,
        root=root,
        wave=wave,
    )
    if (
        episode_1.get("raw_responses") != validated["raw_response_bindings"]
        or episode_1.get("query_attempt_signatures")
        != validated["query_attempt_signatures"]
        or episode_2.get("restart_states") != validated["restart_states"]
    ):
        raise ExternalRetrievalWaveError("arXiv episode-1 evidence manifest changed")
    _verify_arxiv_raw_response_bindings(root, episode_1["raw_responses"])


def _validate_arxiv_mixed_state_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    root: Path,
    wave: ProductionRetrievalWave,
) -> dict[str, Any]:
    if (
        dataset.occurrences
        or dataset.canonical_records
        or dataset.duplicate_decisions
        or any(
            page.status is RetrievalCompletionStatus.COMPLETE
            or page.returned_item_count
            or page.occurrence_ids
            or page.native_identifiers
            for page in dataset.retrieval_pages
        )
        or any(
            attempt.status is RetrievalAttemptStatus.SUCCEEDED
            or (
                attempt.response_status is not None
                and 200 <= attempt.response_status < 300
            )
            for attempt in dataset.retrieval_attempts
        )
    ):
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery refused because a successful page or occurrence exists"
        )
    try:
        dataset.validate()
    except ValueError as exc:
        raise ExternalRetrievalWaveError(
            f"arXiv episode-2 checkpoint validation failed: {exc}"
        ) from exc
    specs = _source_query_specs(wave, "arXiv", ieee_credential="")
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError(
            "arXiv mixed recovery requires exactly one retrieval run"
        )
    run = dataset.retrieval_runs[0]
    expected_pause_metadata = {
        "source_database": "arXiv",
        "http_status": 429,
        "retry_after_header_present": False,
        "retry_after": None,
    }
    if (
        run.status is not ProcessingStatus.PARTIAL
        or run.completion_status is not RetrievalCompletionStatus.RUNNING
        or run.retrieval_cutoff_date is not None
        or run.query_plan_hash != _query_plan_hash(specs)
        or run.planned_query_ids != [query.query_id for query in dataset.source_queries]
        or run.source_query_ids != run.planned_query_ids
        or run.errors != ["PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"]
        or run.metadata.get("pause_state") != "PROVIDER_RATE_LIMIT"
        or run.metadata.get("pause_reason")
        != "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
        or run.metadata.get("pause_metadata") != expected_pause_metadata
        or run.metadata.get("session_request_count")
        != ARXIV_MIXED_EXPECTED_ATTEMPTS
        or len(dataset.source_queries) != 5
        or len(dataset.retrieval_pages) != 4
        or len(dataset.retrieval_attempts) != ARXIV_MIXED_EXPECTED_ATTEMPTS
    ):
        raise ExternalRetrievalWaveError(
            "arXiv checkpoint is not the exact episode-2 mixed pause"
        )

    adapter = PAGINATED_SOURCE_ADAPTERS["arXiv"]
    response_store = CheckpointStore(checkpoint_dir)
    raw_response_bindings: list[dict[str, Any]] = []
    query_attempt_signatures: list[dict[str, Any]] = []
    restart_states: list[dict[str, Any]] = []
    seen_attempt_ids: set[str] = set()
    seen_response_paths: set[str] = set()

    for family_index, (query, spec, expected_kinds) in enumerate(
        zip(
            dataset.source_queries,
            specs,
            ARXIV_MIXED_EXPECTED_ATTEMPT_KINDS,
            strict=True,
        )
    ):
        expected_completion = (
            RetrievalCompletionStatus.FAILED
            if family_index < 3
            else (
                RetrievalCompletionStatus.RUNNING
                if family_index == 3
                else RetrievalCompletionStatus.PLANNED
            )
        )
        expected_status = (
            ProcessingStatus.FAILED
            if family_index < 3
            else ProcessingStatus.PARTIAL
        )
        if (
            query.source_database != "arXiv"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
            or query.completion_status is not expected_completion
            or query.status is not expected_status
            or query.result_count != 0
            or query.source_reported_total is not None
            or query.total_is_exact
            or query.completion_proof is not None
        ):
            raise ExternalRetrievalWaveError(
                "arXiv mixed recovery refused changed frozen query/state provenance"
            )
        expected_errors = (
            ["retrieval attempts exhausted before a response"]
            if family_index < 3
            else []
        )
        if query.errors != expected_errors:
            raise ExternalRetrievalWaveError(
                "arXiv mixed recovery refused changed query failure signature"
            )
        pages = [
            page
            for page in dataset.retrieval_pages
            if page.source_query_id == query.query_id
        ]
        if family_index == 4:
            if pages or query.page_ids:
                raise ExternalRetrievalWaveError(
                    "arXiv QF05 must remain unattempted and planned"
                )
            request = adapter.build_request(spec, adapter.initial_state(spec))
            attempts: list[Any] = []
        else:
            if len(pages) != 1 or query.page_ids != [pages[0].page_id]:
                raise ExternalRetrievalWaveError(
                    "arXiv mixed recovery requires one initial page for QF01-QF04"
                )
            page = pages[0]
            expected_page_status = (
                RetrievalCompletionStatus.FAILED
                if family_index < 3
                else RetrievalCompletionStatus.RUNNING
            )
            if (
                page.ordinal != 0
                or page.request_state != {"start": 0}
                or page.status is not expected_page_status
                or page.next_state is not None
                or page.source_reported_total is not None
                or page.returned_item_count != 0
                or page.occurrence_ids
                or page.native_identifiers
                or page.terminal
                or page.truncated
                or (
                    family_index < 3
                    and page.metadata.get("completion_error")
                    != "retrieval attempts exhausted before a response"
                )
                or (family_index == 3 and page.metadata)
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv mixed recovery refused changed initial-page lineage"
                )
            request = adapter.build_request(spec, page.request_state)
            attempt_by_id = {
                item.attempt_id: item for item in dataset.retrieval_attempts
            }
            try:
                attempts = [attempt_by_id[item] for item in page.attempt_ids]
            except KeyError as exc:
                raise ExternalRetrievalWaveError(
                    "arXiv mixed recovery attempt lineage is incomplete"
                ) from exc
        if (
            len(attempts) != len(expected_kinds)
            or [item.attempt_number for item in attempts]
            != list(range(1, len(attempts) + 1))
        ):
            raise ExternalRetrievalWaveError(
                "arXiv mixed recovery attempt count/order changed"
            )
        observed_kinds: list[str] = []
        for index, attempt in enumerate(attempts):
            if (
                attempt.attempt_id in seen_attempt_ids
                or attempt.page_id != page.page_id
                or attempt.status is not RetrievalAttemptStatus.FAILED
                or attempt.request_method != request.method
                or attempt.request_url != request.url
                or attempt.request_params != request.sanitized_params()
                or attempt.request_headers != request.sanitized_headers()
                or attempt.request_hash != request.request_hash()
                or attempt.retry_of_attempt_id
                != (attempts[index - 1].attempt_id if index else None)
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv mixed recovery refused changed request hash/method/lineage"
                )
            seen_attempt_ids.add(attempt.attempt_id)
            if attempt.response_status is None:
                failure_type = str(attempt.error or "").partition(":")[0]
                if (
                    failure_type != "ReadTimeout"
                    or attempt.raw_response_path is not None
                    or attempt.raw_response_hash is not None
                    or attempt.response_headers
                    or attempt.response_url is not None
                    or attempt.actual_request_url is not None
                    or attempt.metadata
                ):
                    raise ExternalRetrievalWaveError(
                        "arXiv mixed recovery encountered non-signature transport failure"
                    )
                observed_kinds.append(failure_type)
                continue
            if (
                family_index != 3
                or attempt.response_status != 429
                or attempt.error != "PROVIDER_RATE_LIMIT_PAUSED_HTTP_429"
                or not attempt.raw_response_path
                or not attempt.raw_response_hash
                or attempt.raw_response_path in seen_response_paths
                or attempt.metadata.get("provider_pause")
                != expected_pause_metadata
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv mixed recovery encountered non-signature HTTP failure"
                )
            try:
                response = response_store.load_response(
                    attempt.raw_response_path, attempt.raw_response_hash
                )
            except (OSError, ValueError) as exc:
                raise ExternalRetrievalWaveError(
                    f"arXiv episode-2 raw response hash/read failure: {exc}"
                ) from exc
            if (
                response.status_code != 429
                or response.headers != attempt.response_headers
                or any(
                    str(key).lower() == "retry-after"
                    for key in response.headers
                )
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv episode-2 persisted response metadata changed"
                )
            response_path = checkpoint_dir / attempt.raw_response_path
            raw = response_path.read_bytes()
            seen_response_paths.add(attempt.raw_response_path)
            raw_response_bindings.append(
                {
                    "episode_2_path": response_path.relative_to(root).as_posix(),
                    "attempt_id": attempt.attempt_id,
                    "byte_size": len(raw),
                    "raw_sha256": attempt.raw_response_hash,
                    "http_status": 429,
                    "retry_after_header_present": False,
                    "retry_after": None,
                }
            )
            observed_kinds.append("HTTP_429")
        if tuple(observed_kinds) != expected_kinds:
            raise ExternalRetrievalWaveError(
                "arXiv episode-2 mixed attempt signature changed"
            )
        query_attempt_signatures.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "attempt_kinds": observed_kinds,
                "request_hash": request.request_hash(),
            }
        )
        restart_states.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "request_state": adapter.initial_state(spec),
                "max_results": spec.limit,
                "request_hash": request.request_hash(),
            }
        )

    if len(seen_attempt_ids) != ARXIV_MIXED_EXPECTED_ATTEMPTS:
        raise ExternalRetrievalWaveError("arXiv episode-2 contains unbound attempts")
    response_files = {
        path.relative_to(checkpoint_dir).as_posix()
        for path in response_store.responses.iterdir()
        if path.is_file()
    }
    if (
        len(raw_response_bindings) != ARXIV_MIXED_EXPECTED_RESPONSES
        or response_files != seen_response_paths
    ):
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 raw-response manifest is missing or has unbound files"
        )
    return {
        "raw_response_bindings": raw_response_bindings,
        "query_attempt_signatures": query_attempt_signatures,
        "restart_states": restart_states,
    }


def _verify_arxiv_mixed_raw_response_bindings(
    root: Path, bindings: list[dict[str, Any]]
) -> None:
    if len(bindings) != ARXIV_MIXED_EXPECTED_RESPONSES:
        raise ExternalRetrievalWaveError(
            "arXiv episode-2 raw-response manifest changed"
        )
    for binding in bindings:
        path = _safe_output_path(root, str(binding.get("episode_2_path", "")))
        if not path.is_file():
            raise ExternalRetrievalWaveError("arXiv episode-2 raw response is missing")
        raw = path.read_bytes()
        if (
            len(raw) != binding.get("byte_size")
            or _sha256(raw) != binding.get("raw_sha256")
            or binding.get("http_status") != 429
            or binding.get("retry_after_header_present") is not False
            or binding.get("retry_after") is not None
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-2 raw response changed during recovery"
            )


def _validate_failed_arxiv_rate_limit_checkpoint(
    *,
    dataset: Any,
    checkpoint_dir: Path,
    root: Path,
    wave: ProductionRetrievalWave,
) -> dict[str, Any]:
    if (
        dataset.occurrences
        or any(
            page.status is RetrievalCompletionStatus.COMPLETE
            or page.returned_item_count
            or page.occurrence_ids
            or page.native_identifiers
            for page in dataset.retrieval_pages
        )
        or any(
            attempt.status is RetrievalAttemptStatus.SUCCEEDED
            or (
                attempt.response_status is not None
                and 200 <= attempt.response_status < 300
            )
            for attempt in dataset.retrieval_attempts
        )
    ):
        raise ExternalRetrievalWaveError(
            "arXiv recovery refused because a successful page or occurrence exists"
        )
    try:
        dataset.validate()
    except ValueError as exc:
        raise ExternalRetrievalWaveError(
            f"arXiv failed checkpoint validation failed: {exc}"
        ) from exc
    specs = _source_query_specs(wave, "arXiv", ieee_credential="")
    if len(dataset.retrieval_runs) != 1:
        raise ExternalRetrievalWaveError(
            "arXiv rate-limit recovery requires exactly one retrieval run"
        )
    run = dataset.retrieval_runs[0]
    if (
        run.completion_status is not RetrievalCompletionStatus.FAILED
        or run.retrieval_cutoff_date is not None
        or run.query_plan_hash != _query_plan_hash(specs)
        or run.planned_query_ids != [query.query_id for query in dataset.source_queries]
        or run.source_query_ids != run.planned_query_ids
        or len(dataset.source_queries) != 5
        or len(dataset.retrieval_pages) != 5
        or len(dataset.retrieval_attempts) != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
    ):
        raise ExternalRetrievalWaveError(
            "arXiv checkpoint is not the known timeout/rate-limit failure"
        )
    if (
        dataset.canonical_records
        or dataset.duplicate_decisions
    ):
        raise ExternalRetrievalWaveError(
            "arXiv recovery refused because a successful page or occurrence exists"
        )

    adapter = PAGINATED_SOURCE_ADAPTERS["arXiv"]
    response_store = CheckpointStore(checkpoint_dir)
    raw_response_bindings = []
    query_attempt_signatures = []
    restart_states = []
    seen_response_paths: set[str] = set()
    expected_run_errors = []

    for query, spec, expected_kinds in zip(
        dataset.source_queries,
        specs,
        ARXIV_RATE_LIMIT_EXPECTED_ATTEMPT_KINDS,
        strict=True,
    ):
        if (
            query.source_database != "arXiv"
            or query.query_text != spec.query_text
            or query.query_version != spec.query_version
            or query.metadata.get("production_query_id")
            != spec.metadata["production_query_id"]
            or query.metadata.get("frozen_request_specification_hash")
            != spec.metadata["frozen_request_specification_hash"]
            or query.completion_status is not RetrievalCompletionStatus.FAILED
            or query.status is not ProcessingStatus.FAILED
            or query.result_count != 0
            or len(query.errors) != 1
        ):
            raise ExternalRetrievalWaveError(
                "arXiv recovery refused changed frozen query/failure provenance"
            )
        pages = [
            page
            for page in dataset.retrieval_pages
            if page.source_query_id == query.query_id
        ]
        if (
            len(pages) != 1
            or query.page_ids != [pages[0].page_id]
            or pages[0].ordinal != 0
            or pages[0].request_state != {"start": 0}
            or pages[0].status is not RetrievalCompletionStatus.FAILED
            or pages[0].next_state is not None
            or pages[0].source_reported_total is not None
            or pages[0].terminal
            or pages[0].truncated
            or pages[0].metadata.get("completion_error") != query.errors[0]
        ):
            raise ExternalRetrievalWaveError(
                "arXiv recovery refused changed initial-page failure lineage"
            )
        page = pages[0]
        request = adapter.build_request(spec, page.request_state)
        attempts = [
            attempt
            for attempt_id in page.attempt_ids
            for attempt in dataset.retrieval_attempts
            if attempt.attempt_id == attempt_id
        ]
        if len(attempts) != 3 or [item.attempt_number for item in attempts] != [1, 2, 3]:
            raise ExternalRetrievalWaveError(
                "arXiv recovery requires exactly three ordered attempts per family"
            )
        observed_kinds = []
        for index, attempt in enumerate(attempts):
            if (
                attempt.page_id != page.page_id
                or attempt.status is not RetrievalAttemptStatus.FAILED
                or attempt.request_method != request.method
                or attempt.request_url != request.url
                or attempt.request_params != request.sanitized_params()
                or attempt.request_headers != request.sanitized_headers()
                or attempt.request_hash != request.request_hash()
                or attempt.retry_of_attempt_id
                != (attempts[index - 1].attempt_id if index else None)
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv recovery refused changed request hash/method/lineage"
                )
            if attempt.response_status is None:
                failure_type = str(attempt.error or "").partition(":")[0]
                if (
                    failure_type != "ReadTimeout"
                    or attempt.raw_response_path is not None
                    or attempt.raw_response_hash is not None
                    or attempt.response_headers
                    or attempt.response_url is not None
                ):
                    raise ExternalRetrievalWaveError(
                        "arXiv recovery encountered non-signature transport failure"
                    )
                observed_kinds.append(failure_type)
                continue
            if (
                attempt.response_status != 429
                or not str(attempt.error or "").startswith("HTTP 429 from ")
                or not attempt.raw_response_path
                or not attempt.raw_response_hash
                or attempt.raw_response_path in seen_response_paths
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv recovery encountered non-signature HTTP failure"
                )
            try:
                response = response_store.load_response(
                    attempt.raw_response_path, attempt.raw_response_hash
                )
            except (OSError, ValueError) as exc:
                raise ExternalRetrievalWaveError(
                    f"arXiv raw response hash/read failure: {exc}"
                ) from exc
            if (
                response.status_code != 429
                or response.headers != attempt.response_headers
            ):
                raise ExternalRetrievalWaveError(
                    "arXiv persisted response metadata changed"
                )
            response_path = checkpoint_dir / attempt.raw_response_path
            raw = response_path.read_bytes()
            retry_after = next(
                (
                    str(value)
                    for key, value in response.headers.items()
                    if str(key).lower() == "retry-after"
                ),
                None,
            )
            seen_response_paths.add(attempt.raw_response_path)
            raw_response_bindings.append(
                {
                    "episode_1_path": response_path.relative_to(root).as_posix(),
                    "attempt_id": attempt.attempt_id,
                    "byte_size": len(raw),
                    "raw_sha256": attempt.raw_response_hash,
                    "http_status": 429,
                    "retry_after_header_present": retry_after is not None,
                    "retry_after": retry_after,
                }
            )
            observed_kinds.append("HTTP_429")
        if tuple(observed_kinds) != expected_kinds:
            raise ExternalRetrievalWaveError(
                "arXiv timeout/429 attempt signature changed"
            )
        if expected_kinds[-1] == "ReadTimeout":
            if query.errors != ["retrieval attempts exhausted before a response"]:
                raise ExternalRetrievalWaveError(
                    "arXiv transport exhaustion failure signature changed"
                )
        elif not query.errors[0].startswith("HTTP 429 from "):
            raise ExternalRetrievalWaveError(
                "arXiv HTTP 429 failure signature changed"
            )
        expected_run_errors.append(f"{query.query_id}: {query.errors[0]}")
        query_attempt_signatures.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "attempt_kinds": observed_kinds,
                "request_hash": request.request_hash(),
            }
        )
        restart_states.append(
            {
                "production_query_id": spec.metadata["production_query_id"],
                "query_id": query.query_id,
                "request_state": adapter.initial_state(spec),
                "max_results": spec.limit,
                "request_hash": request.request_hash(),
            }
        )

    if run.errors != expected_run_errors:
        raise ExternalRetrievalWaveError("arXiv run failure manifest changed")
    response_files = {
        path.relative_to(checkpoint_dir).as_posix()
        for path in response_store.responses.iterdir()
        if path.is_file()
    }
    if (
        len(raw_response_bindings) != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        or response_files != seen_response_paths
    ):
        raise ExternalRetrievalWaveError(
            "arXiv raw-response artifact manifest is missing or has unbound files"
        )
    return {
        "raw_response_bindings": raw_response_bindings,
        "query_attempt_signatures": query_attempt_signatures,
        "restart_states": restart_states,
    }


def _verify_arxiv_raw_response_bindings(
    root: Path, bindings: list[dict[str, Any]]
) -> None:
    if len(bindings) != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES:
        raise ExternalRetrievalWaveError("arXiv recovery raw-response manifest changed")
    for binding in bindings:
        path = _safe_output_path(root, str(binding.get("episode_1_path", "")))
        raw = path.read_bytes()
        if (
            not path.is_file()
            or len(raw) != binding.get("byte_size")
            or _sha256(raw) != binding.get("raw_sha256")
            or binding.get("http_status") != 429
        ):
            raise ExternalRetrievalWaveError(
                "arXiv episode-1 raw response changed during recovery"
            )


def _validate_authorized_arxiv_rate_limit_recovery(
    source_state: dict[str, Any], root: Path, wave: ProductionRetrievalWave
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 2:
        raise ExternalRetrievalWaveError("arXiv episode-2 recovery lineage changed")
    failed, recovered = episodes
    if (
        failed.get("episode_number") != 1
        or failed.get("status") != "FAILED"
        or not failed.get("immutable")
        or failed.get("attempt_count") != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or failed.get("http_429_count") != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        or recovered.get("episode_number") != 2
        or recovered.get("status") != ARXIV_RATE_LIMIT_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 1
        or recovered.get("network_used") is not False
        or recovered.get("immutable") is not False
        or source_state.get("active_episode_number") != 2
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
        or source_state.get("checkpoint_dataset")
        != recovered.get("checkpoint_dataset")
        or source_state.get("completed_query_count") != 0
        or source_state.get("total_query_count") != 5
        or source_state.get("occurrence_count") != 0
        or source_state.get("attempt_count") != 0
        or source_state.get("requests_this_session") != 0
        or source_state.get("preserved_source_attempt_count")
        != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        or source_state.get("preserved_source_raw_response_count")
        != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        or recovered.get("source_episode_checkpoint")
        != failed.get("checkpoint_dataset")
    ):
        raise ExternalRetrievalWaveError("arXiv episode-2 recovery lineage changed")
    failed_checkpoint = _safe_output_path(
        root, failed["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(failed_checkpoint, failed["checkpoint_dataset"], root)
    failed_payload = _load_json(failed_checkpoint)
    if failed.get("source_attempt_manifest_hash") != _hash_payload(
        {"retrieval_attempts": failed_payload.get("retrieval_attempts", [])}
    ):
        raise ExternalRetrievalWaveError("arXiv episode-1 attempt manifest changed")
    failed_dataset = load_review_dataset(failed_checkpoint)
    validated = _validate_failed_arxiv_rate_limit_checkpoint(
        dataset=failed_dataset,
        checkpoint_dir=failed_checkpoint.parent,
        root=root,
        wave=wave,
    )
    if (
        recovered.get("source_raw_responses")
        != validated["raw_response_bindings"]
        or recovered.get("restart_states") != validated["restart_states"]
        or failed.get("query_attempt_signatures")
        != validated["query_attempt_signatures"]
    ):
        raise ExternalRetrievalWaveError("arXiv recovery evidence manifest changed")
    _verify_arxiv_raw_response_bindings(root, recovered["source_raw_responses"])

    recovery_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(
        recovery_checkpoint, recovered["checkpoint_dataset"], root
    )
    dataset = load_review_dataset(recovery_checkpoint)
    specs = _source_query_specs(wave, "arXiv", ieee_credential="")
    if (
        len(dataset.retrieval_runs) != 1
        or dataset.retrieval_runs[0].completion_status
        is not RetrievalCompletionStatus.RUNNING
        or dataset.retrieval_runs[0].query_plan_hash != _query_plan_hash(specs)
        or dataset.retrieval_pages
        or dataset.retrieval_attempts
        or dataset.occurrences
        or dataset.canonical_records
        or dataset.duplicate_decisions
        or [query.query_id for query in dataset.source_queries]
        != [item["query_id"] for item in recovered["restart_states"]]
        or any(
            query.completion_status is not RetrievalCompletionStatus.PLANNED
            or query.status is not ProcessingStatus.PARTIAL
            or query.result_count != 0
            or query.page_ids
            or query.errors
            for query in dataset.source_queries
        )
    ):
        raise ExternalRetrievalWaveError("arXiv episode-2 restart checkpoint changed")
    dataset.validate()


def _validate_authorized_arxiv_mixed_state_recovery(
    source_state: dict[str, Any], root: Path, wave: ProductionRetrievalWave
) -> None:
    episodes = source_state.get("execution_episodes", [])
    if len(episodes) != 3:
        raise ExternalRetrievalWaveError("arXiv episode-3 recovery lineage changed")
    episode_1, episode_2, recovered = episodes
    _validate_arxiv_episode_1_provenance(
        episode_1=episode_1,
        episode_2=episode_2,
        root=root,
        wave=wave,
    )
    if (
        episode_2.get("episode_number") != 2
        or episode_2.get("status") != "PAUSED_PROVIDER_RATE_LIMIT"
        or episode_2.get("immutable") is not True
        or episode_2.get("attempt_count") != ARXIV_MIXED_EXPECTED_ATTEMPTS
        or episode_2.get("completed_query_count") != 0
        or episode_2.get("occurrence_count") != 0
        or recovered.get("episode_number") != 3
        or recovered.get("status") != ARXIV_MIXED_RECOVERY_STATUS
        or recovered.get("recovery_of_episode_number") != 2
        or recovered.get("authorization_reason")
        != "OFFLINE_ARXIV_MIXED_TRANSPORT_AND_RATE_LIMIT_RECOVERY"
        or recovered.get("frozen_wave_manifest_hash") != wave.manifest_hash()
        or recovered.get("frozen_query_plan_hash") != wave.query_plan_hash
        or recovered.get("network_used") is not False
        or recovered.get("immutable") is not False
        or recovered.get("source_attempt_count")
        != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        + ARXIV_MIXED_EXPECTED_ATTEMPTS
        or recovered.get("source_raw_response_count")
        != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        + ARXIV_MIXED_EXPECTED_RESPONSES
        or source_state.get("active_episode_number") != 3
        or source_state.get("active_checkpoint_path")
        != recovered.get("checkpoint_path")
        or source_state.get("checkpoint_dataset")
        != recovered.get("checkpoint_dataset")
        or source_state.get("active_run_id") != recovered.get("run_id")
        or source_state.get("completed_query_count") != 0
        or source_state.get("total_query_count") != 5
        or source_state.get("occurrence_count") != 0
        or source_state.get("attempt_count") != 0
        or source_state.get("requests_this_session") != 0
        or source_state.get("pause_reason")
        != "OFFLINE_MIXED_STATE_RECOVERY_COMPLETE; LIVE_RESUME_REQUIRED"
        or source_state.get("failure_reason") is not None
        or source_state.get("pause_metadata") is not None
        or source_state.get("preserved_source_attempt_count")
        != ARXIV_RATE_LIMIT_EXPECTED_ATTEMPTS
        + ARXIV_MIXED_EXPECTED_ATTEMPTS
        or source_state.get("preserved_source_raw_response_count")
        != ARXIV_RATE_LIMIT_EXPECTED_RESPONSES
        + ARXIV_MIXED_EXPECTED_RESPONSES
    ):
        raise ExternalRetrievalWaveError("arXiv episode-3 recovery lineage changed")

    episode_2_reference = episode_2.get("checkpoint_dataset")
    if (
        not episode_2_reference
        or episode_2_reference.get("raw_sha256")
        != ARXIV_MIXED_EXPECTED_CHECKPOINT_SHA256
    ):
        raise ExternalRetrievalWaveError("arXiv episode-2 checkpoint binding changed")
    episode_2_checkpoint = _safe_output_path(
        root, str(episode_2_reference["path"])
    )
    _verify_file_reference(episode_2_checkpoint, episode_2_reference, root)
    episode_2_payload = _load_json(episode_2_checkpoint)
    expected_attempt_manifest_hash = _hash_payload(
        {"retrieval_attempts": episode_2_payload.get("retrieval_attempts", [])}
    )
    episode_2_dataset = load_review_dataset(episode_2_checkpoint)
    validated = _validate_arxiv_mixed_state_checkpoint(
        dataset=episode_2_dataset,
        checkpoint_dir=episode_2_checkpoint.parent,
        root=root,
        wave=wave,
    )
    if (
        recovered.get("episode_2_attempt_manifest_hash")
        != expected_attempt_manifest_hash
        or recovered.get("episode_2_raw_responses")
        != validated["raw_response_bindings"]
        or recovered.get("episode_2_query_attempt_signatures")
        != validated["query_attempt_signatures"]
        or recovered.get("restart_states") != validated["restart_states"]
        or episode_2_dataset.retrieval_runs[0].run_id != episode_2.get("run_id")
    ):
        raise ExternalRetrievalWaveError("arXiv episode-2 recovery evidence changed")
    _verify_arxiv_mixed_raw_response_bindings(
        root, recovered["episode_2_raw_responses"]
    )

    expected_source_episodes = [
        {
            "episode_number": 1,
            "checkpoint_dataset": dict(episode_1["checkpoint_dataset"]),
            "attempt_manifest_hash": episode_1["source_attempt_manifest_hash"],
            "raw_response_manifest_hash": _hash_payload(
                {"responses": episode_1["raw_responses"]}
            ),
        },
        {
            "episode_number": 2,
            "checkpoint_dataset": dict(episode_2_reference),
            "attempt_manifest_hash": expected_attempt_manifest_hash,
            "raw_response_manifest_hash": _hash_payload(
                {"responses": validated["raw_response_bindings"]}
            ),
        },
    ]
    if recovered.get("source_episodes") != expected_source_episodes:
        raise ExternalRetrievalWaveError("arXiv recovery provenance binding changed")

    recovery_checkpoint = _safe_output_path(
        root, recovered["checkpoint_dataset"]["path"]
    )
    _verify_file_reference(recovery_checkpoint, recovered["checkpoint_dataset"], root)
    dataset = load_review_dataset(recovery_checkpoint)
    specs = _source_query_specs(wave, "arXiv", ieee_credential="")
    recovery_metadata = dataset.retrieval_runs[0].metadata.get(
        "offline_arxiv_mixed_state_recovery", {}
    )
    if (
        len(dataset.retrieval_runs) != 1
        or dataset.retrieval_runs[0].completion_status
        is not RetrievalCompletionStatus.RUNNING
        or dataset.retrieval_runs[0].query_plan_hash != _query_plan_hash(specs)
        or recovery_metadata.get("source_episodes") != expected_source_episodes
        or recovery_metadata.get("restart_states") != validated["restart_states"]
        or recovery_metadata.get("network_used") is not False
        or dataset.retrieval_pages
        or dataset.retrieval_attempts
        or dataset.occurrences
        or dataset.canonical_records
        or dataset.duplicate_decisions
        or [query.query_id for query in dataset.source_queries]
        != [item["query_id"] for item in validated["restart_states"]]
        or any(
            query.completion_status is not RetrievalCompletionStatus.PLANNED
            or query.status is not ProcessingStatus.PARTIAL
            or query.result_count != 0
            or query.page_ids
            or query.errors
            or query.metadata.get("offline_arxiv_mixed_state_recovery", {}).get(
                "restart_state"
            )
            != {"start": 0}
            for query in dataset.source_queries
        )
    ):
        raise ExternalRetrievalWaveError("arXiv episode-3 restart checkpoint changed")
    dataset.validate()


def _validate_authorized_pubmed_retry(
    source_state: dict[str, Any], root: Path
) -> None:
    episodes = source_state.get("execution_episodes", [])
    active_number = source_state.get("active_episode_number")
    if not episodes or active_number is None:
        raise ExternalRetrievalWaveError("authorized PubMed retry lacks episode lineage")
    active = next(
        (item for item in episodes if item["episode_number"] == active_number), None
    )
    if (
        active is None
        or active.get("status") != PUBMED_TRANSPORT_RETRY_STATUS
        or source_state.get("active_checkpoint_path") != active.get("checkpoint_path")
        or source_state.get("active_run_id") != active.get("run_id")
    ):
        raise ExternalRetrievalWaveError("authorized PubMed retry lineage changed")
    if _safe_output_path(root, active["checkpoint_path"]).exists():
        raise ExternalRetrievalWaveError(
            "authorized PubMed retry checkpoint already exists; use normal --resume"
        )
    previous = next(
        (
            item
            for item in episodes
            if item["episode_number"] == active["retry_of_episode_number"]
        ),
        None,
    )
    if previous is None or not previous.get("immutable"):
        raise ExternalRetrievalWaveError("prior PubMed failure episode is not preserved")
    checkpoint = _safe_output_path(root, previous["checkpoint_dataset"]["path"])
    _verify_file_reference(checkpoint, previous["checkpoint_dataset"], root)


def _sync_active_retry_episode(source_state: dict[str, Any]) -> None:
    active_number = source_state.get("active_episode_number")
    if active_number is None:
        return
    active = next(
        (
            item
            for item in source_state.get("execution_episodes", [])
            if item["episode_number"] == active_number
        ),
        None,
    )
    if active is None:
        raise ExternalRetrievalWaveError("active retry episode is missing")
    active.update(
        {
            "status": source_state["status"],
            "started_at_utc": source_state.get("last_session_started_at_utc"),
            "completed_at_utc": source_state.get("last_session_completed_at_utc"),
            "checkpoint_dataset": source_state.get("checkpoint_dataset"),
            "attempt_count": source_state.get("attempt_count", 0),
            "occurrence_count": source_state.get("occurrence_count", 0),
            "completed_query_count": source_state.get("completed_query_count", 0),
            "failure_reason": source_state.get("failure_reason"),
            "pause_reason": source_state.get("pause_reason"),
            "immutable": source_state["status"] in {"COMPLETE", "FAILED"},
        }
    )


def _verify_file_reference(
    path: Path, reference: Mapping[str, Any], root: Path
) -> None:
    if (
        not path.is_file()
        or path.relative_to(root).as_posix() != reference.get("path")
    ):
        raise ExternalRetrievalWaveError("checkpoint file binding changed")
    raw = path.read_bytes()
    if (
        len(raw) != reference.get("byte_size")
        or _sha256(raw) != reference.get("raw_sha256")
    ):
        raise ExternalRetrievalWaveError("checkpoint file hash/size changed")


def _finalize_execution_state(state: dict[str, Any], completed_at: str) -> None:
    def source_complete(source: str, item: Mapping[str, Any]) -> bool:
        substitution = item.get("approved_snapshot_substitution")
        return item["status"] == "COMPLETE" or (
            source == "arXiv"
            and isinstance(substitution, dict)
            and substitution.get("status")
            == "APPROVED_COMPLETE_REPLACEMENT_ROUTE"
        )

    if all(source_complete(source, item) for source, item in state["sources"].items()):
        state["status"] = "COMPLETE"
        state["external_retrieval_completed_at_utc"] = completed_at
        state["external_retrieval_cutoff_date"] = completed_at[:10]
    else:
        state["status"] = "RUNNING"
        state["external_retrieval_completed_at_utc"] = None
        state["external_retrieval_cutoff_date"] = None


def _save_execution_state(path: Path, state: dict[str, Any]) -> None:
    state["updated_at_utc"] = max(
        str(state.get("updated_at_utc") or ""),
        str(
            max(
                (
                    item.get("last_session_completed_at_utc")
                    or item.get("last_session_started_at_utc")
                    or state["created_at_utc"]
                    for item in state["sources"].values()
                ),
                default=state["created_at_utc"],
            )
        ),
    )
    _save_hashed_json(path, state, "state_hash")


def _save_hashed_json(path: Path, payload: dict[str, Any], hash_key: str) -> None:
    material = dict(payload)
    material.pop(hash_key, None)
    payload[hash_key] = _hash_payload(material)
    atomic_write(path, _pretty_json(payload).encode("utf-8"))


def _validate_embedded_hash(payload: dict[str, Any], hash_key: str) -> None:
    material = dict(payload)
    claimed = material.pop(hash_key, None)
    if claimed != _hash_payload(material):
        raise ExternalRetrievalWaveError(f"{hash_key} mismatch")


def _checkpoint_attempt_count(checkpoint_dir: Path) -> int:
    path = checkpoint_dir / "review_dataset.json"
    return len(load_review_dataset(path).retrieval_attempts) if path.is_file() else 0


def _ieee_calls_on_day(root: Path, checkpoint_dir: Path, quota_day: str) -> int:
    verification = _load_json(root / IEEE_VERIFICATION_PATH)
    verification_calls = sum(
        attempt["requested_at_utc"][:10] == quota_day
        for request in verification["requests"]
        for attempt in request["attempts"]
    )
    checkpoint_path = checkpoint_dir / "review_dataset.json"
    retrieval_calls = 0
    if checkpoint_path.is_file():
        dataset = load_review_dataset(checkpoint_path)
        retrieval_calls = sum(
            attempt.started_at[:10] == quota_day
            for attempt in dataset.retrieval_attempts
        )
        recovery = dataset.retrieval_runs[0].metadata.get(
            "offline_repeated_window_recovery", {}
        )
        if recovery.get("quota_day_utc") == quota_day:
            retrieval_calls += int(
                recovery.get("quota_only_rejected_attempt_count", 0)
            )
    return verification_calls + retrieval_calls


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path("."))
    parser.add_argument("--source", choices=list(EXTERNAL_IDENTIFICATION_SOURCES_V2))
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--authorize-live-external-retrieval", action="store_true")
    parser.add_argument("--authorize-transport-retry-reset", action="store_true")
    parser.add_argument("--authorize-pubmed-parser-recovery", action="store_true")
    parser.add_argument(
        "--authorize-europe-pmc-terminal-recovery", action="store_true"
    )
    parser.add_argument(
        "--authorize-ieee-total-drift-recovery", action="store_true"
    )
    parser.add_argument(
        "--authorize-ieee-repeated-window-recovery", action="store_true"
    )
    parser.add_argument(
        "--authorize-arxiv-rate-limit-recovery", action="store_true"
    )
    parser.add_argument(
        "--authorize-arxiv-mixed-state-recovery", action="store_true"
    )
    parser.add_argument(
        "--authorize-arxiv-episode-3-state-reconciliation",
        action="store_true",
    )
    parser.add_argument(
        "--authorize-arxiv-transport-policy-recovery",
        action="store_true",
    )
    parser.add_argument(
        "--authorize-arxiv-retryable-5xx-recovery",
        action="store_true",
    )
    parser.add_argument(
        "--authorize-arxiv-page-size-recovery",
        action="store_true",
    )
    parser.add_argument(
        "--authorize-semantic-scholar-control-5xx-recovery",
        action="store_true",
    )
    parser.add_argument(
        "--authorize-semantic-scholar-candidate-5xx-recovery",
        action="store_true",
    )
    parser.add_argument(
        "--authorize-semantic-scholar-native-id-overlap-recovery",
        action="store_true",
    )
    parser.add_argument(
        "--prepare-arxiv-snapshot-integration", action="store_true"
    )
    parser.add_argument(
        "--authorize-arxiv-snapshot-integration", action="store_true"
    )
    parser.add_argument("--authorize-prior-survey-import", action="store_true")
    parser.add_argument("--prior-survey-package", type=Path)
    parser.add_argument("--prior-survey-package-sha256")
    parser.add_argument("--arxiv-snapshot-package", type=Path)
    parser.add_argument("--arxiv-snapshot-integration-output", type=Path)
    parser.add_argument("--arxiv-snapshot-amendment-v2", type=Path)
    args = parser.parse_args(argv)
    if args.authorize_prior_survey_import:
        other_modes = [
            value
            for key, value in vars(args).items()
            if key.startswith(("authorize_", "prepare_"))
            and key != "authorize_prior_survey_import"
        ]
        if args.resume or args.source or any(other_modes):
            parser.error(
                "prior-survey import is a separate offline authorization boundary"
            )
        if args.prior_survey_package is None:
            parser.error("--prior-survey-package is required")
        if not args.prior_survey_package_sha256:
            parser.error("--prior-survey-package-sha256 is required")
        state, registration = authorize_prior_survey_import(
            root=args.root,
            package_dir=args.prior_survey_package,
            expected_package_manifest_sha256=args.prior_survey_package_sha256,
        )
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "seed_set_id": registration["seed_set_id"],
                    "registration": registration,
                    "prior_survey_seed_imported": state[
                        "prior_survey_seed_imported"
                    ],
                    "identification_set_closed": state[
                        "identification_set_closed"
                    ],
                    "screening_executed": state["screening_executed"],
                    "prisma_generated": state["prisma_generated"],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    snapshot_modes = (
        args.prepare_arxiv_snapshot_integration,
        args.authorize_arxiv_snapshot_integration,
    )
    if any(snapshot_modes):
        if all(snapshot_modes):
            parser.error("snapshot preparation and authorization are separate modes")
        if args.source != "arXiv":
            parser.error("snapshot integration is supported only for --source arXiv")
        other_modes = [
            value
            for key, value in vars(args).items()
            if key.startswith("authorize_")
            and key != "authorize_arxiv_snapshot_integration"
        ]
        if args.prepare_arxiv_snapshot_integration:
            other_modes.append(args.authorize_arxiv_snapshot_integration)
        if args.resume or any(other_modes):
            parser.error(
                "arXiv snapshot integration is a separate offline authorization boundary"
            )
        if args.arxiv_snapshot_package is None:
            parser.error("--arxiv-snapshot-package is required")
        if args.prepare_arxiv_snapshot_integration:
            if args.arxiv_snapshot_integration_output is None:
                parser.error("--arxiv-snapshot-integration-output is required")
            result = prepare_arxiv_snapshot_integration(
                root=args.root,
                package_dir=args.arxiv_snapshot_package,
                output_dir=args.arxiv_snapshot_integration_output,
            )
            print(json.dumps(result, sort_keys=True, indent=2))
            return 0
        if args.arxiv_snapshot_amendment_v2 is None:
            parser.error("--arxiv-snapshot-amendment-v2 is required")
        state = authorize_arxiv_snapshot_integration(
            root=args.root,
            package_dir=args.arxiv_snapshot_package,
            amendment_v2_path=args.arxiv_snapshot_amendment_v2,
        )
        source_state = state["sources"]["arXiv"]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "arXiv",
                    "api_source_status": source_state["status"],
                    "snapshot_substitution": source_state[
                        "approved_snapshot_substitution"
                    ],
                    "identification_set_closed": state[
                        "identification_set_closed"
                    ],
                    "prior_survey_seed_imported": state[
                        "prior_survey_seed_imported"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_semantic_scholar_native_id_overlap_recovery and (
        args.authorize_live_external_retrieval
        or args.authorize_transport_retry_reset
        or args.authorize_pubmed_parser_recovery
        or args.authorize_europe_pmc_terminal_recovery
        or args.authorize_ieee_total_drift_recovery
        or args.authorize_ieee_repeated_window_recovery
        or args.authorize_arxiv_rate_limit_recovery
        or args.authorize_arxiv_mixed_state_recovery
        or args.authorize_arxiv_episode_3_state_reconciliation
        or args.authorize_arxiv_transport_policy_recovery
        or args.authorize_arxiv_retryable_5xx_recovery
        or args.authorize_arxiv_page_size_recovery
        or args.authorize_semantic_scholar_control_5xx_recovery
        or args.authorize_semantic_scholar_candidate_5xx_recovery
        or args.resume
    ):
        parser.error(
            "Semantic Scholar native-ID-overlap recovery is a separate offline "
            "authorization boundary"
        )
    if args.authorize_arxiv_page_size_recovery:
        if args.source != "arXiv":
            parser.error(
                "page-size recovery is supported only for --source arXiv"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.authorize_arxiv_episode_3_state_reconciliation
            or args.authorize_arxiv_transport_policy_recovery
            or args.authorize_arxiv_retryable_5xx_recovery
            or args.authorize_semantic_scholar_control_5xx_recovery
            or args.authorize_semantic_scholar_candidate_5xx_recovery
            or args.authorize_semantic_scholar_native_id_overlap_recovery
            or args.resume
        ):
            parser.error(
                "arXiv page-size recovery is a separate offline "
                "authorization boundary"
            )
        state = authorize_arxiv_page_size_recovery(root=args.root)
        source_state = state["sources"]["arXiv"]
        active = source_state["execution_episodes"][5]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "arXiv",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "checkpoint_dataset": source_state[
                        "checkpoint_dataset"
                    ],
                    "parent_checkpoint_dataset": active[
                        "parent_checkpoint_dataset"
                    ],
                    "page_size_policy": active["page_size_policy"],
                    "request_identity_changes": active[
                        "recovery_provenance"
                    ]["request_identity_changes"],
                    "historical_lineage_counts": active[
                        "recovery_provenance"
                    ]["historical_lineage_counts"],
                    "page_size_causality": active[
                        "recovery_provenance"
                    ]["page_size_causality"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_arxiv_retryable_5xx_recovery:
        if args.source != "arXiv":
            parser.error(
                "retryable-5xx recovery is supported only for --source arXiv"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.authorize_arxiv_episode_3_state_reconciliation
            or args.authorize_arxiv_transport_policy_recovery
            or args.authorize_arxiv_page_size_recovery
            or args.authorize_semantic_scholar_control_5xx_recovery
            or args.authorize_semantic_scholar_candidate_5xx_recovery
            or args.authorize_semantic_scholar_native_id_overlap_recovery
            or args.resume
        ):
            parser.error(
                "arXiv retryable-5xx recovery is a separate offline "
                "authorization boundary"
            )
        state = authorize_arxiv_retryable_5xx_recovery(root=args.root)
        source_state = state["sources"]["arXiv"]
        active = source_state["execution_episodes"][4]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "arXiv",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "checkpoint_dataset": source_state[
                        "checkpoint_dataset"
                    ],
                    "parent_checkpoint_dataset": active[
                        "parent_checkpoint_dataset"
                    ],
                    "restart_states": active["recovery_provenance"][
                        "restart_states"
                    ],
                    "transport_policy": active["transport_policy"],
                    "historical_lineage_counts": active[
                        "recovery_provenance"
                    ]["historical_lineage_counts"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_arxiv_transport_policy_recovery:
        if args.source != "arXiv":
            parser.error(
                "transport-policy recovery is supported only for --source arXiv"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.authorize_arxiv_episode_3_state_reconciliation
            or args.authorize_semantic_scholar_control_5xx_recovery
            or args.authorize_semantic_scholar_candidate_5xx_recovery
            or args.resume
        ):
            parser.error(
                "arXiv transport-policy recovery is a separate offline "
                "authorization boundary"
            )
        state = authorize_arxiv_transport_policy_recovery(root=args.root)
        source_state = state["sources"]["arXiv"]
        active = source_state["execution_episodes"][3]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "arXiv",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "checkpoint_dataset": source_state[
                        "checkpoint_dataset"
                    ],
                    "parent_checkpoint_dataset": active[
                        "parent_checkpoint_dataset"
                    ],
                    "old_transport_policy": active[
                        "old_transport_policy"
                    ],
                    "new_transport_policy": active["transport_policy"],
                    "restart_states": active["restart_states"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_arxiv_episode_3_state_reconciliation:
        if args.source != "arXiv":
            parser.error(
                "episode-3 state reconciliation is supported only for --source arXiv"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.authorize_semantic_scholar_control_5xx_recovery
            or args.authorize_semantic_scholar_candidate_5xx_recovery
            or args.resume
        ):
            parser.error(
                "arXiv episode-3 state reconciliation is a separate offline "
                "authorization boundary"
            )
        state = authorize_arxiv_episode_3_state_reconciliation(root=args.root)
        source_state = state["sources"]["arXiv"]
        evidence = source_state["state_reconciliation"]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "arXiv",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "checkpoint_dataset": source_state["checkpoint_dataset"],
                    "attempt_count": source_state["attempt_count"],
                    "appended_attempt_numbers": evidence[
                        "appended_attempt_numbers"
                    ],
                    "attempt_manifest_hash": evidence[
                        "attempt_manifest_hash"
                    ],
                    "persisted_response_hashes": evidence[
                        "persisted_response_hashes"
                    ],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                    "checkpoint_modified": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_semantic_scholar_native_id_overlap_recovery:
        if args.source != "SemanticScholar":
            parser.error(
                "native-ID-overlap recovery is supported only for --source "
                "SemanticScholar"
            )
        state = authorize_semantic_scholar_native_id_overlap_recovery(
            root=args.root
        )
        source_state = state["sources"]["SemanticScholar"]
        active = next(
            episode
            for episode in source_state["execution_episodes"]
            if episode.get("episode_number")
            == source_state["active_episode_number"]
        )
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "SemanticScholar",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "completed_query_count": source_state[
                        "completed_query_count"
                    ],
                    "checkpoint_dataset": source_state[
                        "checkpoint_dataset"
                    ],
                    "parent_checkpoint_dataset": active[
                        "parent_checkpoint_dataset"
                    ],
                    "adjudicated_occurrences": active[
                        "adjudication_provenance"
                    ]["adjudicated_occurrences"],
                    "continuation": active["continuation_state"],
                    "provider_completeness": active[
                        "adjudication_provenance"
                    ]["provider_completeness"],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_semantic_scholar_candidate_5xx_recovery:
        if args.source != "SemanticScholar":
            parser.error(
                "candidate-5xx recovery is supported only for --source "
                "SemanticScholar"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.authorize_semantic_scholar_control_5xx_recovery
            or args.resume
        ):
            parser.error(
                "Semantic Scholar candidate-5xx recovery is a separate offline "
                "authorization boundary"
            )
        state = authorize_semantic_scholar_candidate_5xx_recovery(root=args.root)
        source_state = state["sources"]["SemanticScholar"]
        active = source_state["execution_episodes"][1]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "SemanticScholar",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "completed_query_count": source_state[
                        "completed_query_count"
                    ],
                    "retained_successful_page_count": active[
                        "retained_successful_page_count"
                    ],
                    "preserved_candidate_response_count": len(
                        active["source_raw_responses"]
                    ),
                    "continuation_states": active["continuation_states"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_semantic_scholar_control_5xx_recovery:
        if args.source != "SemanticScholar":
            parser.error(
                "control-5xx recovery is supported only for --source SemanticScholar"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "Semantic Scholar control-5xx recovery is a separate offline "
                "authorization boundary"
            )
        state = authorize_semantic_scholar_control_5xx_recovery(root=args.root)
        source_state = state["sources"]["SemanticScholar"]
        recovery = source_state["control_gate_recovery"]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "SemanticScholar",
                    "source_status": source_state["status"],
                    "retained_successful_control_ids": recovery[
                        "retained_successful_control_ids"
                    ],
                    "reopened_control_id": recovery["reopened_control_id"],
                    "unattempted_control_ids": recovery[
                        "unattempted_control_ids"
                    ],
                    "preserved_control_request_count": recovery[
                        "source_response_count"
                    ],
                    "candidate_request_count": source_state[
                        "candidate_request_count"
                    ],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_arxiv_mixed_state_recovery:
        if args.source != "arXiv":
            parser.error(
                "mixed-state recovery is supported only for --source arXiv"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.resume
        ):
            parser.error(
                "arXiv mixed-state recovery is a separate offline authorization boundary"
            )
        state = authorize_arxiv_mixed_state_recovery(root=args.root)
        source_state = state["sources"]["arXiv"]
        active = source_state["execution_episodes"][2]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "arXiv",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "restart_states": active["restart_states"],
                    "preserved_attempt_count": active["source_attempt_count"],
                    "preserved_raw_response_count": active[
                        "source_raw_response_count"
                    ],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_arxiv_rate_limit_recovery:
        if args.source != "arXiv":
            parser.error(
                "rate-limit recovery is supported only for --source arXiv"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "arXiv rate-limit recovery is a separate offline authorization boundary"
            )
        state = authorize_arxiv_rate_limit_recovery(root=args.root)
        source_state = state["sources"]["arXiv"]
        active = source_state["execution_episodes"][1]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "arXiv",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "restart_states": active["restart_states"],
                    "preserved_attempt_count": active["source_attempt_count"],
                    "preserved_raw_response_count": len(
                        active["source_raw_responses"]
                    ),
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_ieee_repeated_window_recovery:
        if args.source != "IEEEXplore":
            parser.error(
                "repeated-window recovery is supported only for --source IEEEXplore"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "IEEE repeated-window recovery is a separate offline authorization boundary"
            )
        state = authorize_ieee_repeated_window_recovery(root=args.root)
        source_state = state["sources"]["IEEEXplore"]
        active = source_state["execution_episodes"][2]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "IEEEXplore",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "continuation_plan": active["continuation_plan"],
                    "retained_page_count": active["retained_page_count"],
                    "rejected_page_count": len(active["rejection_evidence"]),
                    "known_daily_calls_preserved": active[
                        "known_daily_calls_preserved"
                    ],
                    "remaining_daily_calls": active["remaining_daily_calls"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_ieee_total_drift_recovery:
        if args.source != "IEEEXplore":
            parser.error(
                "provider-total recovery is supported only for --source IEEEXplore"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "IEEE provider-total recovery is a separate offline authorization boundary"
            )
        state = authorize_ieee_total_drift_recovery(root=args.root)
        source_state = state["sources"]["IEEEXplore"]
        active = source_state["execution_episodes"][1]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "IEEEXplore",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "continuation_plan": active["continuation_plan"],
                    "known_daily_calls_preserved": active[
                        "known_daily_calls_preserved"
                    ],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_europe_pmc_terminal_recovery:
        if args.source != "EuropePMC":
            parser.error(
                "terminal recovery is supported only for --source EuropePMC"
            )
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_pubmed_parser_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "Europe PMC terminal recovery is a separate offline authorization boundary"
            )
        state = authorize_europe_pmc_terminal_recovery(root=args.root)
        source_state = state["sources"]["EuropePMC"]
        active = source_state["execution_episodes"][1]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "EuropePMC",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state[
                        "active_episode_number"
                    ],
                    "query_occurrence_counts": active[
                        "query_occurrence_counts"
                    ],
                    "occurrence_count": source_state["occurrence_count"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_pubmed_parser_recovery:
        if args.source != "PubMed":
            parser.error("parser recovery is supported only for --source PubMed")
        if (
            args.authorize_live_external_retrieval
            or args.authorize_transport_retry_reset
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error("parser recovery is a separate offline authorization boundary")
        state = authorize_pubmed_parser_recovery(root=args.root)
        source_state = state["sources"]["PubMed"]
        active = next(
            item
            for item in source_state["execution_episodes"]
            if item["episode_number"] == source_state["active_episode_number"]
        )
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "PubMed",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state["active_episode_number"],
                    "recovered_pmids": active["recovered_pmids"],
                    "already_fetched_occurrence_count": active[
                        "already_fetched_occurrence_count"
                    ],
                    "remaining_efetch_request_count": active[
                        "remaining_efetch_request_count"
                    ],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_transport_retry_reset:
        if args.source != "PubMed":
            parser.error("transport retry reset is supported only for --source PubMed")
        if (
            args.authorize_live_external_retrieval
            or args.authorize_pubmed_parser_recovery
            or args.authorize_europe_pmc_terminal_recovery
            or args.authorize_ieee_total_drift_recovery
            or args.authorize_ieee_repeated_window_recovery
            or args.authorize_arxiv_rate_limit_recovery
            or args.authorize_arxiv_mixed_state_recovery
            or args.resume
        ):
            parser.error(
                "transport retry reset is a separate offline authorization boundary"
            )
        state = authorize_pubmed_transport_retry(root=args.root)
        source_state = state["sources"]["PubMed"]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": "PubMed",
                    "source_status": source_state["status"],
                    "active_episode_number": source_state["active_episode_number"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "network_used": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return 0
    if args.authorize_live_external_retrieval:
        if not args.source:
            parser.error("--source is required for authorized execution")
        credential = (
            os.environ.get(IEEE_CREDENTIAL_NAME, "")
            if args.source == "IEEEXplore"
            else ""
        )
        state = execute_external_source_session(
            root=args.root,
            source=args.source,
            http=None
            if args.source == "ACMDigitalLibrary"
            else RequestsHttpClient(),
            resume=args.resume,
            ieee_credential=credential,
        )
        source_state = state["sources"][args.source]
        print(
            json.dumps(
                {
                    "execution_status": state["status"],
                    "source": args.source,
                    "source_status": source_state["status"],
                    "completed_query_count": source_state[
                        "completed_query_count"
                    ],
                    "total_query_count": source_state["total_query_count"],
                    "external_retrieval_cutoff_date": state[
                        "external_retrieval_cutoff_date"
                    ],
                    "credential_value_persisted": False,
                },
                sort_keys=True,
                indent=2,
            )
        )
        return (
            0
            if source_state["status"]
            in {
                "COMPLETE",
                "PAUSED_DAILY_QUOTA",
                "PAUSED_PROVIDER_QUOTA",
                "PAUSED_PROVIDER_RATE_LIMIT",
                "PAUSED_TRANSIENT_PROVIDER",
                "PAUSED_TRANSIENT_TRANSPORT",
            }
            else 2
        )
    result = save_external_retrieval_preflight(root=args.root)
    print(
        json.dumps(
            {
                "status": result["status"],
                "wave_id": result["wave_id"],
                "wave_path": result["wave_file"]["path"],
                "wave_sha256": result["wave_file"]["raw_sha256"],
                "preflight_path": result["preflight_file"]["path"],
                "preflight_sha256": result["preflight_file"]["raw_sha256"],
                "query_count": len(result["query_inventory"]),
                "estimated_http_requests": result["request_burden"][
                    "estimated_http_requests"
                ],
                "network_used": False,
            },
            sort_keys=True,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
