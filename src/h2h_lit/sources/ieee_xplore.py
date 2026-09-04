"""IEEE Xplore Metadata API pagination and provenance adapter."""

from __future__ import annotations

from typing import Any

from h2h_lit.pagination import (
    PageRequest,
    PaginationError,
    ParsedPage,
    malformed_identifier,
    native_identifier,
)
from h2h_lit.sources.common import make_record

SEARCH_URL = "https://ieeexploreapi.ieee.org/api/v1/search/articles"
ABSTRACT_CONTENT_POLICY = "external_llm_use_unresolved"


def _total(payload: dict[str, Any]) -> tuple[int, str]:
    for key in ("totalfound", "total_records"):
        if key in payload:
            try:
                return int(payload[key]), key
            except (TypeError, ValueError) as exc:
                raise PaginationError(f"IEEE {key} must be an integer") from exc
    raise PaginationError("IEEE response omitted totalfound/total_records")


def _authors(item: dict[str, Any]) -> list[str]:
    value = item.get("authors") or []
    if isinstance(value, dict):
        value = value.get("authors") or []
    if not isinstance(value, list):
        return []
    ordered = sorted(
        (entry for entry in value if isinstance(entry, dict)),
        key=lambda entry: int(entry.get("author_order") or 10**9),
    )
    return [
        str(entry.get("full_name") or entry.get("name") or "").strip()
        for entry in ordered
        if str(entry.get("full_name") or entry.get("name") or "").strip()
    ]


def _year(item: dict[str, Any]) -> str | int | None:
    return item.get("publication_year") or item.get("publication_date")


def _url(item: dict[str, Any]) -> str | None:
    return item.get("html_url") or item.get("pdf_url") or item.get("abstract_url")


def _record(item: Any, *, query: str, rank: int):
    if not isinstance(item, dict):
        raw = {"raw_item": item, "parser_incomplete": True}
        return make_record(
            title="",
            source_identifier=malformed_identifier(item, rank),
            source_database="IEEEXplore",
            original_metadata=raw,
            source_query=query,
            stage="ieee_xplore_metadata_api",
        )

    article_number = str(item.get("article_number") or "").strip()
    incomplete = not article_number
    source_identifier = article_number or malformed_identifier(item, rank)
    original = dict(item)
    original["text_field_provenance"] = {
        "abstract": {
            "identification_source": "IEEEXplore",
            "content_policy": ABSTRACT_CONTENT_POLICY,
        }
    }
    if incomplete:
        original["parser_incomplete"] = True
        original["parser_error"] = "missing stable article_number"
    record = make_record(
        title=str(item.get("title") or ""),
        abstract=str(item.get("abstract") or ""),
        authors=_authors(item),
        year=_year(item),
        doi=item.get("doi"),
        source_identifier=source_identifier,
        source_database="IEEEXplore",
        source_url=_url(item),
        pdf_url=item.get("pdf_url"),
        journal=item.get("publication_title"),
        is_open_access=(str(item.get("access_type") or "").lower() == "open_access"),
        original_metadata=original,
        source_query=query,
        stage="ieee_xplore_metadata_api",
    )
    record.annotations["content_policy"] = {"abstract": ABSTRACT_CONTENT_POLICY}
    record.provenance[-1].metadata["text_field_provenance"] = original[
        "text_field_provenance"
    ]
    return record


class IeeeXplorePaginator:
    source_database = "IEEEXplore"
    strategy = "start_record"
    version = "1.0.0"

    def initial_state(self, spec: Any) -> dict[str, Any]:
        return {"start_record": 1}

    def build_request(self, spec: Any, state: dict[str, Any]) -> PageRequest:
        query_parameter = str(spec.metadata.get("query_parameter") or "").strip()
        if not query_parameter:
            raise ValueError("IEEE query_parameter must be explicitly frozen")
        sort_field = str(spec.metadata.get("sort_field") or "").strip()
        sort_order = str(spec.metadata.get("sort_order") or "").strip()
        if not sort_field or not sort_order:
            raise ValueError("IEEE sort_field and sort_order must be explicitly frozen")
        params: dict[str, Any] = dict(spec.filters)
        params.update(
            {
                query_parameter: spec.query_text,
                "format": "json",
                "max_records": spec.limit,
                "start_record": int(state["start_record"]),
                "sort_field": sort_field,
                "sort_order": sort_order,
            }
        )
        if spec.credentials.get("api_key"):
            params["apikey"] = spec.credentials["api_key"]
        return PageRequest("GET", spec.endpoint or SEARCH_URL, params=params, state=state)

    def parse_response(self, spec: Any, state: dict[str, Any], response: Any) -> ParsedPage:
        payload = response.json()
        if not isinstance(payload, dict):
            raise PaginationError("IEEE response must be a JSON object")
        items = payload.get("articles") or []
        if not isinstance(items, list):
            raise PaginationError("IEEE articles must be a list")
        total, total_key = _total(payload)
        start_record = int(state["start_record"])
        if start_record < 1 or total < 0:
            raise PaginationError("IEEE start_record/totalfound values are invalid")
        records = [
            _record(item, query=spec.query_text, rank=rank)
            for rank, item in enumerate(items, start=1)
        ]
        mutable_provider_totals = bool(
            getattr(spec, "metadata", {}).get("mutable_provider_totals")
        )
        # IEEE serves fixed ``max_records`` request windows.  A mutable index can
        # make a nonterminal window short; advancing by the returned count would
        # leave the next request inside the same provider window and repeat it.
        next_start = start_record + int(spec.limit)
        terminal = next_start > total
        incomplete_reason = None
        if not items and start_record <= total:
            incomplete_reason = "IEEE returned an empty page before totalfound was reached"
        if any(record.original_metadata.get("parser_incomplete") for record in records):
            incomplete_reason = incomplete_reason or "IEEE page contained malformed records"
        return ParsedPage(
            records=records,
            raw_item_count=len(items),
            next_state={"start_record": next_start} if not terminal else None,
            terminal=terminal,
            completion_proof=(
                "ieee_current_total_exhaustion_observed"
                if terminal and mutable_provider_totals
                else "ieee_totalfound_reconciled"
                if terminal
                else None
            ),
            source_reported_total=total,
            total_is_exact=not mutable_provider_totals,
            incomplete_reason=incomplete_reason,
            native_identifiers=[
                native_identifier(record, rank) for rank, record in enumerate(records, 1)
            ],
            metadata={
                "total_field": total_key,
                "totalfound": total,
                "totalsearched": payload.get("totalsearched")
                or payload.get("total_searched"),
                "start_record": start_record,
                "max_records": spec.limit,
                "rank_start": start_record,
                "rank_end": start_record + len(items) - 1 if items else None,
                "provider_total_observation": total,
                "provider_total_semantics": (
                    "MUTABLE_PAGINATION_OBSERVATION"
                    if mutable_provider_totals
                    else "EXACT_WITHIN_RETRIEVAL_RUN"
                ),
            },
        )


PAGINATOR = IeeeXplorePaginator()
