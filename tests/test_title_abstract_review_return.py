from __future__ import annotations

import zipfile
from pathlib import Path

from h2h_lit.title_abstract_review_return import validate_review_workbook


def _write_return_fixture(path: Path) -> None:
    content_types = """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>"""
    root_relationships = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"""
    workbook = """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets>
    <sheet name="Instructions" sheetId="1" r:id="rId1"/>
    <sheet name="Reviews" sheetId="2" r:id="rId2"/>
  </sheets>
</workbook>"""
    workbook_relationships = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
</Relationships>"""
    instructions = """<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>
  <row r="3"><c r="A3" t="inlineStr"><is><t>Reviewer ID</t></is></c><c r="B3" t="inlineStr"><is><t>Rumi</t></is></c></row>
</sheetData></worksheet>"""
    reviews = """<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>
  <row r="3">
    <c r="A3" t="inlineStr"><is><t>Stable record ID</t></is></c>
    <c r="E3" t="inlineStr"><is><t>E1</t></is></c><c r="F3" t="inlineStr"><is><t>E2</t></is></c>
    <c r="G3" t="inlineStr"><is><t>E3</t></is></c><c r="H3" t="inlineStr"><is><t>E4</t></is></c>
    <c r="I3" t="inlineStr"><is><t>E5</t></is></c><c r="J3" t="inlineStr"><is><t>E6</t></is></c>
    <c r="K3" t="inlineStr"><is><t>E7</t></is></c><c r="L3" t="inlineStr"><is><t>Eligibility status</t></is></c>
    <c r="M3" t="inlineStr"><is><t>Primary exclusion reason</t></is></c><c r="N3" t="inlineStr"><is><t>Full-text escalation</t></is></c>
    <c r="R3" t="inlineStr"><is><t>Excluded-record escalation rationale</t></is></c>
  </row>
  <row r="4">
    <c r="A4" t="inlineStr"><is><t>record:no-plus-uncertain</t></is></c>
    <c r="E4" t="inlineStr"><is><t>NO</t></is></c><c r="F4" t="inlineStr"><is><t>YES</t></is></c>
    <c r="G4" t="inlineStr"><is><t>YES</t></is></c><c r="H4" t="inlineStr"><is><t>YES</t></is></c>
    <c r="I4" t="inlineStr"><is><t>YES</t></is></c><c r="J4" t="inlineStr"><is><t>UNCERTAIN</t></is></c>
    <c r="K4" t="inlineStr"><is><t>YES</t></is></c><c r="L4"><f>IF(COUNTIF(E4:K4,&quot;NO&quot;)&gt;0,&quot;EXCLUDED&quot;,&quot;UNCERTAIN&quot;)</f><v></v></c>
    <c r="M4" t="inlineStr"><is><t>EX_NO_LIFE_SCIENCE_APPLICATION</t></is></c><c r="N4" t="inlineStr"><is><t>NO</t></is></c>
  </row>
  <row r="5">
    <c r="A5" t="inlineStr"><is><t>record:all-yes</t></is></c>
    <c r="E5" t="inlineStr"><is><t>YES</t></is></c><c r="F5" t="inlineStr"><is><t>YES</t></is></c>
    <c r="G5" t="inlineStr"><is><t>YES</t></is></c><c r="H5" t="inlineStr"><is><t>YES</t></is></c>
    <c r="I5" t="inlineStr"><is><t>YES</t></is></c><c r="J5" t="inlineStr"><is><t>YES</t></is></c>
    <c r="K5" t="inlineStr"><is><t>YES</t></is></c><c r="L5" t="str"><f>IF(1=1,&quot;ELIGIBLE&quot;,&quot;&quot;)</f><v>UNCERTAIN</v></c>
    <c r="N5" t="inlineStr"><is><t>NO</t></is></c>
  </row>
  <row r="6">
    <c r="A6" t="inlineStr"><is><t>record:incomplete</t></is></c>
    <c r="E6" t="inlineStr"><is><t>YES</t></is></c><c r="F6" t="inlineStr"><is><t>YES</t></is></c>
    <c r="G6" t="inlineStr"><is><t>YES</t></is></c><c r="H6" t="inlineStr"><is><t>YES</t></is></c>
    <c r="I6" t="inlineStr"><is><t>YES</t></is></c><c r="J6" t="inlineStr"><is><t>YES</t></is></c>
    <c r="L6"><f>IF(COUNTBLANK(E6:K6)&gt;0,&quot;&quot;,&quot;ELIGIBLE&quot;)</f><v></v></c><c r="N6" t="inlineStr"><is><t>NO</t></is></c>
  </row>
</sheetData></worksheet>"""
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_relationships)
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_relationships)
        archive.writestr("xl/worksheets/sheet1.xml", instructions)
        archive.writestr("xl/worksheets/sheet2.xml", reviews)


def test_workbook_return_recomputes_blank_and_stale_formula_caches(tmp_path: Path) -> None:
    workbook = tmp_path / "returned.xlsx"
    _write_return_fixture(workbook)

    report = validate_review_workbook(workbook)

    assert report["reviewer_id"] == "Rumi"
    assert report["computed_outcome_counts"] == {
        "EXCLUDED": 1,
        "ELIGIBLE": 1,
        "INVALID": 1,
    }
    assert report["formula_cache_counts"] == {"BLANK": 2, "STALE_OR_INCORRECT": 1}
    rows = {row["record_id"]: row for row in report["rows"]}
    assert rows["record:no-plus-uncertain"]["computed_outcome"] == "EXCLUDED"
    assert rows["record:all-yes"]["computed_outcome"] == "ELIGIBLE"
    assert rows["record:all-yes"]["cached_outcome"] == "UNCERTAIN"
    assert rows["record:incomplete"]["computed_outcome"] is None
    assert rows["record:incomplete"]["inconsistencies"] == [
        "INCOMPLETE_OR_INVALID_CRITERIA"
    ]


def _write_approved_return_fixture(path: Path) -> None:
    content_types = """<?xml version="1.0" encoding="UTF-8"?>
<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">
  <Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>
  <Default Extension="xml" ContentType="application/xml"/>
  <Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>
  <Override PartName="/xl/worksheets/sheet1.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
  <Override PartName="/xl/worksheets/sheet2.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>
</Types>"""
    root_relationships = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>
</Relationships>"""
    workbook = """<?xml version="1.0" encoding="UTF-8"?>
<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">
  <sheets><sheet name="Instructions" sheetId="1" r:id="rId1"/><sheet name="Reviews" sheetId="2" r:id="rId2"/></sheets>
</workbook>"""
    workbook_relationships = """<?xml version="1.0" encoding="UTF-8"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet1.xml"/>
  <Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet2.xml"/>
</Relationships>"""
    instructions = """<?xml version="1.0" encoding="UTF-8"?>
<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>
  <row r="3"><c r="A3" t="inlineStr"><is><t>Reviewer ID</t></is></c><c r="B3" t="inlineStr"><is><t>Morris</t></is></c></row>
  <row r="7"><c r="A7" t="inlineStr"><is><t>Return schema version</t></is></c><c r="B7" t="inlineStr"><is><t>2.0.0</t></is></c></row>
</sheetData></worksheet>"""
    headers = """<row r="3">
      <c r="A3" t="inlineStr"><is><t>Stable record ID</t></is></c><c r="C3" t="inlineStr"><is><t>Abstract</t></is></c>
      <c r="E3" t="inlineStr"><is><t>E1</t></is></c><c r="F3" t="inlineStr"><is><t>E2</t></is></c><c r="G3" t="inlineStr"><is><t>E3</t></is></c><c r="H3" t="inlineStr"><is><t>E4</t></is></c><c r="I3" t="inlineStr"><is><t>E5</t></is></c><c r="J3" t="inlineStr"><is><t>E6</t></is></c><c r="K3" t="inlineStr"><is><t>E7</t></is></c>
      <c r="L3" t="inlineStr"><is><t>Evidence conflict</t></is></c><c r="M3" t="inlineStr"><is><t>Targeted second review</t></is></c><c r="N3" t="inlineStr"><is><t>Screening outcome</t></is></c><c r="O3" t="inlineStr"><is><t>Next action</t></is></c><c r="P3" t="inlineStr"><is><t>Primary exclusion reason</t></is></c>
    </row>"""
    def criterion_cells(row: int, values: list[str]) -> str:
        columns = ["E", "F", "G", "H", "I", "K"]
        return "".join(
            f'<c r="{column}{row}" t="inlineStr"><is><t>{value}</t></is></c>'
            for column, value in zip(columns, values)
        )

    rows = []
    row_specs = [
        (4, "record:all-yes", ["YES"] * 6, "", "", "", ""),
        (5, "record:e5-no", ["YES", "YES", "YES", "YES", "NO", "YES"], "UNCERTAIN", "FULL_TEXT_ASSESSMENT", "EX_NO_HUMAN_ANALYTIC_RELATIONSHIP", "abstract"),
        (6, "record:e7-no", ["YES", "YES", "YES", "YES", "YES", "NO"], "UNCERTAIN", "FULL_TEXT_ASSESSMENT", "", "abstract"),
        (7, "record:incomplete", ["YES", "YES", "", "YES", "YES", "YES"], "", "", "", "abstract"),
    ]
    for row, record_id, criteria, cached_outcome, cached_action, reason, abstract in row_specs:
        rows.append(
            f'<row r="{row}"><c r="A{row}" t="inlineStr"><is><t>{record_id}</t></is></c>'
            f'<c r="C{row}" t="inlineStr"><is><t>{abstract}</t></is></c>'
            f'{criterion_cells(row, criteria)}'
            f'<c r="J{row}" t="inlineStr"><is><t>NOT_ASSESSED_AT_THIS_STAGE</t></is></c>'
            f'<c r="L{row}" t="inlineStr"><is><t>NO</t></is></c><c r="M{row}" t="inlineStr"><is><t>NO</t></is></c>'
            f'<c r="N{row}" t="str"><f>approved-outcome</f><v>{cached_outcome}</v></c>'
            f'<c r="O{row}" t="str"><f>approved-route</f><v>{cached_action}</v></c>'
            f'<c r="P{row}" t="inlineStr"><is><t>{reason}</t></is></c></row>'
        )
    reviews = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>'
        + headers + "".join(rows) + "</sheetData></worksheet>"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("[Content_Types].xml", content_types)
        archive.writestr("_rels/.rels", root_relationships)
        archive.writestr("xl/workbook.xml", workbook)
        archive.writestr("xl/_rels/workbook.xml.rels", workbook_relationships)
        archive.writestr("xl/worksheets/sheet1.xml", instructions)
        archive.writestr("xl/worksheets/sheet2.xml", reviews)


def test_approved_return_recomputes_outcomes_routes_and_formula_caches(tmp_path: Path) -> None:
    workbook = tmp_path / "approved-return.xlsx"
    _write_approved_return_fixture(workbook)

    report = validate_review_workbook(workbook)

    assert report["return_schema_version"] == "2.0.0"
    assert report["computed_outcome_counts"] == {
        "INCLUDE": 1,
        "EXCLUDED": 1,
        "UNCERTAIN": 1,
        "INVALID": 1,
    }
    rows = {row["record_id"]: row for row in report["rows"]}
    assert rows["record:all-yes"]["computed_next_action"] == "METADATA_RECOVERY"
    assert rows["record:e5-no"]["computed_outcome"] == "EXCLUDED"
    assert rows["record:e5-no"]["computed_next_action"] == "NONE"
    assert rows["record:e5-no"]["cache_status"] == "STALE_OR_INCORRECT"
    assert rows["record:e7-no"]["computed_outcome"] == "UNCERTAIN"
    assert rows["record:incomplete"]["computed_next_action"] == "TARGETED_SECOND_REVIEW"
