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
