import pytest

from edgar_etl.extract import chunk_text, extract_text_from_html, read_filing_html

SAMPLE_FILING = "/Volumes/Transcend/edgar/AEE/000110465926063184/tm2614913d1_8k.htm"

INLINE_IXBRL_HTML = """<?xml version='1.0' encoding='UTF-8'?>
<html xmlns="http://www.w3.org/1999/xhtml" xmlns:ix="http://www.xbrl.org/2013/inlineXBRL">
<body>
<div style="display: none">
  <ix:header><ix:hidden>hidden xbrl metadata</ix:hidden></ix:header>
</div>
<p><b>ITEM 5.07 Submission of Matters to a Vote of Security Holders.</b></p>
<p>At the annual meeting, Cynthia J. Brinkley was elected director.</p>
<table>
  <tr><th>Name</th><th>Votes For</th></tr>
  <tr><td>Cynthia J. Brinkley</td><td>211,811,213</td></tr>
</table>
</body>
</html>"""


@pytest.fixture(scope="module")
def sample_html() -> str:
    try:
        return read_filing_html(SAMPLE_FILING)
    except FileNotFoundError:
        pytest.skip(f"sample filing not available: {SAMPLE_FILING}")


def test_extract_inline_ixbrl_html() -> None:
    text = extract_text_from_html(INLINE_IXBRL_HTML)
    assert "ITEM 5.07" in text
    assert "Cynthia J. Brinkley" in text
    assert "211,811,213" in text
    assert "hidden xbrl metadata" not in text
    assert "ix:nonNumeric" not in text


def test_chunk_inline_html() -> None:
    text = extract_text_from_html(INLINE_IXBRL_HTML)
    chunks = chunk_text(text, chunk_size=80, chunk_overlap=10)
    assert len(chunks) >= 1
    assert chunks[0].chunk_index == 0


def test_extract_text_contains_item_section(sample_html: str) -> None:
    text = extract_text_from_html(sample_html)
    assert "ITEM 5.07" in text
    assert "Submission of Matters to a Vote of Security Holders" in text
    assert "Cynthia J. Brinkley" in text
    assert "ix:nonNumeric" not in text
    assert "xbrli:context" not in text


def test_chunk_text_produces_multiple_chunks(sample_html: str) -> None:
    text = extract_text_from_html(sample_html)
    chunks = chunk_text(text, chunk_size=500, chunk_overlap=50)
    assert len(chunks) >= 2
    assert all(chunk.content.strip() for chunk in chunks)
    assert chunks[0].chunk_index == 0
