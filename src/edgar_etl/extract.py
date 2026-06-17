import html
import re
import warnings
from pathlib import Path

from bs4 import BeautifulSoup, Tag, XMLParsedAsHTMLWarning

from edgar_etl.models import TextChunk

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

ITEM_HEADER_RE = re.compile(
    r"^\s*ITEM\s+[\d.]+\s+.+$",
    re.IGNORECASE,
)
WHITESPACE_RE = re.compile(r"[ \t]+")
MULTI_NEWLINE_RE = re.compile(r"\n{3,}")
HIDDEN_STYLE_RE = re.compile(r"display\s*:\s*none", re.IGNORECASE)


def read_filing_html(local_path: str | Path) -> str:
    path = Path(local_path)
    if not path.is_file():
        raise FileNotFoundError(f"filing not found: {path}")
    return path.read_text(encoding="utf-8", errors="replace")


def extract_text_from_html(html_content: str) -> str:
    parser = "xml" if html_content.lstrip().startswith("<?xml") else "lxml"
    soup = BeautifulSoup(html_content, parser)
    for tag in soup.find_all(["script", "style"]):
        tag.decompose()

    _remove_hidden_xbrl(soup)
    _unwrap_ix_tags(soup)

    body = soup.body or soup
    blocks: list[str] = []

    for table in body.find_all("table"):
        table_text = _table_to_text(table)
        if table_text:
            blocks.append(table_text)
        table.decompose()

    for element in body.find_all(["p", "h1", "h2", "h3", "h4", "li"]):
        text = _normalize_text(element.get_text(" ", strip=True))
        if text:
            blocks.append(text)

    if not blocks:
        blocks.append(_normalize_text(body.get_text("\n", strip=True)))

    return "\n\n".join(blocks)


def _remove_hidden_xbrl(soup: BeautifulSoup) -> None:
    for tag in soup.find_all(["div", "span"]):
        style = tag.get("style", "")
        if HIDDEN_STYLE_RE.search(style):
            tag.decompose()


def _unwrap_ix_tags(soup: BeautifulSoup) -> None:
    for tag in list(soup.find_all(re.compile(r"^ix:", re.I))):
        tag.unwrap()


def _table_to_text(table: Tag) -> str:
    rows: list[str] = []
    for tr in table.find_all("tr"):
        cells = [
            _normalize_text(cell.get_text(" ", strip=True))
            for cell in tr.find_all(["td", "th"])
        ]
        cells = [cell for cell in cells if cell]
        if cells:
            rows.append(" | ".join(cells))
    return "\n".join(rows)


def _normalize_text(text: str) -> str:
    text = html.unescape(text)
    text = text.replace("\xa0", " ")
    text = WHITESPACE_RE.sub(" ", text)
    return text.strip()


def chunk_text(
    text: str,
    *,
    chunk_size: int,
    chunk_overlap: int,
) -> list[TextChunk]:
    sections = _split_by_sections(text)
    chunks: list[TextChunk] = []
    chunk_index = 0

    for section_name, section_text in sections:
        for piece in _split_with_overlap(section_text, chunk_size, chunk_overlap):
            if not piece.strip():
                continue
            chunks.append(
                TextChunk(
                    chunk_index=chunk_index,
                    content=piece.strip(),
                    section=section_name,
                    metadata={"section": section_name} if section_name else {},
                )
            )
            chunk_index += 1

    return chunks


def _split_by_sections(text: str) -> list[tuple[str | None, str]]:
    lines = text.split("\n")
    sections: list[tuple[str | None, str]] = []
    current_section: str | None = None
    current_lines: list[str] = []

    def flush() -> None:
        nonlocal current_lines, current_section
        if current_lines:
            sections.append((current_section, "\n".join(current_lines).strip()))
            current_lines = []

    for line in lines:
        stripped = line.strip()
        if ITEM_HEADER_RE.match(stripped):
            flush()
            current_section = stripped
            current_lines.append(stripped)
            continue
        current_lines.append(line)

    flush()

    if not sections:
        return [(None, text)]

    return sections


def _split_with_overlap(text: str, chunk_size: int, chunk_overlap: int) -> list[str]:
    if len(text) <= chunk_size:
        return [text]

    chunks: list[str] = []
    start = 0
    while start < len(text):
        end = start + chunk_size
        chunk = text[start:end]
        if end < len(text):
            break_at = chunk.rfind("\n\n")
            if break_at < chunk_size // 2:
                break_at = chunk.rfind(". ")
            if break_at >= chunk_size // 2:
                chunk = chunk[: break_at + 1]
                end = start + len(chunk)
        chunks.append(chunk)
        if end >= len(text):
            break
        start = max(end - chunk_overlap, start + 1)

    return chunks
