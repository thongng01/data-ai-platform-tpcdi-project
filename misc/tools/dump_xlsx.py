"""Dump the cells of an xlsx workbook to text, one file per sheet.

Usage:
    ../.venv/bin/python ../misc/tools/dump_xlsx.py <workbook.xlsx> <out_dir>

Used to read the course rubric without opening Excel.
"""

import sys
import zipfile
from pathlib import Path
from xml.etree import ElementTree

NS = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}


def col_index(ref):
    """Turn a cell reference such as 'BC12' into a zero-based column number."""
    letters = "".join(ch for ch in ref if ch.isalpha())
    value = 0
    for ch in letters:
        value = value * 26 + (ord(ch.upper()) - ord("A") + 1)
    return value - 1


def shared_strings(zf):
    """Return the workbook's shared string table as a list of strings."""
    try:
        raw = zf.read("xl/sharedStrings.xml")
    except KeyError:
        return []
    root = ElementTree.fromstring(raw)
    out = []
    for si in root.findall("m:si", NS):
        out.append("".join(t.text or "" for t in si.iterfind(".//m:t", NS)))
    return out


def sheet_names(zf):
    """Return the sheet names in workbook order."""
    root = ElementTree.fromstring(zf.read("xl/workbook.xml"))
    return [s.get("name") for s in root.iterfind(".//m:sheet", NS)]


def dump_sheet(zf, target, strings):
    """Yield (row_number, [cell_text...]) for one sheet."""
    root = ElementTree.fromstring(zf.read(target))
    for row in root.iterfind(".//m:sheetData/m:row", NS):
        cells = {}
        for cell in row.findall("m:c", NS):
            ctype = cell.get("t")
            value = cell.find("m:v", NS)
            if ctype == "s" and value is not None:
                text = strings[int(value.text)]
            elif ctype == "inlineStr":
                node = cell.find("m:is/m:t", NS)
                text = node.text if node is not None else ""
            elif value is not None:
                text = value.text or ""
            else:
                text = ""
            cells[col_index(cell.get("r"))] = text.strip()
        if not cells:
            continue
        width = max(cells) + 1
        yield int(row.get("r")), [cells.get(i, "") for i in range(width)]


def main():
    src = Path(sys.argv[1])
    out_dir = Path(sys.argv[2])
    out_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(src) as zf:
        strings = shared_strings(zf)
        for index, name in enumerate(sheet_names(zf), start=1):
            safe = "".join(ch if ch.isalnum() else "_" for ch in name)
            path = out_dir / f"{index:02d}-{safe}.txt"
            with path.open("w", encoding="utf-8") as fh:
                for number, row in dump_sheet(zf, f"xl/worksheets/sheet{index}.xml", strings):
                    cells = [c for c in row if c]
                    if not cells:
                        continue
                    fh.write(f"r{number}\t" + "\t|\t".join(cells) + "\n")
            print(f"{src.name} -> {path} ({name})")


if __name__ == "__main__":
    main()
