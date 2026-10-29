"""Create, inspect, update, check, and render editable project diagrams."""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
import tempfile
from urllib.parse import unquote
import xml.etree.ElementTree as ET
import zlib

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]
TEMP = ROOT / "misc/tmp/diagrams"
THEME = json.loads((HERE / "theme.json").read_text(encoding="utf-8"))
CANVAS = THEME["canvas"]


def digest(data):
    return hashlib.sha256(data).hexdigest()


def xml_parse(data):
    return ET.fromstring(data, parser=ET.XMLParser(target=ET.TreeBuilder(insert_comments=True)))


def load(data):
    book = xml_parse(data)
    if book.tag != "mxfile" or not book.findall("diagram"):
        raise ValueError("Expected an mxfile with at least one diagram page.")
    for page in book.findall("diagram"):
        if page.find("mxGraphModel") is None:
            if not page.text or not page.text.strip():
                raise ValueError("A diagram page has no model.")
            encoded = zlib.decompress(base64.b64decode(page.text.strip()), -15).decode("utf-8")
            page.append(xml_parse(unquote(encoded)))
            page.text = None
    return book


def encode(book):
    return ET.tostring(book, encoding="utf-8", xml_declaration=True)


def cells(page):
    result = {}
    root = page.find("mxGraphModel/root")
    if root is None:
        raise ValueError("A diagram page has no model root.")
    for item in root:
        cell = item if item.tag == "mxCell" else item.find("mxCell")
        if cell is None:
            continue
        ident = item.get("id", cell.get("id"))
        if not ident or ident in result:
            raise ValueError(f"Missing or repeated cell ID: {ident!r}.")
        result[ident] = (item, cell)
    return result


def label(item, cell):
    return item.get("label", cell.get("value", ""))


def style_values(style):
    return dict(part.split("=", 1) for part in style.split(";") if "=" in part)


def style_update(style, values):
    # Keep named styles and unrecognized tokens when updating known properties.
    parts = [p for p in style.split(";") if p and p.split("=", 1)[0] not in values]
    parts.extend(f"{key}={value}" for key, value in values.items() if value is not None)
    return ";".join(parts) + ";"


def style(role="component", kind="node"):
    colors = THEME["roles"][role]
    values = dict(html=0, whiteSpace="wrap", rounded=0, fontFamily=THEME["font"],
                  fontSize=18, fontColor=THEME["text"], strokeColor=colors["stroke"],
                  fillColor=colors["fill"], strokeWidth=1, spacing=12)
    if kind == "text":
        values.update(fillColor="none", strokeColor="none", align="left", spacing=0)
    if kind == "group":
        values.update(fillColor="none", dashed=0, verticalAlign="top", align="left", fontStyle=1,
                      fontSize=20, strokeWidth=1.5, spacingLeft=16, spacingTop=10,
                      fontColor=THEME["text"] if role == "note" else colors["stroke"])
    return style_update("", values)


def add_cell(root, ident, text, box, styling, parent="1", **attrs):
    cell = ET.SubElement(root, "mxCell", id=ident, value=text, parent=parent,
                         vertex="1", style=styling, **attrs)
    ET.SubElement(cell, "mxGeometry", dict(zip(("x", "y", "width", "height"), map(str, box))), **{"as": "geometry"})
    return cell


def allowed(obj, names, context):
    extra = set(obj) - set(names.split())
    if extra:
        raise ValueError(f"Unknown {context} properties: {sorted(extra)}.")


def create(spec):
    allowed(spec, "version pages", "specification")
    if spec.get("version") != 1 or not spec.get("pages"):
        raise ValueError("A specification needs version 1 and one or more pages.")
    book = ET.Element("mxfile", host="app.diagrams.net", type="device")
    for page_spec in spec["pages"]:
        allowed(page_spec, "id title purpose profile canvas layers nodes edges", "page")
        profile = page_spec.get("profile", "standard")
        width, height = page_spec.get("canvas", [CANVAS["width"], CANVAS["height"]])
        page = ET.SubElement(book, "diagram", id=page_spec["id"], name=page_spec["title"])
        model = ET.SubElement(page, "mxGraphModel", page="1", pageScale="1", grid="1", gridSize="8",
                              pageWidth=str(width), pageHeight=str(height), projectProfile=profile,
                              background="#FFFFFF", projectTheme="1", purpose=page_spec.get("purpose", ""))
        root = ET.SubElement(model, "root")
        ET.SubElement(root, "mxCell", id="0")
        ET.SubElement(root, "mxCell", id="1", parent="0")
        for layer in page_spec.get("layers", []):
            allowed(layer, "id label", "layer")
            ET.SubElement(root, "mxCell", id=layer["id"], value=layer["label"], parent="0")
        add_cell(root, "title", page_spec["title"], [32, 24, width - 64, 48],
                 style_update(style(kind="text"), {"fontSize": 28, "fontStyle": 1}))
        for index, node in enumerate(page_spec.get("nodes", [])):
            allowed(node, "id label kind role box parent fields style link semantic_id", "node")
            ident, kind = node["id"], node.get("kind", "node")
            if kind not in ("node", "data", "group", "text", "table"):
                raise ValueError(f"Unknown node kind: {kind}.")
            default_role = "data" if kind in ("data", "table") else "scope" if kind == "group" else "component"
            role = node.get("role", default_role)
            if role not in THEME["roles"]:
                raise ValueError(f"Unknown color role: {role}.")
            columns = max(1, int((width - 320) // 320) + 1)
            box = node.get("box", [40 + index % columns * 320, 152 + index // columns * 240, 240, 112])
            if len(box) != 4:
                raise ValueError("A box needs x, y, width, and height.")
            if node.get("parent") and "box" not in node:
                raise ValueError("A child node needs a box relative to its parent.")
            fields = node.get("fields", [])
            if fields and kind != "table":
                raise ValueError("Only table nodes accept fields.")
            if kind == "table":
                box = [*box[:3], 44 + 32 * len(fields)]
                cell = add_cell(root, ident, "", box, style_update(style(role), node.get("style", {})), node.get("parent", "1"))
                add_cell(root, ident + ".header", node["label"], [0, 0, box[2], 44],
                         style_update(style(role), {"fontSize": 20, "fontStyle": 1,
                                      "fillColor": THEME["roles"][role]["heading"],
                                      "fontColor": THEME["roles"][role].get("heading_text", "#FFFFFF")}), ident)
                for row, field in enumerate(fields):
                    allowed(field, "id label", "field")
                    add_cell(root, ident + "." + field["id"], field["label"], [0, 44 + row * 32, box[2], 32],
                             style_update(style(role), {"fontFamily": THEME["mono_font"], "fontSize": 16,
                                          "align": "left", "spacing": 8, "fillColor": "#FFFFFF", "strokeWidth": 0.5}), ident)
            else:
                styling = style(role, kind)
                if kind == "data":
                    styling = style_update(styling, {"shape": "document", "size": 0.12})
                cell = add_cell(root, ident, node["label"], box, style_update(styling, node.get("style", {})), node.get("parent", "1"))
            if "link" in node or "semantic_id" in node:
                root.remove(cell)
                wrapper = ET.SubElement(root, "object", id=ident, label=cell.get("value", ""))
                if "link" in node:
                    wrapper.set("link", node["link"])
                if "semantic_id" in node:
                    wrapper.set("semanticId", node["semantic_id"])
                del cell.attrib["id"]
                wrapper.append(cell)
        for edge in page_spec.get("edges", []):
            allowed(edge, "id source target label kind points source_end target_end style", "edge")
            kind = edge.get("kind", "flow")
            if kind not in ("flow", "control", "relationship"):
                raise ValueError(f"Unknown edge kind: {kind}.")
            styling = style_update("", dict(edgeStyle="orthogonalEdgeStyle", rounded=0, html=0,
                                   fontFamily=THEME["font"], fontSize=16, fontColor=THEME["text"],
                                   strokeColor=THEME["line"], strokeWidth=1.5, labelBackgroundColor="#FFFFFF",
                                   endArrow="none" if kind == "relationship" else "open", endFill=0,
                                   dashed=int(kind == "control")))
            if kind == "relationship":
                styling = style_update(styling, {"startArrow": edge.get("source_end", "ERone"),
                                                 "endArrow": edge.get("target_end", "ERmany")})
            cell = ET.SubElement(root, "mxCell", id=edge["id"], value=edge.get("label", ""), edge="1",
                                 parent="1", source=edge["source"], target=edge["target"],
                                 style=style_update(styling, edge.get("style", {})))
            geometry = ET.SubElement(cell, "mxGeometry", relative="1", **{"as": "geometry"})
            if edge.get("points"):
                points = ET.SubElement(geometry, "Array", **{"as": "points"})
                for x, y in edge["points"]:
                    ET.SubElement(points, "mxPoint", x=str(x), y=str(y))
    check(book)
    return book


def check(book):
    page_ids = set()
    for page in book.findall("diagram"):
        pid = page.get("id")
        if not pid or pid in page_ids:
            raise ValueError("Page IDs must be present and unique.")
        page_ids.add(pid)
        model = page.find("mxGraphModel")
        width, height = float(model.get("pageWidth", 0)), float(model.get("pageHeight", 0))
        profile = model.get("projectProfile", "standard")
        if profile not in ("standard", "master"):
            raise ValueError(f"{pid}: unsupported page profile {profile}.")
        if not all(map(math.isfinite, (width, height))) or min(width, height) <= 0:
            raise ValueError(f"{pid}: invalid canvas dimensions.")
        if profile == "standard" and (width, height) != (CANVAS["width"], CANVAS["height"]):
            raise ValueError(f"{pid}: expected a 960 x 720 drawing canvas.")
        if float(model.get("pageScale", 1)) != 1 or model.get("page") != "1":
            raise ValueError(f"{pid}: page mode must be enabled at scale 1.")
        entries = cells(page)
        if "0" not in entries or "1" not in entries or entries["1"][1].get("parent") != "0":
            raise ValueError(f"{pid}: expected a root and a default layer.")
        boxes = {}

        def bounds(ident, trail=()):
            if ident in trail:
                raise ValueError(f"{pid}: parent cycle at {ident}.")
            if ident in boxes:
                return boxes[ident]
            item, cell = entries[ident]
            parent = cell.get("parent")
            if parent and parent not in entries:
                raise ValueError(f"{pid}/{ident}: missing parent {parent}.")
            px, py, pw, ph = bounds(parent, (*trail, ident)) if parent else (0, 0, width, height)
            geometry = cell.find("mxGeometry")
            if cell.get("vertex") == "1":
                if geometry is None:
                    raise ValueError(f"{pid}/{ident}: missing geometry.")
                x, y, w, h = (float(geometry.get(k, 0)) for k in ("x", "y", "width", "height"))
                if geometry.get("relative") == "1":
                    x, y = x * pw, y * ph
                    offset = geometry.find("mxPoint[@as='offset']")
                    if offset is not None:
                        x += float(offset.get("x", 0))
                        y += float(offset.get("y", 0))
                if not all(map(math.isfinite, (x, y, w, h))) or w <= 0 or h <= 0:
                    raise ValueError(f"{pid}/{ident}: invalid box.")
                result = (px + x, py + y, w, h)
                if result[0] < 0 or result[1] < 0 or result[0] + w > width or result[1] + h > height:
                    raise ValueError(f"{pid}/{ident}: box exceeds the canvas.")
            else:
                result = (px, py, pw, ph)
            boxes[ident] = result
            return result

        for ident, (item, cell) in entries.items():
            bounds(ident)
            size = float(style_values(cell.get("style", "")).get("fontSize", 18))
            if label(item, cell) and (not math.isfinite(size) or size < 16):
                raise ValueError(f"{pid}/{ident}: text must be at least 16 canvas pixels.")
            if re.search("[\u2013\u2014]", label(item, cell)):
                raise ValueError(f"{pid}/{ident}: replace the long dash in the label.")
            if cell.get("edge") == "1":
                for end in ("source", "target"):
                    target = cell.get(end)
                    if target not in entries or entries[target][1].get("vertex") != "1":
                        raise ValueError(f"{pid}/{ident}: {end} must refer to a vertex.")
                geometry = cell.find("mxGeometry")
                if geometry is None:
                    raise ValueError(f"{pid}/{ident}: missing edge geometry.")
                for point in geometry.findall("Array[@as='points']/mxPoint"):
                    x, y = float(point.get("x", 0)), float(point.get("y", 0))
                    px, py, _, _ = bounds(cell.get("parent", "1"))
                    if not (0 <= x + px <= width and 0 <= y + py <= height):
                        raise ValueError(f"{pid}/{ident}: route point exceeds the canvas.")
    return len(page_ids)


def inspect(data):
    book = load(data)
    result = {"source_sha256": digest(data), "pages": []}
    for page in book.findall("diagram"):
        model = page.find("mxGraphModel")
        info = {"id": page.get("id"), "title": page.get("name"), "model": dict(model.attrib), "cells": []}
        for ident, (item, cell) in cells(page).items():
            entry = {"id": ident, "label": label(item, cell), "attributes": dict(cell.attrib)}
            if item is not cell:
                entry["wrapper_attributes"] = dict(item.attrib)
            entry["native_xml"] = ET.tostring(item, encoding="unicode")
            info["cells"].append(entry)
        result["pages"].append(info)
    return result


def apply(data, patch):
    allowed(patch, "source_sha256 operations", "patch")
    if patch.get("source_sha256") != digest(data):
        raise ValueError("The diagram changed after inspection. Inspect it again before applying edits.")
    book = load(data)
    pages = {p.get("id"): p for p in book.findall("diagram")}
    for operation in patch.get("operations", []):
        allowed(operation, "op page id label style geometry attributes xml title profile canvas purpose", "operation")
        page = pages[operation["page"]]
        root = page.find("mxGraphModel/root")
        entries = cells(page)
        op = operation["op"]
        if op == "set-page":
            model = page.find("mxGraphModel")
            if "title" in operation:
                page.set("name", operation["title"])
            if "profile" in operation:
                model.set("projectProfile", operation["profile"])
            if "canvas" in operation:
                width, height = operation["canvas"]
                model.set("pageWidth", str(width))
                model.set("pageHeight", str(height))
                model.set("page", "1")
                model.set("pageScale", "1")
            if "purpose" in operation:
                model.set("purpose", operation["purpose"])
            continue
        if op == "add":
            item = xml_parse(operation["xml"])
            if item.tag != "mxCell" and item.find("mxCell") is None:
                raise ValueError("An added element must contain a native mxCell.")
            root.append(item)
            continue
        item, cell = entries[operation["id"]]
        if operation["id"] in ("0", "1"):
            raise ValueError("The root and default layer cannot be edited through this interface.")
        if op == "remove":
            root.remove(item)
        elif op == "set":
            if "label" in operation:
                if item is not cell and "label" in item.attrib:
                    item.set("label", operation["label"])
                else:
                    cell.set("value", operation["label"])
            if "style" in operation:
                cell.set("style", style_update(cell.get("style", ""), operation["style"]))
            for key, value in operation.get("attributes", {}).items():
                if key in ("id", "value", "style"):
                    raise ValueError(f"Use the dedicated field to update {key}; IDs are stable.")
                if value is None:
                    cell.attrib.pop(key, None)
                else:
                    cell.set(key, str(value))
            if "geometry" in operation:
                geometry = cell.find("mxGeometry")
                if geometry is None:
                    raise ValueError("The cell has no geometry to update.")
                for key, value in operation["geometry"].items():
                    if key == "points":
                        for old in geometry.findall("Array[@as='points']"):
                            geometry.remove(old)
                        if value:
                            points = ET.SubElement(geometry, "Array", **{"as": "points"})
                            for x, y in value:
                                ET.SubElement(points, "mxPoint", x=str(x), y=str(y))
                        continue
                    if key not in ("x", "y", "width", "height", "relative"):
                        raise ValueError(f"Unsupported geometry attribute: {key}.")
                    geometry.set(key, str(value))
        else:
            raise ValueError(f"Unknown operation: {op}.")
    check(book)
    return encode(book)


def write_atomic(path, data, expected=None, new=False):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    if new:
        with path.open("xb") as output:
            output.write(data)
        return
    with tempfile.NamedTemporaryFile(dir=path.parent, prefix=".diagram-", delete=False) as output:
        temporary = Path(output.name)
        output.write(data)
    try:
        if expected is not None and digest(path.read_bytes()) != expected:
            raise ValueError("The diagram changed before saving. Inspect it again.")
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def export(source, output, page_index):
    source, output = Path(source).resolve(), Path(output).resolve()
    output_format = output.suffix.lower().removeprefix(".")
    if output_format not in ("png", "svg") or source == output:
        raise ValueError("The export destination must be a separate PNG or SVG file.")
    data = source.read_bytes()
    book = load(data)
    count = check(book)
    if not 1 <= page_index <= count:
        raise ValueError(f"Page index must be between 1 and {count}.")
    model = book.findall("diagram")[page_index - 1].find("mxGraphModel")
    if model.get("projectProfile") == "master" and output_format != "svg":
        raise ValueError("Export a large master as SVG. Use a standard view for the Markdown PNG.")
    executable = shutil.which("drawio")
    if not executable:
        raise ValueError("Install draw.io Desktop and put drawio on PATH.")
    TEMP.mkdir(parents=True, exist_ok=True)
    work = Path(tempfile.mkdtemp(prefix="export-", dir=TEMP))
    snapshot, preview = work / "source.drawio", work / ("preview." + output_format)
    snapshot.write_bytes(data)
    command = [executable, "--export", "--format", output_format, "--size", "page", "--theme", "light",
               "--scale", str(CANVAS["scale"] if output_format == "png" else 1),
               "--page-index", str(page_index), "--output", str(preview), str(snapshot)]
    if output_format == "svg":
        command.extend(["--embed-svg-images", "--embed-svg-fonts", "true"])
    result = subprocess.run(command, capture_output=True, text=True, timeout=60)
    (work / "export.log").write_text(result.stdout + result.stderr, encoding="utf-8")
    if result.returncode or not preview.exists():
        raise ValueError(f"draw.io export failed. Read {work / 'export.log'}.")
    expected = None
    if output_format == "png":
        expected = (CANVAS["width"] * CANVAS["scale"], CANVAS["height"] * CANVAS["scale"])
        normalize_png(preview, expected, work / "reading-preview.png")
    else:
        normalize_svg(preview)
    rendered = preview.read_bytes()
    if digest(source.read_bytes()) != digest(data):
        raise ValueError("The source changed during export. Run the export again.")
    write_atomic(output, rendered)
    (work / "render.json").write_text(json.dumps({"source": str(source), "source_sha256": digest(data),
        "render_sha256": digest(rendered), "page": page_index, "size": expected,
        "theme_sha256": digest((HERE / "theme.json").read_bytes()), "command": command}, indent=2) + "\n", encoding="utf-8")
    return work


def normalize_svg(path):
    svg = "{http://www.w3.org/2000/svg}"
    xlink = "{http://www.w3.org/1999/xlink}"
    root = xml_parse(path.read_bytes())
    if root.tag != svg + "svg":
        raise ValueError(f"Invalid SVG: {path}.")
    parents = {child: parent for parent in root.iter() for child in parent}
    for rich_text in root.iter(svg + "foreignObject"):
        parent = parents[rich_text]
        fallback = parent.find(svg + "image") if parent.tag == svg + "switch" else None
        if fallback is None or not fallback.get(xlink + "href", fallback.get("href", "")).startswith("data:image/"):
            return
    # Draw.io adds this footer even when every rich-text label has an embedded fallback.
    # Remove only that known warning; preserve it when a renderer could lose label content.
    removed = False
    for node in root.findall(svg + "switch"):
        link = node.find(svg + "a")
        if (link is not None and link.get(xlink + "href") == "https://www.drawio.com/doc/faq/svg-export-text-problems"
                and "".join(link.itertext()) == "Text is not SVG - cannot display"):
            root.remove(node)
            removed = True
    if removed:
        ET.register_namespace("", svg[1:-1])
        ET.register_namespace("xlink", xlink[1:-1])
        path.write_bytes(encode(root))


def normalize_png(path, expected, reading_preview):
    try:
        from PIL import Image, ImageChops
    except ImportError as error:
        raise ValueError("Install misc/tools/diagrams/requirements.txt before PNG export.") from error
    with Image.open(path) as original:
        rendered = original.convert("RGB")
    if rendered.size != expected:
        # Desktop 32.6.1 adds one logical pixel per axis to full-page exports.
        # Remove only its verified white border; never resize diagram content.
        if rendered.size != (expected[0] + 2, expected[1] + 2):
            raise ValueError(f"Unexpected PNG dimensions {rendered.size}; expected {expected}.")
        bands = [rendered.crop((expected[0], 0, rendered.width, rendered.height)),
                 rendered.crop((0, expected[1], rendered.width, rendered.height))]
        if any(ImageChops.difference(band, Image.new("RGB", band.size, "white")).getbbox() for band in bands):
            raise ValueError("The extra export border contains content. Inspect the layout before cropping.")
        rendered = rendered.crop((0, 0, *expected))
    rendered.save(path)
    rendered.resize((CANVAS["width"], CANVAS["height"]), Image.Resampling.LANCZOS).save(reading_preview)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    creation = commands.add_parser("create", help="Create a new draw.io file from a JSON specification.")
    creation.add_argument("spec", type=Path)
    creation.add_argument("output", type=Path)
    for name in ("inspect", "check", "apply", "export"):
        sub = commands.add_parser(name)
        sub.add_argument("source", type=Path)
        if name == "apply":
            sub.add_argument("patch", type=Path)
        if name in ("inspect", "export"):
            sub.add_argument("--output", type=Path)
        if name == "export":
            sub.add_argument("--page", type=int, default=1)
    args = parser.parse_args()
    try:
        if args.command == "create":
            if args.output.suffix != ".drawio":
                raise ValueError("The destination must have the .drawio extension.")
            book = create(json.loads(args.spec.read_text(encoding="utf-8")))
            write_atomic(args.output, encode(book), new=True)
            print(f"Created {args.output}.")
        elif args.command == "inspect":
            if args.output and args.output.resolve() == args.source.resolve():
                raise ValueError("Inspection output must not replace the diagram.")
            report = json.dumps(inspect(args.source.read_bytes()), ensure_ascii=False, indent=2) + "\n"
            if args.output:
                write_atomic(args.output, report.encode("utf-8"))
            else:
                print(report, end="")
        elif args.command == "check":
            count = check(load(args.source.read_bytes()))
            print(f"Checked {count} page(s). Rendered inspection is still required.")
        elif args.command == "apply":
            before = args.source.read_bytes()
            after = apply(before, json.loads(args.patch.read_text(encoding="utf-8")))
            TEMP.mkdir(parents=True, exist_ok=True)
            backup = TEMP / (args.source.stem + "-" + digest(before) + ".drawio")
            backup.write_bytes(before)
            write_atomic(args.source, after, expected=digest(before))
            print(f"Updated {args.source}. Previous bytes: {backup}.")
        elif args.command == "export":
            output = args.output or args.source.with_suffix(".png")
            work = export(args.source, output, args.page)
            print(f"Exported {output}. Render evidence: {work}.")
    except (ValueError, KeyError, TypeError, OSError, ET.ParseError, zlib.error, subprocess.TimeoutExpired) as error:
        parser.exit(1, f"Error: {error}\n")


if __name__ == "__main__":
    main()
