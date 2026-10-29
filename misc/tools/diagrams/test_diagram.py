"""Exercise file preservation, stale updates, and invalid diagram rejection."""
import base64
import copy
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from urllib.parse import quote
import xml.etree.ElementTree as ET
import zlib

import diagram
from PIL import Image


def specification():
    return {"version": 1, "pages": [{"id": "flow", "title": "Example flow", "nodes": [
        {"id": "input", "label": "Input records", "kind": "data"},
        {"id": "job", "label": "Validate records"}],
        "edges": [{"id": "transfer", "source": "input", "target": "job", "label": "Read"}]}]}


def dense_specification():
    page = {"id": "dense", "title": "Nested deployment fixture", "profile": "master", "canvas": [2240, 1780],
            "nodes": [{"id": "host", "kind": "group", "label": "Test host", "box": [64, 112, 2112, 1600]}], "edges": []}
    previous = None
    for group in range(8):
        parent = f"scope-{group}"
        page["nodes"].append({"id": parent, "kind": "group", "label": f"Test scope {group + 1}", "parent": "host",
                              "box": [32 + group % 4 * 512, 64 + group // 4 * 720, 480, 640]})
        for row in range(10):
            ident = f"service-{group}-{row}"
            page["nodes"].append({"id": ident, "label": f"Test service {group * 10 + row + 1}", "parent": parent,
                                  "semantic_id": ident, "box": [24, 56 + row * 52, 432, 40]})
            if previous:
                page["edges"].append({"id": "edge-" + ident, "source": previous, "target": ident})
            previous = ident
    return {"version": 1, "pages": [page]}


class DiagramTest(unittest.TestCase):
    def test_theme_text_contrast(self):
        def luminance(color):
            values = [int(color[i:i + 2], 16) / 255 for i in (1, 3, 5)]
            linear = [v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4 for v in values]
            return sum(v * weight for v, weight in zip(linear, (0.2126, 0.7152, 0.0722)))

        for role, colors in diagram.THEME["roles"].items():
            group_text = diagram.THEME["text"] if role == "note" else colors["stroke"]
            pairs = [(diagram.THEME["text"], colors["fill"]),
                     (colors["heading_text"], colors["heading"]),
                     (group_text, colors["fill"]), (group_text, "#FFFFFF")]
            for foreground, background in pairs:
                with self.subTest(role=role, foreground=foreground, background=background):
                    low, high = sorted((luminance(foreground), luminance(background)))
                    self.assertGreaterEqual((high + 0.05) / (low + 0.05), 4.5)

    def setUp(self):
        diagram.TEMP.mkdir(parents=True, exist_ok=True)
        self.workspace = tempfile.TemporaryDirectory(dir=diagram.TEMP, prefix="test-")
        self.addCleanup(self.workspace.cleanup)
        self.directory = Path(self.workspace.name)
        self.data = diagram.encode(diagram.create(specification()))

    def patch(self, operations, data=None):
        data = self.data if data is None else data
        return diagram.apply(data, {"source_sha256": diagram.digest(data), "operations": operations})

    def test_compressed_pages_wrappers_and_native_details_survive_updates(self):
        book = diagram.load(self.data)
        page = book.find("diagram")
        root = page.find("mxGraphModel/root")
        item, cell = diagram.cells(page)["input"]
        root.remove(item)
        wrapper = ET.SubElement(root, "object", id="input", label="Input records", custom="preserve me")
        del cell.attrib["id"]
        wrapper.append(cell)
        cell.set("style", "nativeStyle;" + cell.get("style") + "vendorFlag=keep;")
        ET.SubElement(cell, "customData", meaning="retained").text = "native metadata"
        cell.find("mxGeometry").append(ET.Element("mxPoint", {"as": "offset", "x": "2", "y": "3"}))
        root.append(ET.Comment("Keep the source note."))
        second = copy.deepcopy(page)
        second.set("id", "second")
        book.append(second)
        original_second = ET.tostring(second)
        model = page.find("mxGraphModel")
        compressor = zlib.compressobj(wbits=-15)
        encoded = quote(ET.tostring(model, encoding="unicode"), safe="").encode()
        page.remove(model)
        page.text = base64.b64encode(compressor.compress(encoded) + compressor.flush()).decode()
        data = diagram.encode(book)
        after = diagram.load(self.patch([{"op": "set", "page": "flow", "id": "input",
            "label": "Source records", "style": {"fillColor": "#FFFFFF"}, "geometry": {"x": 48}}], data))
        item, cell = diagram.cells(after.find("diagram"))["input"]
        self.assertEqual(item.get("label"), "Source records")
        self.assertEqual(item.get("custom"), "preserve me")
        self.assertIn("nativeStyle;", cell.get("style"))
        self.assertIn("vendorFlag=keep;", cell.get("style"))
        self.assertEqual(cell.find("customData").text, "native metadata")
        self.assertEqual(cell.find("mxGeometry/mxPoint").get("x"), "2")
        self.assertEqual(ET.tostring(after.findall("diagram")[1]), original_second)
        self.assertIn(b"<!--Keep the source note.-->", diagram.encode(after))

    def test_stale_patch_is_rejected(self):
        patch = {"source_sha256": diagram.digest(self.data), "operations": []}
        with self.assertRaisesRegex(ValueError, "changed after inspection"):
            diagram.apply(self.data + b"\n", patch)

    def test_svg_warning_is_removed_only_when_text_has_embedded_fallbacks(self):
        for embedded in (True, False):
            with self.subTest(embedded=embedded):
                fallback = '<image xlink:href="data:image/png;base64,iVBORw0KGgo="/>' if embedded else ''
                data = ('<svg xmlns="http://www.w3.org/2000/svg" xmlns:xlink="http://www.w3.org/1999/xlink">'
                        '<switch><foreignObject><div xmlns="http://www.w3.org/1999/xhtml">A label</div></foreignObject>'
                        + fallback + '</switch><switch><g requiredFeatures="unsupported"/><a '
                        'xlink:href="https://www.drawio.com/doc/faq/svg-export-text-problems">'
                        '<text>Text is not SVG - cannot display</text></a></switch></svg>')
                path = self.directory / 'export.svg'
                path.write_text(data, encoding='utf-8')
                diagram.normalize_svg(path)
                result = diagram.xml_parse(path.read_bytes())
                self.assertEqual('Text is not SVG - cannot display' in ''.join(result.itertext()), not embedded)
                self.assertIn('A label', ''.join(result.itertext()))
                self.assertEqual(len(result.findall('.//{http://www.w3.org/2000/svg}foreignObject')), 1)

    def test_invalid_changes_are_rejected(self):
        cases = [
            {"op": "remove", "page": "flow", "id": "input"},
            {"op": "set", "page": "flow", "id": "input", "attributes": {"parent": "input"}},
            {"op": "set", "page": "flow", "id": "input", "geometry": {"x": 950}},
            {"op": "set", "page": "flow", "id": "input", "geometry": {"x": float("nan")}},
            {"op": "set", "page": "flow", "id": "input", "style": {"fontSize": 10}},
            {"op": "add", "page": "flow", "xml": '<mxCell id="input" parent="1"/>'},
        ]
        for operation in cases:
            with self.subTest(operation=operation), self.assertRaises(ValueError):
                self.patch([operation])

    def test_remove_edge_then_node(self):
        result = self.patch([{"op": "remove", "page": "flow", "id": "transfer"},
                             {"op": "remove", "page": "flow", "id": "input"}])
        self.assertNotIn("input", diagram.cells(diagram.load(result).find("diagram")))

    def test_field_connections_and_container_relative_positions(self):
        spec = {"version": 1, "pages": [{"id": "model", "title": "Example model", "nodes": [
            {"id": "group", "kind": "group", "label": "Records", "box": [32, 112, 896, 520]},
            {"id": "customer", "kind": "table", "label": "Customer", "parent": "group", "box": [24, 56, 300, 0],
             "fields": [{"id": "key", "label": "PK  CustomerID : INT"}]},
            {"id": "account", "kind": "table", "label": "Account", "parent": "group", "box": [544, 56, 300, 0],
             "fields": [{"id": "customer", "label": "FK  CustomerID : INT"}]}
        ], "edges": [{"id": "owns", "kind": "relationship", "source": "customer.key", "target": "account.customer"}]}]}
        self.assertEqual(diagram.check(diagram.create(spec)), 1)
        spec["pages"][0]["nodes"][1]["fields"][0]["id"] = "header"
        with self.assertRaisesRegex(ValueError, "repeated cell ID"):
            diagram.create(spec)

    def test_cli_refuses_to_replace_existing_drawio(self):
        spec = self.directory / "input.json"
        spec.write_text(json.dumps(specification()), encoding="utf-8")
        destination = self.directory / "existing.drawio"
        destination.write_bytes(self.data)
        result = subprocess.run([sys.executable, str(diagram.HERE / "diagram.py"), "create", str(spec), str(destination)],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(destination.read_bytes(), self.data)

    def test_stale_save_keeps_current_file(self):
        path = self.directory / "file.drawio"
        path.write_bytes(self.data + b"\n")
        with self.assertRaisesRegex(ValueError, "changed before saving"):
            diagram.write_atomic(path, b"replacement", expected=diagram.digest(self.data))
        self.assertEqual(path.read_bytes(), self.data + b"\n")

    def test_unknown_spec_property_is_rejected(self):
        spec = specification()
        spec["pages"][0]["nodes"][0]["colour"] = "red"
        with self.assertRaisesRegex(ValueError, "Unknown node properties"):
            diagram.create(spec)

    def test_dense_master_and_standard_page_rules(self):
        data = diagram.encode(diagram.create(dense_specification()))
        snapshot = diagram.inspect(data)
        self.assertEqual(len(snapshot["pages"][0]["cells"]), 171)
        result = self.patch([{"op": "set", "page": "dense", "id": "service-7-9", "label": "Updated service"}], data)
        item, cell = diagram.cells(diagram.load(result).find("diagram"))["service-7-9"]
        self.assertEqual(item.get("semanticId"), "service-7-9")
        self.assertEqual(diagram.label(item, cell), "Updated service")
        with self.assertRaisesRegex(ValueError, "960 x 720"):
            self.patch([{"op": "set-page", "page": "dense", "profile": "standard"}], data)

    def test_export_normalization_does_not_resize_or_remove_content(self):
        path, preview = self.directory / "raw.png", self.directory / "preview.png"
        image = Image.new("RGB", (1922, 1442), "white")
        image.putpixel((500, 500), (0, 0, 0))
        image.save(path)
        diagram.normalize_png(path, (1920, 1440), preview)
        with Image.open(path) as result:
            self.assertEqual(result.size, (1920, 1440))
            self.assertEqual(result.getpixel((500, 500)), (0, 0, 0))
        with Image.open(preview) as result:
            self.assertEqual(result.size, (960, 720))
        image.putpixel((1921, 1441), (0, 0, 0))
        image.save(path)
        with self.assertRaisesRegex(ValueError, "contains content"):
            diagram.normalize_png(path, (1920, 1440), preview)

    def test_route_update_preserves_other_geometry(self):
        after = self.patch([{"op": "set", "page": "flow", "id": "transfer", "geometry": {"points": [[300, 250]]}}])
        geometry = diagram.cells(diagram.load(after).find("diagram"))["transfer"][1].find("mxGeometry")
        self.assertEqual(geometry.get("relative"), "1")
        self.assertEqual(geometry.find("Array/mxPoint").get("x"), "300")


if __name__ == "__main__":
    unittest.main()
