"""Test draw.io updates, geometry, theme contrast, and PNG size handling."""
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
import erd
from PIL import Image


def specification():
    """Return a fresh two-node flow so each test can change its own specification."""
    return {"version": 1, "pages": [{"id": "flow", "title": "Example flow", "nodes": [
        {"id": "input", "label": "Input records", "kind": "data"},
        {"id": "job", "label": "Validate records"}],
        "edges": [{"id": "transfer", "source": "input", "target": "job", "label": "Read"}]}]}


def dense_specification():
    """Build a large nested page with 80 services linked in one sequence."""
    page = {"id": "dense", "title": "Nested deployment fixture", "profile": "large", "canvas": [2240, 1780],
            "nodes": [{"id": "host", "kind": "group", "label": "Test host", "box": [64, 112, 2112, 1600]}], "edges": []}
    previous = None
    # Four columns and two rows keep the scopes inside the large canvas. Connect
    # every service in sequence to exercise references across nested parents.
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
    """Check document preservation, invalid edits, and export image rules."""

    def test_theme_text_contrast(self):
        """Require the guide's 4.5:1 text contrast for each theme role."""
        def luminance(color):
            """Convert a hexadecimal sRGB color to relative luminance."""
            values = [int(color[i:i + 2], 16) / 255 for i in (1, 3, 5)]
            # The contrast formula uses linear light values, not encoded sRGB.
            # Convert each channel before weighting its contribution to luminance.
            linear = [v / 12.92 if v <= 0.04045 else ((v + 0.055) / 1.055) ** 2.4 for v in values]
            return sum(v * weight for v, weight in zip(linear, (0.2126, 0.7152, 0.0722)))

        for role, colors in diagram.THEME["roles"].items():
            group_text = diagram.THEME["text"] if role == "note" else colors["stroke"]
            pairs = [(diagram.THEME["text"], colors["fill"]),
                     (colors["heading_text"], colors["heading"]),
                     (group_text, colors["fill"]), (group_text, "#FFFFFF")]
            if role in {r for k, r in erd.KINDS.items() if k not in erd.QUIET_KINDS}:
                pairs.append(("#FFFFFF", colors["stroke"]))  # Solid ERD header with white table name.
            for foreground, background in pairs:
                with self.subTest(role=role, foreground=foreground, background=background):
                    low, high = sorted((luminance(foreground), luminance(background)))
                    self.assertGreaterEqual((high + 0.05) / (low + 0.05), 4.5)

    def setUp(self):
        """Create an isolated workspace and a valid diagram for each test."""
        diagram.TEMP.mkdir(parents=True, exist_ok=True)
        self.workspace = tempfile.TemporaryDirectory(dir=diagram.TEMP, prefix="test-")
        self.addCleanup(self.workspace.cleanup)
        self.directory = Path(self.workspace.name)
        self.data = diagram.encode(diagram.create(specification()))

    def patch(self, operations, data=None):
        """Apply operations using the hash of the selected diagram bytes."""
        data = self.data if data is None else data
        return diagram.apply(data, {"source_sha256": diagram.digest(data), "operations": operations})

    def test_compressed_pages_wrappers_and_native_details_survive_updates(self):
        """Keep wrappers, native metadata, comments, and untouched pages during a patch."""
        book = diagram.load(self.data)
        page = book.find("diagram")
        root = page.find("mxGraphModel/root")
        item, cell = diagram.cells(page)["input"]
        root.remove(item)
        # Native editors can put IDs and metadata on wrappers instead of cells.
        # Add details outside the patch so the assertions can detect accidental loss.
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
        # Match draw.io's URL encoding, raw DEFLATE, and base64 page format.
        # Only the first page is compressed and edited; the second must stay intact.
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
        """Reject a patch when even a trailing newline changes the inspected bytes."""
        patch = {"source_sha256": diagram.digest(self.data), "operations": []}
        with self.assertRaisesRegex(ValueError, "changed after inspection"):
            diagram.apply(self.data + b"\n", patch)

    def test_invalid_changes_are_rejected(self):
        """Reject broken references, cycles, invalid boxes, small text, and duplicate IDs."""
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
        """Allow node removal when the same patch also removes its dependent edge."""
        result = self.patch([{"op": "remove", "page": "flow", "id": "transfer"},
                             {"op": "remove", "page": "flow", "id": "input"}])
        self.assertNotIn("input", diagram.cells(diagram.load(result).find("diagram")))

    def test_container_relative_positions_and_erd_only_shapes(self):
        """Check child positions against the canvas and reject ERD-only element kinds."""
        spec = {"version": 1, "pages": [{"id": "model", "title": "Example scope", "nodes": [
            {"id": "group", "kind": "group", "label": "Records", "box": [32, 112, 896, 520]},
            {"id": "job", "label": "Validate records", "parent": "group", "box": [24, 56, 240, 112]},
            {"id": "store", "kind": "data", "label": "Rejected rows", "parent": "group", "box": [544, 56, 240, 112]}
        ], "edges": [{"id": "reject", "source": "job", "target": "store", "label": "Write"}]}]}
        self.assertEqual(diagram.check(diagram.create(spec)), 1)
        # The parent's x offset makes the child's right edge exceed the canvas,
        # even though the child's own x and width would fit without that offset.
        spec["pages"][0]["nodes"][2]["box"] = [700, 56, 240, 112]
        with self.assertRaisesRegex(ValueError, "exceeds the canvas"):
            diagram.create(spec)
        table = copy.deepcopy(specification())
        table["pages"][0]["nodes"][0]["kind"] = "table"
        relationship = copy.deepcopy(specification())
        relationship["pages"][0]["edges"][0]["kind"] = "relationship"
        for spec in (table, relationship):
            with self.subTest(spec=spec), self.assertRaisesRegex(ValueError, "ERD"):
                diagram.create(spec)

    def test_cli_refuses_to_replace_existing_drawio(self):
        """Keep an existing source unchanged when CLI creation targets the same path."""
        spec = self.directory / "input.json"
        spec.write_text(json.dumps(specification()), encoding="utf-8")
        destination = self.directory / "existing.drawio"
        destination.write_bytes(self.data)
        result = subprocess.run([sys.executable, str(diagram.HERE / "diagram.py"), "create", str(spec), str(destination)],
                                capture_output=True, text=True)
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(destination.read_bytes(), self.data)

    def test_stale_save_keeps_current_file(self):
        """Leave newer file bytes intact when the final save hash does not match."""
        path = self.directory / "file.drawio"
        path.write_bytes(self.data + b"\n")
        with self.assertRaisesRegex(ValueError, "changed before saving"):
            diagram.write_atomic(path, b"replacement", expected=diagram.digest(self.data))
        self.assertEqual(path.read_bytes(), self.data + b"\n")

    def test_unknown_spec_property_is_rejected(self):
        """Report unknown node properties instead of ignoring them."""
        spec = specification()
        spec["pages"][0]["nodes"][0]["colour"] = "red"
        with self.assertRaisesRegex(ValueError, "Unknown node properties"):
            diagram.create(spec)

    def test_dense_large_page_and_small_page_rules(self):
        """Preserve shared identity in a dense page and reject an incompatible profile."""
        data = diagram.encode(diagram.create(dense_specification()))
        snapshot = diagram.inspect(data)
        # Two native roots, one title, nine scopes, 80 services, and 79 edges.
        self.assertEqual(len(snapshot["pages"][0]["cells"]), 171)
        result = self.patch([{"op": "set", "page": "dense", "id": "service-7-9", "label": "Updated service"}], data)
        item, cell = diagram.cells(diagram.load(result).find("diagram"))["service-7-9"]
        self.assertEqual(item.get("semanticId"), "service-7-9")
        self.assertEqual(diagram.label(item, cell), "Updated service")
        with self.assertRaisesRegex(ValueError, "960 x 720"):
            self.patch([{"op": "set-page", "page": "dense", "profile": "small"}], data)

    def test_size_profiles(self):
        """Check fixed canvas sizes and require an explicit larger canvas for large pages."""
        medium = specification()
        medium["pages"][0]["profile"] = "medium"
        model = diagram.create(medium).find("diagram/mxGraphModel")
        self.assertEqual((model.get("pageWidth"), model.get("pageHeight")), ("1440", "1080"))
        cases = [({"profile": "standard"}, "unsupported page profile"),
                 ({"profile": "medium"}, "1440 x 1080"),
                 ({"profile": "large", "canvas": [960, 720]}, "larger than a small page")]
        for change, message in cases:
            with self.subTest(change=change), self.assertRaisesRegex(ValueError, message):
                self.patch([{"op": "set-page", "page": "flow", **change}])
        large = specification()
        large["pages"][0]["profile"] = "large"
        with self.assertRaisesRegex(ValueError, "explicit canvas"):
            diagram.create(large)

    def test_png_placement_centers_render_on_size_canvas(self):
        """Center a render without scaling it and reject an image that cannot fit."""
        path, preview = self.directory / "render.png", self.directory / "preview.png"
        Image.new("RGB", (100, 50), "black").save(path)
        diagram.place_png(path, (2880, 2160), preview)
        with Image.open(path) as result:
            self.assertEqual(result.size, (2880, 2160))
            self.assertEqual(result.getpixel((1440, 1080)), (0, 0, 0))
            self.assertEqual(result.getpixel((0, 0)), (255, 255, 255))
        with Image.open(preview) as result:
            self.assertEqual(result.size, (960, 720))
        Image.new("RGB", (2000, 1500), "black").save(path)
        with self.assertRaisesRegex(ValueError, "does not fit"):
            diagram.place_png(path, (1920, 1440), preview)

    def test_export_normalization_does_not_resize_or_remove_content(self):
        """Crop the known blank border but reject that border when it contains content."""
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
        # Place a mark in the extra bottom-right area. Cropping it would lose
        # content, so the same dimensions must now fail normalization.
        image.putpixel((1921, 1441), (0, 0, 0))
        image.save(path)
        with self.assertRaisesRegex(ValueError, "contains content"):
            diagram.normalize_png(path, (1920, 1440), preview)

    def test_route_update_preserves_other_geometry(self):
        """Change explicit route points without removing the edge's relative geometry."""
        after = self.patch([{"op": "set", "page": "flow", "id": "transfer", "geometry": {"points": [[300, 250]]}}])
        geometry = diagram.cells(diagram.load(after).find("diagram"))["transfer"][1].find("mxGeometry")
        self.assertEqual(geometry.get("relative"), "1")
        self.assertEqual(geometry.find("Array/mxPoint").get("x"), "300")


if __name__ == "__main__":
    unittest.main()
