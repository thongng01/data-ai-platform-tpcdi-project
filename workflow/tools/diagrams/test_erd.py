"""Exercise ERD source checks, kind tags, size classes, theme mapping, and rendering."""
from pathlib import Path
import shutil
import tempfile
import unittest

import diagram
import erd

SOURCE = """erDiagram
    direction LR
    Customer ||--o{ Account : owns
    Customer:::entity {
        NUM(11) SK_CustomerID PK
    }
    Account:::event {
        NUM(11) SK_AccountID PK
        NUM(11) SK_CustomerID FK
    }
"""


class ErdTest(unittest.TestCase):
    """Check ERD source rules, theme mapping, sizing, and optional rendering."""

    def setUp(self):
        """Create a temporary directory and register cleanup for each test."""
        diagram.TEMP.mkdir(parents=True, exist_ok=True)
        self.workspace = tempfile.TemporaryDirectory(dir=diagram.TEMP, prefix="test-")
        self.addCleanup(self.workspace.cleanup)
        self.directory = Path(self.workspace.name)

    def test_source_without_styling_is_accepted(self):
        """Accept tagged tables and source comments without user-supplied styling."""
        erd.check(SOURCE)
        erd.check("%% Gold zone\n" + SOURCE)
        self.assertEqual(erd.entity_kinds(SOURCE), {"Customer": "entity", "Account": "event"})

    def test_every_table_needs_one_known_kind(self):
        """Require one known kind for tables in definitions and relationships."""
        cases = {
            "untagged": (SOURCE.replace("Account:::event {", "Account {"), "Untagged: Account"),
            "untagged relationship": (SOURCE + "    Account ||--o{ Trade : places\n", "Untagged: Trade"),
            "unknown": (SOURCE.replace(":::event", ":::hot"), "Unknown kind in Account:::hot"),
            "two kinds": (SOURCE.replace("Customer ||", "Customer:::state ||"), "two kinds"),
            "unparsed line": (SOURCE + "    Account }o--|| Customer\n", "Unrecognized ERD line"),
        }
        for name, (text, message) in cases.items():
            with self.subTest(name), self.assertRaisesRegex(ValueError, message):
                erd.check(text)
        # A standalone tag can declare a table without a field block. Its later
        # untagged relationship reference must keep that declared kind.
        erd.check(SOURCE + "    Trade:::event\n    Account ||--o{ Trade : places\n")

    def test_styling_and_other_diagrams_are_rejected(self):
        """Reject source styling, dashed relationships, and non-ERD syntax."""
        cases = {
            "init directive": "%%{init: {'theme': 'dark'}}%%\n" + SOURCE,
            "front matter": "---\nconfig:\n  layout: dagre\n---\n" + SOURCE,
            "style statement": SOURCE + "    style Customer fill:#f00\n",
            "class definition": SOURCE + "    classDef hot fill:#f00\n",
            "dashed line": SOURCE.replace("||--o{", "||..o{"),
            "dashed alias": SOURCE.replace("||--o{", "only one optionally to zero or more"),
            "flowchart": "flowchart LR\n    A --> B\n",
        }
        for name, text in cases.items():
            with self.subTest(name), self.assertRaises(ValueError):
                erd.check(text)

    def test_size_class_includes_margin(self):
        """Keep exact margin boundaries in each size and promote one-pixel overflow."""
        sizes, margin = diagram.SIZES, diagram.CANVAS["margin"]
        # A 32-pixel margin on each side makes 896 x 656 exactly fill small.
        # Crossing either fixed canvas boundary must choose the next size.
        self.assertEqual(erd.size_class((896, 656), sizes, margin), "small")
        self.assertEqual(erd.size_class((897, 656), sizes, margin), "medium")
        self.assertEqual(erd.size_class((1376, 1016), sizes, margin), "medium")
        self.assertEqual(erd.size_class((1376, 1017), sizes, margin), "large")

    def test_config_uses_theme_values(self):
        """Read layout, fonts, and each kind's colors from the shared theme."""
        config = erd.config(diagram.THEME)
        variables = config["themeVariables"]
        data = diagram.THEME["roles"]["data"]
        self.assertEqual(config["layout"], "elk")
        self.assertEqual(variables["primaryBorderColor"], data["stroke"])
        self.assertEqual(variables["rowEven"], data["fill"])
        self.assertEqual(variables["lineColor"], diagram.THEME["line"])
        self.assertIn(diagram.THEME["mono_font"], config["themeCSS"])
        for kind, role in erd.KINDS.items():
            colors = diagram.THEME["roles"][role]
            with self.subTest(kind=kind):
                self.assertIn(f".node.{kind} .row-rect-even path:first-child {{ fill: {colors['fill']}", config["themeCSS"])
                self.assertIn(f"classDef {kind} stroke:{colors['stroke']}", erd.class_definitions(diagram.THEME))

    @unittest.skipUnless(shutil.which("node") and erd.MMDC.exists(), "Node.js and npm ci are needed to render.")
    def test_small_erd_renders_on_small_canvas(self):
        """Export a small ERD to the fixed PNG canvas and reject an SVG destination."""
        source = self.directory / "small.mmd"
        source.write_text(SOURCE, encoding="utf-8")
        _, profile = diagram.export(source, self.directory / "small.png")
        self.assertEqual(profile, "small")
        from PIL import Image
        with Image.open(self.directory / "small.png") as result:
            self.assertEqual(result.size, (1920, 1440))
        with self.assertRaisesRegex(ValueError, "PNG"):
            diagram.export(source, self.directory / "small.svg")


if __name__ == "__main__":
    unittest.main()
