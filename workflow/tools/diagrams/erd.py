"""Check and render Mermaid ERD files with the project theme and the ELK layout."""
from __future__ import annotations

import json
from pathlib import Path
import re
import shutil
import subprocess
import xml.etree.ElementTree as ET

HERE = Path(__file__).resolve().parent
MMDC = HERE / "node_modules/.bin/mmdc"

# The tool owns the appearance, so an ERD file holds entities, fields, relationships, and kind tags.
STYLING = "The diagram tool supplies the ERD styling."
RULES = [
    (re.compile(r"%%\{"), f"Remove the init directive. {STYLING}"),
    (re.compile(r"\A\s*---\s*$", re.M), f"Remove the front matter. {STYLING}"),
    (re.compile(r"^\s*(style|classDef|class)\s", re.M), f"Remove the style statement. {STYLING}"),
    # The course rubric limits dashed lines to secondary flows, so every ERD relationship is solid.
    (re.compile(r"[|}o]{2}\.\.[|{o]{2}|\boptionally to\b"),
     "Use a solid relationship line (--). Name a match that is not a key in the relationship label."),
]

# A table's kind is its role in the data, so one business object keeps its color in every layer.
# Each kind uses a theme color role; reference tables keep a pale header so they recede.
KINDS = {"entity": "scope", "event": "component", "state": "data", "reference": "note"}
QUIET_KINDS = {"reference"}
NAME = r"[A-Za-z_][A-Za-z0-9_-]*"
TAGGED = re.compile(rf"^({NAME})(?::::({NAME}))?$")
BLOCK = re.compile(rf"^({NAME}(?::::{NAME})?)\s*\{{$")
RELATIONSHIP = re.compile(rf"^({NAME}(?::::{NAME})?)\s+[|}}o]{{2}}--[|{{o]{{2}}\s+({NAME}(?::::{NAME})?)\s*:")


def entity_kinds(text):
    """Return each entity's kind tag, or None for an untagged entity."""
    kinds, inside = {}, False
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("%%") or line == "erDiagram" or line.startswith("direction "):
            continue
        if inside:
            inside = line != "}"
            continue
        if block := BLOCK.match(line):
            references, inside = [block.group(1)], True
        elif relationship := RELATIONSHIP.match(line):
            references = [relationship.group(1), relationship.group(2)]
        elif TAGGED.match(line):
            references = [line]
        else:
            raise ValueError(f"Unrecognized ERD line: {line}")
        for reference in references:
            name, kind = TAGGED.match(reference).groups()
            if kind and kinds.get(name) not in (None, kind):
                raise ValueError(f"{name} has two kinds: {kinds[name]} and {kind}.")
            kinds[name] = kind or kinds.get(name)
    return kinds


def check(text):
    for pattern, message in RULES:
        if pattern.search(text):
            raise ValueError(message)
    lines = [line.strip() for line in text.splitlines() if line.strip() and not line.strip().startswith("%%")]
    if not lines or lines[0] != "erDiagram":
        raise ValueError("An ERD file must start with erDiagram.")
    kinds = entity_kinds(text)
    untagged = sorted(name for name, kind in kinds.items() if kind is None)
    if untagged:
        raise ValueError(f"Tag each table with :::kind, using {', '.join(KINDS)}. Untagged: {', '.join(untagged)}.")
    unknown = sorted(f"{name}:::{kind}" for name, kind in kinds.items() if kind not in KINDS)
    if unknown:
        raise ValueError(f"Unknown kind in {', '.join(unknown)}. Use {', '.join(KINDS)}.")


def class_definitions(theme):
    # Mermaid writes a classDef fill as an inline !important style on the rows, which CSS cannot
    # override, so classDef carries only the border and the stylesheet sets every fill.
    return "".join(f"classDef {kind} stroke:{theme['roles'][role]['stroke']}\n" for kind, role in KINDS.items())


def kind_css(theme):
    rules = [".row-rect-odd path:first-child { fill: #FFFFFF !important; }"]
    for kind, role in KINDS.items():
        colors = theme["roles"][role]
        quiet = kind in QUIET_KINDS
        rules.append(f".node.{kind} .outer-path path:first-child {{ fill: {colors['heading' if quiet else 'stroke']} !important; }}")
        rules.append(f".node.{kind} .row-rect-even path:first-child {{ fill: {colors['fill']} !important; }}")
        if not quiet:
            rules.append(f".node.{kind} .label.name text, .node.{kind} .label.name tspan {{ fill: #FFFFFF !important; }}")
    return " ".join(rules)


def config(theme):
    data = theme["roles"]["data"]
    font = f"{theme['font']}, sans-serif"
    mono = f"'{theme['mono_font']}', monospace"
    return {
        "theme": "base",
        "layout": "elk",
        "htmlLabels": False,
        "fontFamily": font,
        "er": {"useMaxWidth": False},
        "themeVariables": {
            "fontFamily": font, "fontSize": "16px", "background": "#FFFFFF",
            "primaryColor": data["heading"], "primaryBorderColor": data["stroke"], "nodeBorder": data["stroke"],
            "primaryTextColor": theme["text"], "textColor": theme["text"], "lineColor": theme["line"],
            "rowOdd": "#FFFFFF", "rowEven": data["fill"],
            "tertiaryColor": "#FFFFFF", "edgeLabelBackground": "#FFFFFF",
        },
        # Mermaid exposes these through CSS only: no shadows, the guide's 20-pixel bold table names,
        # monospace types and keys, 1.5-pixel connectors, 16-pixel connector labels, and the kind colors.
        "themeCSS": (
            ".node .outer-path, .node path, .node rect { filter: none !important; }"
            " .label.name, .label.name * { font-weight: 600 !important; font-size: 20px !important; }"
            " .label.attribute-type, .label.attribute-type *, .label.attribute-keys, .label.attribute-keys *"
            f" {{ font-family: {mono} !important; }}"
            " .relationshipLine { stroke-width: 1.5px !important; }"
            " .edgeLabel .label, .edgeLabel text, .edgeLabel tspan { font-size: 16px !important; }"
            # Mermaid sizes the label background before this CSS enlarges the text; the white
            # outline restores the gap between the label and its line.
            " .edgeLabel rect.background { fill: #FFFFFF !important; stroke: #FFFFFF !important; stroke-width: 8px !important; } "
            + kind_css(theme)
        ),
    }


def svg_size(path):
    root = ET.fromstring(Path(path).read_bytes())
    _, _, width, height = (float(value) for value in root.get("viewBox", "").split())
    return width, height


def size_class(size, sizes, margin):
    width, height = size
    for name in ("small", "medium"):
        canvas_width, canvas_height = sizes[name]
        if width + 2 * margin <= canvas_width and height + 2 * margin <= canvas_height:
            return name
    return "large"


def render(source, work, theme):
    """Render the ERD as PNG in work and return the PNG path, size class, size, and commands."""
    node = shutil.which("node")
    if not node or not MMDC.exists():
        raise ValueError("Load Node.js and run npm ci in workflow/tools/diagrams before ERD export.")
    (work / "source.mmd").write_bytes(source)
    snapshot = work / "render.mmd"
    snapshot.write_text(source.decode("utf-8").rstrip("\n") + "\n" + class_definitions(theme), encoding="utf-8")
    (work / "mermaid.json").write_text(json.dumps(config(theme), indent=2) + "\n", encoding="utf-8")
    # Chrome's sandbox needs kernel features that WSL can lack; the input is a local project file.
    (work / "puppeteer.json").write_text('{"args": ["--no-sandbox"]}\n', encoding="utf-8")

    def mmdc(destination, *extra):
        command = [node, str(MMDC), "--quiet", "--input", str(snapshot), "--output", str(destination),
                   "--configFile", str(work / "mermaid.json"), "--puppeteerConfigFile", str(work / "puppeteer.json"),
                   "--backgroundColor", "white", *extra]
        result = subprocess.run(command, capture_output=True, text=True, timeout=180)
        with (work / "export.log").open("a", encoding="utf-8") as log:
            log.write(" ".join(command) + "\n" + result.stdout + result.stderr)
        if result.returncode or not destination.exists():
            raise ValueError(f"Mermaid export failed. Read {work / 'export.log'}.")
        return command

    sizes, margin = theme["canvas"]["sizes"], theme["canvas"]["margin"]
    # Mermaid's SVG gives the exact logical size, which decides the size class.
    measured = work / "measure.svg"
    commands = [mmdc(measured)]
    size = svg_size(measured)
    profile = size_class(size, sizes, margin)
    preview = work / "preview.png"
    commands.append(mmdc(preview, "--scale", str(theme["canvas"]["scale"])))
    return preview, profile, size, commands
