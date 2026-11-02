# Diagram Tools

Read the [diagram guide](../../Diagram-Guide.md) before creating, changing, or exporting a project diagram.

`diagram.py` checks and exports both source types. A draw.io file is the source for most diagrams; the owner edits it in draw.io, and the assistant creates and patches it with the commands below. An ERD is a Mermaid `.mmd` file that either can edit as text. JSON specifications and patches are optional internal tools for the assistant. The owner does not need to maintain them.

## Setup

Run the commands from the repository root in Ubuntu WSL. Rendering needs the Ubuntu Sans and Ubuntu Sans Mono fonts.

| Need | Used for | Install |
| --- | --- | --- |
| Pillow | Every PNG export | `.venv/bin/python -m pip install -r workflow/tools/diagrams/requirements.txt` |
| draw.io Desktop with `--size page` and a display such as WSLg | draw.io exports | The [official desktop releases](https://github.com/jgraph/drawio-desktop/releases), outside the repository |
| Node.js on `PATH` and the pinned npm packages | ERD exports | `(cd workflow/tools/diagrams && npm ci)` |

With nvm, run `source ~/.nvm/nvm.sh` first in a non-interactive shell. `npm ci` installs Mermaid CLI in the ignored `node_modules/` and downloads the matching headless Chrome to `~/.cache/puppeteer`. Mermaid needs a browser because it measures text to size the boxes. The `yauzl` package unpacks the download on systems without `unzip`. If the download fails, run `npx puppeteer browsers install chrome-headless-shell` in `workflow/tools/diagrams`. Rendering disables Chrome's sandbox, which WSL cannot always provide. Only local project files are rendered.

## Export

```bash
.venv/bin/python workflow/tools/diagrams/diagram.py check workflow/diagrams/style-examples/data-integration.drawio
.venv/bin/python workflow/tools/diagrams/diagram.py export \
  workflow/diagrams/style-examples/data-integration.drawio --output local/tmp/diagrams/integration.png
.venv/bin/python workflow/tools/diagrams/diagram.py check docs/diagrams/data-specification/gold-erd.mmd
.venv/bin/python workflow/tools/diagrams/diagram.py export docs/diagrams/data-specification/gold-erd.mmd
```

The default output is a PNG beside the source, with the same file name. The exporter writes PNG only. A `small` PNG is exactly 1920 x 1440, a `medium` PNG is exactly 2880 x 2160, and a `large` PNG is twice its canvas. Use `--page 2` to select another draw.io page.

Export writes logs, a source snapshot, a render manifest, and a 960-pixel reading preview to a unique folder in `local/tmp/diagrams/`. The manifest records the source and render hashes, the command, the page, the size class, and the theme hash. If the source changes during export, publication fails.

**draw.io**: The page's `projectProfile` sets the size. For a manual export, use the full page, a white background, and scale 2. Draw.io 32.6.1 adds two output pixels per axis at scale 2. The tool removes that extra area only if it is white. It does not resize diagram content or crop a nonwhite border. Other unexpected dimensions fail.

**ERDs**: The tool builds the Mermaid configuration from [theme.json](theme.json): ELK layout, `data` role colors, Ubuntu Sans labels, and Ubuntu Sans Mono types and keys. It renders an SVG to measure the diagram, then the PNG at scale 2. Including the 32-pixel margin, a render that fits 960 x 720 is `small`, one that fits 1440 x 1080 is `medium`, and any larger render is `large`. A `small` or `medium` PNG is centered on its size's canvas. A `large` PNG has the margin around the render. The export folder also holds the Mermaid configuration.

## ERD Sources

An ERD file starts with `erDiagram`. The [gold layer ERD](../../../docs/diagrams/data-specification/gold-erd.mmd) is an example. Write one table per definition line, tagged with its kind: `DimCustomer:::entity {`. A table without fields can stand alone as `Name:::kind`. Relationships use the symbol form, such as `DimBroker ||--o{ DimAccount : manages`. The kinds are `entity`, `event`, `state`, and `reference`; the [diagram guide](../../Diagram-Guide.md#erds) defines them.

`check` and `export` reject:
- A table without a kind, an unknown kind, or two kinds for one table.
- A line outside a field block that is not a table, a relationship, a comment, or `direction`.
- Init directives, front matter, `style`, `classDef`, and `class` statements, because the tool supplies the styling.
- Dashed relationship lines (`..` or `optionally to`), because the course rubric limits dashed lines to secondary flows.

At export, the tool adds one `classDef` per kind with the kind's border color. Mermaid writes a `classDef` fill as an inline `!important` style that a stylesheet cannot override, so the fills, white header names, 20-pixel names, and 16-pixel relationship labels come from CSS in the generated configuration. The export folder keeps the source snapshot and the rendered input with these additions.

## Assistant Creation and Updates

The [integration specification](examples/integration.spec.json) is a minimal creation example. The [editable diagram examples](../../Diagram-Examples.md) show larger views with native symbols, embedded product icons, and nested scopes.

```bash
.venv/bin/python workflow/tools/diagrams/diagram.py create \
  workflow/tools/diagrams/examples/integration.spec.json local/tmp/diagrams/example.drawio
.venv/bin/python workflow/tools/diagrams/diagram.py inspect \
  local/tmp/diagrams/example.drawio --output local/tmp/diagrams/example-inspection.json
```

Open and save the `.drawio` file normally. Before an automated update, save and close active edits so that a later GUI save cannot replace the updated file.

Creation refuses to overwrite an existing file. Inspection reads compressed and uncompressed mxfile pages and emits labels, attributes, native cell XML, and the exact source SHA-256 hash. IDs are scoped to each page.

Creation reads template colors and fonts from [theme.json](theme.json). Native style overrides control individual elements. Follow the diagram guide for visual rules. Icons are optional. Use draw.io's built-in libraries first; Noun Project can supply missing concepts. Keep one consistent style for generic icons and use official product icons for named technologies. Embed imported SVGs in the draw.io file and record their source, creator, license, and required credit beside the assets. The [Azure asset record](assets/azure/README.md) documents the product symbols used in the [examples](../../Diagram-Examples.md).

Changing the theme affects new draw.io creation and every later ERD export. It does not restyle an existing draw.io file. Inspect that file and apply an explicit style patch when an update is needed. Keep labels, geometry, native shapes, and product icon colors outside that patch unless the task requires their revision.

Represent a symbol and its label as a native group with no visible border. Give its children stable IDs such as `worker.icon` and `worker.label`. This lets the owner move them together in draw.io and lets the assistant update either child. Connect side routes to the symbol. For a vertical route, use the group's outer edge or route around the label. A heading icon stays inside its scope. Keep the native shape style or embedded SVG intact when changing other properties.

| Creation field | Meaning |
| --- | --- |
| `version`, `pages` | Set version to 1. Each page has an `id`, `title`, optional `purpose`, `nodes`, and `edges`. |
| `profile`, `canvas` | Sizes are `small` (the default, 960 x 720), `medium` (1440 x 1080), and `large`. A `large` page needs an explicit canvas larger than `small`. |
| `layers` | Supply optional native layers with `id` and `label`. Use a layer ID as a node's parent. |
| Node `id`, `label` | Supply a stable identifier and visible text. Newlines are allowed. |
| Node `kind`, `role` | Kinds are `node`, `data`, `group`, and `text`. Tables and their fields belong in an ERD. Blue roles are `scope`, `serve`, and `external`. Orange roles are `component` and `control`. Green uses `data`; neutral notes use `note`. Groups default to `scope`. |
| Node `box`, `parent` | Supply `[x, y, width, height]` relative to the parent. Top-level nodes can use a default grid. Children need explicit coordinates. |
| Node `semantic_id`, `link` | Optionally record shared identity across views and a native hyperlink. |
| Node or edge `style` | Supply native draw.io style properties for stencils, icons, attachment points, or local adjustments. |
| Edge `id`, `source`, `target`, `label` | Name the connection, its terminal IDs, and its meaning. |
| Edge `kind` | Choose `flow` or `control`. Data relationships belong in an ERD. |
| Edge `points` | Supply route points as `[[x, y], ...]` in page coordinates. |

The title reserves ID `title`. Native IDs `0` and `1` identify the root and default layer. The default grid does not solve dense layouts.

A patch contains `source_sha256` from a fresh inspection and ordered `operations`. Each operation names a `page`. Cell operations name an `id`, except `add`, which gets its ID from XML.

```json
{
  "source_sha256": "COPY_THE_CURRENT_INSPECTION_HASH",
  "operations": [
    {
      "op": "set",
      "page": "integration",
      "id": "process",
      "label": "Validate records",
      "geometry": {"x": 32, "y": 112},
      "style": {"fontSize": 18}
    }
  ]
}
```

```bash
.venv/bin/python workflow/tools/diagrams/diagram.py apply \
  local/tmp/diagrams/example.drawio local/tmp/diagrams/example-patch.json
```

| Operation | Behavior |
| --- | --- |
| `set` | Changes `label`, `style`, `geometry`, or native `attributes`. IDs remain stable. A null style or attribute value removes that property. |
| `set` with `geometry.points` | Replaces route points. An empty list removes the explicit route. Other geometry stays intact. |
| `add` | Appends an `mxCell` or wrapper from `xml`. |
| `remove` | Removes a cell. Remove or reconnect dependent edges and children explicitly in the same patch. |
| `set-page` | Changes `title`, `purpose`, `profile`, or `canvas`. The page title and visible title cell are separate. |

The tool validates the final document before writing, saves previous bytes in `local/tmp/diagrams/`, and checks the source hash again before replacing the file. These checks detect stale snapshots; they are not an editing lock.

Inspection output is not a creation specification. Apply a targeted patch to the current document. Rebuilding from an old specification does not merge native edits. Unknown native XML remains unless a requested operation changes it. XML serialization and compression can change.

## Tests

```bash
.venv/bin/python -m unittest discover -s workflow/tools/diagrams -p 'test_*.py' -v
```

The tests cover text contrast, native XML preservation, compressed pages, page-scoped IDs, stale updates, invalid references and geometry, container positions, size profiles, dense large pages, export borders, PNG placement, ERD source checks, kind tags, and size classes, the Mermaid theme mapping, and solid-header contrast. One test renders a small ERD; it is skipped when Node.js or the npm packages are missing. Structural checks do not establish visual quality. Inspect each changed render for wrapping, overlap, routes, containment, and icon proportions.
