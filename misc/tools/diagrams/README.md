# Diagram tools

Read the [diagram guide](../../../docs/Diagram-Guide.md) before creating, changing, or exporting a project diagram.

The normal interaction is direct: the assistant creates a draw.io file, the owner edits it in draw.io, and either can export it. The current draw.io file is the source. JSON specifications and patches are optional internal tools for the assistant. The owner does not need to maintain them.

Run the commands below from the repository root in Ubuntu WSL. Rendering needs draw.io Desktop with the `--size page` option, Ubuntu Sans, Ubuntu Sans Mono, and a display such as WSLg. PNG processing also needs Pillow:

```bash
.venv/bin/python -m pip install -r misc/tools/diagrams/requirements.txt
```

## Export and native editing

Open and save the `.drawio` file normally. Before an automated update, save and close active edits so that a later GUI save cannot replace the updated file.

For a manual PNG export, use the full page, a white background, and scale 2. A standard page is 960 x 720 logical pixels. Some draw.io versions add a border. Use the shared exporter when exact pixel dimensions are required:

```bash
.venv/bin/python misc/tools/diagrams/diagram.py check docs/diagrams/style-examples/record-relationships.drawio
.venv/bin/python misc/tools/diagrams/diagram.py export \
  docs/diagrams/style-examples/record-relationships.drawio --output misc/tmp/diagrams/records.png
.venv/bin/python misc/tools/diagrams/diagram.py export \
  docs/diagrams/style-examples/analytics-deployment.drawio --output misc/tmp/diagrams/deployment.svg
```

Use `--page 2` to select another page. Standard PNGs are exactly 1920 x 1440. Larger master pages export as SVG for separate viewing. SVG export requests embedded images and fonts.

Draw.io includes image fallbacks for rich-text labels in SVG. The exporter removes its generic unsupported-text footer only when every rich-text label has an embedded image fallback. It preserves the footer if any fallback is missing.

Export writes logs, a source snapshot, a render manifest, and the PNG's 960-pixel reading preview to a unique folder in `misc/tmp/diagrams/`. The manifest records source and render hashes, the command, page, and theme hash. If the source changes during export, publication fails.

Draw.io 32.6.1 adds two output pixels per axis at scale 2. The tool removes that extra area only if it is white. It does not resize diagram content or crop a nonwhite border. Other unexpected dimensions fail.

## Assistant creation and updates

The [integration specification](examples/integration.spec.json) is a minimal creation example. The [editable diagram examples](../../../docs/Diagram-Examples.md) show larger views with native symbols, embedded product icons, nested scopes, and field relationships.

```bash
.venv/bin/python misc/tools/diagrams/diagram.py create \
  misc/tools/diagrams/examples/integration.spec.json misc/tmp/diagrams/example.drawio
.venv/bin/python misc/tools/diagrams/diagram.py inspect \
  misc/tmp/diagrams/example.drawio --output misc/tmp/diagrams/example-inspection.json
```

Creation refuses to overwrite an existing file. Inspection reads compressed and uncompressed mxfile pages and emits labels, attributes, native cell XML, and the exact source SHA-256 hash. IDs are scoped to each page.

Creation reads template colors and fonts from [theme.json](theme.json). Native style overrides control individual elements. Follow the diagram guide for visual rules. Icons are optional. Use draw.io's built-in libraries first; Noun Project can supply missing concepts. Keep one consistent style for generic icons and use official product icons for named technologies. Embed imported SVGs in the draw.io file and record their source, creator, license, and required credit beside the assets. The [Azure asset record](assets/azure/README.md) documents the product symbols used in the [examples](../../../docs/Diagram-Examples.md).

Changing the theme affects new creation. It does not restyle an existing draw.io file. Inspect that file and apply an explicit style patch when an update is needed. Keep labels, geometry, native shapes, and product icon colors outside that patch unless the task requires their revision.

Represent a symbol and its label as a native group with no visible border. Give its children stable IDs such as `worker.icon` and `worker.label`. This lets the owner move them together in draw.io and lets the assistant update either child. Connect side routes to the symbol. For a vertical route, use the group's outer edge or route around the label. A heading icon stays inside its scope. Keep the native shape style or embedded SVG intact when changing other properties.

| Creation field | Meaning |
| --- | --- |
| `version`, `pages` | Set version to 1. Each page has an `id`, `title`, optional `purpose`, `nodes`, and `edges`. |
| `profile`, `canvas` | The default is `standard` with `[960, 720]`. A `master` page can use a larger canvas. |
| `layers` | Supply optional native layers with `id` and `label`. Use a layer ID as a node's parent. |
| Node `id`, `label` | Supply a stable identifier and visible text. Newlines are allowed. |
| Node `kind`, `role` | Kinds are `node`, `data`, `group`, `text`, and `table`. Blue roles are `scope`, `serve`, and `external`. Orange roles are `component` and `control`. Green uses `data`; neutral notes use `note`. Groups default to `scope`. |
| Node `box`, `parent` | Supply `[x, y, width, height]` relative to the parent. Top-level nodes can use a default grid. Children need explicit coordinates. |
| Node `fields` | Supply table rows with `id` and `label`. Height follows the row count. Field IDs become `table-id.field-id`; the heading uses `table-id.header`. |
| Node `semantic_id`, `link` | Optionally record shared identity across views and a native hyperlink. |
| Node or edge `style` | Supply native draw.io style properties for stencils, icons, attachment points, or local adjustments. |
| Edge `id`, `source`, `target`, `label` | Name the connection, its terminal IDs, and its meaning. A terminal can be a table field row. |
| Edge `kind` | Choose `flow`, `control`, or `relationship`. |
| Edge `points` | Supply route points as `[[x, y], ...]` in page coordinates. |
| Edge `source_end`, `target_end` | Set relationship cardinality with native endpoints such as `ERmandOne` and `ERzeroToMany`. |

The title reserves ID `title`. Native IDs `0` and `1` identify the root and default layer. Keep generated field IDs free of collisions. The default grid does not solve dense layouts.

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
.venv/bin/python misc/tools/diagrams/diagram.py apply \
  misc/tmp/diagrams/example.drawio misc/tmp/diagrams/example-patch.json
```

| Operation | Behaviour |
| --- | --- |
| `set` | Changes `label`, `style`, `geometry`, or native `attributes`. IDs remain stable. A null style or attribute value removes that property. |
| `set` with `geometry.points` | Replaces route points. An empty list removes the explicit route. Other geometry stays intact. |
| `add` | Appends an `mxCell` or wrapper from `xml`. |
| `remove` | Removes a cell. Remove or reconnect dependent edges and children explicitly in the same patch. |
| `set-page` | Changes `title`, `purpose`, `profile`, or `canvas`. The page title and visible title cell are separate. |

The tool validates the final document before writing, saves previous bytes in `misc/tmp/diagrams/`, and checks the source hash again before replacing the file. These checks detect stale snapshots; they are not an editing lock.

Inspection output is not a creation specification. Apply a targeted patch to the current document. Rebuilding from an old specification does not merge native edits. Unknown native XML remains unless a requested operation changes it. XML serialization and compression can change.

## Checks and installation

```bash
.venv/bin/python -m unittest discover -s misc/tools/diagrams -p test_diagram.py -v
```

The tests cover text contrast, native XML preservation, compressed pages, page-scoped IDs, stale updates, invalid references and geometry, table fields, dense master pages, and export borders. Structural checks do not establish visual quality. Inspect each changed render for wrapping, overlap, routes, containment, and icon proportions.

Install draw.io outside the repository from the [official desktop releases](https://github.com/jgraph/drawio-desktop/releases). Application binaries do not belong in this directory.
