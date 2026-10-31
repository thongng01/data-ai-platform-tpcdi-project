# Diagram Guide

Project diagrams use editable source files and a shared visual language. Most diagrams are draw.io files. An ERD is a Mermaid file; see [ERDs](#erds). The [diagram tools](../misc/tools/diagrams/README.md) check and export both.

## Canvas and Reading Size

Each diagram has one of three sizes. A Markdown page has a fixed width, so a larger canvas shows its text smaller unless the reader zooms in. Choose the smallest size that holds the content at the text sizes below. Embed every size directly in its document as a PNG. The Paseo mobile app shows SVG images as their alt text, so documents use PNG only.

| Size | Canvas | Export | Text at a 960-pixel reading width |
| --- | --- | --- | --- |
| `small` | 960 x 720 | PNG, 1920 x 1440 | Full size: an 18-pixel label shows at 18 pixels. |
| `medium` | 1440 x 1080 | PNG, 2880 x 2160 | Two-thirds size: an 18-pixel label shows at 12 pixels. Readable on a wide screen or zoomed in. |
| `large` | Larger than `small`, any size | PNG, twice the canvas | Read zoomed in. |

The Paseo mobile app shows a zoomed image at about screen resolution, so a `large` diagram stays blurry there. Read it on desktop, on the web, or in GitHub.

The export writes a reading preview 960 pixels wide. Inspect it, because it shows the text size that a reader sees. A larger exported image improves image quality, but it does not make text larger inside a fixed-width Markdown page.

Keep 32 canvas pixels around the content. Use an 8-pixel alignment grid and leave space for connectors and their labels. Keep the whole page in the export. If a view is too dense, change its scope or arrangement before reducing text size.

A large architecture or deployment diagram can show the complete system, with `small` or `medium` views beside it for the parts that a section explains.

## Visual Language

Use a white canvas with dark text, open space around components, and thin colored scope boundaries. Show most components as a symbol with a label below it. Use boxes when they represent a scope, table, or a component whose text needs more space. Group the content by function and arrange its main flow from left to right. Put shared capabilities in a separate band below the main flow when they apply to the whole view.

| Element | Rule |
| --- | --- |
| Typography | Use Ubuntu Sans for labels and Ubuntu Sans Mono for fields and code identifiers. |
| Text size | Use 28 pixels for the title, 20 for table headings, 18 for normal labels, and at least 16 for fields and connector labels. These are canvas sizes. |
| Background | Use white, with pale fills for large groups when needed. Keep text contrast at or above 4.5:1. |
| Components and work | Use a 48 to 56-pixel symbol with a centered label below it. Use a native process symbol for a generic processing step. Keep labels to one or two lines where possible. |
| Data | Use document shapes for files and cylinders for databases. Show tables, fields, and their relationships in an [ERD](#erds). |
| External systems | Use blue symbols with dark labels. Add an outer scope only when its boundary helps explain the system. |
| Notes | Use plain text or a light neutral fill for a short interpretation note. Put longer explanations in the document. |
| Groups | Use named rectangular scopes with solid outlines. Use dashed boundaries for network or access scopes and name them explicitly. Nest groups to show containment. Do not use containment to imply data flow. |
| Flow connectors | Use solid orthogonal gray lines with open arrowheads pointing toward the receiver. Label each flow with what moves or what happens. |
| Control connectors | Use dashed arrows for triggers, scheduling, or control outside the main flow, and keep them few; the course rubric limits dashed arrows to secondary flows. State the action in the label. |
| Data relationships | Use an ERD with crow's-foot notation and solid lines. Show the actual cardinality and optionality. |
| Generic icons | Use draw.io's built-in shapes and libraries first. Keep a consistent outline weight and scale. Use 32-pixel icons inside scope headings or shared-service bands. |
| Product icons | Use the official icon only for the actual named product or service. Preserve its proportions and approved appearance. Pair it with a label and record the source and use terms. |

Text, grouping, and connector labels carry the meaning. Color supports them. Give each color a consistent role within a diagram. Prefer left-to-right flow and short orthogonal routes. Align related boxes and keep unrelated connectors away from their labels.

Use 1.5-pixel scope borders and connectors, and about 1.5 to 2 pixels for generic symbol outlines. Leave 8 pixels between a symbol and its label, at least 24 pixels inside a scope, and at least 32 pixels between adjacent labeled components. Reserve 48 pixels at the top of a scope for its heading. Allocate separate space for routes before adding more nodes. Do not route through a symbol, label, or heading. A dot marks a shared junction; crossing lines without a dot are not connected.

### Color Roles

Use the accent for generic symbols, scope borders, and scope headings. Use pale fills for scopes, table headers, and storage symbols. Keep ordinary labels dark. Product icons retain their own colors. The shared values are stored in [theme.json](../misc/tools/diagrams/theme.json).

| Role | Accent | Pale fill |
| --- | --- | --- |
| Sources, system scopes, and interfaces | `#244F72` | `#EEF3F6` |
| Data and storage | `#397A32` | `#EDF7E9` |
| Processing, scheduling, and workload | `#B65300` | `#FFF6ED` |

Use `#183246` for ordinary text, `#486477` for connectors, and `#F4F6F8` for neutral supporting bands. External symbols can use white fills. Pair color with labels and shapes so color is not the only way to identify a role. Color does not change a connector's meaning. Do not color every connector to match its source. Avoid gradients, shadows, decorative number badges, and large saturated backgrounds.

Use pale table headers with dark text by default. A dense table can use a solid role-colored header with white text if the contrast remains at least 4.5:1. ERDs use solid headers for their main tables and give the colors table-kind meanings; see [ERDs](#erds).

### Icons

[Noun Project](https://thenounproject.com/) is an optional source when the built-in libraries lack a useful symbol. Select a matching collection or creator instead of mixing unrelated icon styles. Use SVG assets and embed them in the draw.io file so it remains portable. Keep the labels and connectors editable.

Check each external icon's license and record its creator, source, and required credit beside the asset. Noun Project's [terms](https://thenounproject.com/legal/) distinguish public-domain icons, free CC BY 3.0 icons that require creator attribution, and paid licenses that remove that attribution requirement. Put required credit in the accompanying document where the license permits it.

The [diagram examples](Diagram-Examples.md) show a source flow, a deployment with nested scopes, and a small ERD. Each image has its editable source beside it.

## ERDs

An ERD is a Mermaid file (`.mmd`) beside its PNG, for example [gold-erd.mmd](diagrams/data-specification/gold-erd.mmd). Every diagram of tables, fields, and their relationships is an ERD. The file holds entities, fields, relationships, and one kind tag per table, without styling directives. Every relationship uses a solid line (`--`); a match that is not a key says so in its label. The shared tool supplies the theme colors, the fonts, and the ELK layout. ELK places the entities and routes the lines, so the file has no positions. To change the arrangement, change the order of the declarations or split the ERD into smaller views.

Tag each table with its kind on its definition line, for example `DimCustomer:::entity {`. The kind is the table's role in the data, not its layer, so one business object keeps its color in bronze, silver, and gold. Choose the kind deliberately: layer prefixes such as `raw_` and `stg_` do not identify it.

| Kind | Header | Meaning | Examples |
| --- | --- | --- | --- |
| `entity` | Solid blue `#244F72` | A business object that persists over time | Gold `Dim*` tables except `DimDate` and `DimTime`; silver customer and account tables |
| `event` | Solid orange `#B65300` | Something that happened, or a measurement at a point in time | Gold `Fact*` tables; trade, cash transaction, and daily market tables |
| `state` | Solid green `#397A32` | A current snapshot overwritten in place, or derived values | `Prospect`, `Financial`, `feat_` tables |
| `reference` | Pale gray `#EDF0F4` | Codes, calendars, and load logs | `DimDate`, code lookups, `DImessages` |

Solid headers use white 20-pixel bold names; each passes the 4.5:1 contrast rule. Reference tables keep a pale header with dark text, so the lookups recede. Rows alternate between white and the kind's pale fill. These colors mark table kinds only inside ERDs; in other diagrams the same colors keep their [color roles](#color-roles). State the kind colors in one line under each ERD.

The tool measures the render and assigns its size. A `small` or `medium` ERD is centered on that size's canvas. A `large` ERD keeps its own size, with the 32-pixel margin around it.

## Content and Multiple Views

Start from the question that the diagram must answer. Name the objects, relationships, and scope from the source material before placing them. For a source-based reference, keep source terms and conditions intact. For a project design, distinguish decided elements from proposals in the accompanying text.

Use architecture views to explain system responsibilities and interactions. Use deployment views to show actual hosts, runtimes, services, and their containment. An ERD shows entities, fields, and data relationships. Choose connectors that match that subject.

For a large system, preserve stable identifiers for the same object across the large diagram and its related views. Select the objects needed for each question. Review labels, relationships, and scope across those views after an edit. The current tool does not synchronize separate views automatically. A file link or a native draw.io hyperlink can connect an overview to its details.

## Editable Source and Revision Workflow

The current source file is authoritative: the draw.io file for content and layout, or the `.mmd` file for an ERD's content. The owner can edit either directly or ask the assistant to make changes. The owner does not need to maintain a JSON specification or patch.

1. Create the diagram from its purpose, source objects, relationships, and scope. Each displayed relationship must have a stated meaning.
2. Edit the current source directly or ask the assistant to revise it. Save interactive edits before the assistant reads the file. The assistant reads the latest version and preserves changes outside the requested update.
3. Export the PNG. A draw.io file can export from draw.io with the full page and scale 2, but some draw.io versions add a border; the shared tool produces exact dimensions. An ERD exports only with the shared tool, which applies the theme and layout.
4. Inspect the reading-size image. Every label must fit, every connection must be traceable, and related views must remain consistent. Keep the editable source beside its render, with the same file name. Embed the render alone in the document.

The [tool instructions](../misc/tools/diagrams/README.md) describe the commands. For draw.io, a JSON creation specification is optional input for new documents. Inspection exposes content, geometry, styles, native XML, and a file hash. Targeted patches update the current document. This return path supports agent revisions without making the owner manage two sources. It does not reconstruct omitted reasoning or the original prose specification. Creation refuses to overwrite an existing file, because regeneration from an earlier specification would discard native edits.

## Implementation Choices and Limits

Python's standard XML library reads and updates native draw.io documents. Shared templates handle common nodes, groups, tables, and connectors. Draw.io Desktop renders draw.io files. Mermaid CLI renders ERDs in headless Chrome, because Mermaid measures text in a browser to size the boxes. Pillow checks each PNG, removes a verified blank export border when needed, and produces the reading-size preview.

This keeps diagram-specific content in data files and concentrates parsing, update checks, and export rules in one tool. Updates modify the existing native XML rather than rebuilding the document from a separate object model.

Updates preserve XML elements and attributes outside the requested changes, including wrappers, custom metadata, geometry children, and styles. Compressed pages are decoded before updates. Serialization and compression can change, so preservation means XML structure and values, not identical file bytes. Each update saves the previous bytes and checks the source hash before saving. These checks detect stale snapshots; they are not a collaborative editing lock. Save and close active edits before an automated update.

For draw.io files, the tool checks page dimensions, identifiers, references, parent cycles, vertex bounds, explicit route points, minimum text sizes, and selected label rules. For ERDs, it checks for styling directives and dashed lines; Mermaid itself reports syntax errors during export. The tool does not prove correct content, detect every shape overlap, measure rendered text overflow, or find a good route through a dense graph. These remain part of rendered inspection. Native style overrides support draw.io shapes and icons; their appearance must also be inspected.

