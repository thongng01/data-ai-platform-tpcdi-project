# Diagram guide

Project diagrams use editable draw.io files and a shared visual language. The normal Markdown image is 1920 x 1440 pixels. These rules apply across the project, except ERDs: an ERD is native Mermaid inside its document, without styling directives.

## Canvas and reading size

Draw a standard view on a 960 x 720 canvas and export it at twice that size. Inspect the result at 960 pixels wide, where an 18-pixel canvas label displays at about 18 pixels. A larger exported image improves image quality, but it does not make text larger inside a fixed-width Markdown page.

Keep 32 canvas pixels around the content. Use an 8-pixel alignment grid and leave space for connectors and their labels. Keep the whole page in the export. If a view is too dense, change its scope or arrangement before reducing text size.

Large architecture and deployment diagrams can also have a master page with a larger canvas. Keep the complete master editable in draw.io and export it as SVG for separate viewing and zooming. Use standard views for the overview and detailed explanations inside Markdown. A larger master is an explicit exception to the normal canvas, not an alternative size for an ordinary inline image.

## Visual language

Use a white canvas with dark text, open space around components, and thin colored scope boundaries. Show most components as a symbol with a label below it. Use boxes when they represent a scope, table, or a component whose text needs more space. Group the content by function and arrange its main flow from left to right. Put shared capabilities in a separate band below the main flow when they apply to the whole view.

| Element | Rule |
| --- | --- |
| Typography | Use Ubuntu Sans for labels and Ubuntu Sans Mono for fields and code identifiers. |
| Text size | Use 28 pixels for the title, 20 for table headings, 18 for normal labels, and at least 16 for fields and connector labels. These are canvas sizes. |
| Background | Use white, with pale fills for large groups when needed. Keep text contrast at or above 4.5:1. |
| Components and work | Use a 48 to 56-pixel symbol with a centered label below it. Use a native process symbol for a generic processing step. Keep labels to one or two lines where possible. |
| Data | Use document shapes for files and cylinders for databases. Use native tables with pale headers for fields, and crow's-foot notation for relationships. |
| External systems | Use blue symbols with dark labels. Add an outer scope only when its boundary helps explain the system. |
| Notes | Use plain text or a light neutral fill for a short interpretation note. Put longer explanations in the document. |
| Groups | Use named rectangular scopes with solid outlines. Use dashed boundaries for network or access scopes and name them explicitly. Nest groups to show containment. Do not use containment to imply data flow. |
| Flow connectors | Use solid orthogonal gray lines with open arrowheads pointing toward the receiver. Label each flow with what moves or what happens. |
| Control connectors | Use dashed arrows for triggers, scheduling, or control. State the action in the label. |
| Data relationships | Use crow's-foot notation and connect the relevant field rows where possible. Show the actual cardinality and optionality. |
| Generic icons | Use draw.io's built-in shapes and libraries first. Keep a consistent outline weight and scale. Use 32-pixel icons inside scope headings or shared-service bands. |
| Product icons | Use the official icon only for the actual named product or service. Preserve its proportions and approved appearance. Pair it with a label and record the source and use terms. |

Text, grouping, and connector labels carry the meaning. Color supports them. Give each color a consistent role within a diagram. Prefer left-to-right flow and short orthogonal routes. Align related boxes and keep unrelated connectors away from their labels.

Use 1.5-pixel scope borders and connectors, and about 1.5 to 2 pixels for generic symbol outlines. Leave 8 pixels between a symbol and its label, at least 24 pixels inside a scope, and at least 32 pixels between adjacent labeled components. Reserve 48 pixels at the top of a scope for its heading. Allocate separate space for routes before adding more nodes. Do not route through a symbol, label, or heading. A dot marks a shared junction; crossing lines without a dot are not connected.

### Color roles

Use the accent for generic symbols, scope borders, and scope headings. Use pale fills for scopes, table headers, and storage symbols. Keep ordinary labels dark. Product icons retain their own colors. The shared values are stored in [theme.json](../misc/tools/diagrams/theme.json).

| Role | Accent | Pale fill |
| --- | --- | --- |
| Sources, system scopes, and interfaces | `#244F72` | `#EEF3F6` |
| Data and storage | `#397A32` | `#EDF7E9` |
| Processing, scheduling, and workload | `#B65300` | `#FFF6ED` |

Use `#183246` for ordinary text, `#486477` for connectors, and `#F4F6F8` for neutral supporting bands. External symbols can use white fills. Pair color with labels and shapes so color is not the only way to identify a role. Color does not change a connector's meaning. Do not color every connector to match its source. Avoid gradients, shadows, decorative number badges, and large saturated backgrounds.

Use pale table headers with dark text by default. A dense table can use a solid role-colored header with white text if the contrast remains at least 4.5:1. The same palette supports both open architecture views and compact field tables; their content determines the layout.

### Icons

[Noun Project](https://thenounproject.com/) is an optional source when the built-in libraries lack a useful symbol. Select a matching collection or creator instead of mixing unrelated icon styles. Use SVG assets and embed them in the draw.io file so it remains portable. Keep the labels and connectors editable.

Check each external icon's license and record its creator, source, and required credit beside the asset. Noun Project's [terms](https://thenounproject.com/legal/) distinguish public-domain icons, free CC BY 3.0 icons that require creator attribution, and paid licenses that remove that attribution requirement. Put required credit in the accompanying document where the license permits it.

The [diagram examples](Diagram-Examples.md) show a source flow, a deployment with nested scopes, and field relationships. Each view has an editable draw.io source.

## Content and multiple views

Start from the question that the diagram must answer. Name the objects, relationships, and scope from the source material before placing them. For a source-based reference, keep source terms and conditions intact. For a project design, distinguish decided elements from proposals in the accompanying text.

Use architecture views to explain system responsibilities and interactions. Use deployment views to show actual hosts, runtimes, services, and their containment. An ERD shows entities, fields, and data relationships. Choose connectors that match that subject.

For a large system, preserve stable identifiers for the same object across the master and its related views. Select the objects needed for each question. Review labels, relationships, and scope across those views after an edit. The current tool does not synchronize separate views automatically. A file link or a native draw.io hyperlink can connect an overview to its details.

## Editable source and revision workflow

The assistant creates a draw.io file. The owner can edit that file directly, ask the assistant to make changes, and export images in draw.io. The assistant can also export with the shared tool. The current draw.io file is authoritative for diagram content and layout. The owner does not need to maintain a JSON specification or patch.

1. Create the diagram from its purpose, source objects, relationships, and scope. Each displayed relationship must have a stated meaning.
2. Edit the current draw.io file directly or ask the assistant to revise it. Save interactive edits before the assistant reads the file. The assistant reads the latest version and preserves changes outside the requested update.
3. Export from draw.io or ask the assistant to use the shared tool. For manual PNG export, select the full page and scale 2. Use the shared tool when exact 1920 x 1440 dimensions are required because some draw.io versions add a border.
4. Inspect the reading-size image. Every label must fit, every connection must be traceable, and related views must remain consistent. Keep the editable file and render beside their document, with a link beside the embedded image.

The [tooling instructions](../misc/tools/diagrams/README.md) describe the assistant's internal commands. A JSON creation specification is optional input for new documents. Inspection exposes content, geometry, styles, native XML, and a file hash. Targeted patches update the current document. This return path supports agent revisions without making the owner manage two sources. It does not reconstruct omitted reasoning or the original prose specification. Creation refuses to overwrite an existing file, because regeneration from an earlier specification would discard native edits.

## Implementation choices and limits

Python's standard XML library reads and updates native draw.io documents. Shared templates handle common nodes, groups, tables, and connectors. Draw.io Desktop performs rendering. Pillow checks the PNG, removes a verified blank export border when needed, and produces the reading-size preview.

This keeps diagram-specific content in data files and concentrates parsing, update checks, and export rules in one tool. Updates modify the existing native XML rather than rebuilding the document from a separate object model.

Updates preserve XML elements and attributes outside the requested changes, including wrappers, custom metadata, geometry children, and styles. Compressed pages are decoded before updates. Serialization and compression can change, so preservation means XML structure and values, not identical file bytes. Each update saves the previous bytes and checks the source hash before saving. These checks detect stale snapshots; they are not a collaborative editing lock. Save and close active edits before an automated update.

The tool checks page dimensions, identifiers, references, parent cycles, vertex bounds, explicit route points, minimum text sizes, and selected label rules. It does not automatically prove correct content, detect every shape overlap, measure rendered text overflow, or find a good route through a dense graph. These remain part of rendered inspection. Native style overrides support draw.io shapes and icons; their appearance must also be inspected.

