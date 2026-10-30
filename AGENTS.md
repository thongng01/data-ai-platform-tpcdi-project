# Working on This Project

Build the data, ML, and agent platform with the owner, and explain the engineering so the owner learns it.

## Working Agreement
- **Owner drafts**: Refine in place. Keep structure, density, and tone; fix facts; add sections only when asked.
- **Decisions**: Record a choice as decided only when the owner made it. Agent proposals stay proposals.
- **Status record**: One short record: position, open decisions, next step. Update it at the end of each task.
- **Resuming**: Start from the status record. Open older records only when it points to them.
- **Restructuring**: Moving, renaming, splitting, and merging documents is normal. Update incoming links in the same change, then run the documentation checks.
- **Finding things**: The [documentation guide](docs/Documentation-Guide.md) maps each document to its purpose; the README shows the repository layout.
- **Supporting files**: Temporary work in `misc/tmp/`, reusable tools in `misc/tools/`. Follow the [misc guide](misc/README.md).

## Documents
- **Writing**: Follow the documentation guide's style section.
- **ERDs**: Native Mermaid inside the document, without styling directives.
- **Other diagrams**: Editable draw.io with the shared tooling. Read the [diagram guide](docs/Diagram-Guide.md) and the [tool instructions](misc/tools/diagrams/README.md) first.

## Visual Artifacts

These rules apply to architecture, deployment, data flow, conceptual, and hybrid diagrams.

Make the content and relationships easy to understand. Use consistent elements, clear grouping, alignment, spacing, and readable connections. Choose legible fonts deliberately, avoiding Inter and generic defaults. Text must have enough size, weight, and contrast at the intended viewing size.

Use icons when they help identify a component or role. Use recognizable product icons for named technologies and a consistent set of symbols for abstract concepts. Keep icons consistent in scale and visual weight, preserve their proportions, and pair them with readable labels. Give shapes, colors, line styles, and arrow directions consistent meanings. Make boundaries and connection labels clear where the explanation depends on them.

Use direct titles and labels. Avoid decorative numbering, zero-padded headings, filler one-liners, and crowded legends or footnotes. Keep notes that help interpret the visual; place supporting explanation in accompanying text. Inspect the rendered result at its intended viewing size for readability, crowding, unclear connections, and awkward wrapping.

## Code Structure and Comments

Let code structure explain the normal behavior. Use clear names, functions, and interfaces. Comments should explain non-obvious reasons, constraints, assumptions, or invariants. Put longer teaching explanations and design discussions in documentation. Avoid comments that narrate operations already clear from the code.
