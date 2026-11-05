# Working on This Project

Build the data, ML, and agent platform with the owner, and explain the engineering so the owner learns it.

## Working Agreement
- **Owner drafts**: Refine in place. Keep structure, density, and tone; fix facts; add sections only when asked.
- **Decisions**: Work through design and architecture choices with the [design method](workflow/Design-Method.md) in conversation, and record the distilled result in the owning document ([decision rules](workflow/Documentation-Guide.md#4-recording-design-decisions)). Only choices accepted by the owner enter the system design as settled decisions; open items stay in [Status](workflow/Status.md).
- **Status record**: Keep working state in [Status](workflow/Status.md): position, accepted and open decisions, next step, what exists, build progress, and known defects. Update it at the end of each task.
- **Public docs**: `README.md` and `docs/` state the designed system in the present tense, as if it were built. Build progress, comparisons with the code that exists today ("current POC", "not yet", "target, not current"), `(open)` markers, and `Status.md` links go only in Status.
- **Resuming**: Start from [Status](workflow/Status.md). Open older records in `local/archive/` only when it points to them.
- **Restructuring**: Moving, renaming, splitting, and merging documents is normal. Update incoming links in the same change, then run `.venv/bin/python workflow/tools/check_docs.py`.
- **Finding things**: The [workflow overview](workflow/README.md) and [documentation guide](workflow/Documentation-Guide.md) map each directory and document to its purpose; the [root README](README.md) shows the repository layout.
- **Supporting files**: Temporary work in `local/tmp/`, local archives and references in `local/`, and reusable tools in `workflow/tools/`. Follow the [workflow guide](workflow/README.md).

## Documents
- **Terms**: Name concepts in documents, code, and configuration with the terms in the [glossary](docs/Glossary.md). When the owner settles a new term or renames one, update the glossary in the same change.
- **Writing**: Follow the [documentation guide](workflow/Documentation-Guide.md).
- **Diagrams**: ERDs are Mermaid `.mmd` files with a kind tag per table and no styling directives; other diagrams are editable draw.io files. Both render with the shared tooling. Read the [diagram guide](workflow/Diagram-Guide.md) and the [tool instructions](workflow/tools/diagrams/README.md) first.

## Visual Artifacts

These rules apply to architecture, deployment, data flow, conceptual, and hybrid diagrams.

Make the content and relationships easy to understand. Use consistent elements, clear grouping, alignment, spacing, and readable connections. Choose legible fonts deliberately, avoiding Inter and generic defaults. Text must have enough size, weight, and contrast at the intended viewing size.

Use icons when they help identify a component or role. Use recognizable product icons for named technologies and a consistent set of symbols for abstract concepts. Keep icons consistent in scale and visual weight, preserve their proportions, and pair them with readable labels. Give shapes, colors, line styles, and arrow directions consistent meanings. Make boundaries and connection labels clear where the explanation depends on them.

Use direct titles and labels. Avoid decorative numbering, zero-padded headings, filler one-liners, and crowded legends or footnotes. Keep notes that help interpret the visual; place supporting explanation in accompanying text. Inspect the rendered result at its intended viewing size for readability, crowding, unclear connections, and awkward wrapping.

## Code Structure and Comments

Write code for the owner to learn from and maintain. Use clear names and small, focused functions, supported by useful comments.

Start each source file with a short description of its purpose. Give every function and class a docstring that describes what it does, as required by coursework rubric row 98 (`docs/course/rubric.xlsx`). Add details about inputs, outputs, state changes, and important conditions when they help the reader.

Use comments to explain meaningful blocks, unfamiliar techniques, business rules, assumptions, and reasons for behavior. Explain both what the code achieves and how it works when the owner needs that context. Do not assume that code familiar to an experienced developer is clear to the owner.

Avoid comments that merely repeat individual statements, unnecessary labels for obvious steps, and notes about a particular editing session. Describe lasting behavior and constraints instead.

Keep explanations needed to follow the code beside the code. Put longer tutorials and design discussions in documentation. Update comments and docstrings when the behavior changes.
