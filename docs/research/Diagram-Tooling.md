# Diagram tooling

This assessment compares drawpyo's object model with native draw.io XML editing. The [diagram guide](../Diagram-Guide.md) defines the project workflow, and the [tool instructions](../../misc/tools/diagrams/README.md) describe its commands.

Drawpyo can simplify object construction, but object construction alone does not determine diagram quality. Both approaches need deliberate content, table shapes, layout, typography, connector routing, and rendered inspection. A library comparison must also check whether imports preserve direct edits.

This assessment uses the published drawpyo 0.2.5 wheel and its matching source tag. [PyPI](https://pypi.org/project/drawpyo/0.2.5/) specifies Python 3.10 or later. The source uses the [MIT license](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/LICENSE). On 2026-10-28, PyPI listed 0.2.5, while GitHub also had tag 0.2.6 and later changes on `main`. Examples from current documentation can therefore exceed the installed release.

## What the library supplies

| Requirement | Support in 0.2.5 |
| --- | --- |
| Shapes and containment | `File`, `Page`, and `Object` represent diagram elements. Parent/child objects keep positions relative to their containers. `Group` moves several objects together in Python; it does not create a Draw.io group. [Object source](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/src/drawpyo/diagram/objects.py) defines both behaviours. |
| Table fields | The [`List` object](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/src/drawpyo/diagram/extended_objects.py) provides a title and stacked child rows. A table with separate key, name, and type columns still needs a small project component. [`ClassDiagram`](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/src/drawpyo/diagram_types/class_diagram.py) is unfinished and is not an ERD generator. |
| Relationships | [`Edge`](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/src/drawpyo/diagram/edges.py) accepts source/target objects, labels, connection positions, and explicit intermediate points. Its [style database](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/src/drawpyo/formatting_database/edge_styles.toml) includes orthogonal and entity-relation routing plus crow's-foot endpoints such as `ERmandOne` and `ERzeroToMany`. |
| Placement | [`TreeDiagram.auto_layout()`](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/src/drawpyo/diagram_types/tree.py) arranges hierarchical trees. It is not a general layout engine for ERDs or data flows with multiple parents and cycles. These views need explicit placement rules or another layout engine. |
| Styles and icons | [`TextFormat`](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/src/drawpyo/diagram/text_format.py) exposes font settings. [TOML libraries](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/docs/usage/shape_libs.md), attribute dictionaries, and Draw.io style strings support reusable styles and custom shapes. Native XML icon-library imports are absent from 0.2.5; [0.2.6 adds them](https://github.com/MerrimanInd/drawpyo/compare/0.2.5...0.2.6). |

## Existing diagrams and manual edits

Version 0.2.5 includes `load_diagram()`, but its [parser](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/src/drawpyo/drawio_import/drawio_parser.py) does not preserve a complete file. It collects cells from every page into one dictionary keyed by cell ID. If two pages reuse an ID, the later cell replaces the earlier one. It does not decode compressed pages, retain page settings, or preserve all geometry and metadata. Loading and saving a multi-page native file through this parser can therefore discard content.

Version 0.2.6 and [later changes on main](https://github.com/MerrimanInd/drawpyo/compare/0.2.6...98e66337caa35eca2f23bab367c0fabcce8ab40b) improve imports. Their presence does not establish lossless round trips for this project. The [session log](../Status.md#session-log) records a successful generation-and-export probe with 0.2.5 and a failed import of the existing two-page file.

The [`File.write()` method](https://github.com/MerrimanInd/drawpyo/blob/0.2.5/src/drawpyo/file.py) overwrites by default. Generating a new file from Python replaces manual edits unless those edits are incorporated into the input or maintained in a separate layout definition. A successful generation probe establishes basic compatibility. It does not establish reliable imports or better visual results.
