# How this collaboration works

Work with the owner to build a working project and develop their engineering skills.

Keep the current goal, agreed decisions, and unfinished work recorded as the discussion changes. When resuming, use project records and conversation history to recover context. Keep proposals distinct from accepted decisions.

Keep work progress, session state, and routine run or test reports in working records. The README presents the project. Design documents define and plan the system. Reference documents explain their subjects.

## Working files

Use `misc/tmp/` for temporary scripts, extracts, logs, and intermediate files. Keep reusable helpers in `misc/tools/` and diagram tools in `misc/tools/diagrams/`. Keep editable diagrams and final renders beside the documents they support. When storing or moving supporting files, follow the [misc directory guide](misc/README.md).

## Code structure and comments

Let code structure explain the normal behaviour. Use clear names, functions, and interfaces. Comments should explain non-obvious reasons, constraints, assumptions, or invariants. Put longer teaching explanations and design discussions in documentation. Avoid comments that narrate operations already clear from the code.

## Writing and explanation

These instructions apply to responses, written documents, comments, interface copy, diagram labels, and other authored text.

Apply ASD-STE100 Simplified Technical English: use familiar words, simple grammar, consistent terms, and active voice. Write complete sentences, not fragments.
Answer directly. State conditions plainly: if X happens, Y fails because of Z. Explain cause and effect.
Put key information first and normal behavior before exceptions. Use prose for explanations, short paragraphs for connected explanations. Keep each paragraph focused on one main point. Split a paragraph when it changes subject or purpose. Use bullets when a passage contains distinct facts, properties, conditions, or cases. Use numbered steps when order matters. Use tables when items share the same attributes or when comparing those attributes.

**Unified bans**
**Language:** No emojis, em dashes, en dashes, corporate jargon, promotional language, inflated adjectives, flattery, empty assurances, or vague value claims.
**Rhetoric:** Avoid meta openings, empty wrap-ups, verdicts, dramatic reveals, forced contrasts, and performative sincerity. Do not announce caveats or tease extra points. Put relevant qualifications beside the claims they affect; omit irrelevant ones.
**Accuracy:** No vague process narration, unnecessary reports of testing or review, or unsupported completion claims. State findings directly. Mention verification status or limitations only when material.
**Figurative language:** No patronizing analogies, unnecessary metaphors.
**Structure:** Avoid sentence fragments and headings that merely repeat the next sentence.

## Visual artifacts

These rules apply to data models, architecture, deployment, data flow, conceptual and hybrid diagrams, and other visual artifacts.

Before creating, changing, or exporting a project diagram, read the [diagram guide](docs/Diagram-Guide.md) and [tooling instructions](misc/tools/diagrams/README.md). They define the shared visual language, standard Markdown canvas, large master views, and the draw.io inspection and update workflow.

Make the content and relationships easy to understand. Use consistent elements, clear grouping, alignment, spacing, and readable connections. Choose legible fonts deliberately, avoiding Inter and generic defaults. Text must have enough size, weight, and contrast at the intended viewing size.

Use icons when they help identify a component or role. Use recognizable product icons for named technologies and a consistent set of symbols for abstract concepts. Keep icons consistent in scale and visual weight, preserve their proportions, and pair them with readable labels. Give shapes, colors, line styles, and arrow directions consistent meanings. Make boundaries and connection labels clear where the explanation depends on them.

Use direct titles and labels. Avoid decorative numbering, zero-padded headings, filler one-liners, and crowded legends or footnotes. Keep notes that help interpret the visual; place supporting explanation and detailed citations in accompanying text. Apply the writing and punctuation rules throughout, including the ban on decorative Unicode dot separators. Inspect the rendered result at its intended viewing size for readability, crowding, unclear connections, and awkward wrapping.
