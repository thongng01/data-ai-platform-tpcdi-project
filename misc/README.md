# Supporting files

Keep reusable tools in Git. Put temporary work in `misc/tmp/`. Keep project documentation and finished diagrams under `docs/`.

| Path | Purpose | Git treatment |
| --- | --- | --- |
| `tools/` | Contains reusable checks, inspection scripts, and SQL queries in `tools/sql/`. | Git tracks these files. |
| `tools/diagrams/` | Contains diagram scripts and the [draw.io instructions](tools/diagrams/README.md). Install the draw.io application outside the repository. | Git tracks the scripts and instructions. |
| `tmp/` | Holds one-off scripts, extracted text, logs, downloads, and intermediate output. | Git ignores these files. |
| `archive/` | Preserves earlier document copies and handoffs for recovery. These are historical records, not current instructions or runnable migration scripts. | Git ignores these local copies. |
| `tpcdi/` | Holds the pinned upstream TPC-DI checkout and SF3 reference dataset. Existing fixture tools use this path. | Git ignores the checkout and data. |
| `generator-feasibility/` | Holds the pinned Databricks generator checkout and the original compatibility probe and its evidence. | Git ignores this local study material. |
| `sample_run/` | Holds the existing generator output that the current pipeline reads. | Git ignores the generated data. |

Use a task subdirectory under `tmp/` when a task produces several files. Move a useful helper into `tools/` before another task starts to depend on it. Record findings needed for future work in the relevant document before discarding temporary output.

An ignored file is not backed up by Git. Retain the local archives and reference data until their recovery or research value has ended. The archive copies preserve their original text, so their paths and instructions can be outdated.

Run the documentation checks from the repository root in WSL:

```bash
make check-docs
```

Some existing helpers operate services or write data. Read the helper before running it. Moving a helper into this directory does not establish that it works with the current platform.
