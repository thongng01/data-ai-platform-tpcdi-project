"""Test setup for the pipeline tests.

Puts `pipelines/` on the import path so a test can `from dp1 import schemas`.

The generator's suite runs from `generator/` with the package installed
editable. This suite runs from the repository root against the pipeline
directories in place, because R12's single-package layout is recorded as
outstanding debt and re-laying-out working code mid-build is not worth it.

Only the modules that do not import PySpark are testable here. `schemas.py` and
`config.py` are pure Python, and they hold the contract that is easiest to break
silently: the column list per batch, and the business key per batch.
"""

import sys
from pathlib import Path

PIPELINES = Path(__file__).resolve().parent.parent / "pipelines"

if str(PIPELINES) not in sys.path:
    sys.path.insert(0, str(PIPELINES))
