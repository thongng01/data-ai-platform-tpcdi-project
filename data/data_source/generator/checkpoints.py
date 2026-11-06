"""Bind completed work to exact code and configuration before a run reuses it."""

import hashlib
import json
from pathlib import Path
import shutil


LAYOUT_KEYS = {"customer_group_size", "export_rows"}


def atomic_json(path, value):
    """Replace one JSON receipt only after its complete contents reach the file."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".partial")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    temporary.replace(path)


class Checkpoints:
    """Keep one run context and completion receipts separate from mutable work files."""

    def __init__(self, root, config, implementation_files, runtime_keys):
        """Refuse changed code, semantic settings, or intermediate layout before reuse."""
        self.root = Path(root)
        self.directory = self.root / "_checkpoints"
        self.context = {
            "implementation_files": implementation_files,
            "config": {key: value for key, value in config.as_dict().items()
                       if key not in runtime_keys or key in LAYOUT_KEYS},
        }
        # JSON stores tuples as lists. Compare its canonical value on both sides,
        # including when a caller constructed Config directly instead of loading it.
        self.context = json.loads(json.dumps(self.context))
        path = self.directory / "context.json"
        if path.exists():
            previous = json.loads(path.read_text(encoding="utf-8"))
            if previous.get("implementation_files") != implementation_files:
                raise ValueError("Cannot resume: implementation file hashes differ from the checkpoint.")
            if previous.get("config") != self.context["config"]:
                raise ValueError("Cannot resume: semantic or intermediate-layout configuration differs from the checkpoint.")
        else:
            atomic_json(path, self.context)
        self.digest = hashlib.sha256(json.dumps(self.context, sort_keys=True).encode()).hexdigest()

    def completion(self, result=None):
        """Identify the exact context that produced a sealed part or a step result."""
        return {"context_sha256": self.digest, "result": result}

    def read(self, name):
        """Return a matching completed step, or require the caller to redo it."""
        path = self.directory / f"{name}.json"
        if not path.exists():
            return None
        receipt = json.loads(path.read_text(encoding="utf-8"))
        return receipt if receipt.get("context_sha256") == self.digest else None

    def save(self, name, result=None):
        """Commit a step result after all of that step's files are complete."""
        atomic_json(self.directory / f"{name}.json", self.completion(result))

    def part(self, name):
        """Reuse only a sealed part with a matching context and all named files."""
        directory = self.root / "_work" / "source-parts" / name
        marker = directory / "complete.json"
        if not marker.exists():
            return None
        receipt = json.loads(marker.read_text(encoding="ascii"))
        if receipt.get("context_sha256") != self.digest:
            return None
        if any(Path(filename).name != filename or not (directory / filename).is_file()
               for filename in receipt["families"].values()):
            return None
        return receipt

    def discard_part(self, name):
        """Remove only one incomplete producer namespace before recreating it."""
        path = self.root / "_work" / "source-parts" / name
        if path.exists():
            shutil.rmtree(path)


def existing_checkpoints(root):
    """Open the saved run context for a group without rehashing code per group."""
    path = Path(root) / "_checkpoints" / "context.json"
    if not path.exists():
        return None
    context = json.loads(path.read_text(encoding="utf-8"))
    checkpoints = object.__new__(Checkpoints)
    checkpoints.root = Path(root)
    checkpoints.directory = path.parent
    checkpoints.context = context
    checkpoints.digest = hashlib.sha256(json.dumps(context, sort_keys=True).encode()).hexdigest()
    return checkpoints
