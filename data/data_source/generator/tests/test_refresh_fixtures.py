"""Keep format fixture coverage intact when a required upstream source is absent."""

from contextlib import nullcontext
import io
import json
from pathlib import Path

import pytest

from data.data_source.generator.tools import refresh_fixtures


@pytest.fixture
def isolated_outputs(tmp_path, monkeypatch):
    """Redirect every fixture output to a private tree with known existing bytes."""
    output = tmp_path / "outputs"
    fixtures = output / "tpcdi_sf3"
    manifest = output / "manifest.json"
    for relative in refresh_fixtures.SLICES:
        path = fixtures / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(f"existing fixture: {relative}\r\n".encode("ascii"))
    manifest.write_bytes(b'{"existing_manifest": true}\n')
    (output / "keep.bin").write_bytes(b"unrelated output\x00\xff")
    monkeypatch.setattr(refresh_fixtures, "GENERATOR_ROOT", output)
    monkeypatch.setattr(refresh_fixtures, "FIXTURE_ROOT", fixtures)
    monkeypatch.setattr(refresh_fixtures, "MANIFEST_PATH", manifest)
    return output


def _snapshot(root):
    """Capture every output file's bytes so partial replacement cannot go unnoticed."""
    return {str(path.relative_to(root)): path.read_bytes() for path in root.rglob("*") if path.is_file()}


def _reference(root, missing=None, directory=None):
    """Create all required fake source files except an optional absent or invalid one."""
    for relative in refresh_fixtures.SLICES:
        path = root / "sf_3" / "output_data" / relative
        if relative == missing:
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        if relative == directory:
            path.mkdir()
        else:
            path.write_bytes(b"first|one|1\r\nsecond|two|2\r\nthird|three|3\r\n")
    return root


def _assert_preflight_failure(arguments, output):
    """Require a nonzero failure and unchanged outputs, even if failure comes late."""
    before = _snapshot(output)
    try:
        with pytest.raises(SystemExit) as failure:
            refresh_fixtures.main(arguments)
    finally:
        assert _snapshot(output) == before
    assert failure.value.code != 0


@pytest.mark.parametrize("relative", tuple(refresh_fixtures.SLICES))
def test_missing_required_file_preserves_every_fixture_and_manifest(tmp_path, isolated_outputs, relative):
    """Every required source, including the last one, fails before any output write."""
    root = _reference(tmp_path / "reference", missing=relative)
    _assert_preflight_failure([str(root)], isolated_outputs)


def test_nonexistent_explicit_reference_preserves_outputs(tmp_path, isolated_outputs):
    """An explicit missing checkout cannot replace the manifest with an empty one."""
    _assert_preflight_failure([str(tmp_path / "absent")], isolated_outputs)


def test_nonexistent_default_reference_preserves_outputs(tmp_path, isolated_outputs, monkeypatch):
    """The default checkout must also exist before fixture refresh can start."""
    monkeypatch.setattr(refresh_fixtures, "DEFAULT_REFERENCE", tmp_path / "absent-default")
    _assert_preflight_failure([], isolated_outputs)


def test_required_path_must_be_a_regular_file(tmp_path, isolated_outputs):
    """A directory at a required source path fails preflight before earlier writes."""
    relative = tuple(refresh_fixtures.SLICES)[-1]
    root = _reference(tmp_path / "reference", directory=relative)
    _assert_preflight_failure([str(root)], isolated_outputs)


def test_missing_sources_do_not_create_an_output_tree(tmp_path, monkeypatch):
    """An incomplete reference cannot create even a new fixture directory."""
    output = tmp_path / "new-output"
    monkeypatch.setattr(refresh_fixtures, "FIXTURE_ROOT", output / "tpcdi_sf3")
    monkeypatch.setattr(refresh_fixtures, "MANIFEST_PATH", output / "manifest.json")
    with pytest.raises(SystemExit):
        refresh_fixtures.main([str(tmp_path / "absent")])
    assert not output.exists()


def test_complete_reference_writes_all_required_slices(tmp_path, isolated_outputs):
    """Successful refresh retains the complete 13-file contract and exact CRLF bytes."""
    root = _reference(tmp_path / "reference")
    assert refresh_fixtures.main([str(root)]) == 0
    manifest = json.loads((isolated_outputs / "manifest.json").read_text())
    assert len(manifest["files"]) == 13
    assert set(manifest["files"]) == set(refresh_fixtures.SLICES)
    source_lines = [b"first|one|1\r\n", b"second|two|2\r\n", b"third|three|3\r\n"]
    for relative, limit in refresh_fixtures.SLICES.items():
        actual = (isolated_outputs / "tpcdi_sf3" / relative).read_bytes()
        assert actual == b"".join(source_lines[:limit])
        assert manifest["files"][relative]["lines"] == min(limit, 3)
        assert manifest["files"][relative]["fields"] == 3
    assert (isolated_outputs / "keep.bin").read_bytes() == b"unrelated output\x00\xff"


def test_slice_reads_only_requested_lines(monkeypatch):
    """Extract a bounded prefix instead of reading a large upstream file into memory."""
    stream = io.BytesIO(b"first|1\r\nsecond|2\r\n" + b"unused|3\r\n" * 10000)

    def open_prefix(path, *args, **kwargs):
        """Supply a trackable binary stream without closing it after extraction."""
        return nullcontext(stream)

    def reject_whole_file(path):
        """Fail the old whole-file read even when the requested prefix is small."""
        raise AssertionError("Fixture slicing must not read the whole source file.")

    monkeypatch.setattr(Path, "open", open_prefix)
    monkeypatch.setattr(Path, "read_bytes", reject_whole_file)
    assert refresh_fixtures.slice_lines(Path("source.txt"), 2) == b"first|1\r\nsecond|2\r\n"
    assert stream.tell() == len(b"first|1\r\nsecond|2\r\n")


def test_slice_keeps_nonempty_crlf_line_behavior(tmp_path):
    """Empty records do not consume the requested count; a short file stays usable."""
    source = tmp_path / "source.txt"
    source.write_bytes(b"\r\nfirst|1\r\n\r\nsecond|2")
    assert refresh_fixtures.slice_lines(source, 5) == b"first|1\r\nsecond|2\r\n"
