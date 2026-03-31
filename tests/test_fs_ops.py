"""Tests for polars_fs_ops file system operations."""

import os
import sys
import tempfile

import polars as pl
import pytest

from polars_fs_ops import (
    check_valid_parent_dir,
    cp_file,
    cpx_file,
    file_exists,
    ls_dir,
    mv_file,
    rm_file,
    uucp_file,
    uumv_file,
)


@pytest.fixture
def tmp_dir():
    """Create a temporary directory that is cleaned up after the test."""
    with tempfile.TemporaryDirectory() as d:
        yield d


def _create_file(path: str, content: str = "hello") -> str:
    """Create a file with the given content and return its path.

    Args:
        path: Absolute path for the new file.
        content: Text content to write.

    Returns:
        The same path that was passed in.
    """
    with open(path, "w") as f:
        f.write(content)
    return path


# ── file_exists ──────────────────────────────────────────────────────────────


class TestFileExists:
    """Tests for the file_exists expression."""

    def test_existing_file(self, tmp_dir: str):
        """Return True for an existing file path."""
        path = _create_file(os.path.join(tmp_dir, "a.txt"))
        df = pl.DataFrame({"fp": [path]})
        result = df.select(file_exists("fp"))
        assert result["fp"].to_list() == [True]

    def test_non_existing_file(self):
        """Return False for a missing file path."""
        df = pl.DataFrame({"fp": ["/tmp/__nonexistent_file_xyz__"]})
        result = df.select(file_exists("fp"))
        assert result["fp"].to_list() == [False]

    def test_existing_directory(self, tmp_dir: str):
        """Return False for directory paths."""
        df = pl.DataFrame({"fp": [tmp_dir]})
        result = df.select(file_exists("fp"))
        assert result["fp"].to_list() == [False]

    def test_multiple_paths(self, tmp_dir: str):
        """Evaluate file existence row by row."""
        existing = _create_file(os.path.join(tmp_dir, "a.txt"))
        df = pl.DataFrame({"fp": [existing, "/tmp/__nonexistent__"]})
        result = df.select(file_exists("fp"))
        assert result["fp"].to_list() == [True, False]

    def test_null_value(self):
        """Propagate null input values."""
        df = pl.DataFrame({"fp": [None]}, schema={"fp": pl.String})
        result = df.select(file_exists("fp"))
        assert result["fp"].to_list() == [None]


class TestCheckValidParentDir:
    """Tests for the check_valid_parent_dir expression."""

    def test_valid_parent_dir(self, tmp_dir: str):
        """Return True when the parent directory exists."""
        path = os.path.join(tmp_dir, "nonexistent.txt")
        df = pl.DataFrame({"fp": [path]})
        result = df.select(check_valid_parent_dir("fp"))
        assert result["fp"].to_list() == [True]

    def test_invalid_parent_dir(self, tmp_dir: str):
        """Return False when the parent directory is missing."""
        path = os.path.join(tmp_dir, "nonexistent_dir", "nonexistent.txt")
        df = pl.DataFrame({"fp": [path]})
        result = df.select(check_valid_parent_dir("fp"))
        assert result["fp"].to_list() == [False]

    def test_null_value(self):
        """Propagate null input values."""
        df = pl.DataFrame({"fp": [None]}, schema={"fp": pl.String})
        result = df.select(check_valid_parent_dir("fp"))
        assert result["fp"].to_list() == [None]


# ── cp_file ──────────────────────────────────────────────────────────────────


class TestCpFile:
    """Tests for the cp_file expression."""

    def test_copy_file_to_file(self, tmp_dir: str):
        """Copy a file to a new file path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)

    def test_copy_file_to_dir(self, tmp_dir: str):
        """Reject directory targets for std::fs copies."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst_dir = os.path.join(tmp_dir, "dest_dir")
        os.makedirs(dst_dir)
        df = pl.DataFrame({"src": [src], "dst": [dst_dir]})
        # cp_file uses check_file_to_file, which returns False for file-to-dir
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [False]

    def test_copy_success(self, tmp_dir: str):
        """Copy file contents to the destination path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)
        with open(dst) as f:
            assert f.read() == "data"
        # source still exists after copy
        assert os.path.exists(src)

    def test_copy_nonexistent_source(self, tmp_dir: str):
        """Return False for a missing source path."""
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": ["/tmp/__no_such_file__"], "dst": [dst]})
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [False]

    def test_copy_multiple(self, tmp_dir: str):
        """Copy multiple source and destination pairs."""
        src1 = _create_file(os.path.join(tmp_dir, "a.txt"), "aaa")
        src2 = _create_file(os.path.join(tmp_dir, "b.txt"), "bbb")
        dst1 = os.path.join(tmp_dir, "a_copy.txt")
        dst2 = os.path.join(tmp_dir, "b_copy.txt")
        df = pl.DataFrame({"src": [src1, src2], "dst": [dst1, dst2]})
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [True, True]

    def test_copy_broadcast_single_dest(self, tmp_dir: str):
        """Support broadcasting a single destination expression."""
        src1 = _create_file(os.path.join(tmp_dir, "a.txt"), "aaa")
        dst = os.path.join(tmp_dir, "copy.txt")
        df = pl.DataFrame({"src": [src1]})
        result = df.select(cp_file("src", pl.lit(dst)))
        assert result["src"].to_list() == [True]

    def test_copy_invalid_paths(self, tmp_dir: str):
        """Return False for invalid source inputs."""
        # Empty string, None, and invalid path
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame(
            {"src": ["", None, "::invalid::"], "dst": [dst, dst, dst]},
            schema={"src": pl.String, "dst": pl.String},
        )
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [False, False, False]

    def test_copy_different_file_extensions(self, tmp_dir: str):
        """Allow copies across different file extensions."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.csv")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)

    def test_copy_no_file_extensions(self, tmp_dir: str):
        """Allow copies when neither path has an extension."""
        src = _create_file(os.path.join(tmp_dir, "src"), "data")
        dst = os.path.join(tmp_dir, "dst")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)

    def test_copy_same_path(self, tmp_dir: str):
        """Handle copying a file to the same path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        df = pl.DataFrame({"src": [src], "dst": [src]})
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [False]
        assert os.path.exists(src)
        with open(src) as f:
            assert f.read() == "data"

    def test_copy_dry_run_success(self, tmp_dir: str):
        """Report success without creating the destination file."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(cp_file("src", "dst", dry_run=True))
        assert result["src"].to_list() == [True]
        assert not os.path.exists(dst)
        # source still exists after dry-run copy
        assert os.path.exists(src)

    def test_copy_dry_run_invalid_paths(self, tmp_dir: str):
        """Return False for invalid inputs during dry runs."""
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame(
            {"src": ["", None, "::invalid::"], "dst": [dst, dst, dst]},
            schema={"src": pl.String, "dst": pl.String},
        )
        result = df.select(cp_file("src", "dst", dry_run=True))
        assert result["src"].to_list() == [False, False, False]


# ── mv_file ──────────────────────────────────────────────────────────────────


class TestMvFile:
    """Tests for the mv_file expression."""

    def test_move_file_to_file(self, tmp_dir: str):
        """Move a file to a new file path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(mv_file("src", "dst"))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)
        assert not os.path.exists(src)

    def test_move_preserve_ext_file_to_file(self, tmp_dir: str):
        """Reject differing extensions when preservation is enabled."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.csv")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(mv_file("src", "dst", True))
        assert result["src"].to_list() == [False]
        assert os.path.exists(src)
        assert not os.path.exists(dst)

    def test_move_file_to_dir(self, tmp_dir: str):
        """Move a file into an existing destination directory."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst_dir = os.path.join(tmp_dir, "dest_dir")
        moved_path = os.path.join(dst_dir, "src.txt")
        os.makedirs(dst_dir)
        df = pl.DataFrame({"src": [src], "dst": [dst_dir]})
        result = df.select(mv_file("src", "dst"))
        assert result["src"].to_list() == [True]
        assert not os.path.exists(src)
        assert os.path.exists(moved_path)
        with open(moved_path) as f:
            assert f.read() == "data"

    def test_move_success(self, tmp_dir: str):
        """Move file contents to the destination path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(mv_file("src", "dst"))
        assert result["src"].to_list() == [True]
        assert not os.path.exists(src)
        assert os.path.exists(dst)
        with open(dst) as f:
            assert f.read() == "data"

    def test_move_nonexistent_source(self, tmp_dir: str):
        """Return False for a missing source path."""
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": ["/tmp/__no_such_file__"], "dst": [dst]})
        result = df.select(mv_file("src", "dst"))
        assert result["src"].to_list() == [False]

    def test_move_invalid_paths(self, tmp_dir: str):
        """Return False for invalid move inputs."""
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame(
            {"src": ["", None, "::invalid::"], "dst": [dst, dst, dst]},
            schema={"src": pl.String, "dst": pl.String},
        )
        result = df.select(mv_file("src", "dst"))
        assert result["src"].to_list() == [False, False, False]

    def test_move_same_path(self, tmp_dir: str):
        """Handle moving a file to the same path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        df = pl.DataFrame({"src": [src], "dst": [src]})
        result = df.select(mv_file("src", "dst"))
        assert result["src"].to_list() == [False]
        assert os.path.exists(src)
        with open(src) as f:
            assert f.read() == "data"

    def test_move_dry_run_success(self, tmp_dir: str):
        """Report a successful move without changing the filesystem."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(mv_file("src", "dst", dry_run=True))
        assert result["src"].to_list() == [True]
        assert os.path.exists(src)
        assert not os.path.exists(dst)

    def test_move_dry_run_invalid_paths(self, tmp_dir: str):
        """Return False for invalid inputs during move dry runs."""
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame(
            {"src": ["", None, "::invalid::"], "dst": [dst, dst, dst]},
            schema={"src": pl.String, "dst": pl.String},
        )
        result = df.select(mv_file("src", "dst", dry_run=True))
        assert result["src"].to_list() == [False, False, False]


# ── rm_file ──────────────────────────────────────────────────────────────────


class TestRmFile:
    """Tests for the rm_file expression."""

    def test_remove_success(self, tmp_dir: str):
        """Remove an existing file."""
        path = _create_file(os.path.join(tmp_dir, "a.txt"))
        df = pl.DataFrame({"fp": [path]})
        result = df.select(rm_file("fp"))
        assert result["fp"].to_list() == [True]
        assert not os.path.exists(path)

    def test_remove_nonexistent(self):
        """Return False for a missing file."""
        df = pl.DataFrame({"fp": ["/tmp/__no_such_file__"]})
        result = df.select(rm_file("fp"))
        assert result["fp"].to_list() == [False]

    def test_remove_multiple(self, tmp_dir: str):
        """Remove multiple files in one expression."""
        p1 = _create_file(os.path.join(tmp_dir, "a.txt"))
        p2 = _create_file(os.path.join(tmp_dir, "b.txt"))
        df = pl.DataFrame({"fp": [p1, p2]})
        result = df.select(rm_file("fp"))
        assert result["fp"].to_list() == [True, True]
        assert not os.path.exists(p1)
        assert not os.path.exists(p2)

    def test_remove_invalid_paths(self, tmp_dir: str):
        """Handle invalid and null removal inputs."""
        df = pl.DataFrame({"fp": ["", None, "::invalid::"]}, schema={"fp": pl.String})
        result = df.select(rm_file("fp"))
        # Accept None for None input if that's the actual behavior
        assert result["fp"].to_list() == [False, None, False]

    def test_remove_dry_run_success(self, tmp_dir: str):
        """Report success without deleting the file."""
        path = _create_file(os.path.join(tmp_dir, "a.txt"))
        df = pl.DataFrame({"fp": [path]})
        result = df.select(rm_file("fp", dry_run=True))
        assert result["fp"].to_list() == [True]
        assert os.path.exists(path)

    def test_remove_dry_run_invalid_paths(self, tmp_dir: str):
        """Handle invalid and null inputs during dry runs."""
        df = pl.DataFrame({"fp": ["", None, "::invalid::"]}, schema={"fp": pl.String})
        result = df.select(rm_file("fp", dry_run=True))
        assert result["fp"].to_list() == [False, None, False]


# ── ls_dir ───────────────────────────────────────────────────────────────────


class TestLsDir:
    """Tests for the ls_dir expression."""

    def test_list_directory(self, tmp_dir: str):
        """Return directory entries for a populated folder."""
        _create_file(os.path.join(tmp_dir, "a.txt"))
        _create_file(os.path.join(tmp_dir, "b.txt"))
        df = pl.DataFrame({"dir": [tmp_dir]})
        result = df.select(ls_dir("dir"))
        entries = result["dir"][0].to_list()
        entry_names = [os.path.basename(e) for e in entries]
        assert "a.txt" in entry_names
        assert "b.txt" in entry_names

    def test_list_empty_directory(self, tmp_dir: str):
        """Return an empty list for an empty folder."""
        empty = os.path.join(tmp_dir, "empty")
        os.makedirs(empty)
        df = pl.DataFrame({"dir": [empty]})
        result = df.select(ls_dir("dir"))
        assert result["dir"][0].to_list() == []

    def test_list_nonexistent_directory(self):
        """Return null for a missing directory."""
        df = pl.DataFrame({"dir": ["/tmp/__no_such_dir_xyz__"]})
        result = df.select(ls_dir("dir"))
        assert result["dir"][0] is None

    def test_explode(self, tmp_dir: str):
        """Produce string rows after exploding list output."""
        _create_file(os.path.join(tmp_dir, "x.txt"))
        _create_file(os.path.join(tmp_dir, "y.txt"))
        df = pl.DataFrame({"dir": [tmp_dir]})
        result = df.select(ls_dir("dir")).explode("dir")
        assert result.shape[0] == 2
        assert result["dir"].dtype == pl.String


# ── uucp_file ────────────────────────────────────────────────────────────────


class TestUucpFile:
    """Tests for the uucp_file expression."""

    def test_copy_file_to_file(self, tmp_dir: str):
        """Copy a file to a destination file with uutils."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(uucp_file("src", "dst", False, False))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)

    def test_copy_file_to_dir(self, tmp_dir: str):
        """Copy a file into an existing destination directory with uutils."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst_dir = os.path.join(tmp_dir, "dest_dir")
        os.makedirs(dst_dir)
        df = pl.DataFrame({"src": [src], "dst": [dst_dir]})
        # uucp_file supports check_file_to_file || check_file_to_dir
        result = df.select(uucp_file("src", "dst", False, False))
        assert result["src"].to_list() == [True]
        assert os.path.exists(os.path.join(dst_dir, "src.txt"))

    def test_copy_success(self, tmp_dir: str):
        """Copy file contents into the destination directory."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "uucp data")
        dst_dir = os.path.join(tmp_dir, "dest")
        os.makedirs(dst_dir)
        df = pl.DataFrame({"src": [src], "dst": [dst_dir]})
        result = df.select(uucp_file("src", "dst", False, False))
        assert result["src"].to_list() == [True]
        assert os.path.exists(os.path.join(dst_dir, "src.txt"))

    def test_copy_nonexistent_source(self, tmp_dir: str):
        """Return False for a missing source path."""
        dst_dir = os.path.join(tmp_dir, "dest")
        os.makedirs(dst_dir)
        df = pl.DataFrame({"src": ["/tmp/__no_such_file__"], "dst": [dst_dir]})
        result = df.select(uucp_file("src", "dst", False, False))
        assert result["src"].to_list() == [False]

    def test_copy_different_file_extensions(self, tmp_dir: str):
        """Allow copies across different file extensions."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.csv")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(uucp_file("src", "dst"))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)

    def test_copy_same_path(self, tmp_dir: str):
        """Handle copying a file to the same path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        df = pl.DataFrame({"src": [src], "dst": [src]})
        result = df.select(uucp_file("src", "dst", False, False))
        assert result["src"].to_list() == [False]
        assert os.path.exists(src)
        with open(src) as f:
            assert f.read() == "data"

    def test_copy_dry_run_success(self, tmp_dir: str):
        """Report success without creating the copied file."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "uucp data")
        dst_dir = os.path.join(tmp_dir, "dest")
        os.makedirs(dst_dir)
        df = pl.DataFrame({"src": [src], "dst": [dst_dir]})
        result = df.select(uucp_file("src", "dst", False, True))
        assert result["src"].to_list() == [True]
        assert not os.path.exists(os.path.join(dst_dir, "src.txt"))


# ── uumv_file ────────────────────────────────────────────────────────────────


class TestUumvFile:
    """Tests for the uumv_file expression."""

    def test_move_file_to_file(self, tmp_dir: str):
        """Move a file to a destination file with uutils."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(uumv_file("src", "dst", True, progress_bar=False, dry_run=False))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)
        assert not os.path.exists(src)

    def test_move_preserve_ext_file_to_file(self, tmp_dir: str):
        """Reject differing extensions when preservation is enabled."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.csv")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(uumv_file("src", "dst", True, progress_bar=False, dry_run=False))
        assert result["src"].to_list() == [False]
        assert os.path.exists(src)
        assert not os.path.exists(dst)

    def test_move_file_to_dir(self, tmp_dir: str):
        """Move a file into an existing directory with uutils."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst_dir = os.path.join(tmp_dir, "dest_dir")
        os.makedirs(dst_dir)
        df = pl.DataFrame({"src": [src], "dst": [dst_dir]})
        moved_path = os.path.join(dst_dir, "src.txt")
        result = df.select(uumv_file("src", "dst", True, progress_bar=False, dry_run=False))
        assert result["src"].to_list() == [True]
        assert not os.path.exists(src)
        assert os.path.exists(moved_path)
        with open(moved_path) as f:
            assert f.read() == "data"

    def test_move_success(self, tmp_dir: str):
        """Move file contents to the destination path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "uumv data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(uumv_file("src", "dst", True, progress_bar=False, dry_run=False))
        assert result["src"].to_list() == [True]
        assert not os.path.exists(src)
        assert os.path.exists(dst)

    def test_move_nonexistent_source(self, tmp_dir: str):
        """Return False for a missing source path."""
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": ["/tmp/__no_such_file__"], "dst": [dst]})
        result = df.select(uumv_file("src", "dst", True, progress_bar=False, dry_run=False))
        assert result["src"].to_list() == [False]

    def test_move_same_path(self, tmp_dir: str):
        """Handle moving a file to the same path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "uumv data")
        df = pl.DataFrame({"src": [src], "dst": [src]})
        result = df.select(uumv_file("src", "dst", True, progress_bar=False, dry_run=False))
        assert result["src"].to_list() == [False]
        assert os.path.exists(src)
        with open(src) as f:
            assert f.read() == "uumv data"

    def test_move_dry_run_success(self, tmp_dir: str):
        """Report success without moving the file."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "uumv data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(uumv_file("src", "dst", True, progress_bar=False, dry_run=True))
        assert result["src"].to_list() == [True]
        assert os.path.exists(src)
        assert not os.path.exists(dst)


# ── cpx_file ─────────────────────────────────────────────────────────────────


class TestCpxFile:
    """Tests for the cpx_file expression."""

    @pytest.mark.skipif(sys.platform != "linux", reason="cpx_file is only supported on Linux")
    def test_copy_file_to_file(self, tmp_dir: str):
        """Copy a file to a destination file with cpx."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(cpx_file("src", "dst", 0))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)

    @pytest.mark.skipif(sys.platform != "linux", reason="cpx_file is only supported on Linux")
    def test_copy_file_to_dir(self, tmp_dir: str):
        """Copy a file into an existing destination directory with cpx."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst_dir = os.path.join(tmp_dir, "dest_dir")
        os.makedirs(dst_dir)
        df = pl.DataFrame({"src": [src], "dst": [dst_dir]})
        # cpx_file uses check_file_to_file, which returns True for file-to-dir
        result = df.select(cpx_file("src", "dst", 0))
        assert result["src"].to_list() == [True]

    @pytest.mark.skipif(sys.platform != "linux", reason="cpx_file is only supported on Linux")
    def test_copy_success(self, tmp_dir: str):
        """Copy file contents to the destination path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "cpx data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(cpx_file("src", "dst", 0))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)
        with open(dst) as f:
            assert f.read() == "cpx data"

    @pytest.mark.skipif(sys.platform != "linux", reason="cpx_file is only supported on Linux")
    def test_copy_nonexistent_source(self, tmp_dir: str):
        """Return False for a missing source path."""
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": ["/tmp/__no_such_file__"], "dst": [dst]})
        result = df.select(cpx_file("src", "dst", 0))
        assert result["src"].to_list() == [False]

    @pytest.mark.skipif(sys.platform != "linux", reason="cpx_file is only supported on Linux")
    def test_copy_different_file_extensions(self, tmp_dir: str):
        """Allow copies across different file extensions."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "data")
        dst = os.path.join(tmp_dir, "dst.csv")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(cpx_file("src", "dst"))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)

    @pytest.mark.skipif(sys.platform != "linux", reason="cpx_file is only supported on Linux")
    def test_copy_same_path(self, tmp_dir: str):
        """Handle copying a file to the same path."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "cpx data")
        df = pl.DataFrame({"src": [src], "dst": [src]})
        result = df.select(cpx_file("src", "dst", 0))
        assert result["src"].to_list() == [False]
        assert os.path.exists(src)
        with open(src) as f:
            assert f.read() == "cpx data"

    @pytest.mark.skipif(sys.platform != "linux", reason="cpx_file is only supported on Linux")
    def test_copy_dry_run_success(self, tmp_dir: str):
        """Report success without creating the destination file."""
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "cpx data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(cpx_file("src", "dst", 0, True))
        assert result["src"].to_list() == [True]
        assert not os.path.exists(dst)

    def test_cpx_file_not_supported(self, tmp_dir: str):
        """Raise on unsupported platforms."""
        from unittest.mock import patch

        with patch.object(sys, "platform", "win32"):
            with pytest.raises(NotImplementedError, match="cpx_file is only supported on Linux"):
                cpx_file("src", "dst", 0)
