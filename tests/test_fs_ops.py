import os
import tempfile

import polars as pl
import pytest

from polars_fs_ops import (
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
    with tempfile.TemporaryDirectory() as d:
        yield d


def _create_file(path: str, content: str = "hello") -> str:
    with open(path, "w") as f:
        f.write(content)
    return path


# ── file_exists ──────────────────────────────────────────────────────────────


class TestFileExists:
    def test_existing_file(self, tmp_dir: str):
        path = _create_file(os.path.join(tmp_dir, "a.txt"))
        df = pl.DataFrame({"fp": [path]})
        result = df.select(file_exists("fp"))
        assert result["fp"].to_list() == [True]

    def test_non_existing_file(self):
        df = pl.DataFrame({"fp": ["/tmp/__nonexistent_file_xyz__"]})
        result = df.select(file_exists("fp"))
        assert result["fp"].to_list() == [False]

    def test_existing_directory(self, tmp_dir: str):
        df = pl.DataFrame({"fp": [tmp_dir]})
        result = df.select(file_exists("fp"))
        assert result["fp"].to_list() == [True]

    def test_multiple_paths(self, tmp_dir: str):
        existing = _create_file(os.path.join(tmp_dir, "a.txt"))
        df = pl.DataFrame({"fp": [existing, "/tmp/__nonexistent__"]})
        result = df.select(file_exists("fp"))
        assert result["fp"].to_list() == [True, False]

    def test_null_value(self):
        df = pl.DataFrame({"fp": [None]}, schema={"fp": pl.String})
        result = df.select(file_exists("fp"))
        assert result["fp"].to_list() == [None]


# ── cp_file ──────────────────────────────────────────────────────────────────


class TestCpFile:
    def test_copy_success(self, tmp_dir: str):
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
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": ["/tmp/__no_such_file__"], "dst": [dst]})
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [False]

    def test_copy_multiple(self, tmp_dir: str):
        src1 = _create_file(os.path.join(tmp_dir, "a.txt"), "aaa")
        src2 = _create_file(os.path.join(tmp_dir, "b.txt"), "bbb")
        dst1 = os.path.join(tmp_dir, "a_copy.txt")
        dst2 = os.path.join(tmp_dir, "b_copy.txt")
        df = pl.DataFrame({"src": [src1, src2], "dst": [dst1, dst2]})
        result = df.select(cp_file("src", "dst"))
        assert result["src"].to_list() == [True, True]

    def test_copy_broadcast_single_dest(self, tmp_dir: str):
        src1 = _create_file(os.path.join(tmp_dir, "a.txt"), "aaa")
        dst = os.path.join(tmp_dir, "copy.txt")
        df = pl.DataFrame({"src": [src1]})
        result = df.select(cp_file("src", pl.lit(dst)))
        assert result["src"].to_list() == [True]


# ── mv_file ──────────────────────────────────────────────────────────────────


class TestMvFile:
    def test_move_success(self, tmp_dir: str):
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
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": ["/tmp/__no_such_file__"], "dst": [dst]})
        result = df.select(mv_file("src", "dst"))
        assert result["src"].to_list() == [False]


# ── rm_file ──────────────────────────────────────────────────────────────────


class TestRmFile:
    def test_remove_success(self, tmp_dir: str):
        path = _create_file(os.path.join(tmp_dir, "a.txt"))
        df = pl.DataFrame({"fp": [path]})
        result = df.select(rm_file("fp"))
        assert result["fp"].to_list() == [True]
        assert not os.path.exists(path)

    def test_remove_nonexistent(self):
        df = pl.DataFrame({"fp": ["/tmp/__no_such_file__"]})
        result = df.select(rm_file("fp"))
        assert result["fp"].to_list() == [False]

    def test_remove_multiple(self, tmp_dir: str):
        p1 = _create_file(os.path.join(tmp_dir, "a.txt"))
        p2 = _create_file(os.path.join(tmp_dir, "b.txt"))
        df = pl.DataFrame({"fp": [p1, p2]})
        result = df.select(rm_file("fp"))
        assert result["fp"].to_list() == [True, True]
        assert not os.path.exists(p1)
        assert not os.path.exists(p2)


# ── ls_dir ───────────────────────────────────────────────────────────────────


class TestLsDir:
    def test_list_directory(self, tmp_dir: str):
        _create_file(os.path.join(tmp_dir, "a.txt"))
        _create_file(os.path.join(tmp_dir, "b.txt"))
        df = pl.DataFrame({"dir": [tmp_dir]})
        result = df.select(ls_dir("dir"))
        entries = result["dir"][0].to_list()
        entry_names = [os.path.basename(e) for e in entries]
        assert "a.txt" in entry_names
        assert "b.txt" in entry_names

    def test_list_empty_directory(self, tmp_dir: str):
        empty = os.path.join(tmp_dir, "empty")
        os.makedirs(empty)
        df = pl.DataFrame({"dir": [empty]})
        result = df.select(ls_dir("dir"))
        assert result["dir"][0].to_list() == []

    def test_list_nonexistent_directory(self):
        df = pl.DataFrame({"dir": ["/tmp/__no_such_dir_xyz__"]})
        result = df.select(ls_dir("dir"))
        assert result["dir"][0] is None

    def test_explode(self, tmp_dir: str):
        _create_file(os.path.join(tmp_dir, "x.txt"))
        _create_file(os.path.join(tmp_dir, "y.txt"))
        df = pl.DataFrame({"dir": [tmp_dir]})
        result = df.select(ls_dir("dir")).explode("dir")
        assert result.shape[0] == 2
        assert result["dir"].dtype == pl.String


# ── uucp_file ────────────────────────────────────────────────────────────────


class TestUucpFile:
    def test_copy_success(self, tmp_dir: str):
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "uucp data")
        dst_dir = os.path.join(tmp_dir, "dest")
        os.makedirs(dst_dir)
        df = pl.DataFrame({"src": [src], "dst": [dst_dir]})
        result = df.select(uucp_file("src", "dst", False))
        assert result["src"].to_list() == [True]
        assert os.path.exists(os.path.join(dst_dir, "src.txt"))

    def test_copy_nonexistent_source(self, tmp_dir: str):
        dst_dir = os.path.join(tmp_dir, "dest")
        os.makedirs(dst_dir)
        df = pl.DataFrame({"src": ["/tmp/__no_such_file__"], "dst": [dst_dir]})
        result = df.select(uucp_file("src", "dst", False))
        assert result["src"].to_list() == [False]


# ── uumv_file ────────────────────────────────────────────────────────────────


class TestUumvFile:
    def test_move_success(self, tmp_dir: str):
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "uumv data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(uumv_file("src", "dst", False))
        assert result["src"].to_list() == [True]
        assert not os.path.exists(src)
        assert os.path.exists(dst)

    def test_move_nonexistent_source(self, tmp_dir: str):
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": ["/tmp/__no_such_file__"], "dst": [dst]})
        result = df.select(uumv_file("src", "dst", False))
        assert result["src"].to_list() == [False]


# ── cpx_file ─────────────────────────────────────────────────────────────────


class TestCpxFile:
    def test_copy_success(self, tmp_dir: str):
        src = _create_file(os.path.join(tmp_dir, "src.txt"), "cpx data")
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": [src], "dst": [dst]})
        result = df.select(cpx_file("src", "dst", 0))
        assert result["src"].to_list() == [True]
        assert os.path.exists(dst)
        with open(dst) as f:
            assert f.read() == "cpx data"

    def test_copy_nonexistent_source(self, tmp_dir: str):
        dst = os.path.join(tmp_dir, "dst.txt")
        df = pl.DataFrame({"src": ["/tmp/__no_such_file__"], "dst": [dst]})
        result = df.select(cpx_file("src", "dst", 0))
        assert result["src"].to_list() == [False]
