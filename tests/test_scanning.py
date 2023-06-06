"""Tests which check the scanner results are the same as those on disk."""
import datetime as dt
import os
import pathlib
import random
import subprocess as sp

from gws_volume_scanner.scanner import models

from . import conftest


def simple_path_walk(path: pathlib.Path) -> models.File:
    """Do a simple walk incorporating all children of a path into the object."""
    fileobj = models.File(str(path), dt.datetime.now(), "test_scan_id")
    for dirpath, dirnames, filenames in os.walk(path):
        for dir_ in dirnames:
            fileobj.incorporate_child(os.path.join(path, dirpath, dir_))
        for file in filenames:
            fileobj.incorporate_child(os.path.join(path, dirpath, file))
    return fileobj


def test_single_file_size(file_tree: conftest.FileTestTreeInfo) -> None:
    """Test that a single file's size is correctly identified."""
    file = random.choice(list(file_tree[2]))
    duresult = int(sp.run(["du", "-B1", file], capture_output=True, check=True).stdout.split()[0])
    fileobj = models.File(str(file), dt.datetime.now(), "test_scan_id")
    assert fileobj.size == duresult


def test_whole_tree_size(file_tree: conftest.FileTestTreeInfo) -> None:
    """Do a simple walk of the tree adding all children and check the size is the same as du."""
    path = file_tree[0]
    duresult = int(
        sp.run(["du", "-B1", "-s", path], capture_output=True, check=True).stdout.split()[0]
    )
    fileobj = simple_path_walk(path)
    assert fileobj.size == duresult


def test_whole_tree_count(file_tree: conftest.FileTestTreeInfo) -> None:
    """Do a simple walk of the tree adding all children and check the number of files is the same as find finds."""
    path = file_tree[0]
    findresult = len(
        sp.run(["find", path, "-print"], capture_output=True, check=True).stdout.splitlines()
    )
    fileobj = simple_path_walk(path)
    assert fileobj.count == findresult
