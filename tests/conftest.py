"""Test configuration and global fixtures."""
# nosec
import mimetypes
import pathlib
import random
import string
import typing

import pytest


def name(length: int = 12) -> str:
    return "".join(random.choice(string.ascii_lowercase) for i in range(length))


def create_subtree(
    path: pathlib.Path,
    level: int = 0,
    count: int = 10,
    max_level: int = 5,
    max_size: int = 5**8,
) -> typing.Tuple[typing.Set[pathlib.Path], typing.Set[pathlib.Path]]:
    files = set()
    folders = set()
    for _ in range(count):
        foldername = name()
        randint = random.randint(0, 2)
        if randint == 0:
            # Empty directory.
            filename = path / foldername
            filename.mkdir()
            folders.add(filename)
        elif (level < max_level) and randint == 1:
            # Nested directory.
            filename = path / foldername
            filename.mkdir()
            nestedfolders, nestedfiles = create_subtree(
                path / foldername,
                level=level + 1,
                count=count,
                max_level=max_level,
            )
            folders |= nestedfolders
            files |= nestedfiles
            folders.add(filename)
        elif randint == 2:
            # File of type.
            extension = random.choice(list(mimetypes.types_map.keys()))
            filename = path / (foldername + extension)
            with open(filename, "wb") as thefile:
                thefile.truncate(random.randint(1, max_size))
            files.add(filename)
    return (folders, files)


FileTestTreeInfo = typing.Tuple[pathlib.Path, typing.Set[pathlib.Path], typing.Set[pathlib.Path]]


@pytest.fixture(scope="session")
def file_tree(
    tmp_path_factory,
) -> FileTestTreeInfo:
    """Create and populate a fake file tree to scan."""
    path = tmp_path_factory.mktemp("filetree")
    folder, file = create_subtree(path)
    return (path, folder, file)
