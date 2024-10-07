"""Utilities for categorising scanned files."""

import bisect
import datetime as dt
import logging
import mimetypes
import os
import pwd
import stat
import typing

from .. import constants

logger = logging.getLogger()


def get_time_bin(datetime: dt.datetime) -> str:
    """Return the time bin of a file given it's atime."""
    values = [dt.datetime.now() - x["from"] for x in reversed(constants.TIME_BUCKETS)]
    keys = [x["key"] for x in reversed(constants.TIME_BUCKETS)]
    try:
        result = str(keys[bisect.bisect_right(values, datetime)])
    except IndexError:
        result = constants.TIME_BUCKETS[0]["key"]
        logger.warning(
            "get_time_bin given datatime which is could not categorise because it's in the future."
        )
    return result


def get_size_bin(size: int) -> str:
    """Return the size bin of a file given it's size."""
    values = [x["from"] for x in constants.SIZE_BUCKETS]
    keys = [x["key"] for x in constants.SIZE_BUCKETS]
    return str(keys[bisect.bisect_right(values, size)])


def detect_filetype(path: str, stat_: typing.Optional[os.stat_result] = None) -> str:
    """Given a file path, work out what type the file is."""
    # pylint: disable=R0912
    # Pylint complains that this function has too many branches.
    # I cannot think of a more efficient way of testing for all the
    # types of file that we need to.

    if stat_ is None:
        stat_ = os.lstat(path)

    # If a file is unknown, it mean's its not one of the types in the
    # big if/else below.
    ftype = "__unknown__"

    mode = stat_.st_mode
    # If the file type information is contained in it's mode,
    # it is much more accurate and efficient to use that information
    # first.
    if stat.S_ISDIR(mode):
        ftype = "__directory__"
    elif stat.S_ISCHR(mode):
        ftype = "__character_device__"
    elif stat.S_ISBLK(mode):
        ftype = "__block_device__"
    elif stat.S_ISFIFO(mode):
        ftype = "__named_pipe__"
    elif stat.S_ISLNK(mode):
        ftype = "__symlink__"
    elif stat.S_ISSOCK(mode):
        ftype = "__socket__"
    elif stat.S_ISDOOR(mode):
        ftype = "__door__"
    elif stat.S_ISPORT(mode):
        ftype = "__port__"
    elif stat.S_ISWHT(mode):
        ftype = "__whiteout__"
    elif stat.S_ISREG(mode):
        # Otherwise, if the file is a regular file, we can safely guess it's type from
        # it's extension. This does not inspect the file itself, only the path.
        guess = mimetypes.guess_type(path, strict=False)
        if guess[0] is None:
            ftype = "__unknown_file__"
        else:
            ftype = guess[0]
    return ftype.replace(".", "__")


def username_from_uid(uid: int) -> str:
    """Convert a UID to a username."""
    try:
        username = pwd.getpwuid(uid).pw_name.replace(".", "__")
    except KeyError:
        username = f"__unknown_uid_{uid}__"
    return username


def filesystem_info(path: str) -> typing.Dict[str, typing.Any]:
    """Parse /proc/mounts (which has fstab format) to find out the type of a filesystem."""
    # Get the data and split into a list of mounts.
    with open("/proc/mounts", "r") as f:
        procmounts_raw = f.read()
    procmounts = procmounts_raw.strip().split("\n")

    # Find all mount points that match our path.
    possiblemounts = {}
    for mnt in procmounts:
        items = mnt.split()
        if path.startswith(items[1]):
            possiblemounts[items[1]] = items

    # Find the longest mountpoint that matches our path.
    if len(possiblemounts) > 0:
        ordered = sorted(possiblemounts.keys(), key=len, reverse=True)
        longest_key = ordered[0]
        longest = possiblemounts[longest_key]
        return {
            "fs_spec": longest[0],
            "fs_file": longest[1],
            "fs_vfstype": longest[2],
            "fs_mntops": longest[3],
            "fs_freq": longest[4],
            "fs_passno": longest[5],
        }
    raise FileNotFoundError
