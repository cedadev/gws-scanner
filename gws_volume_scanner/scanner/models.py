"""Elasticsearch Model for the GWS Scanner."""
import collections
import datetime as dt
import multiprocessing as mp
import os
import typing
import warnings

import elasticsearch_dsl as esd

from ..client import queries
from . import categorize, errors


def dict_count_size() -> typing.Dict[str, int]:
    """Construct a dict of size and count."""
    return {"count": 0, "size": 0}


def dict_count_size_heat() -> typing.Dict[str, typing.Any]:
    """Construct a dict of size, count and heat."""
    return {
        "count": 0,
        "size": 0,
        "heat_bins": collections.defaultdict(dict_count_size),
    }


ES_PATHTYPE = esd.Keyword(
    fields={
        "tree": esd.Text(
            analyzer=esd.analyzer(
                "path_tree",
                tokenizer=esd.tokenizer("path_tokenizer", type="path_hierarchy"),
            ),
            fielddata=True,
        ),
        "reverse_tree": esd.Text(
            analyzer=esd.analyzer(
                "reverse_path_tree",
                tokenizer=esd.tokenizer(
                    "reverse_path_tokenizer", type="path_hierarchy", reverse=True
                ),
            ),
            fielddata=True,
        ),
    },
)


class File(esd.Document):
    """This class defines the shape of how GWS data is stored in in elasticsearch."""

    __lock = mp.RLock()

    class Meta:
        dynamic = esd.MetaField("strict")

    path = ES_PATHTYPE
    scan_id = esd.Keyword()

    start_timestamp = esd.Date()

    size = esd.Long()
    count = esd.Long()

    owner = esd.Text()
    atime = esd.Date()

    includes_children = esd.Boolean()

    filetype = esd.Keyword()

    mean_heat = esd.Long()

    filetypes = esd.Object(dynamic=True)
    size_bins = esd.Object(dynamic=True)
    heat_bins = esd.Object(dynamic=True)
    users = esd.Object(dynamic=True)

    def __init__(
        self,
        path: str,
        start_timestamp: dt.datetime,
        scan_id: str,
        stat_: typing.Optional[os.stat_result] = None,
    ) -> None:
        """Elasticsearch File Object Constructor.

        Takes a path and gets all the required information about a file or folder.
        Optionally pass an existing stat result.
        """
        try:
            if stat_ is None:
                stat_ = os.lstat(path)
        except (FileNotFoundError, PermissionError) as err:
            warnings.warn(errors.FileNotFoundWarning(err))
        else:
            with self.__lock:
                kwargs: typing.Dict[str, typing.Any] = {}
                # Because of how the base class works, we cannot set these ourselves.
                # Cobnstruct arguments to pass to the constructor instead.

                # Nasty workaround to make sure no bad characters get to elasticsearch.
                # This will limit the character set to Latin1, and probably print weird
                # things if other character sets are in the input.
                kwargs["path"] = (
                    (str(path).rstrip("/"))
                    .encode("utf-8", "surrogateescape")
                    .decode("ISO-8859-1")
                )

                kwargs["size"] = stat_.st_size
                username = categorize.username_from_uid(stat_.st_uid)
                kwargs["owner"] = username
                kwargs["count"] = 1

                atime = dt.datetime.fromtimestamp(stat_.st_atime)
                kwargs["atime"] = atime

                ftype = categorize.detect_filetype(path, stat_)
                kwargs["filetype"] = ftype

                kwargs["includes_children"] = False

                # We don't know what chidren of these objects will exist yet.
                # While all this information is also collected above, if
                # we don't do it here we won't know about aggregated children.
                kwargs["filetypes"] = collections.defaultdict(dict_count_size)
                kwargs["size_bins"] = collections.defaultdict(dict_count_size)
                kwargs["heat_bins"] = collections.defaultdict(dict_count_size)
                kwargs["users"] = collections.defaultdict(dict_count_size)

                # Add the values for this object. Children might be agregated later.
                # Types.
                kwargs["filetypes"][ftype]["count"] += 1
                kwargs["filetypes"][ftype]["size"] += stat_.st_size

                # Size bins.
                bin_ = categorize.get_size_bin(stat_.st_size)
                kwargs["size_bins"][bin_]["count"] += 1
                kwargs["size_bins"][bin_]["size"] += stat_.st_size

                # Heat.
                heat = categorize.get_time_bin(atime)
                kwargs["heat_bins"][heat]["count"] += 1
                kwargs["heat_bins"][heat]["size"] += stat_.st_size

                # Users.
                kwargs["users"][username]["count"] += 1
                kwargs["users"][username]["size"] += stat_.st_size

                # Add a timestamp to the object in elasticsearch.
                kwargs["start_timestamp"] = start_timestamp
                kwargs["scan_id"] = scan_id

                # Calculate the age of the object. This will become a mean if children
                # Are incorprated.
                kwargs["mean_heat"] = (start_timestamp - atime).total_seconds()

                super().__init__(**kwargs)

    def incorporate_child(
        self, path: str, stat_: typing.Optional[os.stat_result] = None
    ) -> None:
        """Agregate the information about another file into this file."""
        try:
            if stat_ is None:
                stat_ = os.lstat(path)
        except (FileNotFoundError, PermissionError) as err:
            warnings.warn(errors.FileNotFoundWarning(err))
        else:
            with self.__lock:
                self.size += stat_.st_size
                self.count += 1

                ftype = categorize.detect_filetype(path, stat_)
                self.filetypes[ftype]["count"] += 1
                self.filetypes[ftype]["size"] += stat_.st_size

                bin_ = categorize.get_size_bin(stat_.st_size)
                self.size_bins[bin_]["count"] += 1
                self.size_bins[bin_]["size"] += stat_.st_size

                heat = categorize.get_time_bin(
                    dt.datetime.fromtimestamp(stat_.st_atime)
                )
                self.heat_bins[heat]["count"] += 1
                self.heat_bins[heat]["size"] += stat_.st_size

                username = categorize.username_from_uid(stat_.st_uid)
                self.users[username]["count"] += 1
                self.users[username]["size"] += stat_.st_size

                # Add this object's age into the mean.
                atime = dt.datetime.fromtimestamp(stat_.st_atime)
                age = (self.start_timestamp - atime).total_seconds()
                self.mean_heat = (
                    (self.mean_heat * (self.count - 1)) + age
                ) / self.count

                self.includes_children = True


class Volume(esd.Document):
    """Elasticsearch document model to store a timeseries of volume information."""

    class Meta:
        dynamic = esd.MetaField("strict")

    path = ES_PATHTYPE
    start_timestamp = esd.Date()
    end_timestamp = esd.Date()

    # Information for tracing the scan progress.
    length = esd.Double()
    status = esd.Keyword()

    # This information is equivalant to df.
    vol_size = esd.Long()
    vol_size_avail = esd.Long()
    vol_size_used = esd.Long()

    vol_count = esd.Long()
    vol_count_avail = esd.Long()
    vol_count_used = esd.Long()

    fs_type = esd.Keyword()
    fs_spec = esd.Keyword()

    # This information is the result of the scan.
    size = esd.Long()
    count = esd.Long()

    mean_heat = esd.Long()

    @classmethod
    def new(
        cls,
        path: str,
    ):
        """Initialise a volume information document."""
        kwargs: typing.Dict[str, typing.Any] = {}

        kwargs["start_timestamp"] = dt.datetime.now()
        kwargs["status"] = "in_progress"

        kwargs["path"] = path

        return cls(**kwargs)

    def add_volume_information(self) -> None:
        """Do a df on the volume in question and incorporate that information into the document."""
        # Force filesystem to mount.
        os.scandir(self.path)

        statvfs = os.statvfs(self.path)
        # This is a copy of what the python builtin shutil.disk_usage does.
        # Not calling that libray because I want the statvfs object to record the counts too.
        # Note that free is the total free, avail is that available to non-root users.
        self.vol_size = statvfs.f_blocks * statvfs.f_frsize
        self.vol_size_avail = statvfs.f_bavail * statvfs.f_frsize
        self.vol_size_used = (statvfs.f_blocks - statvfs.f_bfree) * statvfs.f_frsize

        self.vol_count = statvfs.f_files
        self.vol_count_avail = statvfs.f_favail
        self.vol_count_used = statvfs.f_files - statvfs.f_ffree

        # Parse /proc/mounts to get some information about this filesysten
        try:
            fs_info = categorize.filesystem_info(self.path)
        except FileNotFoundError:
            # MacOS does not have /proc
            self.fs_type = "__unknown_fs_type__"
            self.fs_spec = "__unknown_fs_spec__"
        else:
            self.fs_type = fs_info["fs_vfstype"]
            self.fs_spec = fs_info["fs_spec"]

    def add_endofscan(self, scan_result_index: str) -> None:
        """Aggregate the results of a scan into the long-term volume stats."""
        size_count_q = queries.count_size(self.path, scan_result_index, self.meta.id)
        self.size = int(size_count_q["aggregations"]["count_size"]["size"]["value"])
        self.count = int(size_count_q["aggregations"]["count_size"]["count"]["value"])

        self.status = "complete"
        self.end_timestamp = dt.datetime.now()
        self.length = (self.end_timestamp - self.start_timestamp).total_seconds()


class GranularRecord(esd.Document):
    """A record for storing per-category per-scan size and counts."""

    class Meta:
        dynamic = esd.MetaField("strict")

    path = ES_PATHTYPE
    scan_id = esd.Keyword()
    category = esd.Keyword()

    start_timestamp = esd.Date()
    end_timestamp = esd.Date()

    identifier = esd.Keyword()
    size = esd.Long()
    count = esd.Long()

    mean_heat = esd.Long()
