"""Queries for filesystem data in elasticsearch."""
import copy
import typing

import elasticsearch_dsl as esd

from .. import constants

# Pull in the possible categories for different types of bucket.
HEAT_BUCKETS = [x["key"] for x in constants.TIME_BUCKETS]
SIZE_BUCKETS = [x["key"] for x in constants.SIZE_BUCKETS]


def _get_mapping(index: str, mapping_name: str) -> typing.Any:
    """Query the real categories currently in a mapping."""
    active_index = esd.Index(index)
    mapping = active_index.get_mapping()
    mappinglist = mapping[list(mapping.keys())[0]]["mappings"]["properties"]
    return mappinglist[mapping_name]["properties"].keys()


def _list_tree_above(path: str) -> str:
    """For a given path, let a list of all paths above it in the tree."""
    subpaths: list[str] = []
    for part in path.strip("/").split("/"):
        if len(subpaths) == 0:
            subpaths.append("/" + part)
        else:
            subpaths.append(subpaths[-1] + "/" + part)
    return subpaths


def scans(
    path: str,
    index: str,
    sort: tuple[str, ...] = ("status", "-end_timestamp", "-start_timestamp"),
) -> typing.Any:
    """Query the available scans for a file path."""
    subpaths = _list_tree_above(path)

    search = esd.Search(index=index)
    search = search.filter("terms", path__reverse_tree=subpaths)
    search = search.sort(*sort)
    search = search.extra(track_total_hits=True)

    total = search.count()
    search = search[0:total]
    return search.execute().to_dict()


def latest_scan_id(path: str, index: str) -> typing.Optional[str]:
    """Return the newest complete scan for a filepath."""
    all_scans = scans(path, index)["hits"]["hits"]
    if all_scans:
        return typing.cast(str, all_scans[0]["_id"])
    return None


def old_scan_ids(
    path: str, index: str, new: int = 3
) -> typing.Optional[typing.List[str]]:
    """Return all but the most recent n scan_ids for a fielpath."""
    all_scans = scans(path, index, sort=("-start_timestamp"))["hits"]["hits"]
    all_scans = all_scans[new:]
    if all_scans:
        old_scans = [x["_id"] for x in all_scans]
        return old_scans
    return None


def children(path: str, index: str, scan_id: str) -> typing.Any:
    """Query all direct children of a path."""
    # We're all about the aggregations.
    search = esd.Search(index=index).extra(size=0)

    # Add field labelling paths by their parent directly below `path`.
    search = search.extra(
        runtime_mappings={
            "agg_parent": esd.Keyword(
                # Note that the content of source is NOT PYTHON.
                # It is painless (ha!), elasticsearch's own scripting language.
                # .join().split() shennaigans are to strip the newlines.
                script={
                    "source": " ".join(
                        """
                        def path = doc['path'].value; /* The path of the file. */
                        def prefix = params['path']; /* The prefix we are looking for. */
                        if (path != null) {
                            /* Check file is in the path we are interested in */
                            if (path.startsWith(prefix)) {
                                /* Strip the prefix */
                                def trailing = path.substring(prefix.length());
                                if (trailing.length() > 0) {
                                    /* The first part of the remainder is the parent
                                    that we are interested in */
                                    def toplevel = trailing.splitOnToken('/')[1];
                                    emit(toplevel);
                                }
                            }
                        }
                    """.split()
                    ),
                    "params": {"path": path},
                }
            )
        }
    )

    # Filter to only the path tree we are looking at.
    # AND to only the scanner run we are looking at.
    paths = esd.A(
        "filter",
        bool={
            "must": [
                {"term": {"path.tree": path.rstrip("/")}},
                {"term": {"scan_id": scan_id}},
            ]
        },
    )

    # Calculate some information about the top level object.
    paths.metric("size", esd.A("sum", field="size"))
    paths.metric("count", esd.A("sum", field="count"))

    # Bucket into direct children.
    buckets = paths.bucket(
        "children",
        "terms",
        field="agg_parent",
        size=1000,
    )
    # Calculate some metrics for those buckets.
    buckets.metric("size", esd.A("sum", field="size"))
    buckets.metric("count", esd.A("sum", field="count"))
    buckets.metric(
        "mean_heat",
        esd.A("weighted_avg", value={"field": "mean_heat"}, weight={"field": "count"}),
    )

    search.aggs.bucket("path", paths)  # pylint: disable=no-member
    return search.execute().to_dict()  # pylint: disable=no-member


def hotness(path: str, index: str, scan_id: str) -> typing.Any:
    """Query the heat statistics for a path."""
    # We're all about the aggregations.
    search = esd.Search(index=index).extra(size=0)

    # Filter to only the path tree we are looking at.
    # AND to only the scanner run we are looking at.
    paths = esd.A(
        "filter",
        bool={
            "must": [
                {"term": {"path.tree": path.rstrip("/")}},
                {"term": {"scan_id": scan_id}},
            ]
        },
    )

    # Bucket into hotness.
    counts = copy.deepcopy(paths)
    sizes = copy.deepcopy(paths)
    for hot in HEAT_BUCKETS:
        sizes.metric(f"{hot}", esd.A("sum", field=f"heat_bins.{hot}.size"))
        counts.metric(f"{hot}", esd.A("sum", field=f"heat_bins.{hot}.count"))

    search.aggs.bucket("counts", counts)  # pylint: disable=no-member
    search.aggs.bucket("sizes", sizes)  # pylint: disable=no-member
    return search.execute().to_dict()  # pylint: disable=no-member


def filetypes(path: str, index: str, scan_id: str) -> typing.Any:
    """Query filetype statistics for a path."""
    # We're all about the aggregations.
    search = esd.Search(index=index).extra(size=0)

    # Filter to only the path tree we are looking at.
    # AND to only the scanner run we are looking at.
    paths = esd.A(
        "filter",
        bool={
            "must": [
                {"term": {"path.tree": path.rstrip("/")}},
                {"term": {"scan_id": scan_id}},
            ]
        },
    )

    # Bucket into filetypes.
    counts = copy.deepcopy(paths)
    sizes = copy.deepcopy(paths)
    for type_ in _get_mapping(index, "filetypes"):
        sizes.metric(f"{type_}", esd.A("sum", field=f"filetypes.{type_}.size"))
        counts.metric(f"{type_}", esd.A("sum", field=f"filetypes.{type_}.count"))

    search.aggs.bucket("counts", counts)  # pylint: disable=no-member
    search.aggs.bucket("sizes", sizes)  # pylint: disable=no-member
    return search.execute().to_dict()  # pylint: disable=no-member


def users(path: str, index: str, scan_id: str) -> typing.Any:
    """Query statics for users associated with a filepath."""
    # We're all about the aggregations.
    search = esd.Search(index=index).extra(size=0)

    # Filter to only the path tree we are looking at.
    # AND to only the scanner run we are looking at.
    paths = esd.A(
        "filter",
        bool={
            "must": [
                {"term": {"path.tree": path.rstrip("/")}},
                {"term": {"scan_id": scan_id}},
            ]
        },
    )
    # Bucket into users.
    counts = copy.deepcopy(paths)
    sizes = copy.deepcopy(paths)
    for user in _get_mapping(index, "users"):
        sizes.metric(f"{user}", esd.A("sum", field=f"users.{user}.size"))
        counts.metric(f"{user}", esd.A("sum", field=f"users.{user}.count"))

    search.aggs.bucket("counts", counts)  # pylint: disable=no-member
    search.aggs.bucket("sizes", sizes)  # pylint: disable=no-member
    return search.execute().to_dict()  # pylint: disable=no-member


def count_size(path: str, index: str, scan_id: str) -> typing.Any:
    """Query basic size and count of a filepath."""
    # We're all about the aggregations.
    search = esd.Search(index=index).extra(size=0)

    # Filter to only the path tree we are looking at.
    # AND to only the scanner run we are looking at.
    paths = esd.A(
        "filter",
        bool={
            "must": [
                {"term": {"path.tree": path.rstrip("/")}},
                {"term": {"scan_id": scan_id}},
            ]
        },
    )

    paths.metric("size", esd.A("sum", field="size"))
    paths.metric("count", esd.A("sum", field="count"))

    search.aggs.bucket("count_size", paths)  # pylint: disable=no-member
    return search.execute().to_dict()  # pylint: disable=no-member


def children_records(path: str, index: str, scan_id: str) -> typing.Any:
    """Query all direct children of a path."""
    # We're all about the aggregations.
    search = esd.Search(index=index).extra(size=10000)

    search = search.filter("term", path__tree=path.rstrip("/"))
    search = search.filter("term", scan_id=scan_id)

    return search.execute().to_dict()  # pylint: disable=no-member
