"""Module which does the scanning of the filesystem."""
import copy
import datetime as dt
import multiprocessing as mp
import os
import queue as queue_
import threading as th
import typing

from ..vendor import os as vos
from . import config, errors, models, util

ToScan = typing.Tuple[
    str, typing.Sequence[str], typing.Sequence[str], bool, bool, dt.datetime, str
]


def queuescan(
    path: str,
    queue: queue_.Queue[ToScan],
    config_: config.ScannerConfig,
    start_timestamp: dt.datetime,
    scan_id: str,
) -> None:
    """Walk the given path and places objects to scan in a queue."""
    gwsconfig = config_.gws_config(str(path))

    # Because we vendor the os module, it's type hints do not get picked up by mypy.
    for dirpath, dirnames, filenames in vos.walk(path):  # type: ignore[no-untyped-call]
        depth = len(dirpath.removeprefix(str(path)).strip("/").split("/"))

        walk_items = dirpath in gwsconfig["full_item_walk_dirs"]
        aggregate_subdirs = (
            (depth >= gwsconfig["scan_depth"])
            or (dirpath in gwsconfig["aggregate_subdir_paths"])
            or (
                dirpath.strip("/").rpartition("/")[2]
                in gwsconfig["aggregate_subdir_names"]
            )
        )

        if aggregate_subdirs:
            pass_dirnames = copy.deepcopy(dirnames)
            dirnames.clear()
        else:
            pass_dirnames = dirnames
        queue.put(
            (
                dirpath,
                pass_dirnames,
                filenames,
                walk_items,
                aggregate_subdirs,
                start_timestamp,
                scan_id,
            )
        )


def worker(
    inqueue: queue_.Queue[ToScan],
    outqueue: queue_.Queue[models.File],
    config_: config.ScannerSchema,
    shutdown: th.Event,
    abort: th.Event,
) -> None:
    """Consume results of an OS Walk and runs processing on the paths provided."""
    proc = mp.current_process()
    proc.name = f"scan-{proc.name}"

    innershutdown = mp.Event()

    thread_q: queue_.Queue[tuple[models.File, str]] = util.CancellableQueue(
        maxsize=config_["scan_max_threads_per_process"]
        * config_["queue_length_scale_factor"],
        abort_event=abort,
    )
    thread_p = mp.pool.ThreadPool(
        processes=config_["scan_max_threads_per_process"],
        initializer=threadworker,
        initargs=((thread_q, innershutdown, abort)),
    )
    while (not shutdown.is_set()) and (not abort.is_set()):
        try:
            (
                dirpath,
                dirnames,
                filenames,
                walk_items,
                aggregate_subdirs,
                start_timestamp,
                scan_id,
            ) = inqueue.get(timeout=10)
        except queue_.Empty:
            continue

        folder = models.File(dirpath, start_timestamp, scan_id)

        if walk_items:
            for file in filenames:
                filepath = os.path.join(dirpath, file)
                outqueue.put(
                    models.File(filepath, start_timestamp, scan_id).to_dict(True)
                )
        else:
            for file in filenames:
                filepath = os.path.join(dirpath, file)
                thread_q.put((folder, filepath))

        if aggregate_subdirs:
            for dir_ in dirnames:
                # Because we vendor the os module, it's type hints do not get picked up by mypy.
                for idirpath, _, ifilenames in vos.walk(os.path.join(dirpath, dir_)):  # type: ignore[no-untyped-call]
                    thread_q.put((folder, idirpath))
                    for file in ifilenames:
                        thread_q.put(
                            (
                                folder,
                                os.path.join(idirpath, file),
                            )
                        )

        thread_q.join()
        outqueue.put(folder.to_dict(True))
        inqueue.task_done()

    # Cleanup tasks.
    thread_q.join()

    innershutdown.set()
    thread_p.close()
    thread_p.join()

    del thread_q


def threadworker(
    thread_q: queue_.Queue[tuple[models.File, str]],
    shutdown: th.Event,
    abort: th.Event,
) -> None:
    """Worker to aggredate subdirectories into a Folder object."""
    while (not shutdown.is_set()) and (not abort.is_set()):
        try:
            folder, path = thread_q.get(timeout=10)
        except queue_.Empty:
            continue
        try:
            folder.incorporate_child(path)
        except OSError as err:
            abort.set()
            raise errors.AbortError from err

        thread_q.task_done()
