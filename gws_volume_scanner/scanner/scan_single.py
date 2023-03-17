"""Scan a single GWS."""
import multiprocessing as mp
import multiprocessing.queues
import queue as queue_

import elasticsearch.exceptions
import elasticsearch.helpers as esh
import elasticsearch_dsl as esd

from ..client import queries
from . import aggregate, cli, config, elastic, errors, models, scanner, util


def scan_single_gws(
    path: str,
    config_: config.ScannerConfig,
    elastic_q: queue_.Queue[models.File],
    log_q: multiprocessing.queues.Queue,
) -> None:
    """Scan a single GWS."""
    logger = util.getLogger(__name__, queue=log_q)

    abort = mp.Event()

    scanner_q = util.ScanQueueWorker(config_.scanner, elastic_q, abort)

    # Add this scan to the volumes index.
    volumestats = models.Volume.new(path)
    volumestats.add_volume_information()
    volumestats.save()

    # Add paths into the queue for processing.
    scanner.queuescan(
        path,
        scanner_q.queue,
        config_,
        volumestats.start_timestamp,
        volumestats.meta.id,
        abort,
    )

    # Shutdown workers.
    scanner_q.shutdown()

    # If this scan has been aborted, bail.
    if abort.is_set():
        raise errors.AbortError

    # Query aggregate data and save the aggregations into es.
    results = []
    try:
        results += aggregate.aggregate_filetypes(path, config_.scanner["elastic"], volumestats)
        results += aggregate.aggregate_users(path, config_.scanner["elastic"], volumestats)
        results += aggregate.aggregate_heat(path, config_.scanner["elastic"], volumestats)
    except elasticsearch.exceptions.ConnectionTimeout:
        logger.error("Failed to generate aggregate data for %s", path)
    else:
        connection = elastic.get_connection(config_.scanner["elastic"])
        esh.bulk(
            connection,
            results,
            index=config_.scanner["elastic"]["aggregate_index_name"],
        )

    # Cleanup old views of the tree of this filesystem.
    old_scan_ids = queries.scan_ids(path, config_.scanner["elastic"]["volume_index_name"])

    # Wait for all the new data to be submitted to elasticsearch before doing deletions.
    elastic_q.join()

    if old_scan_ids:
        try:
            old_scan_ids.remove(volumestats.meta.id)
        except ValueError:
            pass
        for oldscan in old_scan_ids:
            scanstatus = models.Volume.get(id=oldscan)
            if scanstatus.status in ["complete", "in_progress"]:
                search = esd.Search(index=config_.scanner["elastic"]["data_index_name"]).filter(
                    "term", scan_id=oldscan
                )
                # Paper-over error where elasticsearch tries to delete the same documents twice.
                search.params(conflicts="proceed")
                try:
                    search.delete()
                except elasticsearch.exceptions.ConflictError:
                    logger.error("Failed to delete old data for scan id %s", oldscan)
                if scanstatus.status == "complete":
                    scanstatus.status = "removed"
                elif scanstatus.status == "in_progress":
                    scanstatus.status = "failed"
                scanstatus.save()

    # Do a query on all the data we just collected and save it to long term stats.
    volumestats.add_endofscan(config_.scanner["elastic"]["data_index_name"])
    volumestats.save()


def main() -> None:
    """GWS Volume scanner.

    Given a path, the scanner with catalogue information
    about the path to elasticsearch.
    """
    args = cli.parse_single_args()
    config_ = config.ScannerConfig(args.config_file)

    queue_log_handler = util.QueueLogger(
        __name__,
        log_config={},
    )

    # Create or update the index in elasticsearch.
    elastic.init(config_.scanner["elastic"])

    # Create queue/worker for sending data to elasticsearch.
    elastic_q = util.ElasticQueueWorker(config_.scanner)

    try:
        scan_single_gws(args.gws_path, config_, elastic_q.queue, queue_log_handler.queue)
    finally:
        # Shutdown workers.
        elastic_q.shutdown()
        queue_log_handler.shutdown()


if __name__ == "__main__":
    main()
