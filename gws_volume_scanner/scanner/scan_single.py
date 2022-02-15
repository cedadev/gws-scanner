"""Test different ways of walking a filesystem."""
import queue as queue_

import elasticsearch.helpers as esh

from . import aggregate, cli, config, elastic, models, scanner, util


def scan_single_gws(
    path: str, config_: config.ScannerConfig, elastic_q: queue_.Queue[models.File]
) -> None:
    """Scan a single GWS."""
    scanner_q = util.ScanQueueWorker(config_.scanner, elastic_q)

    # Add this scan to the volumes index.
    volumestats = models.Volume(path)
    volumestats.add_volume_information()
    volumestats.save()

    # Add paths into the queue for processing.
    scanner.queuescan(
        path, scanner_q.queue, config_, volumestats.start_timestamp, volumestats.meta.id
    )

    # Shutdown workers.
    scanner_q.shutdown()

    # Do a query on all the data we just collected and save it to long term stats.
    volumestats.add_endofscan(config_.scanner["elastic"]["data_index_name"])
    volumestats.save()

    # Query aggregate data and save the aggregations into es.
    results = []
    results += aggregate.aggregate_filetypes(
        path, config_.scanner["elastic"], volumestats
    )
    results += aggregate.aggregate_users(path, config_.scanner["elastic"], volumestats)
    results += aggregate.aggregate_heat(path, config_.scanner["elastic"], volumestats)
    connection = elastic.get_connection(config_.scanner["elastic"])
    esh.bulk(
        connection,
        results,
        index=config_.scanner["elastic"]["aggregate_index_name"],
    )


def main() -> None:
    """GWS Volume scanner.

    Given a path, the scanner with catalogue information
    about the path to elasticsearch.
    """
    args = cli.parse_single_args()
    config_ = config.ScannerConfig(args.config_file)

    # Create or update the index in elasticsearch.
    elastic.init(config_.scanner["elastic"])

    # Create queue/worker for sending data to elasticsearch.
    elastic_q = util.ElasticQueueWorker(config_.scanner)

    scan_single_gws(args.gws_path, config_, elastic_q.queue)

    # Shutdown workers.
    elastic_q.shutdown()


if __name__ == "__main__":
    main()
