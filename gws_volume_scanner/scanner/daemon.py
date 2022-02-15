"""Continuously scan all GWSes."""
import os

from . import cli, config, elastic, scan_single, util


def main() -> None:
    args = cli.parse_daemon_args()
    config_ = config.ScannerConfig(args.config_file)

    # Create or update the index in elasticsearch.
    elastic.init(config_.scanner["elastic"])
    elastic_q = util.ElasticQueueWorker(config_.scanner)

    for gws in config_.scanner["daemon"]["gws_list"]:
        try:
            os.scandir(gws)
        except FileNotFoundError:
            print(f"{gws} does not exist.")
        else:
            scan_single.scan_single_gws(gws, config_, elastic_q.queue)

    elastic_q.shutdown()


if __name__ == "__main__":
    main()
