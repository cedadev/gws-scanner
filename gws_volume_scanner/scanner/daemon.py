"""Continuously scan all GWSes."""
import os

from . import cli, config, elastic, scan_single, util


def main() -> None:
    args = cli.parse_daemon_args()
    config_ = config.ScannerConfig(args.config_file)

    # Create or update the index in elasticsearch.
    elastic.init(config_.scanner["elastic"])
    elastic_q = util.ElasticQueueWorker(config_.scanner)

    while True:
        with open(config_.scanner["daemon"]["gws_list_file"], "r") as f:
            toscan = f.readlines()
        print(f"###### Loaded {len(toscan)} paths to scan. ######")
        while toscan:
            gws = toscan.pop().strip().rstrip("/")
            try:
                os.scandir(gws)
            except FileNotFoundError:
                print(f"{gws} does not exist.")
            else:
                print(f"Started scan of {gws}.")
                scan_single.scan_single_gws(gws, config_, elastic_q.queue)
                print(f"Successfully scanned {gws}. {len(toscan)} left.")

    elastic_q.shutdown()


if __name__ == "__main__":
    main()
