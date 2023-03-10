"""Continuously scan all GWSes."""
import os

from . import cli, config, elastic, errors, scan_single, util

FAIL_THRESHOLD = 5


def main() -> None:
    args = cli.parse_daemon_args()
    config_ = config.ScannerConfig(args.config_file)

    # Create or update the index in elasticsearch.
    elastic.init(config_.scanner["elastic"])
    elastic_q = util.ElasticQueueWorker(config_.scanner)

    fail_count = 0
    total_successful_scans = 0

    while True:
        with open(config_.scanner["daemon"]["gws_list_file"], "r") as f:
            toscan = f.readlines()
        print(f"###### Loaded {len(toscan)} paths to scan. ######")
        toscan.reverse()  # It is unintuitive to loop through the file backwards.
        while toscan:
            gws = toscan.pop().strip().rstrip("/")
            try:
                os.scandir(gws)
            except FileNotFoundError:
                print(f"{gws} does not exist.")
            else:
                print(f"Started scan of {gws}.")
                try:
                    scan_single.scan_single_gws(gws, config_, elastic_q.queue)
                except errors.AbortError as err:
                    print(f"Scan of {gws} aborted due to an error in another process. Skipping.")
                    if fail_count >= FAIL_THRESHOLD:
                        print("Failure threshold reached. The process will exit.")
                        elastic_q.shutdown()
                        raise errors.AbortScanError from err
                    fail_count += 1
                    print(
                        "There have been {fail_count} failures, so far, will exit at {FAIL_THRESHOLD}."
                    )
                else:
                    total_successful_scans += 1
                    print(
                        f"Successfully scanned {gws}. {len(toscan)} left. {total_successful_scans} scans completed in total."
                    )
                    fail_count = 0

        if not args.run_forever:
            break

    elastic_q.shutdown()


if __name__ == "__main__":
    main()
