"""Continuously scan all GWSes."""
import os
import pathlib

import authlib.integrations.httpx_client

from . import cli, config, elastic, errors, scan_single, util


def main() -> None:
    args = cli.parse_daemon_args()
    config_ = config.ScannerConfig(args.config_file)

    queue_log_handler = util.QueueLogger(
        __name__,
        log_config={"handlers": {"console": {"class": "logging.StreamHandler"}}},
    )
    logger = util.getLogger(__name__, queue=queue_log_handler.queue)

    # Create or update the index in elasticsearch.
    elastic.init(config_.scanner["elastic"])
    elastic_q = util.ElasticQueueWorker(config_.scanner)

    fail_count = 0
    total_successful_scans = 0

    while True:
        toscan = get_gws_list(config_.scanner["daemon"])
        logger.info(f"###### Loaded {len(toscan)} paths to scan. ######")
        while toscan:
            gws = toscan.pop().strip().rstrip("/")
            try:
                os.scandir(gws)
            except FileNotFoundError:
                logger.warning(f"{gws} does not exist.")
            else:
                logger.info(f"Started scan of {gws}.")
                try:
                    scan_single.scan_single_gws(
                        gws, config_, elastic_q.queue, queue_log_handler.queue
                    )
                except errors.AbortError as err:
                    logger.error(
                        f"Scan of {gws} aborted due to an error in another process. Skipping."
                    )
                    if fail_count >= config_.scanner["daemon"]["fail_threshold"]:
                        logger.critical("Failure threshold reached. The process will exit.")
                        elastic_q.shutdown()
                        raise errors.AbortScanError from err
                    fail_count += 1
                    logger.error(
                        'There have been {fail_count} failures, so far, will exit at {config_.scanner["daemon"]["fail_threshold"]}'
                    )
                else:
                    total_successful_scans += 1
                    logger.info(
                        f"Successfully scanned {gws}. {len(toscan)} left. {total_successful_scans} scans completed in total."
                    )
                    fail_count = 0

        if not args.run_forever:
            break

    elastic_q.shutdown()
    queue_log_handler.shutdown()


def get_gws_list(daemon_config: config.DaemonSchema) -> list[str]:
    """Get a list of paths to scan from the projects portal."""
    client = authlib.integrations.httpx_client.OAuth2Client(
        daemon_config["client_id"],
        daemon_config["client_secret"],
        scope=" ".join(daemon_config["scopes"]),
    )
    client.fetch_token(daemon_config["token_endpoint"], grant_type="client_credentials")
    projects_services = client.get(
        daemon_config["services_endpoint"],
        headers={"Accept": "application/json"},
    ).json()

    services: list[str] = []
    for service in projects_services:
        # Only look for group workspaces.
        if service["category"] == 1:
            for req in service["requirements"]:
                # If it doesn't start with a / it's probably not a path.
                if req["location"].startswith("/"):
                    # This will remove any double or trailing slashes.
                    sanitized = str(pathlib.PurePath(req["location"]))
                    services.append(sanitized)
    return services


if __name__ == "__main__":
    main()
