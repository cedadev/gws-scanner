"""Continuously scan all GWSes."""
import datetime as dt
import logging
import os
import pathlib

import authlib.integrations.httpx_client

from ..client import queries
from . import cli, config, elastic, errors, scan_single, util


def main() -> None:
    args = cli.parse_daemon_args()
    config_ = config.ScannerConfig(args.config_file)

    queue_log_handler = util.QueueLogger(
        __name__,
        log_config=config_.scanner["daemon"]["logging_config"],
    )
    logger = util.getLogger(__name__, queue=queue_log_handler.queue)

    # Create or update the index in elasticsearch.
    elastic.init(config_.scanner["elastic"])
    elastic_q = util.ElasticQueueWorker(config_.scanner)

    fail_count = 0
    total_successful_scans = 0

    while True:
        toscan = get_gws_list(config_.scanner["daemon"])
        logger.info("###### Loaded %s paths to scan. ######", len(toscan))
        while toscan:
            gws = toscan.pop().strip().rstrip("/")

            if not should_scan(gws, config_.scanner, logger):
                continue

            try:
                os.scandir(gws)
            except FileNotFoundError:
                logger.warning("%s does not exist.", gws)
            except PermissionError:
                logger.warning("PermissionError when accessing %s", gws)
            else:
                logger.info("Started scan of %s.", gws)
                try:
                    scan_single.scan_single_gws(
                        gws, config_, elastic_q.queue, queue_log_handler.queue
                    )
                except errors.AbortError as err:
                    logger.error(
                        "Scan of %s aborted due to an error in another process. Skipping.", gws
                    )
                    if fail_count >= config_.scanner["daemon"]["fail_threshold"]:
                        logger.critical("Failure threshold reached. The process will exit.")
                        elastic_q.shutdown()
                        raise errors.AbortScanError from err
                    fail_count += 1
                    logger.error(
                        "There have been %s failures, so far, will exit at %s",
                        fail_count,
                        config_.scanner["daemon"]["fail_threshold"],
                    )
                else:
                    total_successful_scans += 1
                    logger.info(
                        "Successfully scanned %s. %s left. %s scans completed in total.",
                        gws,
                        len(toscan),
                        total_successful_scans,
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

    services: list[str] = daemon_config["extra_to_scan"]
    for service in reversed(projects_services):
        # Only look for group workspaces.
        if service["category"] == 1:
            for req in service["requirements"]:
                # If it doesn't start with a / it's probably not a path.
                if req["location"].startswith("/"):
                    # This will remove any double or trailing slashes.
                    sanitized = str(pathlib.PurePath(req["location"]))
                    # Remove duplicates any anything in the never_scan list.
                    if (sanitized not in services) and (
                        sanitized not in daemon_config["never_scan"]
                    ):
                        services.append(sanitized)
    return services


def should_scan(path: str, config_: config.ScannerSchema, logger: logging.Logger) -> bool:
    """Check if a GWS should be scanned."""
    last_scan_info = queries.latest_scan_info(path, config_["elastic"]["volume_index_name"])
    if last_scan_info is None:
        return True

    if last_scan_info["status"] == "complete":
        next_scan_allowed = dt.datetime.fromisoformat(
            last_scan_info["end_timestamp"]
        ) + dt.timedelta(days=config_["daemon"]["max_scan_interval_days"])
        if next_scan_allowed > dt.datetime.now():
            logger.warning(
                "%s has been scanned within max_scan_interval_days, skipping scan.", path
            )
            return False

    return True


if __name__ == "__main__":
    main()
