"""Continuously scan all GWSes."""

import argparse
import datetime as dt
import logging
import os
import pathlib
import time
import typing

import authlib.integrations.httpx_client
import httpx
import sdnotify

from ..client import queries
from . import cli, config, elastic, errors, scan_single, util


def entrypoint() -> None:
    """Create long-running queues and ensure they are always gracefully terminated."""
    system_notify = sdnotify.SystemdNotifier()

    args = cli.parse_daemon_args()
    config_ = config.ScannerConfig(args.config_file)

    # Setup logging.
    logging.addLevelName(100, "NOTIFY")
    queue_log_handler = util.QueueLogger(
        __name__,
        log_config=config_.scanner["daemon"]["logging_config"],
    )

    # Create or update the index in elasticsearch.
    elastic.init(config_.scanner["elastic"])
    elastic_q = util.ElasticQueueWorker(config_.scanner)

    # Startup tasks complete.
    system_notify.notify(f"MAINPID={os.getpid()}")
    system_notify.notify("READY=1")
    try:
        main(system_notify, args, config_, elastic_q, queue_log_handler)
    finally:
        # Make sure that the queues always get cleanly shutdown on any exception.
        # This is the reason for having a seperate setup() function.
        system_notify.notify("STOPPING=1")
        elastic_q.shutdown()
        queue_log_handler.shutdown()


def main(
    system_notify: sdnotify.SystemdNotifier,
    args: argparse.Namespace,
    config_: config.ScannerConfig,
    elastic_q: util.ElasticQueueWorker,
    queue_log_handler: util.QueueLogger,
) -> None:
    """Scan GWSs forever in a loop."""
    logger = util.getLogger(__name__, queue=queue_log_handler.queue)

    fail_count = 0
    total_successful_scans = 0
    last_notify_nonexistent = dt.datetime(1, 1, 1)

    while True:
        scan_started_at = dt.datetime.now()
        try:
            toscan = get_gws_list(config_.scanner["daemon"])
        except httpx.RequestError:
            break

        logger.info("###### Loaded %s paths to scan. ######", len(toscan))
        non_existent_gws = set()

        while toscan:
            # Tell systemd we are still alive every loop.
            system_notify.notify("WATCHDOG=1")

            gws = toscan.pop().strip().rstrip("/")

            last_scan_info = queries.latest_scan_info(
                gws, config_.scanner["elastic"]["volume_index_name"]
            )
            if not should_scan(gws, config_.scanner, logger, last_scan_info):
                continue

            # Give the systemd watchdog an idea of how long this will take so that if it's
            # longer the scanner can be killed.
            if last_scan_info:
                predicted_time = last_scan_info.get("length", 259200)
            else:
                predicted_time = 259200
            # watchdog_usec is in milliseconds.
            time_allowed = int(predicted_time * 1.5) * 1000
            system_notify.notify("WATCHDOG=1")
            system_notify.notify(f"WATCHDOG_USEC={time_allowed}")
            system_notify.notify("WATCHDOG=1")

            try:
                os.scandir(gws)
            except FileNotFoundError:
                non_existent_gws.add(gws)
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
                        system_notify.notify("WATCHDOG=trigger")
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

        # Don't spam the slack channel with notifications, just send them once a day.
        if non_existent_gws and (
            last_notify_nonexistent + dt.timedelta(days=1) < dt.datetime.now()
        ):
            for gws in non_existent_gws:
                logger.log(100, "%s does not exist.", gws)
            last_notify_nonexistent = dt.datetime.now()

        if not args.run_forever:
            break

        # If all the GWSs have been scanned recently, sleep for a while.
        if scan_started_at + dt.timedelta(minutes=5) > dt.datetime.now():
            logger.info("Scan of all GWSs finished very rapidly, sleeping for a while.")
            system_notify.notify("WATCHDOG=1")
            system_notify.notify("WATCHDOG_USEC=4200")
            system_notify.notify("WATCHDOG=1")
            time.sleep(3600)


def get_gws_list(daemon_config: config.DaemonSchema) -> list[str]:
    """Get a list of paths to scan from the projects portal."""
    client = authlib.integrations.httpx_client.OAuth2Client(
        daemon_config["client_id"],
        daemon_config["client_secret"],
        scope=" ".join(daemon_config["scopes"]),
        transport=httpx.HTTPTransport(retries=30),
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
                # Only provisioned requrests should be scanned, they have a status of 50.
                # If it doesn't start with a / it's probably not a path.
                if req["status"] == 50 and req["location"].startswith("/"):
                    # This will remove any double or trailing slashes.
                    sanitized = str(pathlib.PurePath(req["location"]))
                    # Remove duplicates any anything in the never_scan list.
                    if (sanitized not in services) and (
                        sanitized not in daemon_config["never_scan"]
                    ):
                        services.append(sanitized)
    return services


def should_scan(
    path: str,
    config_: config.ScannerSchema,
    logger: logging.Logger,
    last_scan_info: typing.Optional[dict[str, typing.Any]],
) -> bool:
    """Check if a GWS should be scanned."""
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
    entrypoint()
