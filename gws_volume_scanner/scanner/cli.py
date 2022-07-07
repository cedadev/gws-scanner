"""Parse command line arguments for the GWS scanner."""

import argparse


def parse_single_args() -> argparse.Namespace:
    """Parse command line arguments for GWS scanner single-GWS run."""
    parser = argparse.ArgumentParser(
        description="The GWS volume scanner scans file path and aggregates data into elasticsearch."
    )
    # Required arguments.
    parser.add_argument(
        "config_file", help="The pain to the main configration file.", type=str
    )
    parser.add_argument("gws_path", help="The path to the GWS to scan.", type=str)

    return parser.parse_args()


def parse_daemon_args() -> argparse.Namespace:
    """Parse command line arguments for GWS scanner single-GWS run."""
    parser = argparse.ArgumentParser(
        description="The GWS volume scanner scans file path and aggregates data into elasticsearch."
    )
    # Required arguments.
    parser.add_argument(
        "config_file", help="The path to the main configration file.", type=str
    )

    parser.add_argument(
        "--run-forever",
        action="store_true",
        help="Scan all the passes paths continuiously in a loop without quiting.",
    )
    return parser.parse_args()
