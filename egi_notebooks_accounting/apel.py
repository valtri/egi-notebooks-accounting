"""APEL Accounting implementation - for debugging and manual processing

APEL records are sent right away during processing the Prometheus. This utility will pick the records stored in the local accounting database.

Configuration:
[default]
site=EXAMPLE-SITE
cloud_type=egi-accounting (k8s)
# cloud_compute_service=
notebooks_db=<notebooks db file>
default_cpu_count=4.0

[apel]
timestamp_file=<timestamp file>
"""

import argparse
import logging
import os
from configparser import ConfigParser
from datetime import date, datetime, timedelta

import dateutil.parser
import pytz
from dirq import QueueSimple

from .model import VM, db_init

CONFIG = "default"
APEL_CONFIG = "apel"
DEFAULT_CONFIG_FILE = "config.ini"
DEFAULT_TIMESTAMP_FILE = "apel-accounting.timestamp"


def get_from_to_dates(args, timestamp_file):
    from_date = None
    if args.from_date:
        from_date = dateutil.parser.parse(args.from_date)
    else:
        try:
            with open(timestamp_file, "r") as tsf:
                try:
                    from_date = dateutil.parser.parse(tsf.read())
                except dateutil.parser.ParserError as e:
                    logging.debug(
                        f"Invalid timestamp content in '{timestamp_file}': {e}"
                    )
        except OSError as e:
            logging.debug(f"Not able to open timestamp file '{timestamp_file}': {e}")
        # no date specified report from yesterday
        if not from_date:
            report_day = date.today() - timedelta(days=1)
            from_date = datetime(
                report_day.year, report_day.month, report_day.day, 0, 0
            )
    if args.to_date:
        to_date = dateutil.parser.parse(args.to_date)
    else:
        # go until yesterday (todays midnight)
        report_day = date.today()
        to_date = datetime(report_day.year, report_day.month, report_day.day, 0, 0)
    utc = pytz.UTC
    from_date = from_date.replace(tzinfo=utc)
    to_date = to_date.replace(tzinfo=utc)
    return from_date, to_date


def generate_day_metrics(
    period_start,
    period_end,
    timestamp_file,
    dry_run,
):
    logging.info(f"Generate metrics from {period_start} to {period_end}")
    # pods ending in between the reporting times
    for pod in VM.select().where(
        (VM.end_time >= period_start) & (VM.end_time < period_end)
    ):
        if pod.valid_apel():
            yield pod.dump()
    # pods starting but not finished between the reporting times
    for pod in VM.select().where(
        (VM.start_time >= period_start) & (VM.end_time is None)
    ):
        if pod.valid_apel():
            yield pod.dump()
    if not dry_run:
        try:
            with open(timestamp_file, "w+") as tsf:
                tsf.write(period_end.strftime("%Y-%m-%dT%H:%M:%SZ"))
        except OSError as e:
            e = str(e)
            logging.debug("Failed to write timestamp file '{timestamp_file}': {e}")


def generate_all_metrics(
    from_date,
    to_date,
    timestamp_file,
    dry_run,
):
    # repeat in 24 hour intervals
    period_start = from_date
    while period_start < to_date:
        period_end = period_start + timedelta(hours=24)
        yield from generate_day_metrics(
            period_start,
            period_end,
            timestamp_file,
            dry_run,
        )
        period_start = period_end + timedelta(minutes=1)


def main():
    parser = argparse.ArgumentParser(
        description="APEL Accounting Generator for manual processing",
        epilog="APEL records are actually sent right away by egi-notebooks-accounting-dump during processing the Prometheus. This utility is for the manual \
processing and it'll pick the records stored in the local accounting database.",
    )
    parser.add_argument(
        "-c", "--config", help="config file", default=DEFAULT_CONFIG_FILE
    )
    parser.add_argument(
        "--dry-run", help="Do not actually send data, just report", action="store_true"
    )
    parser.add_argument("--from-date", help="Start date to report from")
    parser.add_argument("--to-date", help="End date to report to")
    args = parser.parse_args()

    parser = ConfigParser()
    parser.read(args.config)
    config = parser[CONFIG] if CONFIG in parser else {}
    apel_config = parser[APEL_CONFIG] if APEL_CONFIG in parser else {}
    db_file = os.environ.get("NOTEBOOKS_DB", config.get("notebooks_db", None))
    db_init(db_file)

    verbose = os.environ.get("VERBOSE", config.get("verbose", 0))
    verbose = logging.DEBUG if verbose == "1" else logging.INFO
    logging.basicConfig(level=verbose)

    spool_dir = os.environ.get("APEL_SPOOL", config.get("apel_spool"))
    timestamp_file = os.environ.get(
        "TIMESTAMP_FILE", apel_config.get("timestamp_file", DEFAULT_TIMESTAMP_FILE)
    )
    VM.site = config.get("site", VM.site)
    VM.cloud_type = config.get("cloud_type", VM.cloud_type)
    VM.cloud_compute_service = config.get(
        "cloud_compute_service", VM.cloud_compute_service
    )
    VM.default_cpu_count = config.get("default_cpu_count", VM.default_cpu_count)

    # ==== queries ====
    from_date, to_date = get_from_to_dates(args, timestamp_file)
    logging.debug(f"Reporting from {from_date} to {to_date}")
    records = [
        m
        for m in generate_all_metrics(from_date, to_date, timestamp_file, args.dry_run)
    ]
    n = len(records)
    message = "APEL-cloud-message: v0.4\n" + "\n%%\n".join(records)
    if spool_dir:
        if n > 0:
            queue = QueueSimple.QueueSimple(spool_dir)
            queue.add(message)
        logging.debug(f"Dumped {n} records to spool dir")
    else:
        print(message)


if __name__ == "__main__":
    main()
