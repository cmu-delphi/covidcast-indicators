# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_sir_complainsalot`.
"""

import time
from itertools import groupby

import covidcast
from delphi_utils import SlackNotifier
from delphi_utils import get_structured_logger
from delphi_utils import read_params

from .check_source import check_source


def get_logger():
    """Create structured logger."""
    params = read_params()
    return get_structured_logger(
        __name__, filename=params.get("log_filename"),
        log_exceptions=params.get("log_exceptions", True))

LOGGER = get_logger()

def run_module():
    """Run SirCAL."""
    start_time = time.time()
    params = read_params()
    covidcast.use_api_key(params["api_credentials"])
    meta = covidcast.metadata()
    slack_notifier = None
    if "channel" in params and "slack_token" in params:
        slack_notifier = SlackNotifier(params["channel"], params["slack_token"])

    complaints = []
    for data_source in params["sources"].keys():
        complaints.extend(check_source(data_source, meta,
                                       params["sources"], params.get("grace", 0), LOGGER))

    if len(complaints) > 0:
        report_complaints(complaints, slack_notifier)

    elapsed_time_in_seconds = round(time.time() - start_time, 2)
    LOGGER.info("Completed indicator run",
        elapsed_time_in_seconds = elapsed_time_in_seconds)


def split_complaints(complaints, n=49):  # pylint: disable=invalid-name
    """Yield successive n-sized chunks from complaints list."""
    for i in range(0, len(complaints), n):
        yield complaints[i:i + n]


def report_complaints(all_complaints, slack_notifier):
    """Log complaints and optionally post to Slack."""
    for complaints in split_complaints(all_complaints):
        blocks = format_and_log_complaints_aggregated_by_source(complaints)

        if slack_notifier:
            slack_notifier.post_message(blocks)

def get_maintainers_block(complaints):
    """Build a Slack block to alert maintainers to pay attention."""
    maintainers = set()
    for complaint in complaints:
        maintainers.update(complaint.maintainers)

    maintainers_block = {
        "type": "section",
        "text": {
                "type": "mrkdwn",
                "text": "Hi, this is Sir Complains-a-Lot. I need to speak to " +
                        (", ".join("<@{0}>".format(m)
                                   for m in maintainers)) + "."
        }
    }

    return maintainers_block


def format_and_log_complaints_aggregated_by_source(complaints):
    """Build formatted Slack message for posting to the API.

    Complaints are aggregated by source to reduce the number of blocks.
    """
    blocks = [get_maintainers_block(complaints)]

    def message_for_source(complaint):
        return "{main_text} - (last update: {last_updated})".format(
            main_text=complaint.message,
            last_updated=complaint.last_updated.strftime("%Y-%m-%d"))

    for source, complaints_by_source in groupby(
            complaints, key=lambda x: x.data_source):
        for message, complaint_list in groupby(
                complaints_by_source, key=message_for_source):
            signal_and_geo_types = ""
            for complaint in complaint_list:
                signal_and_geo_types += "`{signal}: [{geo_types}]`\n".format(
                    signal=complaint.signal,
                    geo_types=", ".join(complaint.geo_types))

            LOGGER.critical(event="Signal out of SLA",
                            message=message,
                            data_source=source,
                            signal_and_geo_types=signal_and_geo_types)

            blocks.extend([
                {
                    "type": "divider"
                },
                {
                    "type": "section",
                    "text": {
                        "type": "mrkdwn",
                        "text": "*{source_name}* {message}:\n{signals}"
                        .format(
                            source_name=source.upper(),
                            message=message,
                            signals=signal_and_geo_types)
                    }
                }
            ])

    return blocks


def format_complaints(complaints):
    """Build a formatted Slack message for posting to the API.

    To find good formatting for blocks, try the block kit builder:
    https://api.slack.com/tools/block-kit-builder

    """
    blocks = [get_maintainers_block(complaints)]

    for complaint in complaints:
        blocks.append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": complaint.to_md()
                }
            }
        )

    return blocks
