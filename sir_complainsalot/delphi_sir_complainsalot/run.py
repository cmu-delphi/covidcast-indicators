# -*- coding: utf-8 -*-
"""Functions to call when running the function.

This module should contain a function called `run_module`, that is executed
when the module is run with `python -m delphi_sir_complainsalot`.
"""

import sys

from itertools import groupby

from slack import WebClient
from slack.errors import SlackApiError

from delphi_utils import read_params
import covidcast

from .check_source import check_source


def run_module():

    params = read_params()
    meta = covidcast.metadata()

    complaints = []
    for data_source in params["sources"].keys():
        complaints.extend(check_source(data_source, meta,
                                       params["sources"], params.get("grace", 0)))

    if len(complaints) > 0:
        for complaint in complaints:
            print(complaint)

        report_complaints(complaints, params)

        sys.exit(1)


def split_complaints(complaints, n=49):
    """Yield successive n-sized chunks from complaints list."""
    for i in range(0, len(complaints), n):
        yield complaints[i:i + n]


def report_complaints(all_complaints, params):
    """Post complaints to Slack."""
    if not params["slack_token"]:
        print("\b (dry-run)")
        return

    client = WebClient(token=params["slack_token"])

    for complaints in split_complaints(all_complaints):
        blocks = format_complaints_aggregated_by_source(complaints)
        print(f"blocks: {len(blocks)}")
        try:
            client.chat_postMessage(
                channel=params["channel"],
                blocks=blocks
            )
        except SlackApiError as e:
            # You will get a SlackApiError if "ok" is False
            assert False, e.response["error"]

def get_maintainers_block(complaints):
    maintainers = set()
    for c in complaints:
        maintainers.update(c.maintainers)

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


def format_complaints_aggregated_by_source(complaints):
    """Build formatted Slack message for posting to the API, aggregating
    complaints by source to reduce the number of blocks."""

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
