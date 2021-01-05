"""Slack notifier for use in Delphi indicators """
from slack import WebClient
from slack.errors import SlackApiError

class SlackNotifier:
    """Notifies slack channels of messages"""
    def __init__(self, slack_channel, slack_token):
        self.slack_channel = slack_channel
        self.client = WebClient(token = slack_token)

    def post_message(self, blocks):
        """Post a message to the slack channel"""
        #try:
        self.client.chat_postMessage(
            channel=self.slack_channel,
            blocks=blocks
        )
        # except SlackApiError as e:
        #     # You will get a SlackApiError if "ok" is False
        #     assert False, e.response["error"]
