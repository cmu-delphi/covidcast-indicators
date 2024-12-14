"""Slack notifier for use in Delphi indicators."""
from slack import WebClient

class SlackNotifier:
    """Notifies slack channels of messages."""

    def __init__(self, slack_channel, slack_token):
        """Initialize with a slack channel and token."""
        self.slack_channel = slack_channel
        self.client = WebClient(token = slack_token)

    def post_message(self, blocks):
        """Post a message to the slack channel."""
        self.client.chat_postMessage(
            channel=self.slack_channel,
            blocks=blocks
        )
