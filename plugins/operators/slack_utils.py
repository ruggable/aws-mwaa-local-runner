# Wrapper for slack hook

from airflow.providers.slack.hooks.slack import SlackHook


class SlackNotifier:
    def __init__(
            self,
            slack_conn_id: str = "slack_api_default",
            channel: str = "#general",
            username: str = "Airflow",
            icon_url: str = "https://raw.githubusercontent.com/apache/airflow/2.5.0/airflow/www/static/pin_100.png"
    ):
        self.slack_conn_id = slack_conn_id
        self.channel = channel
        self.username = username
        self.icon_url = icon_url

    def send_message(self, text, attachments=[], blocks=[]):
        hook = SlackHook(slack_conn_id=self.slack_conn_id)
        api_call_params = {
            "channel": self.channel,
            "username": self.username,
            "text": text,
            "icon_url": self.icon_url,
            "attachments": attachments,
            "blocks": blocks
        }
        hook.call("chat.postMessage", json=api_call_params)


def slack_failure_callback(slack_conn_id="slack_api_default", text_message="Task failed!", channel="#data-eng-alerts"):
    """
    Send a notification to Slack upon task failure.
    """

    def _notify(context):
        notifier = SlackNotifier(slack_conn_id=slack_conn_id, channel=channel)
        notifier.send_message(text_message)

    return _notify
