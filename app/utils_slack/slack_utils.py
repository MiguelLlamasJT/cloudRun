import os
from datetime import datetime
import requests
import logging
logger = logging.getLogger(__name__)

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")

def get_thread_history(channel_id, thread_ts):
    headers = {"Authorization": f"Bearer {SLACK_BOT_TOKEN}"}
    payload = {"channel": channel_id, "ts": thread_ts, "limit": 200}

    resp = requests.get(
        "https://slack.com/api/conversations.replies",
        headers=headers,
        params=payload
    )
    
    data = resp.json()
    if not data.get("ok"):
        print(f"Slack API error: {data.get('error')}")
        return ""

    messages = data.get("messages", [])
    if not messages:
        return ""

    formatted_messages = []
    for msg in messages:
        ts = msg.get("ts", "")
        text = msg.get("text", "")
        try:
            ts_readable = datetime.fromtimestamp(float(ts)).strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            ts_readable = ts
        formatted_messages.append(f"[{ts_readable}] {text}")

    return "\n".join(formatted_messages)

def send_message(channel, text, thread_ts=None):
    payload = {
        "channel": channel,
        "text": text,
    }
    if thread_ts:
        payload["thread_ts"] = thread_ts

    response = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={"Authorization": f"Bearer {SLACK_BOT_TOKEN}"},
        data=payload
    )

    # Para depuración
    print("Slack API response:", response.json())

