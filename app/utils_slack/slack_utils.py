import os
import time
from datetime import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from app import logger

SLACK_BOT_TOKEN = os.environ.get("SLACK_BOT_TOKEN")
client = WebClient(token=SLACK_BOT_TOKEN)


def get_thread_history(channel_id, thread_ts):

    try:
        response = client.conversations_replies(
            channel=channel_id,
            ts=thread_ts,
            limit=20
        )

        if not response.get("ok", False):
            logger.error(f"Slack API error: {response.get('error')}")
            return ""

        messages = response.get("messages", [])
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
            formatted_messages.append(f"    [{ts_readable}] {text}")

        return "\n".join(formatted_messages)

    except SlackApiError as e:
        logger.error(f"Slack API error: {e.response['error']}")
        return ""


def send_message(channel, text, thread_ts=None):
    """
    Envía un mensaje a Slack (en canal o dentro de un hilo)
    """
    try:
        response = client.chat_postMessage(
            channel=channel,
            text=text,
            thread_ts=thread_ts
        )
        logger.info(f"✅ Mensaje enviado a {channel}: {response['ts']}")
        return response["ts"]

    except SlackApiError as e:
        logger.error(f"❌ Error al enviar mensaje: {e.response['error']}")
        return None

def update_message(channel: str, ts: str, new_text: str):
    try:
        response = client.chat_update(
            channel=channel,
            ts=ts,
            text=new_text
        )
        logger.info(f"✅ Mensaje actualizado en {channel}: {ts}")
        return response.data

    except SlackApiError as e:
        error_msg = e.response.get("error", "unknown_error")
        logger.error(f"❌ Error al actualizar mensaje: {error_msg}")
        return None
    
def add_reaction(channel: str, ts: str, threadts:str, emoji: str):
    try:
        response = client.reactions_add(
            channel=channel,
            name=emoji,
            thread_ts = threadts,
            timestamp=ts
        )
        logger.info(f"✅ Reacción :{emoji}: añadida a {channel} en {ts}")
        return response.data

    except SlackApiError as e:
        error_msg = e.response.get("error", "unknown_error")
        if error_msg == "already_reacted":
            logger.info(f"ℹ️ Ya habías reaccionado con :{emoji}: a {ts}")
        else:
            logger.error(f"❌ Error al añadir reacción: {error_msg}")
        return None
    
def send_thinking_messages(channel, user, threadts, stop_event):
    i = 0
    while not stop_event.is_set():
        try:
            client.chat_postEphemeral(channel=channel, user=user, thread_ts=threadts, text="Thinking...")
        except SlackApiError as e:
            print(f"⚠️ Slack ephemeral error: {e.response.get('error', 'unknown')}")
        i += 1
        time.sleep(5)  # cada 5 segundos