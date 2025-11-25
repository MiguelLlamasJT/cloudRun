import json
from app.utils_slack.validators import is_valid_message_event, is_authorized_user
from app.utils_slack.slack_utils import get_thread_history, send_message
from app.processing import process_question
from app.g_sheets import create_sheet
from app import logger

processed_events = set()

def handler(body: dict):
    
    event = body.get("event", {})
    event_id = body.get("event_id")

    if not is_valid_message_event(event):
        #logger.info(event)
        logger.warning("Invalid or non-message event")
        return

    # Evita reprocesar el mismo evento
    if event_id in processed_events:
        logger.warning("Duplicate event: %s", event_id)
        return
    processed_events.add(event_id)

    logger.info("Processing event: %s", json.dumps(event))
    user, channel, text, thread_ts = (
        event.get("user"),
        event.get("channel"),
        event.get("text"),
        event.get("thread_ts") or event.get("ts")
    )

    if not text:
        logger.debug("Empty message (edited or deleted)")
        return

    if text == "crear hoja":
        logger.info("Creando hoja")
        result = create_sheet(
            "values",
            filename="P&L Test",
            share_with_email="miguel.llamas@jobandtalent.com"
        )

        print("Creado:")
        print("ID:", result["spreadsheet_id"])
        print("URL:", result["url"])
        send_message(channel, result["url"], thread_ts)
        return

    dev = False
    if not is_authorized_user(user,dev):
        logger.warning("Unauthorized user: %s", user)
        send_message(channel, "Under Maintenance.", thread_ts)
        return

    thread_text = get_thread_history(channel, thread_ts)
    process_question(thread_text, channel, user, thread_ts)
    return
