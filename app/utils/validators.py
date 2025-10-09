from app import AUTHORIZED_USERS
import logging
logger = logging.getLogger(__name__)

def is_valid_slack_event(event: dict) -> bool:
    return event.get("type") == "message" and not event.get("bot_id")

def is_authorized_user(user_id: str) -> bool:
    return user_id in AUTHORIZED_USERS
