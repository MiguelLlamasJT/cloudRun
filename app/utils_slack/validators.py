from app import AUTHORIZED_USERSS
from app import logger

def is_valid_message_event(event: dict) -> bool:
    return event.get("type") == "message" and not event.get("bot_id")

def is_authorized_user(user_id: str, dev: bool) -> bool:
    if dev:
        return user_id == "U06BW8J6MRU"
    return user_id in AUTHORIZED_USERSS
