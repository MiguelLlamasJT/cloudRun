import datetime, json, re
from app import logger

def get_current_and_last_monday():
    today = datetime.date.today()
    days_since_monday = today.weekday()
    current_monday = today - datetime.timedelta(days=days_since_monday)
    last_monday = current_monday - datetime.timedelta(days=7)
    return current_monday, last_monday

def resolve_dataweek(filters):
    current, previous = get_current_and_last_monday()

    if "data_week" not in filters or not filters["data_week"]:
        filters["data_week"] = [str(current)]
        return filters

    resolved = []
    for val in filters["data_week"]:
        if val == "CURRENT":
            resolved.append(str(current))
        elif val == "PREVIOUS":
            resolved.append(str(previous))
        else:
            resolved.append(val)

    filters["data_week"] = resolved
    return filters

def safe_json_parse(text: str):

    try:
        match = re.search(r"\{[\s\S]*\}", text)
        if not match:
            raise ValueError("No se encontró un bloque JSON en el texto recibido.")
        
        cleaned = match.group(0)

        cleaned = re.sub(r'(?<!\\)\n', '\\n', cleaned)

        cleaned = cleaned.strip(" \t\r\n")

        return json.loads(cleaned)
    
    except json.JSONDecodeError as e:
        logger.error("Error en JSON")
        raise ValueError(f"Error al parsear JSON: {e}\nTexto limpio:\n{cleaned[:500]}")



def format_for_slack(text: str) -> str:
    # Sustituir Markdown por formato Slack
    text = re.sub(r"\*\*(.*?)\*\*", r"*\1*", text)  # **bold** → *bold*
    text = re.sub(r"##+\s*", ">*", text)  # ## títulos → cita con asterisco
    text = re.sub(r"###\s*", ">*", text)
    text = re.sub(r"\n-{2,}\n", "\n", text)  # eliminar separadores ----
    return text
