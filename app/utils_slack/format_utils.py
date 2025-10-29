import datetime, json
from app import logger
import re
from typing import List

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



def _format_md_table(block: str) -> str:
    lines = [l for l in block.strip().splitlines() if l.strip()]
    if len(lines) >= 2 and re.search(r'^\s*\|?\s*:?-{2,}:?\s*(\|\s*:?-{2,}:?\s*)+\|?\s*$', lines[1]):
        lines.pop(1)
    rows = []
    for ln in lines:
        cells = [re.sub(r'[\*_`]', '', c.strip()) for c in ln.strip().strip('|').split('|')]
        rows.append(cells)
    widths = [max(len(c) for c in col) for col in zip(*rows)]
    out_lines = []
    for i, r in enumerate(rows):
        out_lines.append(" | ".join(c.ljust(w) for c, w in zip(r, widths)))
        if i == 0:
            out_lines.append("-+-".join("-" * w for w in widths))
    return "```\n" + "\n".join(out_lines) + "\n```"

def format_for_slack(text: str) -> str:
    text = text.replace("\\n", "\n").strip()

    # Limpia primero las cabeceras (##, ###) y elimina ** internos
    text = re.sub(r"(?m)^##+\s+\*\*(.*?)\*\*\s*$", r"\n*\1*\n", text)
    text = re.sub(r"(?m)^###\s+\*\*(.*?)\*\*\s*$", r"\n*\1*\n", text)
    text = re.sub(r"(?m)^##+\s+(.*?)$", r"\n*\1*\n", text)
    text = re.sub(r"(?m)^###\s+(.*?)$", r"\n*\1*\n", text)

    # Negritas normales: **texto** → *texto*
    text = re.sub(r"\*\*(.*?)\*\*", r"*\1*", text)

    # Separadores --- → línea
    text = re.sub(r"(?m)^-{3,}$", "────────────────────────", text)

    # Listas
    text = re.sub(r"(?m)^\s*-\s+", "• ", text)
    text = re.sub(r"(?m)^\s*\d+\.\s+", "• ", text)

    # Tablas
    def repl_table(match):
        return "\n" + _format_md_table(match.group(0)) + "\n"

    table_pattern = re.compile(
        r"(?:^\s*\|.+\|\s*\n^\s*\|(?:\s*:?-{3,}:?\s*\|)+\s*\n(?:^\s*\|.*\|\s*\n?)+)",
        re.MULTILINE
    )
    text = table_pattern.sub(repl_table, text)

    # Limpieza final
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()
