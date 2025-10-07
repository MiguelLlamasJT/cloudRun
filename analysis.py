import os, json
from google.cloud import bigquery
import anthropic
import datetime
import pandas as pd
from code_execution import run_code_execution
from pathlib import Path
import re

bq_client = bigquery.Client()
claude = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


def load_prompt(file_name: str, **kwargs) -> str:
    path = Path(file_name)
    text = path.read_text(encoding="utf-8")
    return text.format(**kwargs)

def get_current_and_last_monday():
    today = datetime.date.today()
    days_since_monday = today.weekday()
    current_monday = today - datetime.timedelta(days=days_since_monday)
    last_monday = current_monday - datetime.timedelta(days=7)
    return current_monday, last_monday

def resolve_dataweek(filters):
    current, previous = get_current_and_last_monday()
    resolved = []
    for val in filters.get("data_week", []):
        if val == "CURRENT":
            resolved.append(str(current))
        elif val == "PREVIOUS":
            resolved.append(str(previous))
        else:
            resolved.append(val)
    filters["data_week"] = resolved
    return filters

def call_claude_with_prompt(filename: str, user_input: str) -> str:
    prompt = load_prompt(file_name = filename, user_input = user_input)
    response = claude.messages.create(
        model="claude-3-5-haiku-latest", 
        max_tokens=1000,
        messages=[{"role": "user", "content": prompt}]
    )
    print(response.content[0].text)
    return response.content[0].text

def build_query(filters_json: str) -> str:
    try:
        filters = json.loads(filters_json)
    except json.JSONDecodeError as e:
        print("❌ Claude devolvió JSON inválido:", filters_json)
        raise e
    filters["filters"] = resolve_dataweek(filters.get("filters") or {})
    metrics = filters.get("metrics") or ["revenue", "gross_profit"]
    select_metrics = ", ".join([f"{m}" for m in metrics]) if metrics else "*"
    allowed_columns = {
        "data_week", "sfdc_name_l3", "am_name_l3", "country",
        "service_type_l3", "month", "customer_type", "cohort", "data_type"
    }
    where_clauses = []
    for col, vals in filters.get("filters", {}).items():
        if col not in allowed_columns:
            print(f"⚠️ Ignorando columna no válida: {col}")
            continue
        if vals is None or vals == "":
            continue
        if isinstance(vals, (int, float, str)):
            vals = [vals]
        vals_sql = ", ".join([f"'{v}'" for v in vals])
        where_clauses.append(f"{col} IN ({vals_sql})")
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    sql = f"""
    SELECT *
    FROM `jt-prd-financial-pa.random_data.real_data`
    WHERE {where_clause}
    ORDER BY country, month;
    """
    return sql

def run_query(sql: str):
    try:
        query_job = bq_client.query(sql)
        return query_job.to_dataframe()
    except Exception as e:
        print(f"Error ejecutando query: {e}")
        return pd.DataFrame()


def format_for_slack(text: str) -> str:
    # Sustituir Markdown por formato Slack
    text = re.sub(r"\*\*(.*?)\*\*", r"*\1*", text)  # **bold** → *bold*
    text = re.sub(r"##+\s*", ">*", text)  # ## títulos → cita con asterisco
    text = re.sub(r"###\s*", ">*", text)
    text = re.sub(r"\n-{2,}\n", "\n", text)  # eliminar separadores ----
    return text

def process_question(user_question: str) -> str:
    try:
        queryable_json = json.loads(call_claude_with_prompt("filter_messages.txt", user_question))
        if (queryable_json["is_queryable"] == "no"):
            return(queryable_json["reply_to_user"])
        filters_json = call_claude_with_prompt("query_filters.txt", user_question)
        sql = build_query(filters_json)
        print(f"SQL generated:\n{sql}")
        df = run_query(sql)
        print("Shape:", df.shape)
        code_exec_result = run_code_execution(user_question, df) 
        output = format_for_slack(code_exec_result)
        return (
            #f"Filtros: {filters_json}\n\n"
            #f"Resultados (primeras filas):\n{df.head(5).to_string(index=False)}\n\n"
            f"Code Execution:\n{output}"
        )

    except Exception as e:
        return f"Error procesando la pregunta: {e}"
