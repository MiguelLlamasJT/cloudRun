import os, json
from google.cloud import bigquery
import anthropic
import datetime
import pandas as pd
from code_execution import run_code_execution
from pathlib import Path


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
    where_clauses = []
    for col, vals in filters.get("filters", {}).items():
        if not vals:
            continue
        vals_sql = ", ".join([f"'{v}'" for v in vals])
        where_clauses.append(f"{col} IN ({vals_sql})")
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    sql = f"""
    SELECT {select_metrics}
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
        return (
            #f"Filtros: {filters_json}\n\n"
            #f"Resultados (primeras filas):\n{df.head(5).to_string(index=False)}\n\n"
            f"Code Execution:\n{code_exec_result}"
        )

    except Exception as e:
        return f"Error procesando la pregunta: {e}"
