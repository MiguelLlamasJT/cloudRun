import os, json
from google.cloud import bigquery
import anthropic
import datetime
import pandas as pd
from code_execution import run_code_execution

bq_client = bigquery.Client()
claude = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))


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

def get_filters_from_claude(user_question: str):
    prompt = f"""
You are an assistant that translates natural language business questions into structured JSON 
representing SQL filters for a table in BigQuery.

### Table schema:
- data_week (DATE): snapshot week (always a Monday)
- sfdc_name_l3 (STRING): account name
- am_name_l3 (STRING): account manager name
- country (STRING): country code (BE (belgium), CO, DE, ES, FR, NO, PT, SE, UK, US)
- service_type_l3 (STRING): service type (e.g. Staffing, Outsourcing)
- month (DATE): first day of the month
- customer_type (STRING): EB, NE, Pipeline NB, Pipeline EB
- revenue (FLOAT)
- gross_profit (FLOAT)
- data_type (STRING): actuals or forecast
- cohort (INT): year of account's signing deal

---

### TASK:
Convert the user's natural language business question into **valid JSON** representing SQL filters and metrics selection.

---
### OUTPUT FORMAT (strictly this structure):
{{
  "filters": {
    "column_name": ["value1", "value2"]
  },
  "metrics": ["revenue", "gross_profit"],
  "comparison": "YoY" | "MoM" | "WoW" | "none"
}}

### Rules:
1. Return **only valid JSON** — no explanations, comments, or formatting text.
2. Filters are allowed **only** on:
   [data_week, sfdc_name_l3, am_name_l3, country, service_type_l3, month, customer_type, cohort].
3. If the question implies a **country or service type**, include them explicitly.
4. For **time references**:
   - “Q1 2025”, “first quarter 2025” → months ["2025-01-01", "2025-02-01", "2025-03-01"]
   - “year 2025” → all months in 2025
   - “this year” → current year
   - “last week” → data_week = ["PREVIOUS"]
   - “this week” → data_week = ["CURRENT"]
   - “WoW variations” or “week over week” → data_week = ["CURRENT", "PREVIOUS"]
   - “YoY” → comparison = "YoY"
   - “MoM” → comparison = "MoM"
5. If no week is mentioned, use the most recent data_week.
6. If no metric is mentioned, default to both revenue and gross_profit.
7. The "comparison" field must always exist (even if "none").
8. If the year is missing in a time expression, assume the current year.
9. If the question is ambiguous, pick the **most likely** business interpretation rather than returning an empty field.

---

### Example (complex):
User: "I want the WoW variations for Q1 revenue and gross margin in 2025 for ES Staffing"
JSON:
{{
  "filters": {{
    "country": ["ES"],
    "service_type_l3": ["Staffing"],
    "month": ["2025-01-01","2025-02-01","2025-03-01"],
    "data_week": ["CURRENT","PREVIOUS"]
  }},
  "metrics": ["revenue","gross_profit"],
  "comparison": "WoW"
}}

---

### Actual user question:
"{user_question}"

Respond with JSON only.
    """
    response = claude.messages.create(
        model="claude-3-5-haiku-20241022",  # ✅ current stable choice
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

    # SELECT dinámico
    select_metrics = ", ".join([f"{m}" for m in metrics]) if metrics else "*"

    # WHERE dinámico
    where_clauses = []
    for col, vals in filters.get("filters", {}).items():
        if not vals:
            continue
        vals_sql = ", ".join([f"'{v}'" for v in vals])
        where_clauses.append(f"{col} IN ({vals_sql})")
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    sql = f"""
    SELECT sfdc_name_l3, country, month, {select_metrics}
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
        filters_json = get_filters_from_claude(user_question)
        sql = build_query(filters_json)
        print(f"SQL generated:\n{sql}")
        df = run_query(sql)
        print("Shape:", df.shape)
        code_exec_result = run_code_execution(user_question, df) 
        # Devolver algo legible para Slack (ejemplo: primeras filas)
        return (
            f"Filtros: {filters_json}\n\n"
            #f"Resultados (primeras filas):\n{df.head(5).to_string(index=False)}\n\n"
            f"Code Execution:\n{code_exec_result}"
        )

    except Exception as e:
        return f"Error procesando la pregunta: {e}"
