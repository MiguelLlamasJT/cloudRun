import os, json
from google.cloud import bigquery
import anthropic
import datetime
import pandas as pd


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
- country (STRING): country code (e.g. ES, FR, BE, DE)
- service_type_l3 (STRING): service type (e.g. Staffing, Outsourcing)
- month (DATE): first day of the month
- customer_type (STRING): EB, NE, Pipeline NB, Pipeline EB
- revenue (FLOAT)
- gross_profit (FLOAT)
- data_type (STRING): actuals or forecast
- cohort (INT)

### Rules:
1. Always return **only valid JSON**, with no extra text.
2. JSON must have the structure:
   {{
     "filters": {{"column_name": ["value1","value2"]}},
     "metrics": ["revenue","gross_profit"],
     "comparison": "YoY" | "MoM" | "WoW" | "none"
   }}
3. Filters are allowed only on:  
   [data_week, sfdc_name_l3, am_name_l3, country, service_type_l3, month, customer_type, cohort].
4. About **data_week**:
   - If the user does not mention any week → use the most recent data_week.
   - If the user says "last week" → filter by the previous data_week.
   - If the user says "WoW variations" → include both the current and previous data_week.
5. About **month**:
   - If the user does not mention any year -> use the current year.
6. Comparison field:
   - If "YoY" mentioned → "YoY"
   - If "MoM" or "month over month" → "MoM"
   - If "WoW" or "week over week" → "WoW"
   - Otherwise → "none"

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
    FROM `jt-prd-financial-pa.random_data.anonymized`
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

        # Devolver algo legible para Slack (ejemplo: primeras filas)
        return f"Filtros: {filters_json}\n\nResultados:\n{df.head(5).to_string(index=False)}"

    except Exception as e:
        return f"Error procesando la pregunta: {e}"
