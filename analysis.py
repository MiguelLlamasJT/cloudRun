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
You are a **financial data analyst** that translates natural language business questions 
into structured JSON representing SQL filters for a BigQuery table.

Your goal is to precisely identify filters, metrics, and comparison types from the user question.

---

### Table schema:
- data_week (DATE)'YYYY-MM-DD': snapshot week (always a Monday)
- sfdc_name_l3 (STRING): account name
- am_name_l3 (STRING): account manager name
- country (STRING): country code (BE, CO, DE, ES, FR, NO, PT, SE, UK, US)
- service_type_l3 (STRING): service type (Staffing, Outsourcing)
- month (DATE): first day of the month
- customer_type (STRING): EB, NE, Pipeline NB, Pipeline EB
- revenue (FLOAT)
- gross_profit (FLOAT)
- data_type (STRING): actuals or forecast
- cohort (INT): year of account signing deal

---

### Rules:
1. Always return **only valid JSON**, with no extra text.
2. JSON must have this structure:
   {{
     "filters": {{
       "column_name": ["value1","value2", ...]
     }},
     "metrics" : ["data_week", "sfdc_name_l3","am_name_l3", "country", "service_type_l3", "month", "customer_type", "revenue", "gross_profit", "data_type", "cohort"]
   }}
3. **Allowed filters**: data_week, sfdc_name_l3, am_name_l3, country, service_type_l3, month, customer_type, data_type, cohort.
4. **Gross margin logic:**
   - Note: gross_margin = gross_profit / revenue.
   - Do NOT add it unless explicitly or implicitly requested.
5. **Temporal logic:**
   - If the user mentions a **specific year (e.g. 2024, 2025)** → include the full year.
   - If no year is mentioned → use the **most recent year available**.
   - If no week is mentioned -> use only the most recent week
   - If the user says "last week" → include previous `data_week`.
   - If the user says "this week" → include current `data_week`.
   - If the user says "WoW variations" → include both current and previous `data_week`.
   - If the user says "month" or "quarter" → infer the relevant months within the given period or year.
6. **Default behavior:**
   - If time is mentioned but not granularity (week, month, quarter) → assume monthly.
   - If the user asks for “2025” or any year without months/weeks → return the **entire year**.
   - If no time reference at all → use the most recent available data.

---

### Example: If the user mentions a **specific year (e.g. 2024, 2025)** → include the full year in filters.
User: "Show me the WoW variations for gross margin and revenue in 2025 for Colombia Staffing"
JSON:
{{
  "filters": {{
    "country": ["CO"],
    "service_type_l3": ["Staffing"],
    "data_week": ["CURRENT", "PREVIOUS"],
    "month": ["2025-01-01","2025-02-01","2025-03-01","2025-04-01","2025-05-01","2025-06-01","2025-07-01","2025-08-01","2025-09-01","2025-10-01", "2025-11-01","2025-12-01"]
  }},
  "metrics" : ["data_week", "sfdc_name_l3","am_name_l3", "country", "service_type_l3", "month", "customer_type", "revenue", "gross_profit", "data_type", "cohort"]
}}


### Example 1b:
User: "Show me the WoW variations for gross margin and revenue in Q1 2025 / Q1.25 for Colombia Staffing"
JSON:
{{
  "filters": {{
    "country": ["CO"],
    "service_type_l3": ["Staffing"],
    "data_week": ["CURRENT", "PREVIOUS"],
    "month": ["2025-01-01","2025-02-01","2025-03-01"]
  }},
  "metrics" : ["data_week", "sfdc_name_l3","am_name_l3", "country", "service_type_l3", "month", "customer_type", "revenue", "gross_profit", "data_type", "cohort"]
}}

---

### Actual user question:
"{user_question}"

Respond with JSON only.
"""
    response = claude.messages.create(
        model="claude-3-5-haiku-latest",  # ✅ current stable choice
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
    SELECT 
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
