import os, json
from google.cloud import bigquery
import anthropic
import datetime
import pandas as pd
from code_execution import run_code_execution
from pathlib import Path
import re
from rapidfuzz import fuzz, process

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

def call_claude_with_prompt(prompt: str) -> str:
    try:
        response = claude.messages.create(
            model="claude-3-5-haiku-latest", 
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        return response.content[0].text
    except Exception as e:
        print("Fallo en la llamada a claude.")
        raise
        


def build_query(filters: str) -> str:
    try:
        filters["filters"] = resolve_dataweek(filters.get("filters") or {})
    except Exception as e:
        print("Fallo al procesar dataweek.")
        raise
    
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

def get_customer_list():
    sql = """
    SELECT DISTINCT sfdc_name_l3
    FROM `jt-prd-financial-pa.random_data.real_data`
    WHERE sfdc_name_l3 IS NOT NULL
    """
    df = run_query(sql)
    print("Succesfull customer list.")
    return df

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

def match_customers(mentioned_clients: list, all_customers: list, top_n: int = 10):
    try:
        exact_matches = set()
        fuzzy_candidates = set()

        for name in mentioned_clients:
            results = process.extract(name, all_customers, scorer=fuzz.token_sort_ratio, limit=top_n)
            for match_name, score, _ in results:
                if score >= 85:
                    print(f"{match_name}score: {score}")
                    exact_matches.add(match_name)
                elif 55 <= score < 85:
                    print(f"{match_name}score: {score}")
                    fuzzy_candidates.add(match_name)

        if exact_matches:
            return {"case": "direct_match", "exact": list(exact_matches), "candidates": []}
        elif fuzzy_candidates:
            return {"case": "ambiguous_match", "exact": [], "candidates": list(fuzzy_candidates)}
        else:
            return {"case": "not_found", "exact": [], "candidates": []}
    except Exception as e:
        print("Error en match customers")
        raise e

def process_question(user_question: str) -> str:
    try:
        print("User History: " + user_question)
        queryable_json = json.loads(
            call_claude_with_prompt(
                load_prompt("filter_messages.txt", user_input = user_question)
                )
            )
        print("🧠 Queryable JSON:", queryable_json)
        if (queryable_json["is_queryable"] == "no" or queryable_json["confirmation_required"] == "yes"):
            return(queryable_json["reply_to_user"])
        if (queryable_json["client_related"] == "yes"):
            df_clients = get_customer_list()
            all_clients = df_clients["sfdc_name_l3"].dropna().astype(str).tolist()
            mentioned = queryable_json.get("clients_mentioned") or []
            if mentioned:
                matched = match_customers(mentioned, all_clients, top_n=10)
                print("🔍 Matching result:", matched)
                if matched["case"] == "direct_match":
                    customer_string = ", ".join(matched["exact"])
                    user_question += f"\n\nThese are the exact customers detected: {customer_string}"
                elif matched["case"] == "ambiguous_match":
                    candidates = ", ".join(matched["candidates"])
                    return f"❓ I couldn’t find exact matches for those clients. Did you mean one of these?\n{candidates}"
                elif matched["case"] == "not_found":
                    return "❌ I couldn’t find any customers matching that name. Could you rephrase or check the spelling?"
            else:
                print("⚠️ No clients mentioned, proceeding normally.")
        filters_json = json.loads(call_claude_with_prompt(load_prompt("query_filters.txt", user_input=user_question)))
        print("🧠 Filters created:",filters_json)
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
