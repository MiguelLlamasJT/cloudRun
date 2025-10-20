import os, json, datetime, re, asyncio, httpx
import pandas as pd
from app.execution_code import run_code_execution
from pathlib import Path
from rapidfuzz import fuzz, process
from starlette.concurrency import run_in_threadpool
from app import claude, bq_client, PROMPTS_PATH, logger
from app.utils_slack.slack_utils import send_message, update_message

""". USAR ESTO PARA LLAMADAS I/O, no para procesamiento como pandas, para eso usar ProcessPoolExecutor
from concurrent.futures import ThreadPoolExecutor //Para esto hacen falta 2 CPUs, solo tengo una

# Crea un pool limitado (por ejemplo, 5 threads)
custom_thread_pool = ThreadPoolExecutor(max_workers=5)

Y luego para usarlo en run_thread:
await loop.run_inexecutor(custom_thread_pool, func, *args, **kwargs)

"""

def load_prompt(file_name: str, **kwargs) -> str:
    try:
        path = Path(file_name)
        logger.info("leido file")
        text = path.read_text(encoding="utf-8")
        logger.info("leido texto")
        final_text = text.format(**kwargs)
        logger.info("formateado texto")
        return final_text
    except Exception as e:
        logger.debug("Fallo en cargar prompt")
        raise

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

def call_claude_with_prompt(prompt: str) -> str:
    try:
        #logger.debug(prompt)
        response = claude.messages.create(
            model="claude-haiku-4-5-20251001", 
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
        output = response.content[0].text
        logger.debug(output)
        safe_json = safe_json_parse(output)
        logger.debug(safe_json)
        input_tokens = "\n\nInput tokens: " + str(response.usage.input_tokens)
        logger.debug(input_tokens)
        return safe_json
    except Exception as e:
        logger.debug("Fallo en la llamada a claude.")
        raise
    
        
def build_query(filters: str) -> str:
    
    metrics = filters.get("metrics")
    select_metrics = ", ".join([f"{m}" for m in metrics]) if metrics else "*"
    allowed_columns = {
        "data_week", "sfdc_name_l3", "am_name_l3", "country", "week_label",
        "service_type_l3", "month", "customer_type", "cohort", "data_type"
    }
    where_clauses = []
    for col, vals in filters.get("filters", {}).items():
        if col not in allowed_columns:
            logger.warning("⚠️ Ignorando columna no válida: %s",col)
            continue
        if vals is None or vals == "":
            continue
        if isinstance(vals, (int, float, str)):
            vals = [vals]
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

def build_query_v2(filters: str) -> str:
    
    metrics = filters.get("metrics")
    select_metrics = []
    group_by = []
    i = 0
    while i < len(metrics):
        if (metrics[i] in ( "revenue", "gross_profit")):
            select_metrics.append(f"SUM({metrics[i]}) AS {metrics[i]}")
        else:
            select_metrics.append(f"{metrics[i]}")
            group_by.append(f"{metrics[i]}")
        i+=1
    select_metrics = ", ".join(select_metrics)
    group_by = ", ".join(group_by)

    allowed_columns = {
        "data_week", "week_label", "sfdc_name_l3", "am_name_l3", "country",
        "service_type_l3", "month", "customer_type", "cohort", "data_type"
    }
    where_clauses = []
    for col, vals in filters.get("filters", {}).items():
        if col not in allowed_columns:
            logger.warning("⚠️ Ignorando columna no válida: %s",col)
            continue
        if vals is None or vals == "":
            continue
        if isinstance(vals, (int, float, str)):
            vals = [vals]
        vals_sql = ", ".join([f"'{v}'" for v in vals])
        where_clauses.append(f"{col} IN ({vals_sql})")
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    sql = f"""
    SELECT {select_metrics}
    FROM `jt-prd-financial-pa.random_data.real_data`
    WHERE {where_clause}
    GROUP BY {group_by}
    ORDER BY {group_by};
    """
    return sql

def get_customer_list():
    sql = """
    SELECT DISTINCT sfdc_name_l3
    FROM `jt-prd-financial-pa.random_data.real_data`
    WHERE sfdc_name_l3 IS NOT NULL
    """
    df = run_query(sql)
    logger.debug("Succesfull customer list.")
    return df

def run_query(sql: str):
    #max_tries = 3
    #current_tries = 0
    try:
        query_job = bq_client.query(sql)
        return query_job.to_dataframe()
    except Exception as e:
        logger.debug("Error ejecutando query.")
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
                    logger.debug("%s - score: %s", match_name, score)
                    exact_matches.add(match_name)
                elif 55 <= score < 85:
                    logger.debug("%s - score: %s", match_name, score)
                    fuzzy_candidates.add(match_name)

        if exact_matches:
            return {"case": "direct_match", "exact": list(exact_matches), "candidates": []}
        elif fuzzy_candidates:
            return {"case": "ambiguous_match", "exact": [], "candidates": list(fuzzy_candidates)}
        else:
            return {"case": "not_found", "exact": [], "candidates": []}
    except Exception as e:
        logger.error("Error en match customers")
        raise e

""" FUTURA IMPLEMENTACION DE HILOS CONCURRENTES
async def procesar_async(body):

    tarea_bq = await run_in_threadpool(run_query())
    tarea_claude = await run_in_threadpool(call_claude_with_prompt, body["text"])
    resultados_bq, resultados_claude = await asyncio.gather(tarea_bq, tarea_claude)

    print("✅ BigQuery result:", resultados_bq)
    print("✅ Claude result:", resultados_claude)

def process_questionNew(user_question: str) -> None:
    loop = asyncio.get_event_loop()
    asyncio.run_coroutine_threadsafe(procesar_async(user_question), loop)
"""

def call_claude_simple(user_question: str, df: pd.DataFrame) ->str:
    df_json = df.to_json(orient="records")
    prompt = f"""
    You are a data analyst. I will give you a question and a dataset in JSON format.
    Available columns in source table:
    - date_week: (DATE)'YYYY-MM-DD': snapshot week (always a Monday)
    - week_label: wk0, wk-1
    - salesforce name : client name
    - month: month of the year
    - country (BE, CO, DE, ES, FR, NO, PT, SE, UK, US)
    - service type (Staffing or Outsourcing)
    - customer type: (EB (existing business), NB (new business), Pipeline NB, Pipeline EB)
    - cohort: year the client was signed
    - data_type: (actuals, forecast)
    - revenue
    - gross_profit
    - gross_margin (= gross_profit / revenue)
    Question:
    {user_question}
    Data (JSON):
    {df_json}
    Based on the dataset, answer the question clearly and accurately.
    """
    response = claude.messages.create(
            model="claude-sonnet-4-5-20250929", 
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
    output = response.content[0].text
    #logger.debug(output)
    input_tokens = "\n\nInput tokens: " + str(response.usage.input_tokens)
    logger.debug(input_tokens)
    return output

def process_question(user_question: str, channel:str, user:str, threadts: str) -> str:
    try:
        logger.debug("User History: %s", user_question)
        queryable_json = call_claude_with_prompt(
            load_prompt(PROMPTS_PATH + "filter_messages2.txt", user_input = user_question)
            )
        logger.debug("🧠 Queryable JSON: %s", json.dumps(queryable_json))
        if (queryable_json["is_queryable"] == "no" or queryable_json["confirmation_required"] == "yes"):
            send_message(channel, queryable_json["reply_to_user"], threadts)
            return
        threadts = send_message(channel=channel, thread_ts=threadts, text="💭 Thinking...")
        if (queryable_json["client_related"] == "yes"):
            df_clients = get_customer_list()
            all_clients = df_clients["sfdc_name_l3"].dropna().astype(str).tolist()
            mentioned = queryable_json.get("clients_mentioned") or []
            if mentioned:
                matched = match_customers(mentioned, all_clients, top_n=10)
                logger.debug("🔍 Matching result: %s", json.dumps(matched))
                if matched["case"] == "direct_match":
                    logger.debug("Found exact customers.")
                    customer_string = ", ".join(matched["exact"])
                    user_question += f"\n\nThese are the exact customers detected with an internal function based on the thread: {customer_string}"
                elif matched["case"] == "ambiguous_match":
                    candidates = ", ".join(matched["candidates"])
                    logger.debug("Found similar customers.")
                    update_message(channel,threadts, f"❓ I couldn’t find exact matches for those clients. Did you mean one of these?\n{candidates}")
                    return
                elif matched["case"] == "not_found":
                    logger.debug("No customer found.")
                    update_message(channel,threadts,  "❌ I couldn’t find any customers matching that name. Could you rephrase or check the spelling?")
                    return
            else:
                logger.debug("⚠️ No clients mentioned, proceeding normally.")
        filters_json = call_claude_with_prompt(load_prompt(PROMPTS_PATH + "query_filters.txt", user_input=user_question))
        logger.debug("🧠 Filters created: %s",json.dumps(filters_json))
        #sql = build_query(filters_json)
        sql = build_query_v2(filters_json)
        logger.debug(f"SQL generated:\n{sql}")
        df = run_query(sql)
        logger.debug("Shape:", df.shape)
        if (df.shape[0] > 100):
            code_exec_result = run_code_execution(user_question, df, channel, user, threadts)
            output = format_for_slack(code_exec_result)
            update_message(channel, threadts, output)
        else:
            code_exec_result = call_claude_simple(user_question, df)
            output = format_for_slack(code_exec_result)
            update_message(channel, threadts, output)
        

    except Exception as e:
        send_message(channel, threadts,f"Error procesando la pregunta: {e}")
        
