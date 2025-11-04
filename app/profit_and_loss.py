import json
from app.execution_code import run_code_execution
from app import PROMPTS_PATH, logger
from app.llms import call_claude_with_prompt, load_prompt
from app.bigQuery import run_query, build_query
from datetime import date

def pnlLogic(user_question: str, channel:str, user:str, threadts: str) -> str:
    current_week = calculate_current_week()
    filters_json = call_claude_with_prompt(load_prompt(PROMPTS_PATH + "query_pNl.txt", user_input=user_question, current_week=current_week))
    logger.debug("🧠 Filters created: %s",json.dumps(filters_json))
    sql = build_query(filters_json, "jt-prd-financial-pa.random_data.pnl_data")
    logger.debug(f"SQL generated:\n{sql}")
    df = run_query(sql)
    logger.debug("Shape:", df.shape)
    output = run_code_execution(user_question, df, channel, user, threadts)
    return output

def calculate_current_week() -> str:
    fecha = date.today()
    year, week, weekday = fecha.isocalendar()
    week -= 1
    return f"{year}_{week:02d}"