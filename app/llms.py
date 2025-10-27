import pandas as pd
from app import claude, logger
from pathlib import Path
from app.utils_slack.format_utils import format_for_slack, safe_json_parse

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
            model="claude-haiku-4-5-20251001", 
            max_tokens=1000,
            messages=[{"role": "user", "content": prompt}]
        )
    output = response.content[0].text
    #logger.debug(output)
    input_tokens = "\n\nInput tokens: " + str(response.usage.input_tokens)
    logger.debug(input_tokens)
    return format_for_slack(output)