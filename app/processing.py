import json
from app.execution_code import run_code_execution
from app import claude, bq_client, PROMPTS_PATH, logger
from app.utils_slack.slack_utils import send_message, update_message
from app.llms import call_claude_simple, call_claude_with_prompt, load_prompt
from app.bigQuery import run_query, build_query
from app.clients import clientLogic

def process_question(user_question: str, channel:str, user:str, threadts: str) -> str:
    try:
        #logger.debug("User History: %s", user_question)
        first_response = call_claude_with_prompt(
            load_prompt(PROMPTS_PATH + "filter_messages2.txt", user_input = user_question)
            )
        logger.debug("🧠 Queryable JSON: %s", json.dumps(first_response))
        if (first_response["is_queryable"] == "no"):
            send_message(channel, first_response["reply_to_user"], threadts)
            return
        threadts = send_message(channel=channel, thread_ts=threadts, text="💭 Thinking...")
        if (first_response["client_related"] == "yes"):
            mentioned = first_response.get("clients_mentioned") or []
            proceed, user_question = clientLogic(mentioned, user_question)
            if (proceed == "no"):
                update_message(channel=channel, ts=threadts, new_text=user_question)
                return
        filters_json = call_claude_with_prompt(load_prompt(PROMPTS_PATH + "query_filters.txt", user_input=user_question))
        logger.debug("🧠 Filters created: %s",json.dumps(filters_json))
        sql = build_query(filters_json)
        logger.debug(f"SQL generated:\n{sql}")
        df = run_query(sql)
        logger.debug("Shape:", df.shape)
        if (df.shape[0] > 100 or first_response["file_requested"] == "yes"):
            output = run_code_execution(user_question, df, channel, user, threadts)
            update_message(channel, threadts, output)
        else:
            output = call_claude_simple(user_question, df)
            update_message(channel, threadts, output)
        

    except Exception as e:
        send_message(channel, f"Error procesando la pregunta: {e}", threadts)
        
