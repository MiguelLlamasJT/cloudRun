import json
from app.execution_code import run_code_execution
from app import claude, bq_client, PROMPTS_PATH, logger
from app.utils_slack.slack_utils import send_message, update_message
from app.llms import call_claude_simple, call_claude_with_prompt, load_prompt
from app.bigQuery import run_query, build_query
from app.clients import match_customers, get_customer_list

def process_question(user_question: str, channel:str, user:str, threadts: str) -> str:
    try:
        logger.debug("User History: %s", user_question)
        queryable_json = call_claude_with_prompt(
            load_prompt(PROMPTS_PATH + "filter_messages2.txt", user_input = user_question)
            )
        logger.debug("🧠 Queryable JSON: %s", json.dumps(queryable_json))
        if (queryable_json["is_queryable"] == "no"):
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
        sql = build_query(filters_json)
        logger.debug(f"SQL generated:\n{sql}")
        df = run_query(sql)
        logger.debug("Shape:", df.shape)
        if (df.shape[0] > 100 or queryable_json["chart_requested"] == "yes"):
            output = run_code_execution(user_question, df, channel, user, threadts)
            update_message(channel, threadts, output)
        else:
            output = call_claude_simple(user_question, df)
            update_message(channel, threadts, output)
        

    except Exception as e:
        send_message(channel, f"Error procesando la pregunta: {e}", threadts)
        
