from rapidfuzz import fuzz, process
import json
from app.execution_code import run_code_execution
from app import PROMPTS_PATH, logger
from app.llms import call_claude_with_prompt, load_prompt
from app.bigQuery import run_query, build_query


def clientLogic(first_response, user_question: str, channel:str, user:str, threadts: str) -> str:
    mentioned = first_response["clients_mentioned"] or []
    proceed, user_question = clientSimilar(mentioned, user_question)
    if (proceed == "no"):
        return user_question
    filters_json = call_claude_with_prompt(load_prompt(PROMPTS_PATH + "query_filters.txt", user_input=user_question))
    logger.debug("🧠 Filters created: %s",json.dumps(filters_json))
    sql = build_query(filters_json, "jt-prd-financial-pa.random_data.real_data")
    logger.debug(f"SQL generated:\n{sql}")
    df = run_query(sql)
    logger.debug("Shape:", df.shape)
    output = run_code_execution(user_question, df, channel, user, threadts)
    return output

def clientSimilar(mentioned, user_question):
    df_clients = get_customer_list()
    all_clients = df_clients["sfdc_name_l3"].dropna().astype(str).tolist()
    if mentioned:
        matched = match_customers(mentioned, all_clients, top_n=10)
        logger.debug("🔍 Matching result: %s", json.dumps(matched))
        if matched["case"] == "direct_match":
            logger.debug("Found exact customers.")
            customer_string = ", ".join(matched["exact"])
            user_question += f"\n\nThese are the exact customers detected with an internal function based on the thread: {customer_string}"
            return "yes", user_question
        elif matched["case"] == "ambiguous_match":
            candidates = ", ".join(matched["candidates"])
            logger.debug("Found similar customers.")
            return "no", f"❓ I couldn’t find exact matches for those clients. Did you mean one of these?\n{candidates}"
        elif matched["case"] == "not_found":
            logger.debug("No customer found.")
            return "no","❌ I couldn’t find any customers matching that name. Could you rephrase or check the spelling?"
    else:
        logger.debug("No clients mentioned.")
        return "yes", user_question



def get_customer_list():
    sql = """
    SELECT DISTINCT sfdc_name_l3
    FROM `jt-prd-financial-pa.random_data.real_data`
    WHERE sfdc_name_l3 IS NOT NULL
    """

    df = run_query(sql)
    logger.debug("Succesfull customer list.")
    return df

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