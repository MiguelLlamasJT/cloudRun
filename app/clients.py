from app import logger
from rapidfuzz import fuzz, process
from app.bigQuery import run_query

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