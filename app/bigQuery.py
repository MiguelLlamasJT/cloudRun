import pandas as pd
from app import logger, bq_client


def build_query(filters: str) -> str:
    
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



def run_query(sql: str):
    #max_tries = 3
    #current_tries = 0
    try:
        query_job = bq_client.query(sql)
        return query_job.to_dataframe()
    except Exception as e:
        logger.debug("Error ejecutando query.")
        return pd.DataFrame()
