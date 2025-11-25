import pandas as pd
from app import logger, bq_client


def build_query(filters: dict, table: str, allowed_columns: list, schema: dict) -> str:
    
    metrics = filters.get("metrics")
    select_metrics = []
    group_by = []
    i = 0
    while i < len(metrics):
        if (metrics[i] in ( "revenue", "gross_profit", "amount")):
            select_metrics.append(f"SUM({metrics[i]}) AS {metrics[i]}")
        else:
            select_metrics.append(f"{metrics[i]}")
            group_by.append(f"{metrics[i]}")
        i+=1
    select_metrics = ", ".join(select_metrics)
    group_by = ", ".join(group_by)
    where_clauses = []
    for col, vals in filters.get("filters", {}).items():
        if col not in allowed_columns:
            logger.warning("⚠️ Ignorando columna no válida: %s",col)
            continue
        if vals is None or vals == "":
            continue
        if isinstance(vals, (int, float, str)):
            vals = [vals]
        col_type = schema.get(col)
        if col_type is None:
            logger.warning("Columna %s no encontrada en schema, ignorando", col)
            continue
        if col_type == "int":
            vals_sql = ", ".join([f"{v}" for v in vals])
        else:
            vals_sql = ", ".join([f"'{v}'" for v in vals])
        where_clauses.append(f"{col} IN ({vals_sql})")
    where_clause = " AND ".join(where_clauses) if where_clauses else "1=1"

    sql = f"""
    SELECT {select_metrics}
    FROM `{table}`
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
        print(query_job)
        return query_job.to_dataframe()
    except Exception as e:
        logger.debug("Error ejecutando query.")
        return pd.DataFrame()


def get_table_schema_dict(table_full_id: str, normalizar: bool = False):

    table = bq_client.get_table(table_full_id)
    type_map = {
        "INT64": "int",
        "INTEGER": "int",
        "FLOAT64": "float",
        "FLOAT": "float",
        "NUMERIC": "decimal",
        "BIGNUMERIC": "decimal",
        "BOOL": "bool",
        "BOOLEAN": "bool",
        "STRING": "string",
        "DATE": "date",
        "DATETIME": "datetime",
    }

    schema_dict = {}
    for field in table.schema:
        tipo = field.field_type
        if normalizar:
            tipo = type_map.get(tipo, tipo)
        schema_dict[field.name] = tipo

    return schema_dict