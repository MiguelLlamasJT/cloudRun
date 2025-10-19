import pandas as pd
import numpy as np
import random

# Configuración
N_SF = 100
start_date = "2024-01-01"
end_date = "2025-12-01"

# Posibles valores
countries = ["BE", "CO", "DE", "ES", "FR", "PT", "NO"]
service_types = ["Staffing", "Outsourcing"]
am_names = ["am1", "am2", "am3", "am4"]
customer_types = ["EB", "NE", "Pipeline NB", "Pipeline EB"]
cohorts = [2022, 2023, 2024, 2025]

# Generamos mapping de SF -> atributos fijos
sf_list = [f"sf{i}" for i in range(1, N_SF + 1)]
sf_mapping = {}

for sf in sf_list:
    sf_mapping[sf] = {
        "country": random.choice(countries),
        "service_type_l3": random.choice(service_types),
        "am_name_l3": random.choice(am_names),
        "customer_type": random.choice(customer_types),
        "cohort": random.choice(cohorts),
    }

# Fechas mensuales
months = pd.date_range(start_date, end_date, freq="MS")

# Fecha actual de snapshot
data_week = pd.to_datetime("2025-09-29").strftime("%Y-%m-%d")

rows = []

for sf in sf_list:
    attrs = sf_mapping[sf]
    for month in months:
        # actuals si el mes es <= fecha actual, forecast si es futuro
        data_type = "actuals" if month <= pd.to_datetime("today") else "forecast"
        revenue = round(np.random.uniform(50_000, 500_000), 2)
        gross_profit = round(np.random.uniform(5_000, 40_000), 2)
        
        rows.append({
            "data_week": data_week,
            "sfdc_name_l3": sf,
            "am_name_l3": attrs["am_name_l3"],
            "country": attrs["country"],
            "service_type_l3": attrs["service_type_l3"],
            "month": month.strftime("%Y-%m-%d"),
            "customer_type": attrs["customer_type"],
            "revenue": revenue,
            "gross_profit": gross_profit,
            "data_type": data_type,
            "cohort": attrs["cohort"],
        })

df = pd.DataFrame(rows)
df.to_csv("mock_data.csv", index=False)
print("CSV generado con", len(df), "filas")

