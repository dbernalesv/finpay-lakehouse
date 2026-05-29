import dlt
from pyspark.sql import DataFrame
import pyspark.sql.functions as F

# Importa las funciones de rechazo desde el módulo de utilidades
from rejection_utils import rejected_transactions, rejected_merchants, rejected_users

@dlt.table(
    name="fintech_finpay.silver.quarantine",
    comment="Tabla genérica de registros rechazados con trazabilidad",
    table_properties={"quality": "silver"}
)
def quarantine():
    # Rechazados de transactions
    transactions_rejected = rejected_transactions(dlt.read("fintech_finpay.bronze.transactions"))
    # Rechazados de merchants
    merchants_rejected = rejected_merchants(dlt.read("fintech_finpay.bronze.merchants"))
    # más adelante: 
    users_rejected = rejected_users(dlt.read("fintech_finpay.bronze.users"))

    # Unir todos los rechazados en una sola tabla genérica
    return transactions_rejected.unionByName(merchants_rejected, allowMissingColumns=True) \
                                 .unionByName(users_rejected, allowMissingColumns=True) \
                                 .withColumn("_ingested_at", F.current_date())
