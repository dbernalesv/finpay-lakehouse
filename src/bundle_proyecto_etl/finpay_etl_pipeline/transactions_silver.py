import dlt
import pyspark.sql.functions as F

@dlt.table(
    name="fintech_finpay.silver.transactions_silver",
    comment="Transacciones limpias y tipadas (streaming con desempate)",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_amount", "amount IS NOT NULL AND amount > 0")
def transactions_silver():
    bronze_df = dlt.read_stream("fintech_finpay.bronze.transactions")

    # Mapear status a prioridad: aprobado=1, pendiente=2, rechazado=3
    bronze_df = bronze_df.withColumn(
        "status_priority",
        F.when(F.col("status") == "aprobado", 3)
         .when(F.col("status") == "pendiente", 2)
         .otherwise(1)
    )

    # Resolver duplicados con groupBy + max_by usando struct(status_priority, transaction_date)
    dedup_df = (
        bronze_df.groupBy("transaction_id")
        .agg(
            F.max_by("user_id", F.struct("status_priority", "transaction_date")).alias("user_id"),
            F.max_by("merchant_id", F.struct("status_priority", "transaction_date")).alias("merchant_id"),
            F.max_by("amount", F.struct("status_priority", "transaction_date")).alias("amount"),
            F.max_by("transaction_date", F.struct("status_priority", "transaction_date")).alias("transaction_date"),
            F.max_by("reference_id", F.struct("status_priority", "transaction_date")).alias("reference_id"),
            F.max_by("channel", F.struct("status_priority", "transaction_date")).alias("channel"),
            F.max_by("transaction_type", F.struct("status_priority", "transaction_date")).alias("transaction_type"),
            F.max_by("currency", F.struct("status_priority", "transaction_date")).alias("currency"),
            F.max_by("status", F.struct("status_priority", "transaction_date")).alias("status")
        )
    )

    # Validaciones adicionales
    valid_df = dedup_df.filter(
        (F.col("transaction_id").isNotNull()) &
        (F.col("user_id").isNotNull()) &
        (F.col("merchant_id").isNotNull()) &
        (F.col("amount").isNotNull()) & (F.col("amount") > 0) &
        (F.col("transaction_date").isNotNull()) &
        ~((F.col("transaction_type") == "reversa") & F.col("reference_id").isNull()) &
        (F.lower(F.col("channel")).isin("web","app","pos")) &
        (F.lower(F.col("transaction_type")).isin("pago","reversa","retiro")) &
        (F.upper(F.col("currency")).isin("PEN","USD","COP","MXN","CLP","ARS")) &
        (F.lower(F.col("status")).isin("aprobado","rechazado","pendiente"))
    )

    # Normalizaciones
    valid_df = valid_df.withColumn("channel", F.lower(F.trim("channel")))
    valid_df = valid_df.withColumn("transaction_type", F.lower(F.trim("transaction_type")))
    valid_df = valid_df.withColumn("currency", F.upper(F.trim("currency")))
    valid_df = valid_df.withColumn("status", F.lower(F.trim("status")))

    # Casteos
    valid_df = valid_df.withColumn("amount", F.regexp_replace("amount", "[^0-9.]", "").cast("DECIMAL(18,2)"))
    valid_df = valid_df.withColumn("transaction_date",
        F.coalesce(
            F.to_date("transaction_date", "yyyy-MM-dd"),
            F.to_date("transaction_date", "dd/MM/yyyy")
        )
    )

    # Reference_id regla
    valid_df = valid_df.withColumn("reference_id",
        F.when(F.col("transaction_type") == "reversa", F.col("reference_id"))
         .otherwise(F.lit("none"))
    ).withColumn("_ingested_at", F.current_date())

    return valid_df