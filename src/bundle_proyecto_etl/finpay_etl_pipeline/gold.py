import dlt
import pyspark.sql.functions as F

# Dim Channel
@dlt.table(
    name="fintech_finpay.gold.dim_channel",
    comment="Catálogo de canales de origen",
    table_properties={"quality": "gold"}
)
def dim_channel():
    df = dlt.read_stream("fintech_finpay.silver.transactions_silver") \
             .select("channel").distinct()

    # Asignar IDs fijos a cada canal
    df = df.withColumn(
        "channel_id",
        F.when(F.col("channel")=="web", F.lit(1))
         .when(F.col("channel")=="app", F.lit(2))
         .when(F.col("channel")=="pos", F.lit(3))
         .otherwise(F.lit(99)) # otros
    ).withColumn("_ingested_at", F.current_date())

    return df.select("channel_id", F.col("channel").alias("des_channel"), "_ingested_at")

# Fact Transactions
@dlt.table(
    name="fintech_finpay.gold.fact_transactions",
    comment="Tabla de hechos con métricas y score de riesgo",
    table_properties={"quality": "gold"}
)
def fact_transactions():
    tx = dlt.read_stream("fintech_finpay.silver.transactions_silver") \
            .withColumn("transaction_ts", F.col("transaction_date").cast("timestamp")) \
            .withWatermark("transaction_ts", "1 day")

    merchants = spark.read.table("fintech_finpay.silver.merchants_silver") \
        .withColumn(
            "risk_score",
            F.when(F.lower(F.col("risk_level")) == "bajo", F.lit(1))
             .when(F.lower(F.col("risk_level")) == "medio", F.lit(2))
             .when(F.lower(F.col("risk_level")) == "alto", F.lit(3))
             .otherwise(F.lit(0))
        )

    tx_enriched = tx.join(merchants.select("merchant_id","risk_score"), "merchant_id", "left") \
                    .withColumn("risk_score", F.coalesce(F.col("risk_score"), F.lit(0)))

    # Leer catálogo de canales con dlt.read
    channels = dlt.read("fintech_finpay.gold.dim_channel")
    tx_enriched = tx_enriched.join(channels, tx_enriched["channel"] == channels["des_channel"], "left")

    agg = (
        tx_enriched.groupBy("user_id","merchant_id","channel_id","transaction_date")
          .agg(
              F.sum("amount").alias("total_amount"),
              F.count("*").alias("transaction_count"),
              (F.sum(F.when(F.col("transaction_type")=="reversa",1).otherwise(0)) / F.count("*")).alias("reversal_rate"),
              F.avg("risk_score").alias("avg_risk_score")
          ).withColumn("_ingested_at", F.current_date())
    )

    return agg

# Dim Merchant
@dlt.table(name="fintech_finpay.gold.dim_merchant", comment="Dimensión de comercios")
def dim_merchant():
    return dlt.read_stream("fintech_finpay.silver.merchants_silver").withColumn("_ingested_at", F.current_date())

# Dim User
@dlt.table(name="fintech_finpay.gold.dim_user", comment="Dimensión de usuarios con PII enmascarado")
def dim_user():
    df = dlt.read_stream("fintech_finpay.silver.users_silver")
    return df.select(
        "user_id",
        F.expr("fintech_finpay.silver.mask_full_name(full_name)").alias("full_name"),
        F.expr("fintech_finpay.silver.mask_document_id(document_id)").alias("document_id"),
        F.expr("fintech_finpay.silver.mask_email(email)").alias("email"),
        F.expr("fintech_finpay.silver.mask_phone(phone)").alias("phone"),
        "segment",
        "country",
        "registration_date"
    ).withColumn("_ingested_at", F.current_date())


# Dim Date
@dlt.table(
    name="fintech_finpay.gold.dim_date",
    comment="Dimensión de fechas dinámica basada en transacciones",
    table_properties={"quality": "gold"}
)
def dim_date():
    # Leer fechas distintas desde transacciones
    df = dlt.read_stream("fintech_finpay.silver.transactions_silver") \
             .select("transaction_date").distinct()

    # Generar atributos de calendario
    return (
        df.withColumn("day", F.dayofmonth("transaction_date"))
          .withColumn("week", F.weekofyear("transaction_date"))
          .withColumn("month", F.month("transaction_date"))
          .withColumn("quarter", F.quarter("transaction_date"))
          .withColumn("year", F.year("transaction_date"))
          .withColumnRenamed("transaction_date", "date_id")
          .withColumn("_ingested_at", F.current_date())
    )
