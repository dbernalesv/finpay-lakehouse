import dlt
import pyspark.sql.functions as F

@dlt.table(
    name="fintech_finpay.silver.merchants_silver",
    comment="Merchants limpios y tipados (streaming)",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_affiliation_date", "affiliation_date IS NOT NULL")
def merchants_silver():
    bronze_df = dlt.read_stream("fintech_finpay.bronze.merchants")

    # Prioridad: merchant_name válido = 2, vacío/nulo = 1
    bronze_df = bronze_df.withColumn(
        "name_priority",
        F.when(F.col("merchant_name").isNotNull() & (F.trim(F.col("merchant_name")) != ""), 2)
         .otherwise(1)
    )

    # Resolver duplicados con groupBy + max_by usando struct(name_priority, affiliation_date)
    dedup_df = (
        bronze_df.groupBy("merchant_id")
        .agg(
            F.max_by("merchant_name", F.struct("name_priority", "affiliation_date")).alias("merchant_name"),
            F.max_by("category", F.struct("name_priority", "affiliation_date")).alias("category"),
            F.max_by("country", F.struct("name_priority", "affiliation_date")).alias("country"),
            F.max_by("affiliation_date", F.struct("name_priority", "affiliation_date")).alias("affiliation_date"),
            F.max_by("status", F.struct("name_priority", "affiliation_date")).alias("status"),
            F.max_by("risk_level", F.struct("name_priority", "affiliation_date")).alias("risk_level")
        )
    )

    # Validaciones
    valid_df = dedup_df.filter(
        (F.col("merchant_id").isNotNull()) &
        (F.col("merchant_name").isNotNull()) & (F.trim(F.col("merchant_name")) != "") &
        ((F.lower(F.col("category")).isin("retail","restaurante","farmacia","supermercado",
                                          "tecnologia","transporte","educacion","salud",
                                          "entretenimiento","moda")) | F.col("category").isNull()) &
        ((F.upper(F.col("country")).isin("PE","CO","MX","CL","AR")) | F.col("country").isNull()) &
        (F.to_date("affiliation_date","yyyy-MM-dd").isNotNull()) &
        ((F.lower(F.col("status")).isin("activo","inactivo","suspendido")) | F.col("status").isNull()) &
        ((F.col("risk_level").isNotNull() & F.lower(F.col("risk_level")).isin("bajo","medio","alto")) |
         (F.col("risk_level").isNull() & F.col("category").isNull()))
    )

    # Normalizaciones
    valid_df = valid_df.withColumn("category", F.lower(F.trim("category")))
    valid_df = valid_df.withColumn("status", F.lower(F.trim("status")))
    valid_df = valid_df.withColumn("risk_level", F.lower(F.trim("risk_level")))
    valid_df = valid_df.withColumn("country", F.upper(F.trim("country")))

    # Casteo affiliation_date
    valid_df = valid_df.withColumn("affiliation_date", F.to_date("affiliation_date","yyyy-MM-dd")) \
        .withColumn("_ingested_at", F.current_date())

    return valid_df