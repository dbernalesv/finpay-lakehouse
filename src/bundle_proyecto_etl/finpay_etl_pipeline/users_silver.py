import dlt
import pyspark.sql.functions as F

@dlt.table(
    name="fintech_finpay.silver.users_silver",
    comment="Usuarios limpios y tipados (streaming)",
    table_properties={"quality": "silver"}
)
@dlt.expect("valid_email", "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'")
def users_silver():
    bronze_df = dlt.read_stream("fintech_finpay.bronze.users")

    # Prioridad: document_id válido = 2, nulo = 1
    bronze_df = bronze_df.withColumn(
        "doc_priority",
        F.when(F.col("document_id").isNotNull(), 2).otherwise(1)
    )

    # Resolver duplicados con groupBy + max_by usando struct(doc_priority, registration_date)
    dedup_df = (
        bronze_df.groupBy("user_id")
        .agg(
            F.max_by("full_name", F.struct("doc_priority", "registration_date")).alias("full_name"),
            F.max_by("document_id", F.struct("doc_priority", "registration_date")).alias("document_id"),
            F.max_by("email", F.struct("doc_priority", "registration_date")).alias("email"),
            F.max_by("phone", F.struct("doc_priority", "registration_date")).alias("phone"),
            F.max_by("country", F.struct("doc_priority", "registration_date")).alias("country"),
            F.max_by("segment", F.struct("doc_priority", "registration_date")).alias("segment"),
            F.max_by("registration_date", F.struct("doc_priority", "registration_date")).alias("registration_date")
        )
    )

    # Validaciones
    valid_df = dedup_df.filter(
        (F.col("user_id").isNotNull()) &
        (F.col("full_name").isNotNull()) & (F.trim(F.col("full_name")) != "") &
        (~F.col("full_name").rlike("[,;|]")) &
        (((F.col("country") == "PE") & F.col("document_id").rlike("^[0-9]{8}$")) |
         (F.col("country") != "PE") | F.col("country").isNull()) &
        (F.col("email").isNotNull()) & (F.trim(F.col("email")) != "") &
        (F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")) &
        (F.col("phone").isNotNull()) & (F.col("phone").rlike("^\\+[0-9]{1,3}[0-9]{6,}$")) &
        ((F.upper(F.col("country")).isin("PE","CO","MX","CL","AR")) | F.col("country").isNull()) &
        ((F.lower(F.col("segment")).isin("premium","estandar","nuevo")) | F.col("segment").isNull()) &
        (F.to_date("registration_date","yyyy-MM-dd").isNotNull())
    )

    #normalizaciones
    valid_df = valid_df.withColumn("segment", F.lower(F.trim("segment")))
    valid_df = valid_df.withColumn("country", F.upper(F.trim("country")))

    # Transformaciones finales
    valid_df = valid_df.withColumn("document_id", F.col("document_id").cast("string"))
    valid_df = valid_df.withColumn("registration_date", F.to_date("registration_date","yyyy-MM-dd")) \
                        .withColumn("_ingested_at", F.current_date())

    return valid_df