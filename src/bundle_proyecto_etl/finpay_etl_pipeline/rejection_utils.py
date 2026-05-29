import pyspark.sql.functions as F
from pyspark.sql.window import Window

# Función para identificar registros rechazados de transactions
def rejected_transactions(df):
    w = Window.partitionBy("transaction_id").orderBy(
        F.when(F.col("status") == "aprobado", 1)
         .when(F.col("status") == "pendiente", 2)
         .otherwise(3),
        F.col("transaction_date").desc()
    )
    df_with_rank = df.withColumn("rank", F.row_number().over(w))

    rejected = df_with_rank.filter(
        (F.col("transaction_id").isNull()) |
        (F.col("rank") > 1) |
        (F.col("user_id").isNull()) |
        (F.col("merchant_id").isNull()) |
        (F.col("amount").isNull()) | (F.col("amount") <= 0) |
        (F.col("transaction_date").isNull()) |
        ((F.col("transaction_type") == "reversa") & F.col("reference_id").isNull()) |
        (~F.lower(F.col("channel")).isin("web","app","pos")) |
        (~F.lower(F.col("transaction_type")).isin("pago","reversa","retiro")) |
        (~F.upper(F.col("currency")).isin("PEN","USD","COP","MXN","CLP","ARS")) |
        (~F.lower(F.col("status")).isin("aprobado","rechazado","pendiente"))
    ).withColumn("source_name", F.lit("transactions")) \
     .withColumn(
        "reject_reason",
        F.when(F.col("transaction_id").isNull(), "transaction_id nulo crítico")
         .when(F.col("rank") > 1, "transaction_id duplicado")
         .when(F.col("user_id").isNull(), "user_id nulo crítico")
         .when(F.col("merchant_id").isNull(), "merchant_id nulo crítico")
         .when(F.col("amount").isNull(), "amount nulo crítico")
         .when(F.col("amount") <= 0, "amount inválido (no positivo)")
         .when(F.col("transaction_date").isNull(), "transaction_date nulo crítico")
         .when((F.col("transaction_type") == "reversa") & F.col("reference_id").isNull(),
               "reference_id faltante en reversa")
         .when(~F.lower(F.col("channel")).isin("web","app","pos"), "channel formato incorrecto")
         .when(~F.lower(F.col("transaction_type")).isin("pago","reversa","retiro"),
               "transaction_type inválido")
         .when(~F.upper(F.col("currency")).isin("PEN","USD","COP","MXN","CLP","ARS"),
               "currency inválido")
         .when(~F.lower(F.col("status")).isin("aprobado","rechazado","pendiente"),
               "status inválido")
     ) \
     .withColumn(
        "error_column",
        F.when(F.col("transaction_id").isNull() | (F.col("rank") > 1), "transaction_id")
         .when(F.col("user_id").isNull(), "user_id")
         .when(F.col("merchant_id").isNull(), "merchant_id")
         .when(F.col("amount").isNull() | (F.col("amount") <= 0), "amount")
         .when(F.col("transaction_date").isNull(), "transaction_date")
         .when((F.col("transaction_type") == "reversa") & F.col("reference_id").isNull(),
               "reference_id")
         .when(~F.lower(F.col("channel")).isin("web","app","pos"), "channel")
         .when(~F.lower(F.col("transaction_type")).isin("pago","reversa","retiro"),
               "transaction_type")
         .when(~F.upper(F.col("currency")).isin("PEN","USD","COP","MXN","CLP","ARS"),
               "currency")
         .when(~F.lower(F.col("status")).isin("aprobado","rechazado","pendiente"),
               "status")
     ) \
     .withColumn("processed_at", F.current_timestamp()) \
     .withColumn("raw_record", F.to_json(F.struct([F.col(c) for c in df.columns]))) \
     .select("source_name","reject_reason","error_column","processed_at","raw_record")

    return rejected

# Función para identificar registros rechazados de merchants
def rejected_merchants(df):
    # Detectar duplicados de merchant_id con prioridad: merchant_name presente > affiliation_date más reciente
    w = Window.partitionBy("merchant_id").orderBy(
        F.when(F.col("merchant_name").isNotNull() & (F.trim(F.col("merchant_name")) != ""), 1).otherwise(2),
        F.col("affiliation_date").desc()
    )
    df_with_rank = df.withColumn("rank", F.row_number().over(w))

    rejected = df_with_rank.filter(
        (F.col("merchant_id").isNull()) |
        (F.col("rank") > 1) |
        (F.col("merchant_name").isNull() | (F.trim(F.col("merchant_name")) == "")) |
        (~F.lower(F.col("category")).isin("retail","restaurante","farmacia","supermercado",
                                          "tecnologia","transporte","educacion","salud",
                                          "entretenimiento","moda")) & F.col("category").isNotNull() |
        (~F.upper(F.col("country")).isin("PE","CO","MX","CL","AR")) & F.col("country").isNotNull() |
        (F.to_date("affiliation_date","yyyy-MM-dd").isNull()) |
        (~F.lower(F.col("status")).isin("activo","inactivo","suspendido")) & F.col("status").isNotNull() |
        # Nuevo caso: risk_level nulo pero category presente
        (F.col("risk_level").isNull() & F.col("category").isNotNull()) |
        # risk_level inválido
        (~F.lower(F.col("risk_level")).isin("bajo","medio","alto")) & F.col("risk_level").isNotNull() & F.col("category").isNotNull()
    ).withColumn("source_name", F.lit("merchants")) \
     .withColumn(
        "reject_reason",
        F.when(F.col("merchant_id").isNull(), "merchant_id nulo crítico")
         .when(F.col("rank") > 1, "merchant_id duplicado")
         .when(F.col("merchant_name").isNull() | (F.trim(F.col("merchant_name")) == ""), "merchant_name vacío o nulo")
         .when(~F.lower(F.col("category")).isin("retail","restaurante","farmacia","supermercado",
                                               "tecnologia","transporte","educacion","salud",
                                               "entretenimiento","moda") & F.col("category").isNotNull(),
               "category inválido")
         .when(~F.upper(F.col("country")).isin("PE","CO","MX","CL","AR") & F.col("country").isNotNull(),
               "country inválido")
         .when(F.to_date("affiliation_date","yyyy-MM-dd").isNull(), "affiliation_date inválido")
         .when(~F.lower(F.col("status")).isin("activo","inactivo","suspendido") & F.col("status").isNotNull(),
               "status inválido")
         .when(F.col("risk_level").isNull() & F.col("category").isNotNull(), "risk_level nulo con category presente")
         .when(~F.lower(F.col("risk_level")).isin("bajo","medio","alto") & F.col("risk_level").isNotNull() & F.col("category").isNotNull(),
               "risk_level inválido")
     ) \
     .withColumn(
        "error_column",
        F.when(F.col("merchant_id").isNull() | (F.col("rank") > 1), "merchant_id")
         .when(F.col("merchant_name").isNull() | (F.trim(F.col("merchant_name")) == ""), "merchant_name")
         .when(~F.lower(F.col("category")).isin("retail","restaurante","farmacia","supermercado",
                                               "tecnologia","transporte","educacion","salud",
                                               "entretenimiento","moda") & F.col("category").isNotNull(),
               "category")
         .when(~F.upper(F.col("country")).isin("PE","CO","MX","CL","AR") & F.col("country").isNotNull(),
               "country")
         .when(F.to_date("affiliation_date","yyyy-MM-dd").isNull(), "affiliation_date")
         .when(~F.lower(F.col("status")).isin("activo","inactivo","suspendido") & F.col("status").isNotNull(),
               "status")
         .when(F.col("risk_level").isNull() & F.col("category").isNotNull(), "risk_level")
         .when(~F.lower(F.col("risk_level")).isin("bajo","medio","alto") & F.col("risk_level").isNotNull() & F.col("category").isNotNull(),
               "risk_level")
     ) \
     .withColumn("processed_at", F.current_timestamp()) \
     .withColumn("raw_record", F.to_json(F.struct([F.col(c) for c in df.columns]))) \
     .select("source_name","reject_reason","error_column","processed_at","raw_record")

    return rejected

# Función para identificar registros rechazados de users
def rejected_users(df):
    # Detectar duplicados de user_id con prioridad: document_id presente > registration_date más reciente
    w = Window.partitionBy("user_id").orderBy(
        F.when(F.col("document_id").isNotNull(), 1).otherwise(2),
        F.col("registration_date").desc()
    )
    df_with_rank = df.withColumn("rank", F.row_number().over(w))

    rejected = df_with_rank.filter(
        (F.col("user_id").isNull()) |
        (F.col("rank") > 1) |
        (F.col("full_name").isNull() | (F.trim(F.col("full_name")) == "") |
         F.col("full_name").rlike("[,;|]")) |
        ((F.col("country") == "PE") & ~F.col("document_id").rlike("^[0-9]{8}$")) |
        (F.col("email").isNull() | (F.trim(F.col("email")) == "") |
         ~F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$")) |
        (F.col("phone").isNull() | ~F.col("phone").rlike("^\\+[0-9]{1,3}[0-9]{6,}$")) |
        (~F.upper(F.col("country")).isin("PE","CO","MX","CL","AR")) & F.col("country").isNotNull() |
        (~F.lower(F.col("segment")).isin("premium","estandar","nuevo")) & F.col("segment").isNotNull() |
        (F.to_date("registration_date","yyyy-MM-dd").isNull())
    ).withColumn("source_name", F.lit("users")) \
     .withColumn(
        "reject_reason",
        F.when(F.col("user_id").isNull(), "user_id nulo crítico")
         .when(F.col("rank") > 1, "user_id duplicado")
         .when(F.col("full_name").isNull() | (F.trim(F.col("full_name")) == "") |
               F.col("full_name").rlike("[,;|]"), "full_name inválido")
         .when((F.col("country") == "PE") & ~F.col("document_id").rlike("^[0-9]{8}$"),
               "document_id inválido para PE")
         .when(F.col("email").isNull() | (F.trim(F.col("email")) == "") |
               ~F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"),
               "email inválido")
         .when(F.col("phone").isNull() | ~F.col("phone").rlike("^\\+[0-9]{1,3}[0-9]{6,}$"),
               "phone inválido")
         .when(~F.upper(F.col("country")).isin("PE","CO","MX","CL","AR") & F.col("country").isNotNull(),
               "country inválido")
         .when(~F.lower(F.col("segment")).isin("premium","estandar","nuevo") & F.col("segment").isNotNull(),
               "segment inválido")
         .when(F.to_date("registration_date","yyyy-MM-dd").isNull(), "registration_date inválido")
     ) \
     .withColumn(
        "error_column",
        F.when(F.col("user_id").isNull() | (F.col("rank") > 1), "user_id")
         .when(F.col("full_name").isNull() | (F.trim(F.col("full_name")) == "") |
               F.col("full_name").rlike("[,;|]"), "full_name")
         .when((F.col("country") == "PE") & ~F.col("document_id").rlike("^[0-9]{8}$"), "document_id")
         .when(F.col("email").isNull() | (F.trim(F.col("email")) == "") |
               ~F.col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"), "email")
         .when(F.col("phone").isNull() | ~F.col("phone").rlike("^\\+[0-9]{1,3}[0-9]{6,}$"), "phone")
         .when(~F.upper(F.col("country")).isin("PE","CO","MX","CL","AR") & F.col("country").isNotNull(), "country")
         .when(~F.lower(F.col("segment")).isin("premium","estandar","nuevo") & F.col("segment").isNotNull(), "segment")
         .when(F.to_date("registration_date","yyyy-MM-dd").isNull(), "registration_date")
     ) \
     .withColumn("processed_at", F.current_timestamp()) \
     .withColumn("raw_record", F.to_json(F.struct([F.col(c) for c in df.columns]))) \
     .select("source_name","reject_reason","error_column","processed_at","raw_record")

    return rejected
