import dlt
import json
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, StringType

# Función auxiliar para cargar esquemas desde archivos JSON
def load_schema(path):
    with open(path) as f:
        schema_dict = json.load(f)
    fields = [StructField(k, StringType(), True) for k in schema_dict.keys()]
    return StructType(fields)

# Leer el ingestion_archetypes.json
with open("/Volumes/fintech_finpay/default/vol_landing/metadata/ingestion_archetypes.json") as f:
    archetypes = json.load(f)

# Crear tablas Bronze dinámicamente con Auto Loader (cloudFiles)
for src in archetypes:
    if src["active"]:
        schema = load_schema(src["schema_location"])

        @dlt.table(
            name=f"fintech_finpay.bronze.{src['target_table'].split('.')[-1]}",
            comment=f"Bronze ingestion for {src['source_name']}",
            table_properties={"quality": "bronze"}
        )
        def load_source(src=src, schema=schema):
            reader = (
                spark.readStream
                    .format("cloudFiles")
                    .option("cloudFiles.format", src["file_format"])
                    .option("cloudFiles.schemaLocation", src["schema_location"])
            )

            # Opciones dinámicas según ingestion_archetypes.json
            if src.get("delimiter"):
                reader = reader.option("delimiter", src["delimiter"])
            if src.get("header") is not None:
                reader = reader.option("header", str(src["header"]).lower())
            if src.get("multiline") is not None:
                reader = reader.option("multiline", str(src["multiline"]).lower())

            # Cargar datos con esquema explícito
            df = reader.schema(schema).load(src["source_path"])

            # Columnas técnicas de auditoría
            df = (
                df.withColumn("_ingested_at", F.current_timestamp())
                  .withColumn("_source_file", F.col("_metadata.file_path"))
                  .withColumn("_pipeline_run_id", F.current_timestamp())
            )
            return df