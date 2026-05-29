import dlt
import pyspark.sql.functions as F

# mv_fact_transactions
@dlt.table(
    name="fintech_finpay.gold.mv_fact_transactions",
    comment="vista materializada de fact_transactions",
    table_properties={"quality": "gold"}
)
def mv_fact_transactions():
    df = dlt.read("fintech_finpay.gold.fact_transactions")
    return df

# mv_dim_merchant
@dlt.table(
    name="fintech_finpay.gold.mv_dim_merchant",
    comment="vista materializada de dim_merchant",
    table_properties={"quality": "gold"}
)
def mv_dim_merchant():
    df = dlt.read("fintech_finpay.gold.dim_merchant")
    return df

# mv_dim_user
@dlt.table(
    name="fintech_finpay.gold.mv_dim_user",
    comment="vista materializada de dim_user",
    table_properties={"quality": "gold"}
)
def mv_dim_user():
    df = dlt.read("fintech_finpay.gold.dim_user")
    return df

# mv_dim_channel
@dlt.table(
    name="fintech_finpay.gold.mv_dim_channel",
    comment="vista materializada de dim_channel",
    table_properties={"quality": "gold"}
)
def mv_dim_channel():
    df = dlt.read("fintech_finpay.gold.dim_channel")
    return df

# mv_dim_date
@dlt.table(
    name="fintech_finpay.gold.mv_dim_date",
    comment="vista materializada de dim_date",
    table_properties={"quality": "gold"}
)
def mv_dim_date():
    df = dlt.read("fintech_finpay.gold.dim_date")
    return df


