import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def extra_insight_two(df_work_info: DataFrame,df_personal:DataFrame, df_product: DataFrame) -> DataFrame:
    df_joined12: DataFrame = df_work_info.join(df_personal, on = "id", how="inner")
    df_joined123 = df_joined12.join(df_product, df_product["caller_id"] == df_joined12["id"], how="inner")
    df_loyalty = df_joined123.groupBy("name", "area", "company").agg(
        F.count("sales_amount").alias("repeat_sales"),
        F.sum("sales_amount").alias("total_sales")
    )
    df_loyalty = df_loyalty.withColumn("customer_loyalty_score", F.col("repeat_sales") * F.col("total_sales"))
    df_loyalty = df_loyalty.select("name", "area", "company", "repeat_sales", "total_sales", "customer_loyalty_score")
    df_loyalty.coalesce(1).write.mode("overwrite").csv("extra_insight_two.csv", header=True)
    return df_loyalty
