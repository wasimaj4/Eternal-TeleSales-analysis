import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def department_breakdown(df_work_info: DataFrame,df_personal: DataFrame) -> DataFrame:
    df_joined12: DataFrame = df_work_info.join(df_personal, on="id", how="inner")

    df_agg_breakdown = df_joined12.groupBy("area").agg(
        F.sum("sales_amount").alias("total_sales"),
        F.sum("calls_successful").alias("total_calls_success"),
        F.sum("calls_made").alias("total_calls_made")
    )
    df_agg_breakdown = df_agg_breakdown.withColumn("success_rate", F.when(F.col("total_calls_made") > 0,
        (F.col("total_calls_success") / F.col("total_calls_made")) * 100).otherwise(0))
    df_sales_breakdown = df_agg_breakdown.select(
        "area",
        F.format_number(F.col("total_sales"), 2).alias("total_sales"),
        F.format_number(F.col("success_rate"), 2).alias("success_rate")
    )
    df_sales_breakdown.coalesce(1).write.mode("overwrite").csv("department_breakdown.csv", header=True)
    return df_sales_breakdown
