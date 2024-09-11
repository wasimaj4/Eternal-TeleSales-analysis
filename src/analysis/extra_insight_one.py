import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

def extra_insight_one(df_work_info: DataFrame, df_product: DataFrame) -> DataFrame:
    df_joined13 = df_product.join(df_work_info, df_product["caller_id"] == df_work_info["id"], how="inner")
    df_percent_efficiency = df_joined13.groupBy("country", "area").agg(
        F.sum("calls_successful").alias("total_calls_successful"),
        F.sum("calls_made").alias("total_calls_made")
    )
    df_percent_efficiency = df_percent_efficiency.withColumn("success_rate",
        (F.col("total_calls_successful") / F.col("total_calls_made")) * 100
    )
    window_spec4 = Window.partitionBy("country").orderBy(F.desc("success_rate"))
    df_percent_efficiency = df_percent_efficiency.withColumn("rank", F.row_number().over(window_spec4)).filter(F.col("rank") <= 3)
    df_area_efficiency = df_percent_efficiency.select("country", "area",
        F.format_number(F.col("success_rate"), 2).alias("success_rate")
    )
    df_area_efficiency.coalesce(1).write.mode("overwrite").csv("extra_insight_one.csv", header=True)
    return df_area_efficiency
