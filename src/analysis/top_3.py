import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

def top_3(df_work_info: DataFrame,df_personal:DataFrame) -> DataFrame:
    df_joined12: DataFrame = df_work_info.join(df_personal, on="id", how="inner")

    df_emp_eff = df_joined12.withColumn("success_rate", F.when(F.col("calls_made") > 0,
        (F.col("calls_successful") / F.col("calls_made")) * 100).otherwise(0))
    window_spec = Window.partitionBy("area").orderBy(F.desc("success_rate"), F.desc("sales_amount"))
    df_emp_eff = df_emp_eff.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") <= 3)
    df_top_3 = df_emp_eff.filter(F.col("success_rate") > 75)
    df_top_3 = df_top_3.select("area", "rank", "name", "sales_amount",
        F.format_number(F.col("success_rate"), 2).alias("success_rate"))
    df_top_3.coalesce(1).write.mode("overwrite").csv("top_3.csv", header=True)
    return df_top_3
