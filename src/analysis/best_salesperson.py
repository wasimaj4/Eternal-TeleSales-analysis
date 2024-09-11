import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

def best_salesperson(df_personal: DataFrame, df_product: DataFrame) -> DataFrame:
    df_joined23 = df_personal.join(df_product, df_personal["id"] == df_product["caller_id"], how="inner")
    df_best_person = df_joined23.groupBy("name", "country").agg(F.sum("sales_amount").alias("total_sales"))
    window_spec3 = Window.partitionBy("country").orderBy(F.desc("total_sales"))
    df_best_person = df_best_person.withColumn("rank", F.row_number().over(window_spec3)).filter(F.col("rank") == 1)
    df_best_person = df_best_person.select("name", "country", F.format_number(F.col("total_sales"), 2).alias("total_sales"))
    df_best_person.coalesce(1).write.mode("overwrite").csv("best_salesperson.csv", header=True)
    return df_best_person
