import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window

def top_3_most_sold_per_department_netherlands(df_work_info: DataFrame, df_product: DataFrame) -> DataFrame:
    df_joined13 = df_product.join(df_work_info, df_product["caller_id"] == df_work_info["id"], how='inner')
    df_prod_nl = df_joined13.filter(F.col("country") == "Netherlands")
    df_prod_nl = df_prod_nl.groupBy("area", "product_sold").agg(F.sum("quantity").alias("total_quantity_sold"))
    window_spec2 = Window.partitionBy("area").orderBy(F.desc("total_quantity_sold"))
    df_top3_nl = df_prod_nl.withColumn("rank", F.row_number().over(window_spec2)).filter(F.col("rank") <= 3)
    df_top3_nl = df_top3_nl.select("area", "rank", "product_sold", "total_quantity_sold")
    df_top3_nl.coalesce(1).write.mode("overwrite").csv("top_3_most_sold_per_department_netherlands.csv", header=True)
    return df_top3_nl
