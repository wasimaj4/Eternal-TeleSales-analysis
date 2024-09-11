import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def it_data(df_work_info: DataFrame, df_personal: DataFrame) -> DataFrame:
    df_joined12 = df_work_info.join(df_personal, on="id", how="inner")
    df_it = df_joined12.filter(F.col("area") == "IT").orderBy(F.desc("sales_amount")).limit(100)
    df_it.coalesce(1).write.mode("overwrite").option("header", True).csv("it_data.csv")
    return df_it