import pyspark.sql.functions as F
from pyspark.sql import DataFrame

def marketing_address_info(df_work_info: DataFrame, df_personal: DataFrame) -> DataFrame:

    df_joined12: DataFrame = df_work_info.join(df_personal, on="id", how="inner")

    df_marketing = (df_joined12.filter(F.col("area") == "Marketing")
        .withColumn("zip_code", F.regexp_extract(F.col("address"), r"(\d{4} \w{2})", 0))
        .withColumn("street_and_house_no",
            F.when(F.col("zip_code") == F.trim(F.split(F.col("address"), ",")[0]), "UNKNOWN")
            .otherwise(F.trim(F.split(F.col("address"), ",")[0]))
        )
        .withColumn("city",
            F.when(F.split(F.col("address"), ",").getItem(2).isNotNull(), F.trim(F.split(F.col("address"), ",").getItem(2)))
            .otherwise(F.trim(F.split(F.col("address"), ",").getItem(1)))
        )
        .select("street_and_house_no", "zip_code", "city")
    )
    df_marketing.coalesce(1).write.mode("overwrite").option("header", True).csv("marketing_address_info.csv")
    return df_marketing
