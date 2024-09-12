import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import DoubleType
from src.utils.log_manager import logger


def extra_insight_one(df_work_info: DataFrame, df_product: DataFrame) -> DataFrame:
    """
    Calculate the top 3 most efficient areas for each country based on call success rate.

    This function joins work information and product data, calculates the success rate
    for each area within a country, and returns the top 3 areas per country.

    :param df_work_info: DataFrame containing work-related information, including calls made and successful calls.
    :type df_work_info: pyspark.sql.DataFrame
    :param df_product: DataFrame containing product sales information, including caller ID.
    :type df_product: pyspark.sql.DataFrame
    :return: DataFrame with the top 3 most efficient areas for each country, including country, area, and success rate.
    :rtype: pyspark.sql.DataFrame

    :Example:

    >>> work_info = spark.createDataFrame([...])  # Create work info DataFrame
    >>> product = spark.createDataFrame([...])  # Create product sales DataFrame
    >>> result = extra_insight_one(work_info, product)
    >>> result.show()
    """
    logger.info("Joining product sales data with work information")
    df_joined13 = df_product.join(df_work_info, df_product["caller_id"] == df_work_info["id"], how="inner")

    logger.info("Calculating total successful calls and total calls made by country and area")
    df_percent_efficiency = df_joined13.groupBy("country", "area").agg(
        F.sum("calls_successful").alias("total_calls_successful"),
        F.sum("calls_made").alias("total_calls_made")
    )

    logger.info("Computing success rate for each country and area")
    df_percent_efficiency = df_percent_efficiency.withColumn("success_rate",
        (F.col("total_calls_successful") / F.col("total_calls_made")) * 100
    )

    logger.info("Ranking areas within each country based on success rate")
    window_spec4 = Window.partitionBy("country").orderBy(F.desc("success_rate"))
    df_percent_efficiency = df_percent_efficiency.withColumn("rank",
                                                      F.row_number().over(window_spec4)).filter(F.col("rank") <= 3)

    logger.info("Selecting final columns and rounding success rate")
    df_area_efficiency = df_percent_efficiency.select(
        "country",
        "area",
        F.round(F.col("success_rate"), 2).cast(DoubleType()).alias("success_rate")
    )

    logger.info("Returning top 3 most efficient areas for each country")
    return df_area_efficiency
