import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
from utils.log_manager import logger


def department_breakdown(df_work_info: DataFrame, df_personal: DataFrame) -> DataFrame:
    """
    Generate a department-wise breakdown of sales and call success metrics.

    This function joins work information and personal data, then aggregates the data by department (area)
    to calculate total sales, total successful calls, total calls made, and success rate.

    :param df_work_info: DataFrame containing work-related information.
    :type df_work_info: pyspark.sql.DataFrame
    :param df_personal: DataFrame containing personal information.
    :type df_personal: pyspark.sql.DataFrame
    :return: DataFrame with department-wise breakdown of sales and call success metrics.
    :rtype: pyspark.sql.DataFrame

    :Example:

    >>> work_info = spark.createDataFrame([...])  # Create work info DataFrame
    >>> personal = spark.createDataFrame([...])  # Create personal info DataFrame
    >>> result = department_breakdown(work_info, personal)
    >>> result.show()
    """
    logger.info("Joining work info and personal data")
    df_joined12: DataFrame = df_work_info.join(df_personal, on="id", how="inner")

    logger.info("Aggregating data by department")
    df_agg_breakdown = df_joined12.groupBy("area").agg(
        F.round(F.sum("sales_amount"),2).alias("total_sales"),
        F.sum("calls_successful").alias("total_calls_success"),
        F.sum("calls_made").alias("total_calls_made")
    )

    logger.info("Calculating success rate")
    df_agg_breakdown = df_agg_breakdown.withColumn("success_rate", F.when(F.col("total_calls_made") > 0,
        F.round((F.col("total_calls_success") / F.col("total_calls_made")) * 100, 2)).otherwise(0))

    logger.info("Selecting final columns and casting to appropriate types")
    df_sales_breakdown: DataFrame = df_agg_breakdown.select(
        "area",
        F.col("total_sales").cast(DoubleType()).alias("total_sales"),
        F.col("success_rate").cast(DoubleType()).alias("success_rate")
    )

    logger.info("Returning department-wise breakdown")
    return df_sales_breakdown
