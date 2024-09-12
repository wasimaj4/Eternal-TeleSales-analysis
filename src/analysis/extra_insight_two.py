import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
from src.utils.log_manager import logger

def extra_insight_two(df_work_info: DataFrame, df_personal: DataFrame, df_product: DataFrame) -> DataFrame:
    """
    Calculate customer loyalty scores based on repeat sales and total sales amount.

    This function joins work information, personal data, and product sales data,
    then calculates a customer loyalty score for each salesperson-area-company combination.

    :param df_work_info: DataFrame containing work-related information.
    :type df_work_info: pyspark.sql.DataFrame
    :param df_personal: DataFrame containing personal information of salespeople.
    :type df_personal: pyspark.sql.DataFrame
    :param df_product: DataFrame containing product sales information.
    :type df_product: pyspark.sql.DataFrame
    :return: DataFrame with customer loyalty information, including name, area, company,
             repeat sales, total sales, and customer loyalty score.
    :rtype: pyspark.sql.DataFrame

    :Example:

    >>> work_info = spark.createDataFrame([...])  # Create work info DataFrame
    >>> personal = spark.createDataFrame([...])  # Create personal info DataFrame
    >>> product = spark.createDataFrame([...])  # Create product sales DataFrame
    >>> result = extra_insight_two(work_info, personal, product)
    >>> result.show()
    """
    logger.info("Joining work information and personal data")
    df_joined12: DataFrame = df_work_info.join(df_personal, on="id", how="inner")

    logger.info("Joining combined data with product sales data")
    df_joined123 = df_joined12.join(df_product, df_product["caller_id"] == df_joined12["id"], how="inner")

    logger.info("Calculating repeat sales and total sales")
    df_loyalty = df_joined123.groupBy("name", "area", "company").agg(
        F.count("sales_amount").alias("repeat_sales"),
        F.sum("sales_amount").alias("total_sales")
    )

    logger.info("Computing customer loyalty score")
    df_loyalty = df_loyalty.withColumn("customer_loyalty_score", F.col("repeat_sales") * F.col("total_sales"))

    logger.info("Selecting and formatting final columns")
    df_loyalty = df_loyalty.select(
        "name", "area", "company", "repeat_sales",
        F.round(F.col("total_sales"), 2).cast(DoubleType()).alias("total_sales"),
        F.round(F.col("customer_loyalty_score"), 2).cast(DoubleType()).alias("customer_loyalty_score")
    )

    logger.info("Returning customer loyalty data")
    return df_loyalty
