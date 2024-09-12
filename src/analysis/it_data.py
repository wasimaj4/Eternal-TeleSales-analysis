import logging
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import DoubleType
from src.utils.log_manager import logger

def it_data(df_work_info: DataFrame, df_personal: DataFrame) -> DataFrame:
    """
    Process and filter data for IT employees based on their sales amount.

    This function joins work information and personal data, casts the sales_amount
    to DoubleType, filters for IT area employees, sorts by sales amount in
    descending order, and returns the top 100 records.

    :param df_work_info: DataFrame containing work-related information.
    :type df_work_info: pyspark.sql.DataFrame
    :param df_personal: DataFrame containing personal information.
    :type df_personal: pyspark.sql.DataFrame
    :return: DataFrame with processed and filtered data for top 100 IT employees by sales amount.
    :rtype: pyspark.sql.DataFrame

    :Example:

    >>> work_info = spark.createDataFrame([...])
    >>> personal = spark.createDataFrame([...])
    >>> result = it_data(work_info, personal)
    >>> result.show()
    """
    logger.info("Joining work info and personal data")
    df_joined12 = df_work_info.join(df_personal, on="id", how="inner")

    logger.info("Casting sales_amount to DoubleType")
    df_joined12 = df_joined12.withColumn("sales_amount", F.col("sales_amount").cast(DoubleType()))

    logger.info("Filtering for IT area and sorting by sales_amount")
    df_it = df_joined12.filter(F.col("area") == "IT").orderBy(F.desc("sales_amount")).limit(100)

    logger.info("Returning top 100 IT employees by sales amount")
    return df_it
