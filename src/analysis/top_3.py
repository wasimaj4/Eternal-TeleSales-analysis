import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import DoubleType
from utils.log_manager import logger


def top_3(df_work_info: DataFrame, df_personal: DataFrame) -> DataFrame:
    """
    Identify the top 3 employees in each department based on success rate and sales amount.

    This function joins work information and personal data, calculates success rates,
    ranks employees within each department, and filters for the top 3 with a success rate above 75%.

    :param df_work_info: DataFrame containing work-related information.
    :type df_work_info: pyspark.sql.DataFrame
    :param df_personal: DataFrame containing personal information.
    :type df_personal: pyspark.sql.DataFrame
    :return: DataFrame with top 3 employees per department, their ranks, names, sales amounts, and success rates.
    :rtype: pyspark.sql.DataFrame

    :Example:

    >>> work_info = spark.createDataFrame([...])  # Create work info DataFrame
    >>> personal = spark.createDataFrame([...])  # Create personal info DataFrame
    >>> result = top_3(work_info, personal)
    >>> result.show()
    """
    logger.info("Joining work info and personal data")
    df_joined12: DataFrame = df_work_info.join(df_personal, on="id", how="inner")

    logger.info("Calculating success rate for each employee")
    df_emp_eff = df_joined12.withColumn("success_rate", F.when(F.col("calls_made") > 0,
        F.round((F.col("calls_successful") / F.col("calls_made")) * 100, 2)).otherwise(0))

    logger.info("Ranking employees within each department")
    window_spec = Window.partitionBy("area").orderBy(F.desc("success_rate"), F.desc("sales_amount"))
    df_emp_eff = df_emp_eff.withColumn("rank", F.row_number().over(window_spec)).filter(F.col("rank") <= 3)

    logger.info("Filtering for employees with success rate above 75%")
    df_top_3 = df_emp_eff.filter(F.col("success_rate") > 75)

    logger.info("Selecting and formatting final columns")
    df_top_3 = df_top_3.select("area", "rank", "name",
        F.round(F.col("sales_amount"), 2).cast(DoubleType()).alias("sales_amount"),
        F.col("success_rate").cast(DoubleType()).alias("success_rate"))

    logger.info("Returning top 3 employees per department")
    return df_top_3
