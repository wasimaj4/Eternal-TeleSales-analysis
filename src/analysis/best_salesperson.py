import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from pyspark.sql.types import DoubleType
from utils.log_manager import logger


def best_salesperson(df_personal: DataFrame, df_product: DataFrame) -> DataFrame:
    """
    Identify the best salesperson in each country based on total sales amount.

    This function joins personal and product data, aggregates total sales by person and country,
    ranks salespeople within each country, and selects the top performer for each country.

    :param df_personal: DataFrame containing personal information of salespeople.
    :type df_personal: pyspark.sql.DataFrame
    :param df_product: DataFrame containing product sales information.
    :type df_product: pyspark.sql.DataFrame
    :return: DataFrame with the best salesperson for each country, including name, country, and total sales.
    :rtype: pyspark.sql.DataFrame

    :Example:

    >>> personal = spark.createDataFrame([...])  # Create personal info DataFrame
    >>> product = spark.createDataFrame([...])  # Create product sales DataFrame
    >>> result = best_salesperson(personal, product)
    >>> result.show()
    """
    logger.info("Joining personal and product sales data")
    df_joined23 = df_personal.join(df_product, df_personal["id"] == df_product["caller_id"], how="inner")

    logger.info("Aggregating total sales by salesperson and country")
    df_best_person = df_joined23.groupBy("name", "country").agg(F.sum("sales_amount").alias("total_sales"))

    logger.info("Ranking salespeople within each country based on total sales")
    window_spec3 = Window.partitionBy("country").orderBy(F.desc("total_sales"))
    df_best_person = df_best_person.withColumn("rank", F.row_number().over(window_spec3)).filter(F.col("rank") == 1)

    logger.info("Selecting and formatting final columns")
    df_best_person = df_best_person.select(
        "name",
        "country",
        F.round(F.col("total_sales"), 2).cast(DoubleType()).alias("total_sales")
    )

    logger.info("Returning best salesperson for each country")
    return df_best_person
