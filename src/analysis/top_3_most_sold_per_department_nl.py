import pyspark.sql.functions as F
from pyspark.sql import DataFrame, Window
from utils.log_manager import logger

def top_3_most_sold_per_department_netherlands(df_work_info: DataFrame, df_product: DataFrame) -> DataFrame:
    """
    Identify the top 3 most sold products per department in the Netherlands.

    This function joins work information and product data, filters for Netherlands,
    aggregates the quantity sold by department and product, and then ranks the products
    within each department based on total quantity sold.

    :param df_work_info: DataFrame containing work-related information.
    :type df_work_info: pyspark.sql.DataFrame
    :param df_product: DataFrame containing product sales information.
    :type df_product: pyspark.sql.DataFrame
    :return: DataFrame with top 3 most sold products per department in the Netherlands,
    including area, rank, product sold, and total quantity sold.
    :rtype: pyspark.sql.DataFrame

    :Example:

    >>> work_info = spark.createDataFrame([...])  # Create work info DataFrame
    >>> product = spark.createDataFrame([...])  # Create product sales DataFrame
    >>> result = top_3_most_sold_per_department_netherlands(work_info, product)
    >>> result.show()
    """
    logger.info("Joining product and work info data")
    df_joined13 = df_product.join(df_work_info, df_product["caller_id"] == df_work_info["id"], how='inner')

    logger.info("Filtering data for the Netherlands")
    df_prod_nl = df_joined13.filter(F.col("country") == "Netherlands")

    logger.info("Aggregating total quantity sold by department and product")
    df_prod_nl = df_prod_nl.groupBy("area", "product_sold").agg(F.sum("quantity").alias("total_quantity_sold"))

    logger.info("Ranking products within each department based on total quantity sold")
    window_spec2 = Window.partitionBy("area").orderBy(F.desc("total_quantity_sold"))
    df_top3_nl: DataFrame = df_prod_nl.withColumn("rank", F.row_number().over(window_spec2)).filter(F.col("rank") <= 3)

    logger.info("Selecting final columns for output")
    df_top3_nl = df_top3_nl.select("area", "rank", "product_sold", "total_quantity_sold")

    logger.info("Returning top 3 most sold products per department in the Netherlands")
    return df_top3_nl
