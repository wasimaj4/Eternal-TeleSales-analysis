import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from src.utils.log_manager import logger

def marketing_address_info(df_work_info: DataFrame, df_personal: DataFrame) -> DataFrame:
    """
    Process and extract address information for Marketing employees.

    This function joins work information and personal data, filters for Marketing area employees,
    and extracts address components (street and house number, zip code, and city) from the address field.

    :param df_work_info: DataFrame containing work-related information.
    :type df_work_info: pyspark.sql.DataFrame
    :param df_personal: DataFrame containing personal information.
    :type df_personal: pyspark.sql.DataFrame
    :return: DataFrame with processed address information for Marketing employees.
    :rtype: pyspark.sql.DataFrame

    :Example:

    >>> work_info = spark.createDataFrame([...])  # Create work info DataFrame
    >>> personal = spark.createDataFrame([...])  # Create personal info DataFrame
    >>> result = marketing_address_info(work_info, personal)
    >>> result.show()
    """
    logger.info("Joining work info and personal data")
    df_joined12: DataFrame = df_work_info.join(df_personal, on="id", how="inner")

    logger.info("Processing address information for Marketing employees")
    df_marketing: DataFrame = (df_joined12.filter(F.col("area") == "Marketing")
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

    logger.info("Returning processed address information for Marketing employees")
    return df_marketing
