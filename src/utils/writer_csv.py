from pyspark.sql import DataFrame
from utils.log_manager import logger

def save_df_to_csv(df: DataFrame, file_path: str) -> None:
    """
    Save a PySpark DataFrame to a CSV file.

    This function coalesces the DataFrame into a single partition and writes it to a CSV file
    at the specified file path. The write mode is set to "overwrite", which means it will
    replace any existing file at the given path.

    :param df: The PySpark DataFrame to be saved.
    :type df: pyspark.sql.DataFrame
    :param file_path: The file path where the CSV file will be saved.
    :type file_path: str
    :return: None

    :Example:

    >>> data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]
    >>> columns = ["name", "id"]
    >>> df = spark.createDataFrame(data, columns)
    >>> save_df_to_csv(df, "/path/to/output.csv")
    """
    logger.info(f"Saving DataFrame to CSV file: {file_path}")
    df.coalesce(1).write.mode("overwrite").csv(f"data_output\\{file_path}", header=True)
    logger.info(f"DataFrame successfully saved to {file_path}")
