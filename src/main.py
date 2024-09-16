from pyspark.sql import SparkSession
import os
import sys
from analysis.it_data import it_data
from analysis.marketing_address_info import marketing_address_info
from analysis.department_breakdown import department_breakdown
from analysis.top_3 import top_3
from analysis.top_3_most_sold_per_department_nl import top_3_most_sold_per_department_netherlands
from analysis.best_salesperson import best_salesperson
from analysis.extra_insight_one import extra_insight_one
from analysis.extra_insight_two import extra_insight_two
from utils.writer_csv import save_df_to_csv
from utils.log_manager import logger


os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

def analysis(prod_info: str, seller_info: str, buyer_info: str):
    """
    Perform various analyses on the given datasets and save results to CSV files.

    This function reads three CSV files, performs multiple analyses using different
    functions, and saves the results of each analysis to separate CSV files.

    :param prod_info: File path to the product information CSV.
    :type prod_info: str
    :param seller_info: File path to the seller information CSV.
    :type seller_info: str
    :param buyer_info: File path to the buyer information CSV.
    :type buyer_info: str
    :return: Tuple of DataFrames showing the results of each analysis.
    :rtype: tuple

    :Example:

    >>> file1 = "path/to/dataset_one.csv"
    >>> file2 = "path/to/dataset_two.csv"
    >>> file3 = "path/to/dataset_three.csv"
    >>> analysis(file1, file2, file3)
    """
    # current_dir = os.path.dirname(os.path.abspath(__file__))
    # # Go up one level to the project root
    # project_root = os.path.dirname(current_dir)
    logger.info("Starting Eterna Analysis")
    spark = SparkSession.builder.appName("EternaAnalysis").getOrCreate()

    logger.info("Reading input CSV files")
    df_work_info = spark.read.option("header", True).csv(prod_info)
    df_personal = spark.read.option("header", True).csv(seller_info)
    df_product = spark.read.option("header", True).csv(buyer_info)

    logger.info("Performing IT data analysis")
    it_df = it_data(df_work_info, df_personal)
    save_df_to_csv(it_df, "it_data.csv")

    logger.info("Performing marketing address info analysis")
    marketing_df = marketing_address_info(df_work_info, df_personal)
    save_df_to_csv(marketing_df, "marketing_address_info.csv")

    logger.info("Performing department breakdown analysis")
    sales_breakdown_df = department_breakdown(df_work_info, df_personal)
    save_df_to_csv(sales_breakdown_df, "department_breakdown.csv")

    logger.info("Performing top 3 employees analysis")
    top_employees_df = top_3(df_work_info, df_personal)
    save_df_to_csv(top_employees_df, "top_3.csv")

    logger.info("Performing top 3 most sold products per department in Netherlands analysis")
    top_products_nl_df = top_3_most_sold_per_department_netherlands(df_work_info, df_product)
    save_df_to_csv(top_products_nl_df, "top_3_most_sold_per_department_netherlands.csv")

    logger.info("Performing best salesperson analysis")
    best_salesperson_df = best_salesperson(df_personal, df_product)
    save_df_to_csv(best_salesperson_df, "best_salesperson.csv")

    logger.info("Performing extra insight one analysis")
    area_efficiency_df = extra_insight_one(df_work_info, df_product)
    save_df_to_csv(area_efficiency_df, "extra_insight_one.csv")

    logger.info("Performing extra insight two analysis")
    customer_loyalty_df = extra_insight_two(df_work_info, df_personal, df_product)
    save_df_to_csv(customer_loyalty_df, "extra_insight_two.csv")

    logger.info("Analysis completed.")
    return (it_df.show(), marketing_df.show(), sales_breakdown_df.show(), top_employees_df.show(), top_products_nl_df.show(),
            best_salesperson_df.show(), area_efficiency_df.show(), customer_loyalty_df.show())
def main():
    logger.info("Launching the application...")
    file1 = r"data_sets\dataset_one.csv"
    file2 = r"data_sets\dataset_two.csv"
    file3 = r"data_sets\dataset_three.csv"
    analysis(file1, file2, file3)
    logger.info("Eterna Analysis application finished execution.")

if __name__ == "__main__":
    main()




