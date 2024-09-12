from pyspark.sql import SparkSession
from chispa.dataframe_comparer import *
from src.analysis.it_data import it_data
from src.analysis.marketing_address_info import marketing_address_info
from src.analysis.department_breakdown import department_breakdown
from src.analysis.top_3 import top_3
from src.analysis.top_3_most_sold_per_department_nl import top_3_most_sold_per_department_netherlands
from src.analysis.best_salesperson import best_salesperson
from src.analysis.extra_insight_one import extra_insight_one
from src.analysis.extra_insight_two import extra_insight_two
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType, DoubleType
import sys
import os
from src.utils.log_manager import logger

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
spark = (SparkSession.builder.master("local").appName("test_analysis").getOrCreate())


work_info_data = [
    (1, "Marketing", 100, 80),
    (2, "IT", 50, 40),
    (3, "Marketing", 80, 70),
    (4, "Finance", 70, 60),
    (5, "IT", 90, 85),
    (6, "Marketing", 120, 90),
    (7, "Finance", 60, 50),
    (8, "IT", 80, 70),
]

work_info_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("area", StringType(), True),
    StructField("calls_made", IntegerType(), True),
    StructField("calls_successful", IntegerType(), True)
])

df_work_info = spark.createDataFrame(work_info_data, schema=work_info_schema)

personal_data = [
    (1, "John Doe", "2588 VD, Kropswolde", 5000.43),
    (2, "Jane Smith", "1808 KR, Benningbroek", 4000.54),
    (3, "Bob Johnson", "Lindehof 5, 4133 HB, Nederhemert", 6000.4),
    (4, "Alice Brown", "4273 SW, Wirdum Gn", 5500.0),
    (5, "Charlie Davis", "4431 BT, Balinge", 5200.0),
    (6, "Eva Wilson", "Thijmenweg 38, 7801 OC, Grijpskerk", 5800.6),
    (7, "Frank Miller", "8666 XB, Exloo", 4800.7),
    (8, "Grace Lee", "Tessapad 82, 8487 PZ, Sambeek", 5100.8)
]

personal_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("address", StringType(), True),
    StructField("sales_amount", DoubleType(), True)
])

df_personal = spark.createDataFrame(personal_data, schema=personal_schema)


product_data = [
    (1, 6, 'Boons NV', 'Frida Daelman', 38, 'Belgium', 'Laptop', 14),
    (2, 1, 'Schmitz-Heyse', 'Gustaaf Peeters', 27, 'Belgium', 'Tablet', 20),
    (3, 3, 'The Conqueror BV', 'Vanessa Meert', 31, 'Netherlands', 'Printer', 33),
    (4, 2, 'Smit Enterprises', 'Johannes de Groot', 45, 'Netherlands', 'Phone', 17),
    (5, 4, 'Wolters & Co', 'Pieter Wolters', 52, 'Belgium', 'Laptop', 8),
    (6, 8, 'Vermeulen & Zonen', 'Sofie Vermeulen', 29, 'Belgium', 'Tablet', 5),
    (7, 7, 'De Wit Logistics', 'Karin De Wit', 43, 'Netherlands', 'Printer', 15),
    (8, 5, 'De Boer NV', 'Hans De Boer', 38, 'Belgium', 'Laptop', 12),
]

product_schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("caller_id", IntegerType(), False),
    StructField("company", StringType(), True),
    StructField("recipient", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("product_sold", StringType(), True),
    StructField("quantity", IntegerType(), True)
])

df_product = spark.createDataFrame(product_data, schema=product_schema)
def test_it_data() -> None:
    """
    Test the it_data function.

    This function creates test data, calls the it_data function, and compares the result
    with the expected output using chispa's assert_df_equality and assert_schema_equality.

    :return: None
    """
    logger.info("Starting test_it_data")
    actual_df1 = it_data(df_work_info, df_personal)

    expected_data1 =  [
        (5, "IT", 90, 85, "Charlie Davis", "4431 BT, Balinge", 5200.0),
        (8, "IT", 80, 70, "Grace Lee", "Tessapad 82, 8487 PZ, Sambeek", 5100.8),
        (2, "IT", 50, 40, "Jane Smith", "1808 KR, Benningbroek", 4000.54)
    ]

    expected_schema1 = StructType([
        StructField("id", IntegerType(), False),
        StructField("area", StringType(), True),
        StructField("calls_made", IntegerType(), True),
        StructField("calls_successful", IntegerType(), True),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("sales_amount", DoubleType(), True)
    ])
    expected_df1 = spark.createDataFrame(expected_data1, schema=expected_schema1)

    assert_df_equality(actual_df1, expected_df1)
    assert_schema_equality(actual_df1.schema , expected_schema1)
    logger.info("test_it_data completed successfully")

def test_marketing_address_info() -> None:
    """
    Test the marketing_address_info function.

    This function creates test data, calls the marketing_address_info function, and compares
    the result with the expected output using chispa's assert_df_equality and assert_schema_equality.

    :return: None
    """
    logger.info("Starting test_marketing_address_info")
    actual_df2 = marketing_address_info(df_work_info, df_personal)
    expected_data2 = [
        ("UNKNOWN", "2588 VD", "Kropswolde"),
        ("Lindehof 5", "4133 HB", "Nederhemert"),
        ("Thijmenweg 38", "7801 OC", "Grijpskerk")
    ]

    expected_schema2 = StructType([
        StructField("street_and_house_no", StringType(), True),
        StructField("zip_code", StringType(), True),
        StructField("city", StringType(), True)
    ])
    expected_df2 = spark.createDataFrame(expected_data2, schema=expected_schema2)

    assert_df_equality(actual_df2, expected_df2)
    assert_schema_equality(actual_df2.schema, expected_schema2)
    logger.info("test_marketing_address_info completed successfully")

def test_department_breakdown() -> None:
    """
    Test the department_breakdown function.

    This function creates test data, calls the department_breakdown function, and compares
    the result with the expected output using chispa's assert_df_equality and assert_schema_equality.

    :return: None
    """
    logger.info("Starting test_department_breakdown")
    actual_df3 = department_breakdown(df_work_info, df_personal)
    expected_data3 = [
        ("Finance", 10300.7, 84.62),
        ("Marketing", 16801.43, 80.0),
        ("IT", 14301.34, 88.64)
    ]

    expected_schema3 = StructType([
        StructField("area", StringType(), True),
        StructField("total_sales", DoubleType(), True),
        StructField("success_rate", DoubleType(), True)
    ])
    expected_df3 = spark.createDataFrame(expected_data3, schema=expected_schema3)

    assert_df_equality(actual_df3, expected_df3)
    assert_schema_equality(actual_df3.schema, expected_schema3)
    logger.info("test_department_breakdown completed successfully")

def test_top_3() -> None:
    """
    Test the top_3 function.

    This function creates test data, calls the top_3 function, and compares
    the result with the expected output using chispa's assert_df_equality and assert_schema_equality.

    :return: None
    """
    logger.info("Starting test_top_3")
    actual_df4 = top_3(df_work_info, df_personal)
    expected_data4 = [
        ("Finance", 1, "Alice Brown", 5500.0, 85.71),
        ("Finance", 2, "Frank Miller", 4800.7, 83.33),
        ("IT", 1, "Charlie Davis", 5200.0, 94.44),
        ("IT", 2, "Grace Lee", 5100.8, 87.5),
        ("IT", 3, "Jane Smith", 4000.54, 80.0),
        ("Marketing", 1, "Bob Johnson", 6000.4, 87.5),
        ("Marketing", 2, "John Doe", 5000.43, 80.0)
    ]

    expected_schema4 = StructType([
        StructField("area", StringType(), True),
        StructField("rank", IntegerType(), False),
        StructField("name", StringType(), True),
        StructField("sales_amount", DoubleType(), True),
        StructField("success_rate", DoubleType(), True)
    ])
    expected_df4 = spark.createDataFrame(expected_data4, schema=expected_schema4)

    assert_df_equality(actual_df4, expected_df4)
    assert_schema_equality(actual_df4.schema, expected_schema4)
    logger.info("test_top_3 completed successfully")

def test_top_3_most_sold_per_department_netherlands() -> None:
    """
    Test the top_3_most_sold_per_department_netherlands function.

    This function creates test data, calls the top_3_most_sold_per_department_netherlands function,
    and compares the result with the expected output using chispa's assert_df_equality and assert_schema_equality.

    :return: None
    """
    logger.info("Starting test_top_3_most_sold_per_department_netherlands")
    actual_df5 = top_3_most_sold_per_department_netherlands(df_work_info, df_product)
    expected_data5 = [
        ("Finance", 1, "Printer", 15),
        ("IT", 1, "Phone", 17),
        ("Marketing", 1, "Printer", 33)
    ]

    expected_schema5 = StructType([
        StructField("area", StringType(), True),
        StructField("rank", IntegerType(), False),
        StructField("product_sold", StringType(), True),
        StructField("total_quantity_sold", LongType(), True)
    ])
    expected_df5 = spark.createDataFrame(expected_data5, schema=expected_schema5)

    assert_df_equality(actual_df5, expected_df5)
    assert_schema_equality(actual_df5.schema, expected_schema5)
    logger.info("test_top_3_most_sold_per_department_netherlands completed successfully")

def test_best_salesperson() -> None:
    """
    Test the best_salesperson function.

    This function creates test data, calls the best_salesperson function, and compares
    the result with the expected output using chispa's assert_df_equality and assert_schema_equality.

    :return: None
    """
    logger.info("Starting test_best_salesperson")
    actual_df6 = best_salesperson(df_personal, df_product)
    expected_data6 = [
        ("Eva Wilson", "Belgium", 5800.6),
        ("Bob Johnson", "Netherlands", 6000.4)
    ]

    expected_schema6 =  StructType([
        StructField("name", StringType(), True),
        StructField("country", StringType(), True),
        StructField("total_sales", DoubleType(), True)
    ])
    expected_df6 = spark.createDataFrame(expected_data6, schema=expected_schema6)

    assert_df_equality(actual_df6, expected_df6)
    assert_schema_equality(actual_df6.schema, expected_schema6)
    logger.info("test_best_salesperson completed successfully")