from pyspark.sql import SparkSession



def analysis(prod_info: str, seller_info: str, buyer_info: str):

    spark = SparkSession.builder.appName("EternaAnalysis").getOrCreate()
    df_work_info = spark.read.option("header", True).csv(prod_info)
    df_personal = spark.read.option("header", True).csv(seller_info)
    df_product = spark.read.option("header", True).csv(buyer_info)