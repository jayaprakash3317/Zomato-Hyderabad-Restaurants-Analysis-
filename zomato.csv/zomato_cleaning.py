from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, trim, when, regexp_extract
from pyspark.sql.types import FloatType, IntegerType
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("ZomatoCleaner")

def main():
    logger.info("Starting Zomato Data Cleaning (FINAL VERSION)...")

    spark = SparkSession.builder \
        .appName("ZomatoCleaningFinal") \
        .enableHiveSupport() \
        .getOrCreate()

    expected_columns = [
        "url", "address", "name", "online_order", "book_table", "rate", "votes",
        "phone", "location", "rest_type", "dish_liked", "cuisines",
        "approx_cost(for two people)", "reviews_list", "menu_item",
        "listed_in(type)", "listed_in(city)"
    ]

    df = spark.read \
        .option("header", True) \
        .option("sep", ",") \
        .option("multiLine", True) \
        .option("quote", "\"") \
        .option("escape", "\"") \
        .csv("hdfs:///zomato_data/raw/zomato.csv")

    logger.info(f"Loaded raw data with {len(df.columns)} columns")

    if len(df.columns) >= len(expected_columns):
        df = df.select(df.columns[:len(expected_columns)])
        for i, new_col in enumerate(expected_columns):
            df = df.withColumnRenamed(df.columns[i], new_col)
    else:
        logger.error("Column count mismatch. Check the CSV formatting.")
        df.show(5)
        return

    df = df \
        .withColumnRenamed("approx_cost(for two people)", "approx_cost_for_two") \
        .withColumnRenamed("listed_in(type)", "listed_in_type") \
        .withColumnRenamed("listed_in(city)", "listed_in_city")

    for col_name in df.columns:
        df = df.withColumn(col_name, trim(col(col_name)))

    df = df.withColumn("rate", regexp_extract("rate", r"([0-9.]+)", 1))
    df = df.withColumn("rate", when(col("rate") == "", None).otherwise(col("rate").cast(FloatType())))

    df = df.withColumn("votes", regexp_replace("votes", "[^0-9]", ""))
    df = df.withColumn("votes", when(col("votes") == "", "0").otherwise(col("votes")))
    df = df.withColumn("votes", col("votes").cast(IntegerType()))

    df = df.withColumn("approx_cost_for_two", regexp_replace("approx_cost_for_two", ",", ""))
    df = df.withColumn("approx_cost_for_two", when(col("approx_cost_for_two") == "", None).otherwise(col("approx_cost_for_two").cast(FloatType())))

    df = df.drop("menu_item", "reviews_list")

    df = df.filter(
        (col("name").isNotNull()) &
        (col("location").isNotNull()) &
        (col("cuisines").isNotNull())
    )

    df.printSchema()
    df.show(5, truncate=False)

    hive_path = "hdfs:///zomato_data/cleaned"
    spark.sql("DROP TABLE IF EXISTS zomato_cleaned")

    df.write \
        .mode("overwrite") \
        .format("parquet") \
        .option("path", hive_path) \
        .saveAsTable("zomato_cleaned")

    logger.info("Final cleaned data saved to Hive as 'zomato_cleaned'.")
    spark.stop()

if __name__ == "__main__":
    main()

