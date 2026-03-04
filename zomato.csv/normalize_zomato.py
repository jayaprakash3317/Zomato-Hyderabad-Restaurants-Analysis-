from pyspark.sql import SparkSession
from pyspark.sql.functions import row_number, lower, trim, col, when, round
from pyspark.sql.window import Window


spark = SparkSession.builder \
    .appName("Zomato Normalize Simplified Join") \
    .enableHiveSupport() \
    .getOrCreate()


df = spark.sql("SELECT * FROM zomato_cleaned")


key_columns = ["name", "address", "phone", "online_order", "book_table", "location", "cuisines", "rest_type", "dish_liked"]
for c in key_columns:
    df = df.withColumn(c, lower(trim(col(c))))


df = df.filter(
    (col("name").isNotNull()) & (col("name") != "") &
    (col("address").isNotNull()) & (col("address") != "") &
    (col("location").isNotNull()) & (col("location") != "") &
    (col("cuisines").isNotNull()) & (col("cuisines") != "") &
    (col("rest_type").isNotNull()) & (col("rest_type") != "")
)


w_restaurant = Window.orderBy("name")
dim_restaurant = df.select(
    "name",
    "address",
    "phone",
    "online_order",
    "book_table",
    "rest_type",
    "dish_liked"
).distinct() \
  .withColumn("restaurant_id", row_number().over(w_restaurant))

dim_restaurant.write.mode("overwrite").saveAsTable("dim_restaurant")


w_location = Window.orderBy("location")
dim_location = df.select("location", "listed_in_city").distinct() \
    .withColumn("location_id", row_number().over(w_location))

dim_location.write.mode("overwrite").saveAsTable("dim_location")


w_cuisine = Window.orderBy("cuisines")
dim_cuisine = df.select("cuisines").distinct() \
    .withColumn("cuisine_id", row_number().over(w_cuisine))

dim_cuisine.write.mode("overwrite").saveAsTable("dim_cuisine")


fact_df = df.join(
    dim_restaurant.select("restaurant_id", "name", "address"),
    ["name", "address"],
    "left"
).join(
    dim_location,
    ["location", "listed_in_city"],
    "left"
).join(
    dim_cuisine,
    ["cuisines"],
    "left"
)


fact_df = fact_df.withColumn("rate", when(col("rate").isNull(), -1.0).otherwise(col("rate")))
fact_df = fact_df.withColumn("rate", round(col("rate"), 1))


fact_df = fact_df.withColumn("votes", when(col("votes") == 0, None).otherwise(col("votes")))


fact_table = fact_df.select(
    "restaurant_id",
    "location_id",
    "cuisine_id",
    "rate",
    "votes",
    "approx_cost_for_two"
).dropDuplicates()

fact_table.write.mode("overwrite").saveAsTable("fact_restaurant_data")

spark.stop()

