# ==============================================================================
# SECTION 1: INITIALIZATION & DATA INGESTION
# ==============================================================================
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, max, count, desc

# Initialize Spark Session
spark = SparkSession.builder.appName("practice_assignment").getOrCreate()

# Load the dataset
df = spark.read.option("header", True).option("inferschema", True).csv("weather_dataset.csv")

# Data Preview
print("--- Initial Data Preview (First 5 Rows) ---")
print(df.limit(5).toPandas())

# ==============================================================================
# SECTION 2: DATA TYPE VERIFICATION & TRANSFORMATION
# ==============================================================================
# Checking original dtypes
print("--- Original Schema ---")
print(df.dtypes)

# Changing dtypes -> Sunshine-double, WindGustSpeed-int, WindSpeed9am-int
df = df.withColumn("Sunshine", col("Sunshine").cast("double")) \
       .withColumn("WindGustSpeed", col("WindGustSpeed").cast("int")) \
       .withColumn("WindSpeed9am", col("WindSpeed9am").cast("int"))

# Checking updated dtypes
print("--- Updated Schema ---")
print(df.dtypes)

# ==============================================================================
# SECTION 3: CUSTOM ANALYTICAL FUNCTIONS
# ==============================================================================

# --- Function 1: Rain Tomorrow Count ---
def rain_tomorrow(input_df):
    """Returns the number of days when it rained the next day."""
    days = input_df.filter(col("RainTomorrow") == "Yes").count()
    return days

# --- Function 2: Avg Sunshine on Dry Days ---
def avg_sunshine_with_no_rainfall(input_df):
    """Returns the average sunshine duration on days with no rainfall."""
    # Note: Filtering where Rainfall is 0 as per hint
    avg_sunshine = input_df.filter(col("RainToday") == "No").agg(avg("Sunshine")).collect()[0][0]
    return round(avg_sunshine, 2)

# --- Function 3: Max Temperature at 3 PM ---
def max_temp_3pm(input_df):
    """Returns the maximum temperature recorded at 3 PM."""
    max_temp = input_df.agg(max(col("Temp3pm"))).collect()[0][0]
    return max_temp

# --- Function 4: Avg Humidity Before Rain ---
def avg_humidity_3pm_rained_nextday(input_df):
    """Returns the average humidity at 3 PM on days it rained the next day."""
    filtered_df = input_df.filter(col("RainTomorrow") == "Yes")
    avg_humidity = filtered_df.agg(avg(col("Humidity3pm"))).collect()[0][0]
    return round(avg_humidity, 2)

# --- Function 5: Most Common Wind Direction ---
def common_wind_dir_9am(input_df):
    """Returns the most common wind direction at 9 AM on cloudy days (>5)."""
    cloudy_days = input_df.filter(col("Cloud9am") > 5)
    common_dir = cloudy_days.groupBy("WindDir9am").count().orderBy(desc("count"))
    most_common_dir = common_dir.collect()[0][0]
    return most_common_dir

# ==============================================================================
# SECTION 4: EXECUTION & OUTPUT
# ==============================================================================
print("\n" + "="*40)
print("FINAL ANALYSIS RESULTS")
print("="*40)

# Rain Tomorrow Count
res1 = rain_tomorrow(df)
print(f"1. Total days it rained the next day: {res1}")

# Average Sunshine
res2 = avg_sunshine_with_no_rainfall(df)
print(f"2. Average sunshine duration (No Rainfall): {res2}")

# Max Temperature
res3 = max_temp_3pm(df)
print(f"3. Maximum temperature recorded at 3 PM: {res3}")

# Average Humidity
res4 = avg_humidity_3pm_rained_nextday(df)
print(f"4. Average humidity at 3 PM (Pre-Rain days): {res4}")

# Common Wind Direction
res5 = common_wind_dir_9am(df)
print(f"5. Most common wind direction (Cloudy 9 AM): {res5}")
