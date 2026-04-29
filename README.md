# 🌦️ PySpark Weather Analytics Project

## 📖 Description
This project focuses on performing **Exploratory Data Analysis (EDA)** on a weather dataset using the **PySpark** framework. The goal is to demonstrate proficiency in handling large-scale data processing, schema management, and complex aggregations.

### 🛠️ Key Technical Steps:
1.  **Session Management**: Initializing a `SparkSession` and configuring data ingestion from CSV.
2.  **Schema Transformation**: Converting raw string data into appropriate numerical types (`Double`, `Integer`) for mathematical accuracy.
3.  **Data Filtering**: Extracting insights based on specific weather conditions (e.g., rainfall presence).
4.  **Statistical Logic**: Implementing custom functions for:
    * Mean sunshine duration.
    * Maximum temperature tracking.
    * Mode calculation (most common wind direction) using grouping and sorting.

## 🚀 How to Use
1. Clone the repository: `git clone https://github.com/your-username/PySpark-Weather-Analysis.git`
2. Install dependencies: `pip install pyspark pandas`
3. Run the `weather_analysis.py` script.

## 📊 Dataset
The dataset includes various weather metrics such as:
- **Sunshine**: Duration of sunlight.
- **Rainfall**: Amount of rain recorded.
- **WindDir9am/3pm**: Directions of wind at specific times.
- **Cloud9am**: Cloud cover intensity.
