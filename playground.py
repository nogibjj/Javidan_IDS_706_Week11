# Databricks notebook source
import pandas as pd

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import when, concat_ws

def extract_csv(url):
    pd_df = pd.read_csv(url)

    return pd_df

def load_to_db(dataframe):
    spark_df = spark.createDataFrame(dataframe)

    spark_df.write.format("delta").mode("append").saveAsTable("jk645_olympics_load")

    print('Successfully loaded data')

    return True


def query(query): 
    """queries using spark sql"""
    spark = SparkSession.builder.appName("Olympic").getOrCreate()
    return spark.sql(query).show()


def example_transform(source_table_name, target_table_name):

    spark_df = spark.read.table(source_table_name)

    spark_df = spark_df.withColumn(
        "Medal_Score",
        when(spark_df["Medal"] == "Gold", 3)
        .when(spark_df["Medal"] == "Silver", 2)
        .when(spark_df["Medal"] == "Bronze", 1)
        .otherwise(0)  # Use 0 for non-medal cases, if any
    )

    spark_df = spark_df.withColumn("Discipline_Event"
                       , concat_ws("-", spark_df["Discipline"]
                       , spark_df["Event"]))


    spark_df.write.format("delta").mode("append").saveAsTable(target_table_name)


    return spark_df.show()





# def extract_csv_as_spark_table(
#     url
# ):
#     pd_df = pd.read_csv(url)
#     spark_df = spark.createDataFrame(pd_df)

#     spark_df.write.format("delta").mode("append").saveAsTable("jk645_olympics_load")

#     print('Successfully loaded data')


# df = extract_csv('https://raw.githubusercontent.com/nogibjj/Javidan_IDS_706_Week11/refs/heads/main/data/olympic_summer.csv')
# load_to_db(df)

sql_query = """
SELECT Year, City, Medal, COUNT(*) as cnt
            FROM jk645_olympics_load
            GROUP BY  Year, City, Medal
            ORDER BY cnt DESC
"""


# extract_csv_as_spark_table('https://raw.githubusercontent.com/nogibjj/Javidan_IDS_706_Week11/refs/heads/main/data/olympic_summer.csv')
    

# COMMAND ----------

# Specify the table name
# table_name = "jk645_olympics_load"

# # Read the table into a Spark DataFrame
# df = spark.read.table(table_name)

# # Show the DataFrame
# df.show()



example_transform("jk645_olympics_load", "jk645_olympics_transformed")

# COMMAND ----------

query(sql_query)


# COMMAND ----------

from mylib.lib import extract_csv


df = extract_csv('https://raw.githubusercontent.com/nogibjj/Javidan_IDS_706_Week11/refs/heads/main/data/olympic_summer.csv')
