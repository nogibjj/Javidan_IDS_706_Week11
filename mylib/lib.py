# import os
# from pyspark.sql import SparkSession
# from pyspark.sql.functions import when, concat_ws
# import requests
# from pyspark.sql.types import (
#      StructType, 
#      StructField, 
#      IntegerType, 
#      StringType, 
# )



# def start_spark(appName):
#     spark = SparkSession.builder.appName(appName).getOrCreate()
#     return spark


# def end_spark(spark):
#     spark.stop()
#     return True



# def extract_csv_as_spark_table(
#     url
# ):
#     pass

# def extract_csv(
#     url,
#     file_path,
#     directory="data",
# ):
#     """Extract a url to a file path"""
#     if not os.path.exists(directory):
#         os.makedirs(directory)
#     with requests.get(url) as r:
#         with open(os.path.join(directory, file_path), "wb") as f:
#             f.write(r.content)
#     return os.path.join(directory, file_path)


# def load_data(spark, data="data/olympic_summer.csv"):
#     """load data"""
#     # data preprocessing by setting schema
#     schema = StructType([
#                 StructField("Year", IntegerType(), True),
#                 StructField("City", StringType(), True),
#                 StructField("Sport", StringType(), True),
#                 StructField("Discipline", StringType(), True),
#                 StructField("Athlete", StringType(), True),
#                 StructField("Country", StringType(), True),
#                 StructField("Gender", StringType(), True),
#                 StructField("Event", StringType(), True),
#                 StructField("Medal", StringType(), True)
#             ])
    
#     df = spark.read.option("header", "true").schema(schema).csv(data)

#     return df

# def query(spark, df, query, name = 'temp_table'): 
#     """queries using spark sql"""
#     spark = SparkSession.builder.appName("Olympic").getOrCreate()
#     return spark.sql(query).show()


# def describe(df):
#     df.describe().toPandas().to_markdown()

#     return df.describe().show()



# def example_transform(df):

#     df = df.withColumn(
#         "Medal_Score",
#         when(df["Medal"] == "Gold", 3)
#         .when(df["Medal"] == "Silver", 2)
#         .when(df["Medal"] == "Bronze", 1)
#         .otherwise(0)  # Use 0 for non-medal cases, if any
#     )

#     df = df.withColumn("Discipline_Event"
#                        , concat_ws("-", df["Discipline"]
#                        , df["Event"]))



#     return df.show()


from pyspark.sql import SparkSession
from pyspark.sql.functions import when, concat_ws
import pandas as pd


def extract_csv(url):
    pd_df = pd.read_csv(url)

    print('Reading successful!')
    return pd_df

    
def load_to_db(dataframe):
    spark = SparkSession.builder.getOrCreate()
    spark_df = spark.createDataFrame(dataframe)
    
    spark_df.write.format("delta").mode("append").saveAsTable("jk645_olympics_load")
    
    print('Successfully loaded data')


def query(query): 
    """queries using spark sql"""
    spark = SparkSession.builder.appName("Olympic").getOrCreate()
    return spark.sql(query).show()


def example_transform(source_table_name, target_table_name):
    
    spark = SparkSession.builder.appName('Olympics').getOrCreate()
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

    print(f'Transformed data write into - > {target_table_name}')

    return spark_df.show()




