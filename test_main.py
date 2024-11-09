import pytest
from pyspark.sql import SparkSession
from mylib.lib import (
    extract_csv,
    describe,
    load_data,
    query,
    example_transform,
)
import os


@pytest.fixture(scope="module")
def spark():
    # Start a Spark session
    spark = SparkSession.builder.master("local[*]").appName("test_app").getOrCreate()
    yield spark
    # Stop the Spark session after tests
    spark.stop()


def test_start_spark(spark):
    # Test Spark session is started
    assert spark is not None


def test_extract_csv():

    output_path = extract_csv(
        url="""https://raw.githubusercontent.com/nogibjj/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/refs/heads/main/data/olympic_summer.csv""",
        file_path="test_olympic_summer.csv",
        directory="data",
    )

    assert os.path.exists(output_path)


def test_load_data(spark):
    # Try loading data and perform a simple check
    df = load_data(spark, data="data/test_olympic_summer.csv")
    assert df is not None, "Dataframe is None; load_data failed."
    assert df.count() > 0, "Dataframe is empty; load_data did not load data correctly."
    assert "Medal" in df.columns, "Expected column 'Medal' not found in DataFrame."


def test_query(spark):
    df = load_data(spark, data="data/test_olympic_summer.csv")

    res = query(spark, df, query="SELECT * FROM temp_table")

    assert res is None


def test_describe(spark):
    df = load_data(spark, data="data/test_olympic_summer.csv")
    res = describe(df)

    assert res is None


