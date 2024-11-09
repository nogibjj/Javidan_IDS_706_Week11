"""
Main cli or app entry point
"""

from mylib.lib import (
    extract_csv,
    start_spark,
    end_spark,
    load_data,
    query,
    describe,
    example_transform,
)


def process_csv_spark():

    extract_csv(
        url="https://raw.githubusercontent.com/nogibjj/Javidan_Karimli_IDS706_ComplexSqlQueryDatabricks/refs/heads/main/data/olympic_summer.csv",
        file_path="olympic_summer.csv",
        directory="data",
    )

    sc = start_spark("sparkApp")

    if sc:
        print("Spark Session started!")

    else:
        print("Failed to start session")
        return

    df = load_data(sc)

    res = query(
        sc,
        df,
        query="""
            SELECT Year, City, Medal, COUNT(*) as cnt
            FROM Olympics
            GROUP BY  Year, City, Medal
            ORDER BY cnt DESC
        """,
        name="Olympics",
    )
    describe(df)

    print(res)

    transform_res = example_transform(df)

    print(transform_res)

    ending = end_spark(sc)

    if ending:
        print("Spark Session closed succesfully")

    else:
        print("Failed to terminate session")

    return True


if __name__ == "__main__":
    # pylint: disable=no-value-for-parameter
    process_csv_spark()
