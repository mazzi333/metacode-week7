import argparse

from pyspark.sql import SparkSession, functions as F


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("input_csv")
    parser.add_argument("output_dir")
    parser.add_argument("--min-year", type=int, default=2015)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("Q9 Netflix Genre Counts").getOrCreate()
    try:
        spark.sparkContext.setLogLevel("WARN")
        titles = (
            spark.read.option("header", True)
            .option("multiLine", True)
            .option("escape", '"')
            .csv(args.input_csv)
        )
        counts = (
            titles.where(F.col("release_year").cast("int") >= args.min_year)
            .select("type", F.explode(F.split("listed_in", ",")).alias("genre"))
            .withColumn("genre", F.trim(F.col("genre")))
            .where(F.col("genre") != "")
            .groupBy("type", "genre")
            .agg(F.count("*").alias("title_count"))
        )

        counts.coalesce(1).write.mode("overwrite").option(
            "compression", "snappy"
        ).parquet(args.output_dir)

        print(f"MIN_YEAR={args.min_year}")
        print(f"AGGREGATE_ROWS={counts.count()}")
        counts.orderBy("type", "genre").show(10, truncate=False)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()