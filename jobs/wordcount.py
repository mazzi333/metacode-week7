from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.appName("Q4 WordCount").getOrCreate()
try:
    lines = spark.read.text("/data/wordcount.txt")
    words = lines.select(F.explode(F.split("value", r"\s+")).alias("word"))
    counts = words.where(F.col("word") != "").groupBy("word").count()

    counts.orderBy(F.desc("count"), F.asc("word")).show(20, truncate=False)
finally:
    spark.stop()