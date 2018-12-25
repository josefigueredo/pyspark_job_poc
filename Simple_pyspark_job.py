"""Simple_pyspark_job.py"""
from contextlib import contextmanager
from pyspark import SparkContext
from pyspark import SparkConf


SPARK_MASTER='local'
SPARK_APP_NAME='Word Count'
SPARK_EXECUTOR_MEMORY='2G'

@contextmanager
def spark_manager():
    conf = SparkConf().setMaster(SPARK_MASTER) \
                      .setAppName(SPARK_APP_NAME) \
                      .set("spark.executor.memory", SPARK_EXECUTOR_MEMORY)
    spark_context = SparkContext(conf=conf)

    try:
        yield spark_context
    finally:
        spark_context.stop()

with spark_manager() as context:
    File = "/home/josefigueredo/Workspaces/datascience/data/LongLetter.txt"
    textFileRDD = context.textFile(File)
    wordCounts = textFileRDD.flatMap(lambda line: line.split()).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a+b)
    #wordCounts.coalesce(1).saveAsTextFile("tmp/simple_pyspark_job/output")

print("WordCount - Done")
