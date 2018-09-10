
from pyspark.sql import SparkSession


log_file = './README.md'
spark = SparkSession.builder.appName('simple_spark_job').getOrCreate()
log_data = spark.read.text(log_file).cache()


num_a = log_data.filter(log_data.value.contains('a')).count()
num_b = log_data.filter(log_data.value.contains('b')).count()

print(f'Lines with a: {num_a}, line with b: {num_b}')  # noqa

spark.stop()
