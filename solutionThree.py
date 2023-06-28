from difflib import SequenceMatcher
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, collect_set,substring
from pyspark.sql.functions import udf,monotonically_increasing_id
from pyspark.sql.types import DoubleType,StringType
from pyspark.sql.window import Window

def string_similarity(str1, str2):
    matcher = SequenceMatcher(None, str1, str2)
    similarity_ratio = matcher.ratio()
    return similarity_ratio

# Create a SparkSession
spark = SparkSession.builder.getOrCreate()

# Task 1: Removing Duplicate Counterparty Data Entries
df = spark.read.csv('source_1_2.csv', header=True)

# Drop duplicates based on name and iban columns
df_cleaned = df.dropDuplicates(['name', 'iban'])

# Assign unique 'id' to any duplicated rows
df_cleaned = df_cleaned.withColumn('id', monotonically_increasing_id())

# Save the cleaned DataFrame to a new CSV file
df_cleaned.coalesce(1).write.csv("cleaned_file.csv", header=True, mode='overwrite')

df_cleaned=df_cleaned.withColumn("newIban",substring(col("iban"),1,17))

window_spec = Window.partitionBy("newIban")
df_with_collect_list = df_cleaned.withColumn("names",
                                            collect_set(col("name")).over(window_spec).cast(StringType()))
df_with_collect_list = df_with_collect_list.withColumn("ibans",
                                            collect_set(col("iban")).over(window_spec).cast(StringType()))


df_with_collect_list.coalesce(1).write.csv('linked_file3.csv', header=True, mode='overwrite')

spark.stop()
