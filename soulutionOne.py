from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, IntegerType
from pyspark.sql.functions import monotonically_increasing_id
from difflib import SequenceMatcher
from pyspark.sql.functions import expr
from pyspark.sql import SparkSession
from pyspark.sql.functions import collect_set, col
from fuzzywuzzy import fuzz
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import col, dense_rank, lit, least
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
import uuid
import hashlib


def string_similarity(str1, str2):
    matcher = SequenceMatcher(None, str1, str2)
    similarity_ratio = matcher.ratio()
    return similarity_ratio * 100


def generate_uuid(col1, col2):
    if col1 <= col2:
        input_str = col1 + col2
    else:
        input_str = col2 + col1
    hashed_str = hashlib.md5(input_str.encode()).hexdigest()
    return str(uuid.UUID(hashed_str))


generate_uuidUDf = udf(generate_uuid, StringType())

# Define a UDF to apply string_similarity on the DataFrame columns
string_similarity_udf = udf(lambda col1, col2: string_similarity(col1, col2), DoubleType())
findmin = udf(lambda col1, col2: min(col1, col2), IntegerType())
# Initialize SparkSession

spark = SparkSession.builder.appName("pulsProject").getOrCreate()

# Task 1: Removing Duplicate Counterparty Data Entries
df = spark.read.csv('input.csv', header=True)

# Drop duplicates based on name and iban columns
df_cleaned = df.dropDuplicates(['name', 'iban'])

# Assign unique 'id' to any duplicated rows
df_cleaned = df_cleaned.withColumn('id', monotonically_increasing_id())

# Save the cleaned DataFrame to a new CSV file
df_cleaned.coalesce(1).write.csv("cleaned_file.csv", header=True, mode='overwrite')

# Task 2: Identifying and Linking Similar Counterparties
df_cleaned = spark.read.csv('cleaned_file.csv', header=True)


# Create a new DataFrame with linked counterparties
df_linked = df_cleaned.alias('a').join(df_cleaned.alias('b'),
                                       (col('a.name') != col('b.name')) &
                                       ((col("a.iban") != "") & (col("b.iban") != "")) &
                                       ((string_similarity_udf(col('a.name'), col('b.name')) >= 80) |
                                        (string_similarity_udf(col('a.iban'), col('b.iban')) >= 80)), 'inner') \
    .select(generate_uuidUDf(col('a.name'), col('b.name')).alias("uniqId"),
            col('a.name').alias('name_a'),
            col('b.name').alias('name_b')
            , col('a.iban').alias('iban_a'), col('b.iban').alias('iban_b'))

# Define a window specification
window_spec = Window.partitionBy("uniqID")
# Apply collect_list on the "Value" column within the window
df_with_collect_list = df_linked.withColumn("CollectedNames",
                                            collect_set(col("name_a")).over(window_spec).cast(StringType()))
df_with_collect_list = df_with_collect_list.withColumn("CollectedIbans",
                                                       collect_set(col("iban_a")).over(window_spec).cast(StringType()))

df_with_collect_list=df_with_collect_list.drop("name_a").drop("name_b").drop("iban_a").drop("iban_b")
df_with_collect_list = df_with_collect_list.dropDuplicates(['uniqId','CollectedNames', 'CollectedIbans'])


# Save the linked DataFrame to a new CSV file
df_with_collect_list.coalesce(1).write.csv('linked_file.csv', header=True, mode='overwrite')
print(df_with_collect_list.count())

# Stop SparkSession
spark.stop()
