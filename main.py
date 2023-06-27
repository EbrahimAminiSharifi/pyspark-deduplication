from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType

# Initialize SparkSession
spark = SparkSession.builder.getOrCreate()

# Task 1: Removing Duplicate Counterparty Data Entries
df = spark.read.csv('input.csv', header=True)

# Drop duplicates based on name and iban columns
df_cleaned = df.dropDuplicates(['name', 'iban'])

# Assign unique 'id' to any duplicated rows
df_cleaned = df_cleaned.withColumn('id', monotonically_increasing_id())

# Save the cleaned DataFrame to a new CSV file
df_cleaned.write.csv('cleaned_file.csv', header=True, mode='overwrite')


# Task 2: Identifying and Linking Similar Counterparties
df_cleaned = spark.read.csv('cleaned_file.csv', header=True)

# Function to calculate string similarity using Levenshtein distance
def calculate_similarity(str1, str2):
    return fuzz.ratio(str1, str2)

# Register the similarity function as a UDF
calculate_similarity_udf = udf(calculate_similarity, StringType())

# Calculate similarity between names and create a new column 'similarity_score'
df_cleaned = df_cleaned.withColumn('similarity_score', calculate_similarity_udf(col('name'), col('name')))

# Set similarity threshold (adjust as per your requirements)
similarity_threshold = 80

# Create a new DataFrame with linked counterparties
df_linked = df_cleaned.alias('a').join(df_cleaned.alias('b'),
                                       (col('a.name') != col('b.name')) &
                                       (col('a.similarity_score') >= similarity_threshold),
                                       'inner') \
    .select(col('a.name').alias('unique_counterparty'), col('b.name').alias('linked_entries'))

# Save the linked DataFrame to a new CSV file
df_linked.write.csv('linked_file.csv', header=True, mode='overwrite')

# Stop SparkSession
spark.stop()
