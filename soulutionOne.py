from pyspark.sql import SparkSession
from pyspark.sql.functions import col, monotonically_increasing_id, collect_set, udf
from pyspark.sql.types import DoubleType, StringType
from difflib import SequenceMatcher
from pyspark.sql.window import Window
import sys

def string_similarity(str1, str2):
    matcher = SequenceMatcher(None, str1, str2)
    similarity_ratio = matcher.ratio()
    return similarity_ratio * 100

def equalName(col1, col2):
    if col1 <= col2:
        input_str = col1
    else:
        input_str = col2
    return input_str

equal_name_UDF = udf(equalName, StringType())
string_similarity_udf = udf(lambda col1, col2: string_similarity(col1, col2), DoubleType())

# Initialize SparkSession
spark = SparkSession.builder.appName("pulsProject").getOrCreate()

# Task 1: Removing Duplicate Counterparty Data Entries
try:
    df = spark.read.csv('source_1_2.csv', header=True)
except Exception as e:
    print("Error: Failed to read the input file.")
    sys.exit(1)

# Validate input file columns
required_columns = ['name', 'iban']
missing_columns = [col for col in required_columns if col not in df.columns]
if missing_columns:
    print(f"Error: Input file is missing required columns: {', '.join(missing_columns)}")
    sys.exit(1)

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
    .select(equal_name_UDF(col('a.name'), col('b.name')).alias("uniqId"),
            col('a.name').alias('name_a'),
            col('b.name').alias('name_b'),
            col('a.iban').alias('iban_a'),
            col('b.iban').alias('iban_b'))

# Define a window specification
window_spec = Window.partitionBy("uniqID")

# Apply collect_set on the "Value" column within the window
df_with_collect_list = df_linked.withColumn("CollectedNames", collect_set(col("name_a")).over(window_spec).cast(StringType()))
df_with_collect_list = df_with_collect_list.withColumn("CollectedIbans", collect_set(col("iban_a")).over(window_spec).cast(StringType()))

df_with_collect_list = df_with_collect_list.drop("name_a", "name_b", "iban_a", "iban_b")
df_with_collect_list = df_with_collect_list.dropDuplicates(['uniqId', 'CollectedNames', 'CollectedIbans'])

# Save the linked DataFrame to a new CSV file
df_with_collect_list.coalesce(1).write.csv('linked_file1.csv', header=True, mode='overwrite')
print(df_with_collect_list.count())

# Stop SparkSession
spark.stop()
