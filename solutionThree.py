from pyspark.sql import SparkSession
from pyspark.sql.functions import levenshtein, col, lit, collect_list, struct

# Create a Spark Session
spark = SparkSession.builder.appName("duplicate_removal").getOrCreate()

# Load the data from the CSV file
df = spark.read.format('csv').option('header', 'true').load('source_1_2.csv')

# Task 1: Remove duplicates based on 'id'
df = df.dropDuplicates(['id'])

# Save the cleaned data to a new CSV file
df.write.csv('cleaned_data.csv', header=True,)

# Task 2: Identify and link similar counterparties based on 'name' and 'iban'
# Note: This is a naive implementation with O(n^2) complexity, so it won't scale well for large data

df = df.withColumn("name_iban", df["name"] + df["iban"])
linked_df = df.alias("df1").join(df.alias("df2"), levenshtein(col("df1.name_iban"), col("df2.name_iban")) <= 3)

# Group by id and aggregate all linked names and ibans
linked_df = linked_df.groupby("df1.id").agg(collect_list(struct("df2.name", "df2.iban")).alias("linked_counterparts"))

linked_df.show(20)
# Save the final data to a new CSV file
#linked_df.write.csv('final_data.csv', header=True)

# Stop the Spark Session
spark.stop()