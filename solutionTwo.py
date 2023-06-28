from difflib import SequenceMatcher
from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType


def string_similarity(str1, str2):
    matcher = SequenceMatcher(None, str1, str2)
    similarity_ratio = matcher.ratio()
    return similarity_ratio * 100


# Define a UDF to apply string_similarity on the DataFrame columns

findmin = udf(lambda col1, col2: min(col1, col2), IntegerType())
# Initialize SparkSession

spark = SparkSession.builder.appName("pulsProject").getOrCreate()

# Task 1: Removing Duplicate Counterparty Data Entries
df = spark.read.csv('source_1_2.csv', header=True)

# Drop duplicates based on name and iban columns
df_cleaned = df.dropDuplicates(['name', 'iban'])

# Assign unique 'id' to any duplicated rows
df_cleaned = df_cleaned.withColumn('id', monotonically_increasing_id())

# Save the cleaned DataFrame to a new CSV file
df_cleaned.coalesce(1).write.csv("cleaned_file.csv", header=True, mode='overwrite')

# Task 2: Identifying and Linking Similar Counterparties
df_cleaned = spark.read.csv('cleaned_file.csv', header=True)

# Define a dictionary to store the key-value pairs
key_value_set = {}

# rowClass this is a class for storing each row data
class rowClass:
    def __init__(self):
        self.ibans = set()
        self.names = set()

    def add_iban(self, element):
        self.ibans.add(element)

    def add_names(self, element):
        self.names.add(element)

    def __str__(self):
        # print(self.ibans)
        return str(self.names)

# Processing row by row
for row in df_cleaned.collect():
    obj = rowClass()
    added = False
    name = row["name"]
    iban = row["iban"]
    if len(key_value_set) == 0:
        obj.add_names(name)
        obj.add_iban(iban)
        key_value_set[name] = obj
        added = True
    else:
        for key, value in key_value_set.items():
            if string_similarity(key, name) > 80:
                lastIban = key_value_set.get(key)
                lastIban.add_names(name)
                lastIban.add_iban(iban)
                key_value_set[key] = lastIban
                added = True
    if not added :
        obj.add_names(name)
        obj.add_iban(iban)
        key_value_set[name] = obj
        added = True


schema = ['name', 'names', 'ibans']
df = spark.createDataFrame([(k, str(v.names), str(v.ibans)) for k, v in key_value_set.items()], schema)
# Convert to a dataframe

df.coalesce(1).write.csv('linked_file2.csv', header=True, mode='overwrite')

# Stop SparkSession
spark.stop()
