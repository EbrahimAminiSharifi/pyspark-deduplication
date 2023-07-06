import pandas as pd

csvfile=pd.read_csv("source_1_2.csv")
print(len(csvfile))
csvfile = csvfile.drop_duplicates(["name", "iban"])
print(len(csvfile))
