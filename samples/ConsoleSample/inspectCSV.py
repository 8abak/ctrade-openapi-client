import pandas as pd
df = pd.read_csv("ticks.csv")

print ("Fist 5 rows: ")
print (df.tail(20))

print("\nColumn names: ")
print(df.columns.tolist())

print("\nDataa types: ")
print(df.dtypes)