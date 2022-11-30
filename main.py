from pyspark.sql.dataframe import DataFrame
from pyspark.sql import functions as F
import pyspark as pk

df = pk.createDataFrame([('abcd',)], ['s',])
df.select(F.substring(df.s, 1, 2).alias('s')).collect()
[pk.Row(s='ab')]
F.substring()

print(df)
