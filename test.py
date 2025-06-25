from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim
from pyspark.sql.types import IntegerType

spark = SparkSession.builder.appName("HousePriceAnalysis").getOrCreate()

df = spark.read.parquet("/content/house-price.parquet")

df = df.na.drop()
df = df.na.fill({"Price": 0, "prefarea": "Unknown"})

df = df.withColumn("Price_in_Lakhs", col("Price") / 100000)
df = df.withColumnRenamed("OldColumn", "NewColumn")

df = df.withColumn("prefarea", upper(trim(col("prefarea"))))

df.groupBy("prefarea").avg("Price").orderBy("avg(Price)", ascending=False).show()