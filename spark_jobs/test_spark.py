from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SkillGapRadar").getOrCreate()

data = [("Python", 100), ("SQL", 80), ("Spark", 50)]

columns = ["Skill", "Demand"]

df = spark.createDataFrame(data, columns)

df.show()