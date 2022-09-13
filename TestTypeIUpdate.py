from pyspark.sql import SparkSession
from TypeIUpdateClass import TypeIUpdate
import datetime
from datetime import *
spark = SparkSession.builder.appName("MyApp")\
    .getOrCreate()

dt = datetime.now(timezone.utc)
  
utc_time = dt.replace(tzinfo=timezone.utc)
print(dt)
print(utc_time)

df = spark.createDataFrame([(1,"Ricky",30),(4,"Josh",19),(7,"Mizy",20)],['ID','Name','Age'])

type1 = TypeIUpdate(spark, "./dataTransformationtemplates", df, "ID", datetime.now(timezone.utc))

