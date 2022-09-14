from pyspark.sql import SparkSession
from TypeIUpdateClass import TypeIUpdate
from delta import *
import datetime
from datetime import *
from delta.tables import *


builder = SparkSession.builder.appName("MyApp") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = configure_spark_with_delta_pip(builder).getOrCreate()

df = spark.createDataFrame([(1,"Ricky",30),(4,"Josh",19),(7,"Mizy",20)],['ID','Name','Age'])

type1 = TypeIUpdate(spark, "./DeltaTableTypeI.csv", df, "ID", datetime.now(timezone.utc))

