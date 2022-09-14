from distutils.log import error
from delta.tables import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from datetime import *
from delta import *


class TypeIUpdate:

    def __init__(self,
    spark,
    base_table_path: str,
    updates_dataframe,
    business_key_columns: list,
    last_modification_datetime: datetime
    ):
        self.spark = spark
        self.base_table_path = base_table_path
        self.updates_dataframe = updates_dataframe
        self.business_key_columns = business_key_columns
        self.last_modification_datetime = last_modification_datetime

        try:
            spark.read.format('delta').load(self.base_table_path)
        except Exception as e:
            if "Path does not exist" in str(e):
                print(1)
            elif "is not a Delta table." in str(e):
                print(2)
            else:
                raise e
