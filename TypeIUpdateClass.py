from delta import *
from delta.tables import *
import pyspark.sql.functions as f
from pyspark.sql.types import *
from datetime import *


class TypeIUpdate:

    def __init__(self, spark: object, base_table_path: str, updates_dataframe, business_key_columns: list, last_modification_datetime: datetime) -> None:
        self.spark = spark
        self.base_table_path = base_table_path
        self.updates_dataframe = updates_dataframe
        self.business_key_columns = business_key_columns
        self.last_modification_datetime = last_modification_datetime
        self.audit_columns = ['__business_key_hash']

        try:
            spark.read.format('delta').load(self.base_table_path)

        except Exception as e:
            if "is not a Delta table." in str(e):
                raise ("The path you provided is not a Delta table. Please enter a valid path to a Delta table or a valid empty path to create a new Delta table.")
            elif "Path does not exist" in str(e):
                (self.spark.createDataFrame([], self.updates_dataframe.schema)
                 .withColumn('__business_key_hash', f.lit(None))
                 .write.format('delta').save(self.base_table_path))

            else:
                raise (e)

        assert sorted([x for x in (spark.read.format('delta').load(
            self.base_table_path).columns) if not x in self.audit_columns]) == sorted(self.updates_dataframe.columns)

        self.delta_table = DeltaTable.forPath(self.spark, self.base_table_path)

    def _addAuditColumns(self):

        (self.updates_dataframe.withColumn('__business_key_hash',
         f.sha2(f.concat_ws("|", self.business_key_columns), 256)))

    def typeIMerge(self):
        self._addAuditColumns()

        (self.delta_table.alias('base')
         .merge(
            self.updates_dataframe.alias('updates'),
            'base.__business_key_hash = updates.__business_key_hash'
        )
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute())
