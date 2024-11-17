from lib.utils.Now import Now
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
from pyspark.sql.session import SparkSession


class LogProcess(Now):

    _SHOW_LOG = True
    _END_TIME = Now().now_datetime()

    def __init__(self, spark: SparkSession, table_name: str, database: str, rows_inserted: str, start_time):
        self.Spark = spark
        self._table_name = table_name
        self._database = database
        self._rows_inserted = rows_inserted
        self._start_time = start_time
        print(self._END_TIME)

    def process(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING LOG', start=True, end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        schema = StructType([
            StructField("id", TimestampType(), False),
            StructField("db", StringType(), False),
            StructField("table", StringType(), False),
            StructField("rows_inserted", IntegerType(), False),
            StructField("start_time", TimestampType(), False),
            StructField("end_time", TimestampType(), False),
        ])

        df = self.Spark.createDataFrame(
            [(self._start_time,      # id
              self._database,        # db
              self._table_name,      # table_name
              self._rows_inserted,   # rows_inserted
              self._start_time,      # start_time
              self._END_TIME         # end_time
            )], schema=schema)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING LOG | inserting Log Table', start=True)
        df.show(truncate=False)
        df.write.format('delta').insertInto('info.log')
        self.log_message(show=self._SHOW_LOG, message='SAVING LOG | inserting Log Table | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        del df, schema
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING LOG | OK', start=True, end=True)
        # --------------------------------------------------------------------------------------------------------------
