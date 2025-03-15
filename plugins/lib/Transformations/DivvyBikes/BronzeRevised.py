from pyspark.sql.functions import explode, cast, col, lit
from datetime import datetime
from pyspark.sql.types import StructType, StructField, TimestampType, LongType, StringType, DoubleType, DateType
from lib.Spark.GetSpark import DeltaSpark
from lib.utils.Now import Now
from lib.utils.DivvyBikes.divvy_bikes_path import bronze_path_raw_data
from delta.tables import DeltaTable
from time import sleep


class BronzeRevised(Now):

    _SHOW_LOG = True

    BRONZE_SCHEMA = StructType([
            StructField('type', StringType(), False),
            StructField('data', StringType(), False),
            StructField('last_updated', LongType(), False),
            StructField('ttl', LongType(), False),
            StructField('version', StringType(), True),
            StructField('last_updated_ts', TimestampType(), True),
        ])

    def __init__(self):
        self.spark = DeltaSpark().initialize()

        print(f"┌{'─'*118}┐")
        print(f"│{' '*21}                                                                               {' '*18}│")
        print(f"│{' '*21} ███████████  ███████████      ███████    ██████   █████ ███████████ ██████████{' '*18}│")
        print(f"│{' '*21}░░███░░░░░███░░███░░░░░███   ███░░░░░███ ░░██████ ░░███ ░█░░░░░░███ ░░███░░░░░█{' '*18}│")
        print(f"│{' '*21} ░███    ░███ ░███    ░███  ███     ░░███ ░███░███ ░███ ░     ███░   ░███  █ ░ {' '*18}│")
        print(f"│{' '*21} ░██████████  ░██████████  ░███      ░███ ░███░░███░███      ███     ░██████   {' '*18}│")
        print(f"│{' '*21} ░███░░░░░███ ░███░░░░░███ ░███      ░███ ░███ ░░██████     ███      ░███░░█   {' '*18}│")
        print(f"│{' '*21} ░███    ░███ ░███    ░███ ░░███     ███  ░███  ░░█████   ████     █ ░███ ░   █{' '*18}│")
        print(f"│{' '*21} ███████████  █████   █████ ░░░███████░   █████  ░░█████ ███████████ ██████████{' '*18}│")
        print(f"│{' '*21}░░░░░░░░░░░  ░░░░░   ░░░░░    ░░░░░░░    ░░░░░    ░░░░░ ░░░░░░░░░░░ ░░░░░░░░░░ {' '*18}│")
        print(f"│{' '*21}                                                                               {' '*18}│")
        print(f"└{'─'*118}┘")

    def create_raw_tables(self):
        self.log_message(show=self._SHOW_LOG, message='CREATING RAW DELTA TABLES', start=True, end=True)

        self.spark.sql("""CREATE DATABASE IF NOT EXISTS bronze""")

        # self.spark.sql("""CREATE TABLE IF NOT EXISTS bronze.divvy_bikes""")

        if not DeltaTable.isDeltaTable(self.spark, f'./warehouse/bronze.db/divvy_bikes'):

            self.log_message(show=self._SHOW_LOG,
                             message=f'CREATING RAW DELTA TABLE | bronze.divvy_bikes',
                             start=True)

            empty_info_df = self.spark.createDataFrame([], self.BRONZE_SCHEMA)
            empty_info_df.write.format('delta') \
                .mode('overwrite') \
                .option("overwriteSchema", "True") \
                .saveAsTable(f"bronze.divvy_bikes")

            self.log_message(show=self._SHOW_LOG,
                             message=f'CREATING RAW DELTA TABLES | bronze.divvy_bikes | OK',
                             end=True)

        self.log_message(show=self._SHOW_LOG, message='SAVING BRONZE RAW DATA', start=True, end=True)


    def load_raw_data_to_bronze(self, divvy_path):


        self.log_message(show=self._SHOW_LOG,
                         message=f'LOADING DATA | {divvy_path}.json -> bronze.divvy_divvy_bikes',
                         start=True)

        if not DeltaTable.isDeltaTable(self.spark, f'./warehouse/bronze.db/divvy_divvy_bikes'):

            (self.spark.read.format('json').load(bronze_path_raw_data + '/' + divvy_path + '/')
             .withColumn('type', lit(divvy_path))
             .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
             .select('data', 'last_updated', 'ttl', 'version', 'last_updated_ts')
             .write.format('delta').option("overwriteSchema", "True")
             .saveAsTable(f'bronze.divvy_bikes'))

        else:
            df = (self.spark.read.format('json').load(bronze_path_raw_data + '/' + divvy_path + '/')
                       .withColumn('type', lit(divvy_path))
                       .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
                       .select('type', 'data', 'last_updated', 'ttl', 'version', 'last_updated_ts'))

            print(self.spark.table('bronze.divvy_bikes').columns)

            (df.write.format('delta').option("overwriteSchema", "True")
                       .insertInto(f'bronze.divvy_bikes'))


        self.log_message(show=self._SHOW_LOG,
                         message=f'LOADING DATA | {divvy_path}.json -> bronze.divvy_{divvy_path} | OK',
                         end=True)

        self.log_message(show=self._SHOW_LOG, message='SAVING BRONZE RAW DATA | OK', start=True, end=True)
