from pyspark.sql.functions import explode, cast, col, lit
from datetime import datetime
from pyspark.sql.types import StructType, StructField, TimestampType, LongType, StringType, DoubleType, DateType
from lib.Spark.GetSpark import DeltaSpark
from lib.utils.Now import Now
from lib.utils.DivvyBikes.divvy_bikes_path import bronze_path_raw_data
from delta.tables import DeltaTable


class Bronze(Now):

    _SHOW_LOG = True

    DIVVY_DICT = {'free_bike_status': ('bikes',),
                  'station_information': ('stations',),
                  'station_status': ('stations',),
                  'system_pricing_plan': ('plans',),
                  'vehicle_types': ('vehicle_types',)}

    BRONZE_SCHEMA = StructType([
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

        for table_info in self.DIVVY_DICT.items():

            if not DeltaTable.isDeltaTable(self.spark, f'./warehouse/bronze.db/divvy_{table_info[0]}'):

                self.log_message(show=self._SHOW_LOG,
                                 message=f'CREATING RAW DELTA TABLES | bronze.divvy_{table_info[0]}',
                                 start=True)

                empty_info_df = self.spark.createDataFrame([], self.BRONZE_SCHEMA)
                empty_info_df.write.format('delta') \
                    .mode('overwrite') \
                    .option("overwriteSchema", "True") \
                    .saveAsTable(f"bronze.divvy_{table_info[0]}")

                self.log_message(show=self._SHOW_LOG,
                                 message=f'CREATING RAW DELTA TABLES | bronze.divvy_{table_info[0]} | OK',
                                 end=True)

        self.log_message(show=self._SHOW_LOG, message='CREATING RAW DELTA TABLES | OK', start=True, end=True)

    def load_all_data_to_bronze(self):

        self.log_message(show=self._SHOW_LOG, message='SAVING BRONZE RAW DATA', start=True, end=True)

        for table_info in self.DIVVY_DICT.items():
            self.log_message(show=self._SHOW_LOG,
                             message=f'LOADING DATA | {table_info[0]}.json -> bronze.divvy_{table_info[0]}',
                             start=True)

            if not DeltaTable.isDeltaTable(self.spark, f'./warehouse/bronze.db/divvy_{table_info[0]}'):

                (self.spark.read.format('json').load(bronze_path_raw_data + '/' + table_info[0] + '/')
                 .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
                 .select('data', 'last_updated', 'ttl', 'version', 'last_updated_ts')
                 .write.format('delta').option("overwriteSchema", "True")
                 .saveAsTable(f'bronze.divvy_{table_info[0]}'))

            else:
                df = (self.spark.read.format('json').load(bronze_path_raw_data + '/' + table_info[0] + '/')
                           .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
                           .select('data', 'last_updated', 'ttl', 'version', 'last_updated_ts'))

                (df.write.format('delta').option("overwriteSchema", "True")
                           .insertInto(f'bronze.divvy_{table_info[0]}'))


            self.log_message(show=self._SHOW_LOG,
                             message=f'LOADING DATA | {table_info[0]}.json -> bronze.divvy_{table_info[0]} | OK',
                             end=True)

        self.log_message(show=self._SHOW_LOG, message='SAVING BRONZE RAW DATA | OK', start=True, end=True)
