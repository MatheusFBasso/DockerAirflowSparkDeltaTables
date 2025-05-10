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
        print(f"│{' '*21}                                                          Revised              {' '*18}│")
        print(f"└{'─'*118}┘")


    def load_raw_data_to_bronze(self, divvy_path):


        self.log_message(show=self._SHOW_LOG,
                         message=f'LOADING DATA | {divvy_path}.json -> bronze.divvy_bikes',
                         start=True)

        if not DeltaTable.isDeltaTable(self.spark, f'./warehouse/bronze.db/divvy_bikes'):
            print('Creating Bronze Table')

            (self.spark.read.format('json').load(bronze_path_raw_data + '/' + divvy_path + '/')
             .withColumn('type', lit(divvy_path))
             .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
             .select('type', 'data', 'last_updated', 'ttl', 'version', 'last_updated_ts')
             .write.format('delta')
             .partitionBy('type')
             .mode('append').option("mergeSchema", "true")
             .save('./warehouse/bronze.db/divvy_bikes'))

        else:
            print('Using Bronze Table')
            df = (self.spark.read.format('json').load(bronze_path_raw_data + '/' + divvy_path + '/')
                       .withColumn('type', lit(divvy_path))
                       .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
                       .select('type', 'data', 'last_updated', 'ttl', 'version', 'last_updated_ts'))

            (df.write.format('delta')
                     .mode('append').option("mergeSchema", "true")
                     .partitionBy('type')
                     .save('./warehouse/bronze.db/divvy_bikes'))

        self.log_message(show=self._SHOW_LOG,
                         message=f'LOADING DATA | {divvy_path}.json -> bronze.divvy_{divvy_path} | OK',
                         end=True)

        self.log_message(show=self._SHOW_LOG, message='SAVING BRONZE RAW DATA | OK', start=True, end=True)
