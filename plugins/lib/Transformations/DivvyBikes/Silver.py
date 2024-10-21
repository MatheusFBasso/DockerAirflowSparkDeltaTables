from pyspark.sql.functions import explode, cast, col, lit
from datetime import datetime
from pyspark.sql.types import DateType, DoubleType, StringType, BooleanType, TimestampType, LongType
from lib.Spark.GetSpark import DeltaSpark
from lib.utils.Now import Now
from lib.utils.DivvyBikes.divvy_bikes_path import bronze_path_raw_data
from delta.tables import DeltaTable


class Silver(Now):

    _SHOW_LOG = True

    def __init__(self):

        self.spark = DeltaSpark().initialize()

    def silver_bike_status(self):
        # --------------------------------------------------------------------------------------------------------------
        print(f"┌{'─' * 118}┐")
        print(f"│{' ' * 24}                                                                             {' ' * 17}│")
        print(f"│{' ' * 24}  █████████  █████ █████       █████   █████ ██████████ ███████████          {' ' * 17}│")
        print(f"│{' ' * 24} ███░░░░░███░░███ ░░███       ░░███   ░░███ ░░███░░░░░█░░███░░░░░███         {' ' * 17}│")
        print(f"│{' ' * 24}░███    ░░░  ░███  ░███        ░███    ░███  ░███  █ ░  ░███    ░███         {' ' * 17}│")
        print(f"│{' ' * 24}░░█████████  ░███  ░███        ░███    ░███  ░██████    ░██████████          {' ' * 17}│")
        print(f"│{' ' * 24} ░░░░░░░░███ ░███  ░███        ░░███   ███   ░███░░█    ░███░░░░░███         {' ' * 17}│")
        print(f"│{' ' * 24} ███    ░███ ░███  ░███      █  ░░░█████░    ░███ ░   █ ░███    ░███         {' ' * 17}│")
        print(f"│{' ' * 24}░░█████████  █████ ███████████    ░░███      ██████████ █████   █████ BIKE STATUS{' ' *13}│")
        print(f"│{' ' * 24} ░░░░░░░░░  ░░░░░ ░░░░░░░░░░░      ░░░      ░░░░░░░░░░ ░░░░░   ░░░░░         {' ' * 17}│")
        print(f"│{' ' * 24}                                                                             {' ' * 17}│")
        print(f"└{'─' * 118}┘")
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)
        df = self.spark.read.format('json').load(bronze_path_raw_data+'/free_bike_status/')\
                 .select(explode('data.bikes').alias('bikes'), 'last_updated')
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA', start=True)
        df = df.select(col('bikes.bike_id').cast(StringType()).alias('bike_id'),
                       col('bikes.vehicle_type_id').cast(StringType()).alias('vehicle_type_id'),
                       col('bikes.lat').cast(DoubleType()).alias('lat'),
                       col('bikes.lon').cast(DoubleType()).alias('lon'),
                       col('bikes.current_range_meters').cast(DoubleType()).alias('current_range_meters'),
                       col('bikes.rental_uris').cast(StringType()).alias('rental_uris'),
                       col('bikes.is_reserved').cast(BooleanType()).alias('is_reserved'),
                       col('bikes.is_disabled').cast(BooleanType()).alias('is_disabled'),
                       col('last_updated').cast(LongType()).alias('last_updated'))\
               .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_bikes_status', start=True)
        lit_date = datetime.strptime(Now().now(), "%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d')
        df.withColumn('date_ref_carga', lit(lit_date).cast(DateType()))\
          .write.format('delta')\
          .mode('overwrite')\
          .option("overwriteSchema", "True")\
          .partitionBy('date_ref_carga')\
          .saveAsTable('silver.divvy_bikes_status')
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_bikes_status | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_bikes_status_now', start=True)
        deltaTableSilver = DeltaTable.forPath(sparkSession=self.spark, path='./warehouse/silver.db/divvy_bikes_status')
        deltaTableSilver.alias('as_is').merge(df.alias('as_now'), 'as_is.bike_id = as_now.bike_id'
                                              ).whenMatchedUpdate(
            set={
                'vehicle_type_id': 'as_now.vehicle_type_id',
                'lat': 'as_now.lat',
                'lon': 'as_now.lon',
                'current_range_meters': 'as_now.current_range_meters',
                'rental_uris': 'as_now.rental_uris',
                'is_reserved': 'as_now.is_reserved',
                'is_disabled': 'as_now.is_disabled',
                'last_updated': 'as_now.last_updated',
                'last_updated_ts': 'as_now.last_updated_ts'}
        ).whenNotMatchedInsert(
            values={
                'bike_id': 'as_now.bike_id',
                'vehicle_type_id': 'as_now.vehicle_type_id',
                'lat': 'as_now.lat',
                'lon': 'as_now.lon',
                'current_range_meters': 'as_now.current_range_meters',
                'rental_uris': 'as_now.rental_uris',
                'is_reserved': 'as_now.is_reserved',
                'is_disabled': 'as_now.is_disabled',
                'last_updated': 'as_now.last_updated',
                'last_updated_ts': 'as_now.last_updated_ts'}
        ).execute()
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_bikes_status_now | OK', end=True)

    def silver_station_information(self):
        # --------------------------------------------------------------------------------------------------------------
        print(f"┌{'─' * 118}┐")
        print(f"│{' ' * 24}                                                                             {' ' * 17}│")
        print(f"│{' ' * 24}  █████████  █████ █████       █████   █████ ██████████ ███████████          {' ' * 17}│")
        print(f"│{' ' * 24} ███░░░░░███░░███ ░░███       ░░███   ░░███ ░░███░░░░░█░░███░░░░░███         {' ' * 17}│")
        print(f"│{' ' * 24}░███    ░░░  ░███  ░███        ░███    ░███  ░███  █ ░  ░███    ░███         {' ' * 17}│")
        print(f"│{' ' * 24}░░█████████  ░███  ░███        ░███    ░███  ░██████    ░██████████          {' ' * 17}│")
        print(f"│{' ' * 24} ░░░░░░░░███ ░███  ░███        ░░███   ███   ░███░░█    ░███░░░░░███         {' ' * 17}│")
        print(f"│{' ' * 24} ███    ░███ ░███  ░███      █  ░░░█████░    ░███ ░   █ ░███    ░███         {' ' * 17}│")
        print(f"│{' ' * 24}░░█████████  █████ ███████████    ░░███      ██████████ █████   █████ BIKE STATUS{' ' *13}│")
        print(f"│{' ' * 24} ░░░░░░░░░  ░░░░░ ░░░░░░░░░░░      ░░░      ░░░░░░░░░░ ░░░░░   ░░░░░         {' ' * 17}│")
        print(f"│{' ' * 24}                                                                             {' ' * 17}│")
        print(f"└{'─' * 118}┘")
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)
        df = self.spark.read.format('json').load(bronze_path_raw_data+'/station_information/')\
                 .select(explode('data.stations').alias('stations'), 'last_updated')
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA', start=True)
        df = df.select(col('stations.station_id').cast(StringType()).alias('station_id'),
                       col('stations.name').cast(StringType()).alias('name'),
                       col('stations.short_name').cast(StringType()).alias('short_name'),
                       col('stations.lat').cast(DoubleType()).alias('lat'),
                       col('stations.lon').cast(DoubleType()).alias('lon'),
                       col('stations.rental_uris').cast(StringType()).alias('rental_uris'),
                       col('last_updated').cast(LongType()).alias('last_updated'))\
               .withColumn('last_updated_ts', col('last_updated').cast(TimestampType()))
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_station_information', start=True)
        lit_date = datetime.strptime(Now().now(), "%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d')
        df.withColumn('date_ref_carga', lit(lit_date).cast(DateType()))\
          .write.format('delta')\
          .mode('overwrite')\
          .option("overwriteSchema", "True")\
          .partitionBy('date_ref_carga')\
          .saveAsTable('silver.divvy_station_information')
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_station_information | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_station_information_now', start=True)
        deltaTableSilver = DeltaTable.forPath(sparkSession=self.spark,
                                              path='./warehouse/silver.db/divvy_station_information_now')
        deltaTableSilver.alias('as_is').merge(df.alias('as_now'), 'as_is.station_id = as_now.station_id'
                                              ).whenMatchedUpdate(
            set={
                'name': 'as_now.name',
                'lat': 'as_now.lat',
                'lon': 'as_now.lon',
                'short_name': 'as_now.short_name',
                'rental_uris': 'as_now.rental_uris',
                'last_updated': 'as_now.last_updated'}
        ).whenNotMatchedInsert(
            values={
                'station_id': 'as_now.station_id',
                'name': 'as_now.name',
                'lat': 'as_now.lat',
                'lon': 'as_now.lon',
                'short_name': 'as_now.short_name',
                'rental_uris': 'as_now.rental_uris',
                'last_updated': 'as_now.last_updated'}
        ).execute()
        self.log_message(show=self._SHOW_LOG,
                         message='SAVING DATA | silver.divvy_station_information_now | OK', end=True)