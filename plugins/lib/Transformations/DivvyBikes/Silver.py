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

        print(f"┌{'─' * 118}┐")
        print(f"│{' ' * 24}                                                                             {' ' * 17}│")
        print(f"│{' ' * 24}  █████████  █████ █████       █████   █████ ██████████ ███████████          {' ' * 17}│")
        print(f"│{' ' * 24} ███░░░░░███░░███ ░░███       ░░███   ░░███ ░░███░░░░░█░░███░░░░░███         {' ' * 17}│")
        print(f"│{' ' * 24}░███    ░░░  ░███  ░███        ░███    ░███  ░███  █ ░  ░███    ░███         {' ' * 17}│")
        print(f"│{' ' * 24}░░█████████  ░███  ░███        ░███    ░███  ░██████    ░██████████          {' ' * 17}│")
        print(f"│{' ' * 24} ░░░░░░░░███ ░███  ░███        ░░███   ███   ░███░░█    ░███░░░░░███         {' ' * 17}│")
        print(f"│{' ' * 24} ███    ░███ ░███  ░███      █  ░░░█████░    ░███ ░   █ ░███    ░███         {' ' * 17}│")
        print(f"│{' ' * 24}░░█████████  █████ ███████████    ░░███      ██████████ █████   █████        {' ' * 17}│")
        print(f"│{' ' * 24} ░░░░░░░░░  ░░░░░ ░░░░░░░░░░░      ░░░      ░░░░░░░░░░ ░░░░░   ░░░░░         {' ' * 17}│")
        print(f"│{' ' * 24}                                                                             {' ' * 17}│")
        print(f"└{'─' * 118}┘")

    def silver_bike_status(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)
        df = self.spark.read.format('json').load(bronze_path_raw_data+'/free_bike_status/')\
                 .select(explode('data.bikes').alias('bikes'), 'last_updated')
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

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

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_bikes_status', start=True)
        df.write.format('delta')\
          .insertInto('silver.divvy_bikes_status')
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_bikes_status | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        self.spark.stop()

    def silver_station_information(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)
        df = self.spark.read.format('json').load(bronze_path_raw_data+'/station_information/')\
                 .select(explode('data.stations').alias('stations'), 'last_updated')
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

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

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_station_information', start=True)
        df.write.format('delta')\
          .insertInto('silver.divvy_station_information')
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_station_information | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        self.spark.stop()

    def silver_station_status(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)
        df = self.spark.read.format('json').load(bronze_path_raw_data+'/station_status/')\
                 .select(explode('data.stations').alias('stations'), 'last_updated')
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA', start=True)
        df = df.select(col('stations.num_bikes_disabled').cast(LongType()).alias('num_bikes_disabled'),
                       col('stations.num_docks_disabled').cast(LongType()).alias('num_docks_disabled'),
                       col('stations.is_returning').cast(LongType()).alias('is_returning'),
                       col('stations.is_renting').cast(LongType()).alias('is_renting'),
                       col('stations.vehicle_types_available').cast(StringType()).alias('vehicle_types_available'),
                       col('stations.num_ebikes_available').cast(LongType()).alias('num_ebikes_available'),
                       col('stations.is_installed').cast(LongType()).alias('is_installed'),
                       col('stations.last_reported').cast(LongType()).alias('last_reported'),
                       col('stations.num_scooters_unavailable').cast(LongType()).alias('num_scooters_unavailable'),
                       col('stations.num_docks_available').cast(LongType()).alias('num_docks_available'),
                       col('stations.num_bikes_available').cast(LongType()).alias('num_bikes_available'),
                       col('stations.station_id').cast(StringType()).alias('station_id'),
                       col('stations.num_scooters_available').cast(LongType()).alias('num_scooters_available'),
                       col('last_updated').cast(LongType()).alias('last_updated'),
                       )\
               .withColumn('last_updated_ts', col('last_updated').cast(TimestampType())) \
               .withColumn('last_reported_ts', col('last_reported').cast(TimestampType()))
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_station_status', start=True)
        df.write.format('delta')\
          .insertInto('silver.divvy_station_status')
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_station_status | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------

        self.spark.stop()

    def silver_system_pricing_plan(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)
        df = self.spark.read.format('json').load(bronze_path_raw_data+'/system_pricing_plan/')\
                 .select(explode('data.plans').alias('plans'), 'last_updated')
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA', start=True)
        df = df.select(col('plans.currency').cast(StringType()).alias('currency'),
                       col('plans.description').cast(StringType()).alias('description'),
                       col('plans.name').cast(StringType()).alias('name'),
                       col('plans.price').cast(DoubleType()).alias('price'),
                       col('plans.plan_id').cast(StringType()).alias('plan_id'),
                       col('plans.is_taxable').cast(BooleanType()).alias('is_taxable'),
                       col('plans.per_min_pricing').cast(StringType()).alias('per_min_pricing'),
                       col('last_updated').cast(LongType()).alias('last_updated'),
                       )
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_system_pricing_plan', start=True)
        df.write.format('delta')\
          .insertInto('silver.divvy_system_pricing_plan')
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_system_pricing_plan | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        self.spark.stop()

    def silver_vehicle_types(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)
        df = self.spark.read.format('json').load(bronze_path_raw_data+'/vehicle_types/')\
                 .select(explode('data.vehicle_types').alias('vehicle_types'), 'last_updated')
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA', start=True)
        df = df.select(col('vehicle_types.form_factor').cast(StringType()).alias('form_factor'),
                       col('vehicle_types.propulsion_type').cast(StringType()).alias('propulsion_type'),
                       col('vehicle_types.vehicle_type_id').cast(StringType()).alias('vehicle_type_id'),
                       col('last_updated').cast(LongType()).alias('last_updated'),
                       )
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_vehicle_types', start=True)
        df.write.format('delta')\
          .insertInto('silver.divvy_vehicle_types')
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_vehicle_types | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        self.spark.stop()
