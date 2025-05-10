from lib.utils.Now import Now
from lib.Spark.GetSpark import DeltaSpark
from pyspark.sql.functions import explode, col, max
from lib.utils.DivvyBikes.LogProcess import LogProcess
from pyspark.sql.types import DoubleType, StringType, BooleanType, TimestampType, LongType


class SilverRevised(Now):

    _SHOW_LOG = True

    _START_TIME = Now().now_datetime()

    def __init__(self):

        self.spark = DeltaSpark().initialize()

        print(f"┌{'─' * 118}┐")
        print(f"│{' ' * 24}                                                                     {' ' * 25}│")
        print(f"│{' ' * 24}  █████████  █████ █████       █████   █████ ██████████ ███████████  {' ' * 25}│")
        print(f"│{' ' * 24} ███░░░░░███░░███ ░░███       ░░███   ░░███ ░░███░░░░░█░░███░░░░░███ {' ' * 25}│")
        print(f"│{' ' * 24}░███    ░░░  ░███  ░███        ░███    ░███  ░███  █ ░  ░███    ░███ {' ' * 25}│")
        print(f"│{' ' * 24}░░█████████  ░███  ░███        ░███    ░███  ░██████    ░██████████  {' ' * 25}│")
        print(f"│{' ' * 24} ░░░░░░░░███ ░███  ░███        ░░███   ███   ░███░░█    ░███░░░░░███ {' ' * 25}│")
        print(f"│{' ' * 24} ███    ░███ ░███  ░███      █  ░░░█████░    ░███ ░   █ ░███    ░███ {' ' * 25}│")
        print(f"│{' ' * 24}░░█████████  █████ ███████████    ░░███      ██████████ █████   █████{' ' * 25}│")
        print(f"│{' ' * 24} ░░░░░░░░░  ░░░░░ ░░░░░░░░░░░      ░░░      ░░░░░░░░░░ ░░░░░   ░░░░░ {' ' * 25}│")
        print(f"│{' ' * 24}                                                                     {' ' * 25}│")
        print(f"└{'─' * 118}┘")
        print(self._START_TIME)

    def silver_bike_status(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)
        df = (
              self.spark.read.format("delta")
                             .load('warehouse/bronze.db/divvy_bikes')
                             .filter(col('type') == 'free_bike_status')
                             .orderBy('last_updated_ts', ascending=False)
              )
        df = df.filter(col('last_updated_ts') == df.select(max('last_updated_ts')).collect()[0][0])
        df = df.select(explode('data.bikes').alias('bikes'), 'last_updated').distinct()

        assert df.count() > 1, 'No Data'

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
        (df.write.format('delta')
          .mode('append')
          .option('overwriteSchema', 'True')
          .save('./warehouse/silver.db/divvy_bikes_status')
        )
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_bikes_status | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        # LogProcess(spark=self.spark,
        #            table_name='divvy_bikes_status',
        #            database='silver',
        #            rows_inserted=df.count(),
        #            start_time=self._START_TIME
        #            ).process()
        self.spark.stop()

    def silver_station_information(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)

        df = (
              self.spark.read.format("delta")
                             .load('warehouse/bronze.db/divvy_bikes')
                             .filter(col('type') == 'station_information')
                             .orderBy('last_updated_ts', ascending=False)
              )
        df = df.filter(col('last_updated_ts') == df.select(max('last_updated_ts')).collect()[0][0])
        df = df.select(explode('data.stations').alias('stations'), 'last_updated').distinct()

        assert df.count() > 1, 'No Data'

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
        (df.write.format('delta')
          .mode('append')
          .option('overwriteSchema', 'True')
          .save('./warehouse/silver.db/divvy_station_information')
        )
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_station_information | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        # LogProcess(spark=self.spark,
        #            table_name='divvy_station_information',
        #            database='silver',
        #            rows_inserted=df.count(),
        #            start_time=self._START_TIME
        #            ).process()
        self.spark.stop()

    def silver_station_status(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)

        df = (
              self.spark.read.format("delta")
                             .load('warehouse/bronze.db/divvy_bikes')
                             .filter(col('type') == 'station_status')
                             .orderBy('last_updated_ts', ascending=False)
              )
        df = df.filter(col('last_updated_ts') == df.select(max('last_updated_ts')).collect()[0][0])
        df = df.select(explode('data.stations').alias('stations'), 'last_updated').distinct()

        assert df.count() > 1, 'No Data'

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
        (df.write.format('delta')
          .mode('append')
          .option('overwriteSchema', 'True')
          .save('./warehouse/silver.db/divvy_station_status')
        )
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_station_status | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        # LogProcess(spark=self.spark,
        #            table_name='divvy_station_status',
        #            database='silver',
        #            rows_inserted=df.count(),
        #            start_time=self._START_TIME
        #            ).process()
        self.spark.stop()

    def silver_system_pricing_plan(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)

        df = (
              self.spark.read.format("delta")
                             .load('warehouse/bronze.db/divvy_bikes')
                             .filter(col('type') == 'system_pricing_plan')
                             .orderBy('last_updated_ts', ascending=False)
              )
        df = df.filter(col('last_updated_ts') == df.select(max('last_updated_ts')).collect()[0][0])
        df = df.select(explode('data.plans').alias('plans'), 'last_updated').distinct()

        assert df.count() > 1, 'No Data'

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
        (df.write.format('delta')
          .mode('append')
          .option('overwriteSchema', 'True')
          .save('./warehouse/silver.db/divvy_system_pricing_plan')
        )
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_system_pricing_plan | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        # LogProcess(spark=self.spark,
        #            table_name='divvy_system_pricing_plan',
        #            database='silver',
        #            rows_inserted=df.count(),
        #            start_time=self._START_TIME
        #            ).process()
        self.spark.stop()

    def silver_vehicle_types(self):

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)

        df = (
              self.spark.read.format("delta")
                             .load('warehouse/bronze.db/divvy_bikes')
                             .filter(col('type') == 'vehicle_types')
                             .orderBy('last_updated_ts', ascending=False)
              )
        df = df.filter(col('last_updated_ts') == df.select(max('last_updated_ts')).collect()[0][0])
        df = df.select(explode('data.vehicle_types').alias('vehicle_types'), 'last_updated').distinct()

        assert df.count() > 1, 'No Data'

        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA', start=True)
        df = df.select(col('vehicle_types.form_factor').cast(StringType()).alias('form_factor'),
                       col('vehicle_types.propulsion_type').cast(StringType()).alias('propulsion_type'),
                       col('vehicle_types.vehicle_type_id').cast(StringType()).alias('vehicle_type_id'),
                       col('last_updated').cast(LongType()).alias('last_updated'),
                       )

        assert df.count() > 1, 'No Data'

        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING DATA | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_vehicle_types', start=True)
        print(df.printSchema())
        (df.write.format('delta')
          .mode('append')
          .option('overwriteSchema', 'True')
          .save('./warehouse/silver.db/divvy_vehicle_types')
        )
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | silver.divvy_vehicle_types | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        # LogProcess(spark=self.spark,
        #            table_name='divvy_vehicle_types',
        #            database='silver',
        #            rows_inserted=df.count(),
        #            start_time=self._START_TIME
        #            ).process()
        self.spark.stop()
