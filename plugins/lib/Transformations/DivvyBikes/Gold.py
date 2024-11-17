from lib.utils.Now import Now
from delta.tables import DeltaTable
from pyspark.sql.functions import col
from lib.Spark.GetSpark import DeltaSpark
from lib.utils.DivvyBikes.LogProcess import LogProcess


class Gold(Now):

    _SHOW_LOG = True
    _START_TIME = Now().now_datetime()

    def __init__(self):

        self.spark = DeltaSpark().initialize()

        print(f"┌{'─' * 118}┐")
        print(f"│{' ' * 32}                                                    {' ' * 34}│")
        print(f"│{' ' * 32}                                                    {' ' * 34}│")
        print(f"│{' ' * 32}   █████████     ███████    █████       ██████████  {' ' * 34}│")
        print(f"│{' ' * 32}  ███░░░░░███  ███░░░░░███ ░░███       ░░███░░░░███ {' ' * 34}│")
        print(f"│{' ' * 32} ███     ░░░  ███     ░░███ ░███        ░███   ░░███{' ' * 34}│")
        print(f"│{' ' * 32}░███         ░███      ░███ ░███        ░███    ░███{' ' * 34}│")
        print(f"│{' ' * 32}░███    █████░███      ░███ ░███        ░███    ░███{' ' * 34}│")
        print(f"│{' ' * 32}░░███  ░░███ ░░███     ███  ░███      █ ░███    ███ {' ' * 34}│")
        print(f"│{' ' * 32} ░░█████████  ░░░███████░   ███████████ ██████████  {' ' * 34}│")
        print(f"│{' ' * 32}  ░░░░░░░░░     ░░░░░░░    ░░░░░░░░░░░ ░░░░░░░░░░   {' ' * 34}│")
        print(f"│{' ' * 32}                                                    {' ' * 34}│")
        print(f"│{' ' * 32}                                                    {' ' * 34}│")
        print(f"└{'─' * 118}┘")
        print(self._START_TIME)

    def gold_bike_status(self):
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING SILVER DATA | silver.divvy_bikes_status', start=True)
        df = (self.spark.read.format('delta').load('warehouse/silver.db/divvy_bikes_status')
                  .withColumn('last_updated_ts', col('last_updated').cast('TIMESTAMP')).alias('as_is')
                  .join(
                    self.spark.read.format('delta').load('warehouse/silver.db/divvy_bikes_status')
                        .withColumn('last_updated_ts', col('last_updated').cast('TIMESTAMP'))
                        .agg({"last_updated_ts": "max"})
                        .withColumnRenamed('max(last_updated_ts)', 'last_updated_ts')
                        .alias('as'), on=['last_updated_ts'], how='inner'))
        
        self.log_message(show=self._SHOW_LOG, message='READING SILVER DATA | silver.divvy_bikes_status | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | gold.divvy_bikes_status', start=True)
        delta_table_silver = DeltaTable.forPath(sparkSession=self.spark,
                                                path='./warehouse/gold.db/divvy_bikes_status')

        (delta_table_silver
         .alias('as_is')
         .merge(df.distinct().alias('as_now'), 'as_is.bike_id = as_now.bike_id')
         .whenMatchedUpdate(
            set={
                'vehicle_type_id': 'as_now.vehicle_type_id',
                'lat': 'as_now.lat',
                'lon': 'as_now.lon',
                'current_range_meters': 'as_now.current_range_meters',
                'rental_uris': 'as_now.rental_uris',
                'is_reserved': 'as_now.is_reserved',
                'is_disabled': 'as_now.is_disabled',
                'last_updated': 'as_now.last_updated'})
         .whenNotMatchedInsert(
            values={
                'bike_id': 'as_now.bike_id',
                'vehicle_type_id': 'as_now.vehicle_type_id',
                'lat': 'as_now.lat',
                'lon': 'as_now.lon',
                'current_range_meters': 'as_now.current_range_meters',
                'rental_uris': 'as_now.rental_uris',
                'is_reserved': 'as_now.is_reserved',
                'is_disabled': 'as_now.is_disabled',
                'last_updated': 'as_now.last_updated'})
         .execute())

        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | gold.divvy_bikes_status_now | OK', end=True)

        # LogProcess(spark=self.spark,
        #            table_name='divvy_bikes_status',
        #            database='gold',
        #            rows_inserted=df.count(),
        #            start_time=self._START_TIME
        #            ).process()

        self.spark.stop()

    def gold_station_information(self):
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING SILVER DATA | silver.divvy_station_information',
                         start=True)
        df = (self.spark.read.format('delta').load('warehouse/silver.db/divvy_station_information')
                  .withColumn('last_updated_ts', col('last_updated').cast('TIMESTAMP')).alias('as_is')
                  .join(
                    self.spark.read.format('delta').load('warehouse/silver.db/divvy_station_information')
                        .withColumn('last_updated_ts', col('last_updated').cast('TIMESTAMP'))
                        .agg({"last_updated_ts": "max"})
                        .withColumnRenamed('max(last_updated_ts)', 'last_updated_ts')
                        .alias('as_now'), on=['last_updated_ts'], how='inner'))
        
        self.log_message(show=self._SHOW_LOG, message='READING SILVER DATA | silver.divvy_station_information | OK',
                         end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | gold.divvy_station_information', start=True)
        delta_table_silver = DeltaTable.forPath(sparkSession=self.spark,
                                                path='./warehouse/gold.db/divvy_station_information')

        (delta_table_silver
         .alias('as_is')
         .merge(df.alias('as_now'), 'as_is.station_id = as_now.station_id')
         .whenMatchedUpdate(
            set={
                'name': 'as_now.name',
                'lat': 'as_now.lat',
                'lon': 'as_now.lon',
                'short_name': 'as_now.short_name',
                'rental_uris': 'as_now.rental_uris',
                'last_updated': 'as_now.last_updated'})
         .whenNotMatchedInsert(
            values={
                'station_id': 'as_now.station_id',
                'name': 'as_now.name',
                'lat': 'as_now.lat',
                'lon': 'as_now.lon',
                'short_name': 'as_now.short_name',
                'rental_uris': 'as_now.rental_uris',
                'last_updated': 'as_now.last_updated'})
         .execute())

        self.log_message(show=self._SHOW_LOG,
                         message='SAVING DATA | gold.divvy_station_information_now | OK', end=True)

        # LogProcess(spark=self.spark,
        #            table_name='divvy_station_information_now',
        #            database='gold',
        #            rows_inserted=df.count(),
        #            start_time=self._START_TIME
        #            ).process()

        self.spark.stop()

    def gold_station_status(self):
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING SILVER DATA | silver.divvy_station_status', start=True)
        df = (self.spark.read.format('delta').load('warehouse/silver.db/divvy_station_status')
                  .withColumn('last_updated_ts', col('last_updated').cast('TIMESTAMP')).alias('as_is')
                  .join(
                    self.spark.read.format('delta').load('warehouse/silver.db/divvy_station_status')
                        .withColumn('last_updated_ts', col('last_updated').cast('TIMESTAMP'))
                        .agg({"last_updated_ts": "max"})
                        .withColumnRenamed('max(last_updated_ts)', 'last_updated_ts')
                        .alias('as_now'), on=['last_updated_ts'], how='inner'))

        
        self.log_message(show=self._SHOW_LOG, message='READING SILVER DATA | silver.divvy_station_status | OK',
                         end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | gold.divvy_station_status', start=True)
        delta_table_silver = DeltaTable.forPath(sparkSession=self.spark,
                                                path='./warehouse/gold.db/divvy_station_status')

        (delta_table_silver
         .alias('as_is')
         .merge(df.distinct().alias('as_now'), 'as_is.station_id = as_now.station_id')
         .whenMatchedUpdate(
            set={
                'num_bikes_disabled': 'as_now.num_bikes_disabled',
                'num_docks_disabled': 'as_now.num_docks_disabled',
                'is_returning': 'as_now.is_returning',
                'is_renting': 'as_now.is_renting',
                'vehicle_types_available': 'as_now.vehicle_types_available',
                'num_ebikes_available': 'as_now.num_ebikes_available',
                'is_installed': 'as_now.is_installed',
                'last_reported': 'as_now.last_reported',
                'num_scooters_unavailable': 'as_now.num_scooters_unavailable',
                'num_docks_available': 'as_now.num_docks_available',
                'num_bikes_available': 'as_now.num_bikes_available',
                'num_scooters_available': 'as_now.num_scooters_available',
                'last_updated': 'as_now.last_updated',
                'last_updated_ts': 'as_now.last_updated_ts'})
         .whenNotMatchedInsert(
            values={
                'num_bikes_disabled': 'as_now.num_bikes_disabled',
                'num_docks_disabled': 'as_now.num_docks_disabled',
                'is_returning': 'as_now.is_returning',
                'is_renting': 'as_now.is_renting',
                'vehicle_types_available': 'as_now.vehicle_types_available',
                'num_ebikes_available': 'as_now.num_ebikes_available',
                'is_installed': 'as_now.is_installed',
                'last_reported': 'as_now.last_reported',
                'num_scooters_unavailable': 'as_now.num_scooters_unavailable',
                'num_docks_available': 'as_now.num_docks_available',
                'num_bikes_available': 'as_now.num_bikes_available',
                'station_id': 'as_now.station_id',
                'num_scooters_available': 'as_now.num_scooters_available',
                'last_updated': 'as_now.last_updated',
                'last_updated_ts': 'as_now.last_updated_ts'})
         .execute())

        self.log_message(show=self._SHOW_LOG,
                         message='SAVING DATA | gold.station_status_now | OK', end=True)

        # LogProcess(spark=self.spark,
        #            table_name='station_status_now',
        #            database='gold',
        #            rows_inserted=df.count(),
        #            start_time=self._START_TIME
        #            ).process()

        self.spark.stop()

    def gold_system_pricing_plan(self):
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING SILVER DATA | silver.divvy_system_pricing_plan',
                         start=True)
        df = (self.spark.read.format('delta').load('warehouse/silver.db/divvy_system_pricing_plan')
                  .withColumn('last_updated_ts', col('last_updated').cast('TIMESTAMP')).alias('as_is')
                  .join(
                    self.spark.read.format('delta').load('warehouse/silver.db/divvy_system_pricing_plan')
                        .withColumn('last_updated_ts', col('last_updated').cast('TIMESTAMP'))
                        .agg({"last_updated_ts": "max"})
                        .withColumnRenamed('max(last_updated_ts)', 'last_updated_ts')
                        .alias('as_now'), on=['last_updated_ts'], how='inner'))
        
        self.log_message(show=self._SHOW_LOG, message='READING SILVER DATA | silver.divvy_system_pricing_plan | OK',
                         end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | gold.divvy_system_pricing_plan', start=True)
        delta_table_silver = DeltaTable.forPath(sparkSession=self.spark,
                                                path='./warehouse/gold.db/divvy_system_pricing_plan')

        (delta_table_silver
         .alias('as_is')
         .merge(df.distinct().alias('as_now'), 'as_is.plan_id = as_now.plan_id')
         .whenMatchedUpdate(
            set={
                'currency': 'as_now.currency',
                'description': 'as_now.description',
                'name': 'as_now.name',
                'price': 'as_now.price',
                'is_taxable': 'as_now.is_taxable',
                'per_min_pricing': 'as_now.per_min_pricing',
                'last_updated': 'as_now.last_updated'})
         .whenNotMatchedInsert(
            values={
                'currency': 'as_now.currency',
                'description': 'as_now.description',
                'name': 'as_now.name',
                'price': 'as_now.price',
                'plan_id': 'as_now.plan_id',
                'is_taxable': 'as_now.is_taxable',
                'per_min_pricing': 'as_now.per_min_pricing',
                'last_updated': 'as_now.last_updated'})
         .execute())

        self.log_message(show=self._SHOW_LOG,
                         message='SAVING DATA | gold.divvy_system_pricing_plan_now | OK', end=True)

        # LogProcess(spark=self.spark,
        #            table_name='divvy_system_pricing_plan_now',
        #            database='gold',
        #            rows_inserted=df.count(),
        #            start_time=self._START_TIME
        #            ).process()

        self.spark.stop()

    def gold_vehicle_types(self):
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING SILVER DATA | silver.divvy_vehicle_types', start=True)
        df = (self.spark.read.format('delta').load('warehouse/silver.db/divvy_vehicle_types')
                  .withColumn('last_updated_ts', col('last_updated').cast('TIMESTAMP')).alias('as_is')
                  .join(
                    self.spark.read.format('delta').load('warehouse/silver.db/divvy_vehicle_types')
                        .withColumn('last_updated_ts', col('last_updated').cast('TIMESTAMP'))
                        .agg({"last_updated_ts": "max"})
                        .withColumnRenamed('max(last_updated_ts)', 'last_updated_ts')
                        .alias('as_now'), on=['last_updated_ts'], how='inner'))
        
        self.log_message(show=self._SHOW_LOG, message='READING SILVER DATA | silver.divvy_vehicle_types | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING DATA | gold.divvy_vehicle_types', start=True)
        delta_table_silver = DeltaTable.forPath(sparkSession=self.spark,
                                                path='./warehouse/silver.db/divvy_vehicle_types')

        (delta_table_silver
         .alias('as_is')
         .merge(df.distinct().alias('as_now'), 'as_is.vehicle_type_id = as_now.vehicle_type_id')
         .whenMatchedUpdate(
            set={
                'form_factor': 'as_now.form_factor',
                'propulsion_type': 'as_now.propulsion_type',
                'last_updated': 'as_now.last_updated'})
         .whenNotMatchedInsert(
            values={
                'form_factor': 'as_now.form_factor',
                'propulsion_type': 'as_now.propulsion_type',
                'vehicle_type_id': 'as_now.vehicle_type_id',
                'last_updated': 'as_now.last_updated'})
         .execute())

        self.log_message(show=self._SHOW_LOG,
                         message='SAVING DATA | gold.divvy_vehicle_types_now | OK', end=True)

        # LogProcess(spark=self.spark,
        #            table_name='divvy_vehicle_types_now',
        #            database='gold',
        #            rows_inserted=df.count(),
        #            start_time=self._START_TIME
        #            ).process()

        self.spark.stop()
