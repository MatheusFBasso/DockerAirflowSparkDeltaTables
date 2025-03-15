from lib.Spark.GetSpark import DeltaSpark
from lib.utils.Now import Now
from delta.tables import DeltaTable
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, TimestampType,
                               BooleanType, DateType, LongType, IntegerType)


class SetDeltaTables(Now):

    _SHOW = True

    ####################################################################################################################
    def __init__(self, silver: bool = True, gold: bool = True, log_table: bool = True):
        self._silver = silver
        self._gold = gold
        self._log_table = log_table
        self._spark = DeltaSpark().initialize()

        bool_dict = {'silver': self._silver, 'log_table': self._log_table, 'gold': self._gold}

        # --------------------------------------------------------------------------------------------------------------
        print(f"┌{'─'*118}┐")
        print(f"│{' ' * 22}                                                                          {' ' * 22}│")
        print(f"│{' ' * 22} ██████████   ██████████ █████       ███████████   █████████              {' ' * 22}│")
        print(f"│{' ' * 22}░░███░░░░███ ░░███░░░░░█░░███       ░█░░░███░░░█  ███░░░░░███             {' ' * 22}│")
        print(f"│{' ' * 22} ░███   ░░███ ░███  █ ░  ░███       ░   ░███  ░  ░███    ░███             {' ' * 22}│")
        print(f"│{' ' * 22} ░███    ░███ ░██████    ░███           ░███     ░███████████             {' ' * 22}│")
        print(f"│{' ' * 22} ░███    ░███ ░███░░█    ░███           ░███     ░███░░░░░███             {' ' * 22}│")
        print(f"│{' ' * 22} ░███    ███  ░███ ░   █ ░███      █    ░███     ░███    ░███             {' ' * 22}│")
        print(f"│{' ' * 22} ██████████   ██████████ ███████████    █████    █████   █████            {' ' * 22}│")
        print(f"│{' ' * 22}░░░░░░░░░░   ░░░░░░░░░░ ░░░░░░░░░░░    ░░░░░    ░░░░░   ░░░░░             {' ' * 22}│")
        print(f"│{' ' * 22} ███████████   █████████   ███████████  █████       ██████████  █████████ {' ' * 22}│")
        print(f"│{' ' * 22}░█░░░███░░░█  ███░░░░░███ ░░███░░░░░███░░███       ░░███░░░░░█ ███░░░░░███{' ' * 22}│")
        print(f"│{' ' * 22}░   ░███  ░  ░███    ░███  ░███    ░███ ░███        ░███  █ ░ ░███    ░░░ {' ' * 22}│")
        print(f"│{' ' * 22}    ░███     ░███████████  ░██████████  ░███        ░██████   ░░█████████ {' ' * 22}│")
        print(f"│{' ' * 22}    ░███     ░███░░░░░███  ░███░░░░░███ ░███        ░███░░█    ░░░░░░░░███{' ' * 22}│")
        print(f"│{' ' * 22}    ░███     ░███    ░███  ░███    ░███ ░███      █ ░███ ░   █ ███    ░███{' ' * 22}│")
        print(f"│{' ' * 22}    █████    █████   █████ ███████████  ███████████ ██████████░░█████████ {' ' * 22}│")
        print(f"│{' ' * 22}   ░░░░░    ░░░░░   ░░░░░ ░░░░░░░░░░░  ░░░░░░░░░░░ ░░░░░░░░░░  ░░░░░░░░░  {' ' * 22}│")
        print(f"│{' ' * 22}                                                                          {' ' * 22}│")
        print(f"└{'─'*118}┘")
        # --------------------------------------------------------------------------------------------------------------

        bool_check = 0
        for _name, _bool in bool_dict.items():
            if _bool:
                print(f'{_name.capitalize()} WILL BE CREATED IF NOT EXIST')
                bool_check += 1

        if bool_check > 0:
            print("_" * 120)

        else:
            print("NO TABLE SET TO BE CREATED!")
            print("_" * 120)

    ####################################################################################################################
    def create_delta_tables(self):

        # --------------------------------------------------------------------------------------------------------------
        if self._silver:
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
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"STARTING SILVER DELTA TABLE CREATION", start=True, end=True)
            # ----------------------------------------------------------------------------------------------------------
            # self.log_message(show=self._SHOW, message=f"Creating Database if not exists", start=True)
            # self._spark.sql(f"CREATE DATABASE IF NOT EXISTS silver")
            # self.log_message(show=self._SHOW, message=f"Creating Database if not exists | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/silver.db/divvy_bikes_status'):
                self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_bikes_status", start=True)
                schema = StructType([StructField('bike_id',              StringType(), False),
                                     StructField('vehicle_type_id',      StringType(), True),
                                     StructField('lat',                  DoubleType(), True),
                                     StructField('lon',                  DoubleType(), True),
                                     StructField('current_range_meters', DoubleType(), True),
                                     StructField('rental_uris',          StringType(), True),
                                     StructField('is_reserved',          BooleanType(), True),
                                     StructField('is_disabled',          BooleanType(), True),
                                     StructField('last_updated',         LongType(), True),
                                     StructField('last_updated_ts',      TimestampType(), True)])

                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta')\
                              .mode('overwrite')\
                              .option("overwriteSchema", "True") \
                              .save(f'./warehouse/silver.db/divvy_bikes_status')
                self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_bikes_status | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/silver.db/divvy_station_information'):
                self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_station_information", start=True)
                schema = StructType([StructField('station_id',           StringType(), False),
                                     StructField('name',                 StringType(), True),
                                     StructField('short_name',           StringType(), True),
                                     StructField('lat',                  DoubleType(), True),
                                     StructField('lon',                  DoubleType(), True),
                                     StructField('rental_uris',          StringType(), True),
                                     StructField('last_updated',         LongType(), True),
                                     StructField('last_updated_ts',      TimestampType(), False)])

                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta')\
                              .mode('overwrite')\
                              .option("overwriteSchema", "True")\
                              .save(f'./warehouse/silver.db/divvy_station_information')
                self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_station_information | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/silver.db/divvy_station_status'):
                self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_station_status", start=True)
                schema = StructType([StructField('num_bikes_disabled',          LongType(), True),
                                     StructField('num_docks_disabled',          LongType(), True),
                                     StructField('is_returning',                LongType(), True),
                                     StructField('is_renting',                  LongType(), True),
                                     StructField('vehicle_types_available',     StringType(), True),
                                     StructField('num_ebikes_available',        LongType(), True),
                                     StructField('is_installed',                LongType(), True),
                                     StructField('last_reported',               LongType(), True),
                                     StructField('num_scooters_unavailable',    LongType(), True),
                                     StructField('num_docks_available',         LongType(), True),
                                     StructField('num_bikes_available',         LongType(), True),
                                     StructField('station_id',                  StringType(), True),
                                     StructField('num_scooters_available',      LongType(), True),
                                     StructField('last_updated',                LongType(), True),
                                     StructField('last_updated_ts',             TimestampType(), True),
                                     StructField('last_reported_ts',            TimestampType(), True)])

                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta')\
                              .mode('overwrite')\
                              .option("overwriteSchema", "True")\
                              .save(f'./warehouse/silver.db/divvy_station_status')
                self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_station_status | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/silver.db/divvy_system_pricing_plan'):
                self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_system_pricing_plan", start=True)
                schema = StructType([StructField('currency',        StringType(), True),
                                     StructField('description',     StringType(), True),
                                     StructField('name',            StringType(), True),
                                     StructField('price',           DoubleType(), True),
                                     StructField('plan_id',         StringType(), True),
                                     StructField('is_taxable',      BooleanType(), True),
                                     StructField('per_min_pricing', StringType(), True),
                                     StructField('last_updated',    LongType(), True)])

                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta')\
                              .mode('overwrite')\
                              .option("overwriteSchema", "True")\
                              .save(f'./warehouse/silver.db/divvy_system_pricing_plan')
                self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_system_pricing_plan | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/silver.db/divvy_vehicle_types'):
                self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_vehicle_types", start=True)
                schema = StructType([StructField('form_factor',     StringType(), True),
                                     StructField('propulsion_type', StringType(), True),
                                     StructField('vehicle_type_id', StringType(), True),
                                     StructField('last_updated',    LongType(), True),])

                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta')\
                              .mode('overwrite')\
                              .option("overwriteSchema", "True")\
                              .save(f'./warehouse/silver.db/divvy_vehicle_types')
                self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_vehicle_types | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------

        if self._gold:
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

            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"STARTING GOLD DELTA TABLE CREATION", start=True, end=True)
            # ----------------------------------------------------------------------------------------------------------
            # self.log_message(show=self._SHOW, message=f"Creating Database if not exists", start=True)
            # self._spark.sql(f"CREATE DATABASE IF NOT EXISTS gold")
            # self.log_message(show=self._SHOW, message=f"Creating Database if not exists | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/gold.db/divvy_bikes_status'):
                self.log_message(show=self._SHOW, message=f"CREATING gold.divvy_bikes_status", start=True)
                schema = StructType([StructField('bike_id', StringType(), False),
                                     StructField('vehicle_type_id', StringType(), True),
                                     StructField('lat', DoubleType(), True),
                                     StructField('lon', DoubleType(), True),
                                     StructField('current_range_meters', DoubleType(), True),
                                     StructField('rental_uris', StringType(), True),
                                     StructField('is_reserved', BooleanType(), True),
                                     StructField('is_disabled', BooleanType(), True),
                                     StructField('last_updated', LongType(), True)])
                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta') \
                    .mode('overwrite') \
                    .option("overwriteSchema", "True") \
                    .save(f'./warehouse/gold.db/divvy_bikes_status')
                self.log_message(show=self._SHOW, message=f"CREATING gold.divvy_bikes_status | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/gold.db/divvy_station_information'):
                self.log_message(show=self._SHOW, message=f"CREATING gold.divvy_station_information", start=True)
                schema = StructType([StructField('station_id',           StringType(), False),
                                     StructField('name',                 StringType(), True),
                                     StructField('short_name',           StringType(), True),
                                     StructField('lat',                  DoubleType(), True),
                                     StructField('lon',                  DoubleType(), True),
                                     StructField('rental_uris',          StringType(), True),
                                     StructField('last_updated',         LongType(), True)])
                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta') \
                    .mode('overwrite') \
                    .option("overwriteSchema", "True") \
                    .save(f'./warehouse/gold.db/divvy_station_information')
                self.log_message(show=self._SHOW, message=f"CREATING gold.divvy_station_information | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/gold.db/divvy_station_status'):
                self.log_message(show=self._SHOW, message=f"CREATING gold.divvy_station_status", start=True)
                schema = StructType([StructField('num_bikes_disabled',          LongType(), True),
                                     StructField('num_docks_disabled',          LongType(), True),
                                     StructField('is_returning',                LongType(), True),
                                     StructField('is_renting',                  LongType(), True),
                                     StructField('vehicle_types_available',     StringType(), True),
                                     StructField('num_ebikes_available',        LongType(), True),
                                     StructField('is_installed',                LongType(), True),
                                     StructField('last_reported',               LongType(), True),
                                     StructField('num_scooters_unavailable',    LongType(), True),
                                     StructField('num_docks_available',         LongType(), True),
                                     StructField('num_bikes_available',         LongType(), True),
                                     StructField('station_id',                  StringType(), True),
                                     StructField('num_scooters_available',      LongType(), True),
                                     StructField('last_updated',                LongType(), True),
                                     StructField('last_updated_ts',             TimestampType(), True),
                                     StructField('last_reported_ts',            TimestampType(), True)])

                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta') \
                    .mode('overwrite') \
                    .option("overwriteSchema", "True") \
                    .save(f'./warehouse/gold.db/divvy_station_status')
                self.log_message(show=self._SHOW, message=f"CREATING gold.divvy_station_status | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/gold.db/divvy_system_pricing_plan'):
                self.log_message(show=self._SHOW, message=f"CREATING gold.divvy_system_pricing_plan", start=True)
                schema = StructType([StructField('currency',        StringType(), True),
                                     StructField('description',     StringType(), True),
                                     StructField('name',            StringType(), True),
                                     StructField('price',           DoubleType(), True),
                                     StructField('plan_id',         StringType(), True),
                                     StructField('is_taxable',      BooleanType(), True),
                                     StructField('per_min_pricing', StringType(), True),
                                     StructField('last_updated',    LongType(), True)])

                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta') \
                    .mode('overwrite') \
                    .option("overwriteSchema", "True") \
                    .save(f'./warehouse/gold.db/divvy_system_pricing_plan')
                self.log_message(show=self._SHOW, message=f"CREATING gold.divvy_system_pricing_plan | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/gold.db/divvy_vehicle_types'):
                self.log_message(show=self._SHOW, message=f"CREATING gold.divvy_vehicle_types", start=True)
                schema = StructType([StructField('form_factor',     StringType(), True),
                                     StructField('propulsion_type', StringType(), True),
                                     StructField('vehicle_type_id', StringType(), True),
                                     StructField('last_updated',    LongType(), True),])

                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta') \
                    .mode('overwrite') \
                    .option("overwriteSchema", "True") \
                    .save(f'./warehouse/gold.db/divvy_vehicle_types')
                self.log_message(show=self._SHOW, message=f"CREATING gold.divvy_vehicle_types | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        if self._log_table:
            print(f"┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐")
            print(f"│                                                                                                                      │")
            print(f"│        █████          ███████      █████████     ███████████   █████████   ███████████  █████       ██████████       │")
            print(f"│       ░░███         ███░░░░░███   ███░░░░░███   ░█░░░███░░░█  ███░░░░░███ ░░███░░░░░███░░███       ░░███░░░░░█       │")
            print(f"│        ░███        ███     ░░███ ███     ░░░    ░   ░███  ░  ░███    ░███  ░███    ░███ ░███        ░███  █ ░        │")
            print(f"│        ░███       ░███      ░███░███                ░███     ░███████████  ░██████████  ░███        ░██████          │")
            print(f"│        ░███       ░███      ░███░███    █████       ░███     ░███░░░░░███  ░███░░░░░███ ░███        ░███░░█          │")
            print(f"│        ░███      █░░███     ███ ░░███  ░░███        ░███     ░███    ░███  ░███    ░███ ░███      █ ░███ ░   █       │")
            print(f"│        ███████████ ░░░███████░   ░░█████████        █████    █████   █████ ███████████  ███████████ ██████████       │")
            print(f"│       ░░░░░░░░░░░    ░░░░░░░      ░░░░░░░░░        ░░░░░    ░░░░░   ░░░░░ ░░░░░░░░░░░  ░░░░░░░░░░░ ░░░░░░░░░░        │")
            print(f"│                                                                                                                      │")
            print(f"└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘")
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"\STARTING LOGTABLE DELTA CREATION", start=True, end=True)
            # self.log_message(show=self._SHOW, message=f"Creating Database if not exists")
            # # ----------------------------------------------------------------------------------------------------------
            # self._spark.sql(f"CREATE DATABASE IF NOT EXISTS info")
            # # ----------------------------------------------------------------------------------------------------------
            # self.log_message(show=self._SHOW, message=f"Creating Database if not exists | OK")

            if not DeltaTable.isDeltaTable(self._spark, './warehouse/info.db/log'):
                self.log_message(show=self._SHOW, message=f"Creating info.log first time", start=True)
                # ------------------------------------------------------------------------------------------------------
                schema = StructType([
                    StructField('id', TimestampType(), False),
                    StructField('db', StringType(), False),
                    StructField('table', StringType(), False),
                    StructField('rows_inserted', IntegerType(), False),
                    StructField('start_time', TimestampType(), False),
                    StructField('end_time', TimestampType(), False),
                ])
                empty_info_df = self._spark.createDataFrame([], schema)
                empty_info_df.write.format('delta') \
                    .mode('overwrite') \
                    .partitionBy('id') \
                    .option("overwriteSchema", "True") \
                    .save(f'./warehouse/info.db/log')
                # ------------------------------------------------------------------------------------------------------
                self.log_message(show=self._SHOW, message=f"Creating info.log first time | OK", end=True)
                self.log_message(show=self._SHOW, message=f"LOGTABLE DELTA CREATED", end=True, start=True)
                # ------------------------------------------------------------------------------------------------------

            else:
                self.log_message(show=self._SHOW, message=f"No need for creating info.log | OK", start=True, end=True)

            self._spark.stop()
