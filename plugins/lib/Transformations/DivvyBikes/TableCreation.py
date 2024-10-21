from lib.Spark.GetSpark import DeltaSpark
from lib.utils.Now import Now
from delta.tables import DeltaTable
from pyspark.sql.types import (StructType, StructField, StringType, DoubleType, TimestampType,
                               BooleanType, DateType, LongType, IntegerType)


class SetDeltaTables(Now):

    _SHOW = True

    ####################################################################################################################
    def __init__(self, silver: bool = True, log_table: bool = True):
        self._silver = silver
        self._log_table = log_table
        self._spark = DeltaSpark().initialize()

        bool_dict = {'silver': self._silver, 'log_table': self._log_table}

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
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists", start=True)
            self._spark.sql(f"CREATE DATABASE IF NOT EXISTS silver")
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists | OK", end=True)
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_bikes_status", start=True)
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/silver.db/divvy_bikes_status'):
                schema = StructType([StructField('bike_id',              StringType(), False),
                                     StructField('vehicle_type_id',      StringType(), True),
                                     StructField('lat',                  DoubleType(), True),
                                     StructField('lon',                  DoubleType(), True),
                                     StructField('current_range_meters', DoubleType(), True),
                                     StructField('rental_uris',          StringType(), True),
                                     StructField('is_reserved',          BooleanType(), True),
                                     StructField('is_disabled',          BooleanType(), True),
                                     StructField('last_updated',         LongType(), True),
                                     StructField('date_ref_carga',       DateType(), True)])

                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta')\
                              .mode('overwrite')\
                              .partitionBy('date_ref_carga')\
                              .option("overwriteSchema", "True")\
                              .saveAsTable('silver.divvy_bikes_status')
            self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_bikes_status", end=True)
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_bikes_status", start=True)
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/silver.db/divvy_bikes_status_now'):
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
                    .saveAsTable('silver.divvy_bikes_status_now')
            self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_bikes_status", end=True)
            # ----------------------------------------------------------------------------------------------------------

            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_station_information_now", start=True)
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/silver.db/divvy_station_information'):
                schema = StructType([StructField('station_id',           StringType(), False),
                                     StructField('name',                 StringType(), True),
                                     StructField('short_name',           StringType(), True),
                                     StructField('lat',                  DoubleType(), True),
                                     StructField('lon',                  DoubleType(), True),
                                     StructField('rental_uris',          StringType(), True),
                                     StructField('last_updated',         LongType(), True),
                                     StructField('date_ref_carga',       DateType(), True)])

                brew_silver_df = self._spark.createDataFrame([], schema)
                brew_silver_df.write.format('delta')\
                              .mode('overwrite')\
                              .partitionBy('date_ref_carga')\
                              .option("overwriteSchema", "True")\
                              .saveAsTable('silver.divvy_station_information')
            self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_station_information", end=True)
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_station_information_now", start=True)
            if not DeltaTable.isDeltaTable(self._spark, './warehouse/silver.db/divvy_station_information_now'):
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
                    .saveAsTable('silver.divvy_station_information_now')
            self.log_message(show=self._SHOW, message=f"CREATING silver.divvy_station_information_now", end=True)
            # ----------------------------------------------------------------------------------------------------------

            self.log_message(show=self._SHOW, message=f"Creating Table brewery is not exists | OK")
            self.log_message(show=self._SHOW, message=f"SILVER DELTA TABLE CREATED", end=True, start=True)
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
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists")
            # ----------------------------------------------------------------------------------------------------------
            self._spark.sql(f"CREATE DATABASE IF NOT EXISTS info")
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists | OK")

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
                    .saveAsTable("info.log")
                # ------------------------------------------------------------------------------------------------------
                self.log_message(show=self._SHOW, message=f"Creating info.log first time | OK", end=True)
                self.log_message(show=self._SHOW, message=f"LOGTABLE DELTA CREATED", end=True, start=True)
                # ------------------------------------------------------------------------------------------------------

            else:
                self.log_message(show=self._SHOW, message=f"No need for creating info.log | OK", start=True, end=True)
