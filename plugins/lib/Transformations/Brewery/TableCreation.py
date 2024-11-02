from lib.Spark.GetSpark import DeltaSpark
from lib.utils.Now import Now
from delta.tables import DeltaTable
from pyspark.sql.types import StructType, StructField, TimestampType, IntegerType, StringType, DoubleType, DateType


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
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists")
            # ----------------------------------------------------------------------------------------------------------
            self._spark.sql(f"CREATE DATABASE IF NOT EXISTS silver")
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists | OK")
            # ----------------------------------------------------------------------------------------------------------
            schema = StructType([
                StructField('brewery_type', StringType(), True),
                StructField('city', StringType(), True),
                StructField('country', StringType(), True),
                StructField('id', StringType(), False),
                StructField('latitude', DoubleType(), True),
                StructField('longitude', DoubleType(), True),
                StructField('name', StringType(), True),
                StructField('phone', StringType(), True),
                StructField('postal_code', StringType(), True),
                StructField('state', StringType(), True),
                StructField('street', StringType(), True),
                StructField('website_url', StringType(), True),
                StructField('address', StringType(), True),
                StructField('date_ref_carga', DateType(), True),
            ])

            brew_silver_df = self._spark.createDataFrame([], schema)

            for table_c in ['silver.brewery', 'silver.brewery_daily']:
                self.log_message(show=self._SHOW, message=f"Creating Table {table_c} if not exists", start=True)
                if not DeltaTable.isDeltaTable(self._spark, f'./warehouse/silver.db/{table_c.split(".")[-1]}'):
                    brew_silver_df.write.format('delta')\
                                       .mode('overwrite')\
                                       .partitionBy('date_ref_carga')\
                                       .option("overwriteSchema", "True")\
                                       .saveAsTable(table_c)
                    print(f"{table_c} Created successfully!")
                else:
                    print(f"No need to create table {table_c}")
                self.log_message(show=self._SHOW, message=f"Creating Table {table_c} if not exists | OK", end=True)

            # ----------------------------------------------------------------------------------------------------------
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
