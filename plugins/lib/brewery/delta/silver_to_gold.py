from lib.utils.utils import Now
from pyspark.sql.functions import monotonically_increasing_id, when, col
from pyspark.sql.types import TimestampType, IntegerType
from lib.spark_delta.spark_delta import DeltaSpark
from datetime import datetime


class BreweryGold(Now):

    _SHOW_LOG = True
    _DICT_GOLD_APPEND = {'city': 0, 'state': 0, 'country': 0}
    _START_TIME = datetime.now()
    _DICT_LOG = {'gold_d_brewery_country': [],
                 'gold_d_brewery_state': [],
                 'gold_d_brewery_city': [],
                 'gold_f_brewery': [],
                 }

    ####################################################################################################################
    def __init__(self, auto_set_tables: bool = True):
        # --------------------------------------------------------------------------------------------------------------
        print('_'*120)
        print(f"""{" " * 23} ____                                     _           ____       _     _ """)
        print(f"""{" " * 23}| __ ) _ __ _____      _____ _ __ _   _  | |_ ___    / ___| ___ | | __| |""")
        print(f"""{" " * 23}|  _ \| '__/ _ \ \ /\ / / _ \ '__| | | | | __/ _ \  | |  _ / _ \| |/ _` |""")
        print(f"""{" " * 23}| |_) | | |  __/\ V  V /  __/ |  | |_| | | || (_) | | |_| | (_) | | (_| |""")
        print(f"""{" " * 23}|____/|_|  \___| \_/\_/ \___|_|   \__, |  \__\___/   \____|\___/|_|\__,_|""")
        print(f"""{" " * 23}                                  |___/                                  """)
        print(f"{'_'*120}")
        # --------------------------------------------------------------------------------------------------------------
        self.spark = DeltaSpark().initialize()
        self._set_tables = auto_set_tables
        self._create_tables = None
        # --------------------------------------------------------------------------------------------------------------
        self.country_max_PK = self.spark\
                                  .sql("""SELECT COALESCE(MAX(PK), 0) AS PK FROM gold.gold_D_Brewery_country""")\
                                  .collect()[0][0]
        # --------------------------------------------------------------------------------------------------------------
        self.state_max_PK = self.spark\
                                .sql("""SELECT COALESCE(MAX(PK), 0) AS PK FROM gold.gold_D_Brewery_state""")\
                                .collect()[0][0]
        # --------------------------------------------------------------------------------------------------------------
        self.city_max_PK = self.spark\
                               .sql("""SELECT COALESCE(MAX(PK), 0) AS PK FROM gold.gold_D_Brewery_city""")\
                               .collect()[0][0]

    ####################################################################################################################
    def silver_to_olap(self, temp_view_name: str, dict_dims: dict) -> None:
        # --------------------------------------------------------------------------------------------------------------
        for Dim_table in dict_dims:

            max_pk = dict_dims.get(Dim_table)[0]
            dim_table_name = dict_dims.get(Dim_table)[1]
            pk_range = dict_dims.get(Dim_table)[2]
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW_LOG,
                             message="""UPDATING DIM TABLE gold.{}""".format(Dim_table),
                             start=True)
            # ----------------------------------------------------------------------------------------------------------
            query = f"""
            SELECT FACT_TABLE.{Dim_table}
            FROM {temp_view_name} AS FACT_TABLE
            LEFT JOIN {dim_table_name} AS DIM_TABLE ON FACT_TABLE.{Dim_table} = DIM_TABLE.{Dim_table}
            WHERE DIM_TABLE.PK IS NULL
            ORDER BY FACT_TABLE.id
            """
            # ----------------------------------------------------------------------------------------------------------
            temp_view = self.spark.sql(query)
            temp_view = temp_view.select(Dim_table).distinct().withColumn('PK', monotonically_increasing_id() + (
                max_pk if max_pk > 0 else pk_range)).select('PK', Dim_table)
            # ----------------------------------------------------------------------------------------------------------
            _count_to_dict = temp_view.count()
            print(f'{Dim_table}: {_count_to_dict} to be inserted')
            self._DICT_GOLD_APPEND[Dim_table] = _count_to_dict
            self._DICT_LOG[dim_table_name.split('.')[-1].lower()].append(_count_to_dict)
            self._DICT_LOG[dim_table_name.split('.')[-1].lower()].append(datetime.now())
            # ----------------------------------------------------------------------------------------------------------
            if _count_to_dict > 0:
                temp_view.createTempView("temp_view_for_Dim_tables")
                query = f"""INSERT INTO {dim_table_name} SELECT * FROM temp_view_for_Dim_tables"""
                self.spark.sql(query)
                # ------------------------------------------------------------------------------------------------------
                print(self._DICT_GOLD_APPEND)
                # ------------------------------------------------------------------------------------------------------
                self.spark.catalog.dropTempView("temp_view_for_Dim_tables")
                del max_pk, dim_table_name, pk_range, temp_view, query
                # ------------------------------------------------------------------------------------------------------
                self.log_message(show=self._SHOW_LOG,
                                 message="""UPDATING DIM TABLE gold.{} | OK""".format(Dim_table),
                                 end=True)
            else:
                self.log_message(show=self._SHOW_LOG,
                                 message="""UPDATING DIM TABLE gold.{} | OK NO NEW DATA""".format(Dim_table),
                                 end=True)

    ####################################################################################################################
    def merge(self, df_main, df_upd, df_main_key, df_upd_key):

        self.log_message(show=self._SHOW_LOG, message="""STATING MERGE""", start=True)

        # Generates a list with the name of the columns present in the dataframe
        df_main_cols = df_main.columns
        df_upd_cols = df_upd.columns

        # Adds the suffix "_tmp" to df_upd columns
        add_suffix = [df_upd[f'{col_name}'].alias(col_name + '_tmp') for col_name in df_upd.columns]
        df_upd = df_upd.select(*add_suffix)

        # Doing a full outer join
        df_join = df_main.join(df_upd, df_main[df_main_key] == df_upd[df_upd_key + '_tmp'], "fullouter")
        # Using the for loop to scroll through the columns
        for col in df_main_cols:
            # Implementing the logic to update the rows
            df_join = df_join.withColumn(col, when((df_main[col] != df_upd[col + '_tmp']) | (df_main[col].isNull()),
                                                   df_upd[col + '_tmp']).otherwise(df_main[col]))

        # Selecting only the columns of df_main (or all columns that do not have the suffix "_tmp")
        df_final = df_join.select(*df_main_cols)

        self.log_message(show=self._SHOW_LOG, message="""STATING MERGE | OK""", end=True)

        # Returns the dataframe updated with the merge
        return df_final

    ####################################################################################################################
    def execute(self):

        self.log_message(show=self._SHOW_LOG, message='STARTING',
                         start=True, end=True, sep='=')

        dict_dims = {
            'country': (self.country_max_PK, 'gold.gold_D_Brewery_country', 30000000),
            'state': (self.state_max_PK, 'gold.gold_D_Brewery_state', 20000000),
            'city': (self.city_max_PK, 'gold.gold_D_Brewery_city', 10000000)
        }

        self.silver_to_olap(temp_view_name='silver.brewery', dict_dims=dict_dims)

        initial_count = self.spark.sql("SELECT COUNT(*) FROM gold.gold_f_brewery").collect()[0][0]

        temp = self.spark.sql("""
        SELECT FACT.id            AS id,
               FACT.name          AS name,
               FACT.brewery_type  AS brewery_type,
               FACT.latitude      AS latitude,
               FACT.longitude     AS longitude,
               FACT.phone         AS phone,
               FACT.website_url   AS website_url,
               FACT.address       AS address,
               DIM_COUNTRY.PK     AS p_country,
               DIM_STATE.PK       AS p_state,
               DIM_CITY.PK        AS p_city   

        FROM silver.brewery AS FACT

        LEFT JOIN gold.gold_D_Brewery_country AS DIM_COUNTRY ON FACT.country = DIM_COUNTRY.country
        LEFT JOIN gold.gold_D_Brewery_state   AS DIM_STATE   ON FACT.state = DIM_STATE.state
        LEFT JOIN gold.gold_D_Brewery_city    AS DIM_CITY    ON FACT.city = DIM_CITY.city

        """)

        self.log_message(show=self._SHOW_LOG, message='UPDATING FACT TABLE | OK', end=True)

        temp.createOrReplaceTempView('BreweryTable')

        final_count = self.spark.sql("""SELECT COUNT(*) FROM BreweryTable""").collect()[0][0]

        if final_count == initial_count:
            self.log_message(show=self._SHOW_LOG, message='NO NEW DATA | FINISHING')

        else:

            if self.spark.sql("""SELECT COUNT(*) FROM gold.gold_F_Brewery""").collect()[0][0] == 0:
                query = """
                INSERT INTO gold.gold_F_Brewery
                SELECT * FROM BreweryTable
                """
                self.spark.sql(query)

            else:

                temp = self.merge(self.spark.sql("""SELECT * FROM gold.gold_F_Brewery"""),
                                  self.spark.sql("""SELECT * FROM BreweryTable"""),
                                  df_main_key='id',
                                  df_upd_key='id')
                self.log_message(show=self._SHOW_LOG, message='SAVING TO DELTA', start=True)
                temp.write \
                    .format('delta') \
                    .mode('overwrite') \
                    .option('gold.gold_f_brewery', './warehouse/') \
                    .saveAsTable('gold.gold_f_brewery')
                self.log_message(show=self._SHOW_LOG, message='SAVING TO DELTA| OK', end=True)

                del temp

            self.log_message(show=self._SHOW_LOG, message='FINISHING', start=True, end=True, sep='=')

        # --------------------------------------------------------------------------------------------------------------
        self._DICT_LOG['gold_f_brewery'].append(final_count)
        self._DICT_LOG['gold_f_brewery'].append(datetime.now())
        # --------------------------------------------------------------------------------------------------------------

        self.log_message(show=self._SHOW_LOG, message='STARTING LOG', start=True, end=True, sep='=')

        # print(self._DICT_LOG.items())

        for table in self._DICT_LOG.items():

            self.log_message(show=self._SHOW_LOG, message=f'LOG DELTA TABLE INFO: {table[0]}')

            df_log = self.spark.createDataFrame(
                [
                    {
                        'id': self._START_TIME,
                        'db': 'gold.db',
                        'table': table[0],
                        'rows_inserted': table[1][0],
                        'start_time': self._START_TIME,
                        'end_time': table[1][1]
                    }
                ]
            )

            df_log = (df_log.withColumn("start_time", col('start_time').cast(TimestampType()))
                      .withColumn("end_time", col('end_time').cast(TimestampType()))
                      .withColumn("rows_inserted", col('rows_inserted').cast(IntegerType()))
                      )

            df_log.write\
                  .format('delta')\
                  .mode('append')\
                  .option('info.log', './warehouse/')\
                  .saveAsTable('info.log')

            self.log_message(show=self._SHOW_LOG, message=f'LOG DELTA TABLE INFO: {table[0]} | OK')

        self.log_message(show=self._SHOW_LOG, message='FINISHING LOG', start=True, end=True, sep='=')

        self.spark.stop()
