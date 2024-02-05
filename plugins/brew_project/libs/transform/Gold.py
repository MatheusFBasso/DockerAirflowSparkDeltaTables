from ..utils.utils import DeltaSpark, Now
from pyspark.sql.functions import monotonically_increasing_id, when


class BreweryGold(Now):
    _SHOW_LOG = True
    _DICT_GOLD_APPEND = {'city': 0, 'state': 0, 'country': 0}

    def __init__(self, auto_set_tables: bool = True):
        super().__init__()
        self.spark = DeltaSpark().initialize()
        self._set_tables = auto_set_tables
        self._create_tables = None

        self.country_max_PK = self.spark.sql("""
        SELECT COALESCE(MAX(PK), 0) AS PK 
        FROM gold.gold_D_Brewery_country
        """).collect()[0][0]

        self.state_max_PK = self.spark.sql("""
        SELECT COALESCE(MAX(PK), 0) AS PK
        FROM gold.gold_D_Brewery_state
        """).collect()[0][0]

        self.city_max_PK = city_max_PK = self.spark.sql("""
        SELECT COALESCE(MAX(PK), 0) AS PK
        FROM gold.gold_D_Brewery_city
        """).collect()[0][0]

    @property
    def _set_tables(self):
        return self._create_tables

    @_set_tables.setter
    def _set_tables(self, auto_set_tables: bool):

        if auto_set_tables:

            self.log_message(show=self._SHOW_LOG,
                             message='[TRANSFORM] | [GOLD] | CREATING SCHEMA AND TABLES IF NOT EXISTS',
                             start=True)

            self.spark.sql("CREATE DATABASE IF NOT EXISTS gold")

            country_query = """
            CREATE TABLE IF NOT EXISTS gold.gold_D_Brewery_country (
            PK INT NOT NULL,
            country STRING
            )
            USING DELTA
            PARTITIONED BY (PK)"""

            state_query = """
            CREATE TABLE IF NOT EXISTS gold.gold_D_Brewery_state (
            PK INT NOT NULL,
            state STRING
            )
            USING DELTA
            PARTITIONED BY (PK)"""

            city_query = """
            CREATE TABLE IF NOT EXISTS gold.gold_D_Brewery_city (
            PK INT NOT NULL,
            city STRING
            )
            USING DELTA
            PARTITIONED BY (PK)"""

            dim_query = """
            CREATE TABLE IF NOT EXISTS gold.gold_F_Brewery (
            id           STRING NOT NULL,
            name         STRING,
            brewery_type STRING,
            latitude     STRING,
            longitude    STRING,
            phone        STRING,
            website_url  STRING,
            address      STRING,
            p_country    INT,
            p_state      INT,
            p_city       INT
            )
            USING DELTA
            PARTITIONED BY (id)"""

            self.spark.sql("CREATE DATABASE IF NOT EXISTS gold")

            for sql in [dim_query, country_query, state_query, city_query]:
                self.spark.sql(sql)

            self.log_message(show=self._SHOW_LOG,
                             message='[TRANSFORM] | [GOLD] | CREATING SCHEMA AND TABLES IF NOT EXISTS | OK',
                             end=True)

            self._create_tables = 'OK'
        else:
            pass

    def silver_to_olap(self, temp_view_name: str, dict_dims: dict) -> None:

        for Dim_table in dict_dims:

            max_pk = dict_dims.get(Dim_table)[0]
            dim_table_name = dict_dims.get(Dim_table)[1]
            pk_range = dict_dims.get(Dim_table)[2]

            self.log_message(show=self._SHOW_LOG,
                             message="""[TRANSFORM] | [GOLD] | UPDATING DIM TABLE gold.{}""".format(Dim_table),
                             start=True)

            query = f"""
            SELECT FACT_TABLE.{Dim_table}
            FROM {temp_view_name} AS FACT_TABLE
            LEFT JOIN {dim_table_name} AS DIM_TABLE ON FACT_TABLE.{Dim_table} = DIM_TABLE.{Dim_table}
            WHERE DIM_TABLE.PK IS NULL
            ORDER BY FACT_TABLE.id
            """

            temp_view = self.spark.sql(query)
            temp_view = temp_view.select(Dim_table).distinct().withColumn('PK', monotonically_increasing_id() + (
                max_pk if max_pk > 0 else pk_range)).select('PK', Dim_table)

            temp_view.createTempView("temp_view_for_Dim_tables")
            _count_to_dict = temp_view.count()
            print(f'{Dim_table}: {_count_to_dict} to be inserted')
            self._DICT_GOLD_APPEND[Dim_table] = _count_to_dict

            query = f"""
            INSERT INTO {dim_table_name}
            SELECT * FROM temp_view_for_Dim_tables
            """

            # query = f"""
            # MERGE INTO {dim_table_name} AS TARGET
            # USING TEMP_{dim_table_name} AS SOURCE ON TARGET.PK = SOURCE.PK
            #
            # WHEN MATCHED THEN
            #     UPDATE SET
            #     TARGET.PK = SOURCE.PK,
            #     TARGET.{Dim_table} = SOURCE.{Dim_table}
            #
            # WHEN NOT MATCHED BY TARGET THEN INSERT (TARGET.PK, TARGET.{Dim_table}) VALUES (SOURCE.PK, SOURCE.{Dim_table});
            # """
            #
            if _count_to_dict > 0:
                self.spark.sql(query)

            print(self._DICT_GOLD_APPEND)

            self.spark.catalog.dropTempView("temp_view_for_Dim_tables")
            del max_pk, dim_table_name, pk_range, temp_view, query

            self.log_message(show=self._SHOW_LOG,
                             message="""[TRANSFORM] | [GOLD] | UPDATING DIM TABLE gold.{} | OK""".format(Dim_table),
                             end=True)

    def merge(self, df_main, df_upd, df_main_key, df_upd_key):

        self.log_message(show=self._SHOW_LOG, message="""[TRANSFORM] | [GOLD] | STATING MERGE""", start=True)

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

        self.log_message(show=self._SHOW_LOG, message="""[TRANSFORM] | [GOLD] | STATING MERGE | OK""", end=True)

        # Returns the dataframe updated with the merge
        return df_final

    def execute(self):

        self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [GOLD] | STARTING',
                         start=True, end=True, sep='=')

        dict_dims = {
            'country': (self.country_max_PK, 'gold.gold_D_Brewery_country', 30000000),
            'state': (self.state_max_PK, 'gold.gold_D_Brewery_state', 20000000),
            'city': (self.city_max_PK, 'gold.gold_D_Brewery_city', 10000000)
        }

        self.silver_to_olap(temp_view_name='silver.brewery', dict_dims=dict_dims)

        if self._DICT_GOLD_APPEND.get('country') == 0 \
                and self._DICT_GOLD_APPEND.get('state') == 0 \
                and self._DICT_GOLD_APPEND.get('city') == 0:

            self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [GOLD] | UPDATING FACT TABLE', start=True)

            self.log_message(show=self._SHOW_LOG,
                             message='[TRANSFORM] | [GOLD] | NO ADDITIONS REQUIRED', start=True)

        else:

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

            self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [GOLD] | UPDATING FACT TABLE | OK', end=True)

            temp.createOrReplaceTempView('BreweryTable')

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
                self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [GOLD] | SAVING TO DELTA', start=True)
                temp.write \
                    .format('delta') \
                    .mode('overwrite') \
                    .option('silver.brewery', './brew_project/warehouse/') \
                    .saveAsTable('gold.gold_F_Brewery')
                self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [GOLD] | SAVING TO DELTA| OK', end=True)

                del temp

        self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [GOLD] | FINISHING',
                         start=True, end=True, sep='=')

        self.spark.stop()
