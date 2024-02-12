from lib.spark_delta.spark_delta import DeltaSpark
from lib.utils.utils import Now, warehouse_dir


class SetDeltaTables(Now):

    _SHOW = True

    ####################################################################################################################
    def __init__(self, silver: bool = True, gold: bool = True, log_table: bool = True):
        self._silver = silver
        self._gold = gold
        self._log_table = log_table
        self._spark = DeltaSpark().initialize()

        bool_dict = {'silver': self._silver, 'gold': self._gold, 'log_table': self._log_table}

        # --------------------------------------------------------------------------------------------------------------
        print("_"*120)
        print(f"""{' ' * 34}______     _ _          _____     _     _           """)
        print(f"""{' ' * 34}|  _  \   | | |        |_   _|   | |   | |          """)
        print(f"""{' ' * 34}| | | |___| | |_ __ _    | | __ _| |__ | | ___  ___ """)
        print(f"""{' ' * 34}| | | / _ \ | __/ _` |   | |/ _` | '_ \| |/ _ \/ __|""")
        print(f"""{' ' * 34}| |/ /  __/ | || (_| |   | | (_| | |_) | |  __/\__ \\""")
        print(f"""{' ' * 34}|___/ \___|_|\__\__,_|   \_/\__,_|_.__/|_|\___||___/""")
        print("_"*120)
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
            print("_" * 120)
            print(f"""{' ' * 34} ____  _ _                  _____     _     _      """)
            print(f"""{' ' * 34}/ ___|(_) |_   _____ _ __  |_   _|_ _| |__ | | ___ """)
            print(f"""{' ' * 34}\\___ \\| | \\ \\ / / _ \\ '__|   | |/ _` | '_ \\| |/ _ \\""")
            print(f"""{' ' * 34} ___) | | |\\ V /  __/ |      | | (_| | |_) | |  __/""")
            print(f"""{' ' * 34}|____/|_|_| \\_/ \\___|_|      |_|\\__,_|_.__/|_|\\___|""")
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"STARTING SILVER DELTA TABLE CREATION", start=True, end=True)
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists")
            # ----------------------------------------------------------------------------------------------------------
            self._spark.sql(f"CREATE DATABASE IF NOT EXISTS silver")
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists | OK")
            self.log_message(show=self._SHOW, message=f"Creating Table silver.brewery if not exists")
            # ----------------------------------------------------------------------------------------------------------
            _sql = """
                    CREATE TABLE IF NOT EXISTS silver.brewery (
                                                                brewery_type   STRING,
                                                                city           STRING,
                                                                country        STRING,
                                                                id             STRING NOT NULL,
                                                                latitude       FLOAT ,
                                                                longitude      FLOAT ,
                                                                name           STRING,
                                                                phone          STRING,
                                                                postal_code    STRING,
                                                                state          STRING,
                                                                street         STRING,
                                                                website_url    STRING,
                                                                address        STRING,
                                                                date_ref_carga DATE NOT NULL
                                                                )
                    USING DELTA
                    PARTITIONED BY (date_ref_carga)
            """

            self._spark.sql(_sql)
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"Creating Table brewery is not exists | OK")
            self.log_message(show=self._SHOW, message=f"SILVER DELTA TABLE CREATED", end=True, start=True)
            # ----------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        if self._gold:
            # ----------------------------------------------------------------------------------------------------------
            print("_" * 120)
            print(f"""{34 * ' '}  ____       _     _   _____     _     _           """)
            print(f"""{34 * ' '} / ___| ___ | | __| | |_   _|_ _| |__ | | ___  ___ """)
            print(f"""{34 * ' '}| |  _ / _ \\| |/ _` |   | |/ _` | '_ \\| |/ _ \\/ __|""")
            print(f"""{34 * ' '}| |_| | (_) | | (_| |   | | (_| | |_) | |  __/\\__ \\""")
            print(f"""{34 * ' '} \\____|\\___/|_|\\__,_|   |_|\\__,_|_.__/|_|\\___||___/""")
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"STARTING GOLD DELTA TABLE CREATION", start=True, end=True)
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists")
            # ----------------------------------------------------------------------------------------------------------
            self._spark.sql(f"CREATE DATABASE IF NOT EXISTS gold")
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists | OK")
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

            fact_query = """
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


            sql_dict = {'gold_F_Brewery': fact_query,
                        'gold_D_Brewery_city': city_query,
                        'gold_D_Brewery_state': state_query,
                        'gold_D_Brewery_country': country_query,
                        }

            for table, sql in sql_dict.items():
                print(f"gold.{table} created")
                # print(sql)
                self._spark.sql(sql)
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"GOLD DELTA TABLES CREATED", end=True, start=True)
            # ----------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        if self._log_table:
            print("_" * 120)
            print(f"""{" " * 40} _               _____     _     _      """)
            print(f"""{" " * 40}| |    ___   __ |_   _|_ _| |__ | | ___ """)
            print(f"""{" " * 40}| |   / _ \\ / _` || |/ _` | '_ \\| |/ _ \\""")
            print(f"""{" " * 40}| |__| (_) | (_| || | (_| | |_) | |  __/""")
            print(f"""{" " * 40}|_____\\___/ \\__, ||_|\\__,_|_.__/|_|\\___|""")
            print(f"""{" " * 40}            |___/                       """)
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"\STARTING LOGTABLE DELTA CREATION", start=True, end=True)
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists")
            # ----------------------------------------------------------------------------------------------------------
            self._spark.sql(f"CREATE DATABASE IF NOT EXISTS info")
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"Creating Database if not exists | OK")
            self.log_message(show=self._SHOW, message=f"Creating info.log if not exists")
            # ----------------------------------------------------------------------------------------------------------
            _sql = """
                    CREATE TABLE IF NOT EXISTS info.log (
                                                         id TIMESTAMP NOT NULL,
                                                         db STRING NOT NULL,
                                                         table STRING NOT NULL,
                                                         rows_inserted INTEGER,
                                                         start_time TIMESTAMP NOT NULL,
                                                         end_time TIMESTAMP NOT NULL                                                         
                                                                )
                    USING DELTA
                    PARTITIONED BY (id)
            """

            self._spark.sql(_sql)
            # ----------------------------------------------------------------------------------------------------------
            self.log_message(show=self._SHOW, message=f"Creating LogTable if not exists | OK")
            self.log_message(show=self._SHOW, message=f"LOGTABLE DELTA CREATED", end=True, start=True)
            # ----------------------------------------------------------------------------------------------------------
