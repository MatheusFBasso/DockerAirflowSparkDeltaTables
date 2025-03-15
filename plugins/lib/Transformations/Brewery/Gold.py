from lib.Spark.GetSpark import DeltaSpark
from delta.tables import DeltaTable
from lib.utils.Now import Now
from pyspark.sql.functions import col, lit, cast
from pyspark.sql.types import DateType
from datetime import datetime

class GoldTables(Now):
    _SHOW_LOG = True

    def __init__(self):
        self.spark = DeltaSpark().initialize()

    def brewery_type_total(self):

        print(f"┌{'─' * 118}┐")
        print(f"│{' ' * 33}                                                                     {' '*16}│")
        print(f"│{' ' * 33}   █████████     ███████    █████       ██████████                   {' '*16}│")
        print(f"│{' ' * 33}  ███░░░░░███  ███░░░░░███ ░░███       ░░███░░░░███                  {' '*16}│")
        print(f"│{' ' * 33} ███     ░░░  ███     ░░███ ░███        ░███   ░░███                 {' '*16}│")
        print(f"│{' ' * 33}░███         ░███      ░███ ░███        ░███    ░███                 {' '*16}│")
        print(f"│{' ' * 33}░███    █████░███      ░███ ░███        ░███    ░███                 {' '*16}│")
        print(f"│{' ' * 33}░░███  ░░███ ░░███     ███  ░███      █ ░███    ███                  {' '*16}│")
        print(f"│{' ' * 33} ░░█████████  ░░░███████░   ███████████ ██████████ BREWERY TYPE TOTAL{' '*16}│")
        print(f"│{' ' * 33}  ░░░░░░░░░     ░░░░░░░    ░░░░░░░░░░░ ░░░░░░░░░░                    {' '*16}│")
        print(f"│{' ' * 33}                                                                     {' '*16}│")
        print(f"└{'─' * 118}┘")

        # --------------------------------------------------------------------------------------------------------------
        df = self.spark.read.format('delta').load('./warehouse/silver.db/brewery_daily')
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING TO GOLD', start=True)
        df_count_total_country = df.select('country', 'id').groupBy('country').count()

        df_pivot_brew_type_count = (
            df.select('id', 'brewery_type', 'country')
              .groupBy('country').pivot('brewery_type')
              .count()
              .na.fill(0))

        df_final = (
            df_count_total_country.alias('total')
                                  .join(
                                    df_pivot_brew_type_count.alias('additional'),
                                    on=['country'],
                                    how='left').withColumnRenamed('count', 'Total')
                                  .orderBy('Total', ascending=False))

        # self.spark.sql("""CREATE DATABASE IF NOT EXISTS gold""")

        lit_date = datetime.strptime(Now().now(), "%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d')
        df_final = df_final.withColumn('dat_ref_carga', lit(lit_date).cast(DateType()))
        self.log_message(show=self._SHOW_LOG, message='TRANSFORMING TO GOLD | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='SAVING gold.countries_brewery_type_num', start=True)
        df_final.write\
                .format('delta')\
                .mode('overwrite') \
                .option("overwriteSchema", "True")\
                .partitionBy('dat_ref_carga')\
                .save('./warehouse/gold.db/countries_brewery_type_num')
        self.log_message(show=self._SHOW_LOG, message='SAVING gold.countries_brewery_type_num | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
