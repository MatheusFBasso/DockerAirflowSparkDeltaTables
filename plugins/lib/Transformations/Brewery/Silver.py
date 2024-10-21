from lib.utils.Brewery.brewery_paths import bronze_path_raw_data

from pyspark.sql.functions import lit, col, when, concat, trim, lower, initcap, regexp_replace
from pyspark.sql.types import StringType, StructType, StructField, DateType, TimestampType, IntegerType, DoubleType
from datetime import datetime
from delta.tables import DeltaTable

from lib.utils.Now import Now
from lib.Spark.GetSpark import DeltaSpark


class BrewerySilver(Now):

    _SHOW_LOG = True
    _START_TIME = datetime.now()
    _SCHEMA_RAW_DATA = StructType([
        StructField('id',             StringType(), False),
        StructField('name',           StringType(), True),
        StructField('brewery_type',   StringType(), True),
        StructField('address_1',      StringType(), True),
        StructField('address_2',      StringType(), True),
        StructField('address_3',      StringType(), True),
        StructField('city',           StringType(), True),
        StructField('state_province', StringType(), True),
        StructField('postal_code',    StringType(), True),
        StructField('country',        StringType(), True),
        StructField('longitude',      StringType(), True),
        StructField('latitude',       StringType(), True),
        StructField('phone',          StringType(), True),
        StructField('website_url',    StringType(), True),
        StructField('state',          StringType(), True),
        StructField('street',         StringType(), True),
        StructField('date_ref_carga',   DateType(), False),
    ])

    def __init__(self):
        # --------------------------------------------------------------------------------------------------------------
        print(f"┌{'─' * 118}┐")
        print(f"│{' ' * 24}                                                                             {' ' * 17}│")
        print(f"│{' ' * 24}  █████████  █████ █████       █████   █████ ██████████ ███████████          {' ' * 17}│")
        print(f"│{' ' * 24} ███░░░░░███░░███ ░░███       ░░███   ░░███ ░░███░░░░░█░░███░░░░░███         {' ' * 17}│")
        print(f"│{' ' * 24}░███    ░░░  ░███  ░███        ░███    ░███  ░███  █ ░  ░███    ░███         {' ' * 17}│")
        print(f"│{' ' * 24}░░█████████  ░███  ░███        ░███    ░███  ░██████    ░██████████          {' ' * 17}│")
        print(f"│{' ' * 24} ░░░░░░░░███ ░███  ░███        ░░███   ███   ░███░░█    ░███░░░░░███         {' ' * 17}│")
        print(f"│{' ' * 24} ███    ░███ ░███  ░███      █  ░░░█████░    ░███ ░   █ ░███    ░███         {' ' * 17}│")
        print(f"│{' ' * 24}░░█████████  █████ ███████████    ░░███      ██████████ █████   █████ BREWERY{' ' * 17}│")
        print(f"│{' ' * 24} ░░░░░░░░░  ░░░░░ ░░░░░░░░░░░      ░░░      ░░░░░░░░░░ ░░░░░   ░░░░░         {' ' * 17}│")
        print(f"│{' ' * 24}                                                                             {' ' * 17}│")
        print(f"└{'─' * 118}┘")
        # --------------------------------------------------------------------------------------------------------------

        self._spark = DeltaSpark().initialize()

    def execute(self):

        self.log_message(show=self._SHOW_LOG,
                         message='STARTING', start=True, end=True, sep='=')

        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES', start=True)
        # --------------------------------------------------------------------------------------------------------------

        df = self._spark.read.schema(self._SCHEMA_RAW_DATA).json(bronze_path_raw_data, multiLine=True)

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='READING RAW FILES | OK', end=True)
        self.log_message(show=self._SHOW_LOG, message='VALIDANTING PK', start=True)
        # --------------------------------------------------------------------------------------------------------------

        assert df.select('id').groupBy('id').count().filter(col('count') > 1).collect() == [], 'ID IS DUPLICATED'

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='VALIDANTING PK | OK', end=True)
        self.log_message(show=self._SHOW_LOG, message='STARTING DATA TREATMENT', start=True)
        # --------------------------------------------------------------------------------------------------------------

        df = df.withColumn(
            'address',
            concat(
                when(col('address_1').isNull(), '').otherwise(concat(col('address_1'), lit(' | '))),
                when(col('address_2').isNull(), '').otherwise(col('address_2')),
                when(col('address_3').isNull(), '').otherwise(concat(lit(' | '), col('address_3'))),
            )
        ).drop('address_1', 'address_2', 'address_3')

        # --------------------------------------------------------------------------------------------------------------
        df = df.withColumn('brewery_type', lower(trim(col('brewery_type'))))\
               .withColumn('city',
                           regexp_replace(
                               regexp_replace(
                                   regexp_replace(
                                       trim(initcap(col('city'))), ' ', '<>'),
                                   '><', ''),
                               '<>', ' '))\
               .withColumn('country',
                           regexp_replace(
                               regexp_replace(
                                   regexp_replace(
                                       trim(initcap(col('country'))), ' ', '<>'),
                                   '><', ''),
                               '<>', ' '))\
               .withColumn('name',
                           regexp_replace(
                               regexp_replace(
                                   regexp_replace(
                                       trim(initcap(col('name'))), ' ', '<>'),
                                   '><', ''),
                               '<>', ' '))\
               .withColumn('state',
                           regexp_replace(
                               regexp_replace(
                                   regexp_replace(
                                       trim(
                                           initcap(col('state'))), ' ', '<>'),
                                   '><', ''),
                               '<>', ' '))\
               .withColumn('state_province',
                           regexp_replace(
                               regexp_replace(
                                   regexp_replace(
                                       trim(initcap(col('state_province'))), ' ', '<>'),
                                   '><', ''),
                               '<>', ' '))\
               .withColumn('street',
                           regexp_replace(
                               regexp_replace(
                                   regexp_replace(
                                       trim(initcap(col('street'))), ' ', '<>'),
                                   '><', ''),
                               '<>', ' '))\
               .withColumn('latitude',
                           regexp_replace('latitude', ',', '.').cast(DoubleType())) \
               .withColumn('longitude',
                           regexp_replace('latitude', ',', '.').cast(DoubleType())) \
               .withColumn('latitude',
                           when((col('latitude') > 90) | (col('latitude') < -90), 0.0
                                ).otherwise(col('latitude'))) \
               .withColumn('longitude',
                           when((col('longitude') > 180) | (col('longitude') < -180), 0.0
                                ).otherwise(col('longitude'))) \
               .withColumn('latitude', col('latitude').cast(DoubleType())) \
               .withColumn('longitude', col('latitude').cast(DoubleType()))
        # --------------------------------------------------------------------------------------------------------------

        # THOUGH AN INITIAL ANALYSIS THE COLUMN "state_province" IS THE SAME AS THE COLUMN "state", SO WE CAN DROP IT
        df = df.drop('state_province')

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='DATA TREATMENT | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        lit_date = datetime.strptime(Now().now(), "%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d')
        df = df.withColumn("date_ref_carga", lit(lit_date).cast(DateType()))
        del lit_date
        # --------------------------------------------------------------------------------------------------------------

        if df.count() > 0:
            self.log_message(show=self._SHOW_LOG, message='SAVING DAILY TABLE', start=True)
            df.write\
              .format('delta')\
              .mode('overwrite')\
              .option('silver.brewery_daily', '.warehouse/')\
              .partitionBy('date_ref_carga')\
              .saveAsTable('silver.brewery_daily')
            self.log_message(show=self._SHOW_LOG, message='SAVING DAILY TABLE | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
            try:
                brewery_count = self._spark.sql("""SELECT COUNT(*) FROM silver.brewery""").collect()[0][0]
            except Exception as e:
                if "Delta table `silver`.`brewery` doesn't exist." in str(e):
                    self.log_message(show=self._SHOW_LOG, message='SAVING BREWERY TABLE | FIRST EXECUTION', start=True)
                    df.write \
                        .format('delta') \
                        .mode('overwrite') \
                        .option("overwriteSchema", "True") \
                        .option('silver.brewery', '.warehouse/') \
                        .partitionBy('date_ref_carga') \
                        .saveAsTable('silver.brewery')
                    self.log_message(show=self._SHOW_LOG, message='SAVING BREWERY TABLE | FIRST EXECUTION | OK',
                                     end=True)
                else:
                    raise(e)

            if self._spark.sql("""SELECT COUNT(*) FROM silver.brewery""").collect()[0][0] == 0:
                self.log_message(show=self._SHOW_LOG, message='SAVING BREWERY TABLE | FIRST EXECUTION', start=True)
                df.write\
                  .format('delta')\
                  .mode('overwrite') \
                  .option("overwriteSchema", "True") \
                  .option('silver.brewery', '.warehouse/')\
                  .partitionBy('date_ref_carga')\
                  .saveAsTable('silver.brewery')
                self.log_message(show=self._SHOW_LOG, message='SAVING BREWERY TABLE | FIRST EXECUTION | OK', end=True)
            else:

                self.log_message(show=self._SHOW_LOG, message='UPSERT BREWERY TABLE', start=True)
                deltaTableSilver = DeltaTable.forPath(sparkSession=self._spark, path='./warehouse/silver.db/brewery')

                deltaTableSilver.alias('as_is').merge(df.alias('as_now'), 'as_is.id = as_now.id'
                ).whenMatchedUpdate(
                    set={
                            'brewery_type': 'as_now.brewery_type',
                            'city': 'as_now.city',
                            'country': 'as_now.country',
                            'latitude': 'as_now.latitude',
                            'longitude': 'as_now.longitude',
                            'name': 'as_now.name',
                            'phone': 'as_now.phone',
                            'postal_code': 'as_now.postal_code',
                            'state': 'as_now.state',
                            'street': 'as_now.street',
                            'website_url': 'as_now.website_url',
                            'address': 'as_now.address',
                            'date_ref_carga': 'as_now.date_ref_carga'}
                ).whenNotMatchedInsert(
                    values={
                            'id': 'as_now.id',
                            'brewery_type': 'as_now.brewery_type',
                            'city': 'as_now.city',
                            'country': 'as_now.country',
                            'latitude': 'as_now.latitude',
                            'longitude': 'as_now.longitude',
                            'name': 'as_now.name',
                            'phone': 'as_now.phone',
                            'postal_code': 'as_now.postal_code',
                            'state': 'as_now.state',
                            'street': 'as_now.street',
                            'website_url': 'as_now.website_url',
                            'address': 'as_now.address',
                            'date_ref_carga': 'as_now.date_ref_carga'}
                ).execute()

                self.log_message(show=self._SHOW_LOG, message='UPSERT BREWERY TABLE | OK', end=True)

        # --------------------------------------------------------------------------------------------------------------
        else:
            self.log_message(show=self._SHOW_LOG, message='NO DATA TO BE INSERTED')
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(
            show=self._SHOW_LOG,
            message='TABLE silver.brewery SUCCESFULLY CREATE AND DATA STORED',
            start=True, end=True)
        # --------------------------------------------------------------------------------------------------------------
        if df.count() > 0:
            self.log_message(show=self._SHOW_LOG, message='SAVING LOG')

            df_log = self._spark.createDataFrame(
                [
                    {
                        'id': self._START_TIME,
                        'db': 'silver',
                        'table': 'brewery',
                        'rows_inserted': df.count(),
                        'start_time': self._START_TIME,
                        'end_time': datetime.now()
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

            self.log_message(show=self._SHOW_LOG, message='SAVING LOG | OK')
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='FINISHING',
                         start=True, end=True, sep='=')
        # --------------------------------------------------------------------------------------------------------------

        self._spark.stop()
