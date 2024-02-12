from lib.utils.utils import Now, bronze_path_raw_data
from lib.spark_delta.spark_delta import DeltaSpark
from pyspark.sql.functions import lit, col, when, concat, trim, lower, initcap, upper, udf, regexp_replace
from pyspark.sql.types import StringType, FloatType, StructType, StructField, DateType, TimestampType, IntegerType
from datetime import datetime


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
        print('_' * 120)
        print(f"""{' ' * 22} ____                            _          ____ ___ _ __     _______ ____  """)
        print(f"""{' ' * 22}| __ ) _ __ ___  _ __  _______  | |_ ___   / ___|_ _| |\\ \\   / / ____|  _ \\ """)
        print(f"""{' ' * 22}|  _ \\| '__/ _ \\| '_ \\|_  / _ \\ | __/ _ \\  \\___ \\| || | \\ \\ / /|  _| | |_) |""")
        print(f"""{' ' * 22}| |_) | | | (_) | | | |/ /  __/ | || (_) |  ___) | || |__\\ V / | |___|  _ < """)
        print(f"""{' ' * 22}|____/|_|  \\___/|_| |_/___\\___|  \\__\\___/  |____/___|_____\\_/  |_____|_| \\_\\""")
        print('_'*120)
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
        def remove_multiple_spaces(_string: str) -> str:
            """
            Remove extra spaces between the words

            :param _string: the string on wich we want the extra spaces to be removed
            :return: a string without the extra spaces
            """

            _string = str(_string).replace(' ', '<>').replace('><', '').replace('<>', ' ')
            return _string

        # --------------------------------------------------------------------------------------------------------------

        remove_multiple_spaces_udf = udf(lambda _string: remove_multiple_spaces(_string=_string), StringType())

        # --------------------------------------------------------------------------------------------------------------
        df = df.withColumn(
            'brewery_type', lower(trim(col('brewery_type')))) \
            .withColumn('city', remove_multiple_spaces_udf(trim(initcap(col('city'))))) \
            .withColumn('country', remove_multiple_spaces_udf(trim(initcap(col('country'))))) \
            .withColumn('name', remove_multiple_spaces_udf(trim(initcap(col('name'))))) \
            .withColumn('state', remove_multiple_spaces_udf(trim(initcap(col('state'))))) \
            .withColumn('state_province', remove_multiple_spaces_udf(trim(initcap(col('state_province'))))) \
            .withColumn('street', remove_multiple_spaces_udf(trim(upper(col('street')))))\
            .withColumn(
            'latitude', regexp_replace('latitude', ',', '.').cast(FloatType())) \
            .withColumn(
            'longitude', regexp_replace('latitude', ',', '.').cast(FloatType())) \
            .withColumn(
            'latitude', when(
                (col('latitude') > 90) | (col('latitude') < -90), 0.0).otherwise(col('latitude'))) \
            .withColumn(
            'longitude', when(
                (col('longitude') > 180) | (col('longitude') < -180), 0.0).otherwise(col('longitude'))) \
            .withColumn('latitude', col('latitude').cast(FloatType())) \
            .withColumn('longitude', col('latitude').cast(FloatType()))
        # --------------------------------------------------------------------------------------------------------------

        # THOUGH AN INITIAL ANALUYSIS THE COLUMN "state_province" IS THE SAME AS THE COLUMN "state", SO WE CAN DROP IT
        df = df.drop('state_province')

        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW_LOG, message='DATA TREATMENT | OK', end=True)
        # --------------------------------------------------------------------------------------------------------------
        lit_date = datetime.strptime(Now().now(), "%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d')
        df = df.withColumn("date_ref_carga", lit(lit_date).cast(DateType()))
        del lit_date
        # --------------------------------------------------------------------------------------------------------------
        if df.count() > 0:
            df.write\
              .format('delta')\
              .mode('overwrite')\
              .option('silver.brewery', './warehouse/')\
              .saveAsTable('silver.brewery')
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
