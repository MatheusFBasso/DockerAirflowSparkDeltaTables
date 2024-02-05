from ..utils.utils import Now, DeltaSpark, bronze_path_raw_data, bronze_path_bkp_raw_data
from pyspark.sql.functions import lit, col, when, concat, trim, lower, initcap, upper, udf, regexp_replace
from pyspark.sql.types import StringType, FloatType, StructType, StructField, DateType
from datetime import datetime, timedelta
import shutil
import os
from glob import glob

# TODO: Revisar código em busca de melhorias e otimizações no processamento
# TODO: Adicionar tabela de log com as informações resumidas sobre o processamento da camada


class BrewerySilver(Now):

    _SHOW_LOG = True
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
    _TODAY_FORMATED = datetime(datetime.now().year, datetime.now().month, datetime.now().day).strftime("%Y_%m_%d")

    def _string_to_date(self, date_string: str, format_string: str ='%Y_%m_%d') -> datetime | None:

        """
        Converts a date string to a datetime object.

        Parameters:
        - date_string: The date as a string.
        - format_string: The format of the date string. Defaults to '%Y_%m_%d'.

        Returns:
        - A datetime object.
        """

        try:
            return datetime.strptime(date_string, format_string)
        except ValueError as e:
            print(f"An error occurred: {e}")
            return None

    def _move_all_files(self, src_dir: str, dst_dir: str, days: int, pattern: str='PART_*.json', subfolder_name:str=None) -> None:
        """
        Moves all files from src_dir to dst_dir that match the given pattern into a new subfolder.

        Parameters:
        - src_dir: The source directory from which to move files.
        - dst_dir: The destination directory to which to move files.
        - pattern: The pattern to match files against. Defaults to 'PART_*.json' which matches all files.
        - subfolder_name: The name of the new subfolder to create in the destination directory.

        Returns:
        - None
        """
        self.log_message(show=self._SHOW_LOG, message='[LOAD] | [BRONZE] | CHECKING DIRECTORIES')

        # if not os.path.exists(src_dir):
        #     print(f"The source directory '{src_dir}' does not exist.")
        #     raise FileNotFoundError(f"The dir does not exist: {src_dir}")
        #
        # if not os.path.exists(dst_dir):
        #     print(f"The destination directory '{dst_dir}' does not exist.")
        #     raise FileNotFoundError(f"The dir does not exist: {dst_dir}")
        #
        # if subfolder_name is None:
        #     raise ValueError(f"The subfolder wasn't defined.")

        new_dst_dir = os.path.join(dst_dir, subfolder_name)
        os.makedirs(new_dst_dir, exist_ok=True)

        self.log_message(show=self._SHOW_LOG, message='LOAD] | [BRONZE] | CHECKING DIRECTORIES SUCCESS')
        self.log_message(show=self._SHOW_LOG, message='[LOAD] | [BRONZE] | STARTING TO MOVE BACKUP FILES')

        for file_name in glob(os.path.join(src_dir, pattern)):
            if os.path.isfile(file_name):
                shutil.copy(file_name, new_dst_dir)
                os.remove(file_name)

        self.log_message(show=self._SHOW_LOG, message='[LOAD] | [BRONZE] | BACKUP FILES MOVED SUCCESSFULLY')

    def _delete_folder(self, folder_path: str, root_name: str, days: str = 7):
        """
        Deletes a folder and all its contents.

        Parameters:
        - folder_path: The path to the folder to be deleted.

        Returns:
        - None
        """

        folder_path = [_file for _file in sorted(glob(bronze_path_bkp_raw_data + '/*')) if self._string_to_date(
            _file.replace('\\', '/')
                 .split('/')[-1]
                 .replace('BKP_', ''))
                       <=
                       (datetime(datetime.now().year, datetime.now().month, datetime.now().day) -
                        timedelta(days=days))]

        self.log_message(
            show=self._SHOW_LOG,
            message="""[TRANSFORM] | [BRONZE] | DELETING OLD BACKUP FILES. PARAM DAYS = {}""".format(days))

        for folder in folder_path:
            print(folder)

            if os.path.exists(folder):
                try:
                    shutil.rmtree(folder)
                    self.log_message(
                        show=self._SHOW_LOG,
                        message="""[TRANSFORM] | [BRONZE] | DELETED SUCCESSFULLY. FOLDER: {}""".format(
                            folder.split('/')[-1]))

                except Exception as e:
                    print(f"An error occurred while deleting the folder: {e}")
                    raise e
            else:
                print(f"The folder '{folder}' does not exist.")

    def execute(self):

        self.log_message(show=self._SHOW_LOG,
                         message='[TRANSFORM] | [SILVER] | STARTING', start=True, end=True, sep='=')

        spark = DeltaSpark().initialize()

        self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [SILVER] | READING RAW FILES', start=True)

        df = spark.read.schema(self._SCHEMA_RAW_DATA).json(bronze_path_raw_data, multiLine=True)

        self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [SILVER] | READING RAW FILES | OK', end=True)
        self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [SILVER] | VALIDANTING PK', start=True)

        assert df.select('id').groupBy('id').count().filter(col('count') > 1).collect() == [], 'ID IS DUPLICATED'

        self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [SILVER] | VALIDANTING PK | OK', end=True)
        self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [SILVER] | STARTING DATA TREATMENT', start=True)

        df = df.withColumn(
            'address',
            concat(
                when(col('address_1').isNull(), '').otherwise(concat(col('address_1'), lit(' | '))),
                when(col('address_2').isNull(), '').otherwise(col('address_2')),
                when(col('address_3').isNull(), '').otherwise(concat(lit(' | '), col('address_3'))),
            )
        ).drop('address_1', 'address_2', 'address_3')

        def remove_multiple_spaces(_string: str) -> str:
            """
            Remove extra spaces between the words

            :param _string: the string on wich we want the extra spaces to be removed
            :return: a string without the extra spaces
            """

            _string = str(_string).replace(' ', '<>').replace('><', '').replace('<>', ' ')
            return _string

        remove_multiple_spaces_udf = udf(lambda _string: remove_multiple_spaces(_string=_string), StringType())

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

        # THOUGH AN INITIAL ANALUYSIS THE COLUMN "state_province" IS THE SAME AS THE COLUMN "state", SO WE CAN DROP IT
        df = df.drop('state_province')

        self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [SILVER] | DATA TREATMENT | OK', end=True)


        spark.sql("CREATE DATABASE IF NOT EXISTS silver")


        self.log_message(
            show=self._SHOW_LOG,
            message='[TRANSFORM] | [SILVER] | CREATING BREWERY SILVER TABLE NAMED: silver.brewery', start=True)

        spark.sql("""
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
        """)

        lit_date = datetime.strptime(Now().now(), "%Y-%m-%dT%H:%M:%S").strftime('%Y-%m-%d')
        df = df.withColumn("date_ref_carga", lit(lit_date).cast(DateType()))
        del lit_date

        df.write\
          .format('delta')\
          .mode('overwrite')\
          .option('silver.brewery', './brew_project/warehouse/')\
          .saveAsTable('silver.brewery')

        # spark.sql("""TRUNCATE TABLE silver.brewery""")
        #
        # spark.sql("""
        # INSERT INTO silver.brewery
        # SELECT brewery_type, city, country, id, latitude, longitude, name, phone, postal_code, state, street, website_url, address
        # FROM silver_save
        # """)

        self.log_message(
            show=self._SHOW_LOG,
            message='[TRANSFORM] | [SILVER] | TABLE silver.brewery SUCCESFULLY CREATE AND DATA STORED',
            start=True, end=True)

        self._move_all_files(src_dir=f"{bronze_path_raw_data}/",
                             dst_dir=f"{bronze_path_bkp_raw_data}/",
                             days=2,
                             subfolder_name='BKP_' + self._TODAY_FORMATED)

        self._delete_folder(folder_path=bronze_path_bkp_raw_data, root_name="")

        self.log_message(show=self._SHOW_LOG, message='[TRANSFORM] | [SILVER] | FINISHING',
                         start=True, end=True, sep='=')

        spark.stop()
