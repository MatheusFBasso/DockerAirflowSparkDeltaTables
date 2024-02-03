from datetime import datetime
import pytz
from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip

# System variables for delta path
bronze_path_raw_data = './brew_project/bronze/raw_data'
bronze_path_bkp_raw_data = './brew_project/bronze/raw_data_bkp'
warehouse_dir = './brew_project/warehouse/'


class Now:
    ####################################################################################################################
    def __init__(self, show: bool = True):
        self.__show = show

    ####################################################################################################################
    @staticmethod
    def now(timezone='America/Sao_Paulo') -> str:
        return datetime.now(pytz.timezone(timezone)).strftime('%Y-%m-%dT%H:%M:%S')

    ####################################################################################################################
    def log_message(self, message: str, start: bool = False, end: bool = False, sep: str = '-',
                    line_length: int = 120, show: bool = True) -> None:

        if show:
            length_fill_line = line_length - len(message) - len(self.now()) - 4

            if start:
                print(f"""\n{sep * line_length}""")

            print(f"""{self.now()} | {message} |{sep * length_fill_line if length_fill_line <= 120 else ""}""")

            if end:
                print(f"""{sep * line_length}\n""")


########################################################################################################################
class DeltaSpark(Now):

    _SHOW = True

    ####################################################################################################################
    def __init__(self, app_name: str = 'BreweryProject', warehouse_dir: str = warehouse_dir):
        super().__init__()
        self._appName = app_name
        self._warehouse_dir = warehouse_dir

    ####################################################################################################################
    def initialize(self) -> SparkSession:
        self.log_message(show=self._SHOW, start=True, sep='=', message='STARTING SPARK')

        spark = SparkSession\
            .builder\
            .master('local')\
            .appName(self._appName)\
            .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')\
            .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')\
            .config('spark.sql.warehouse.dir', self._warehouse_dir)\
            .enableHiveSupport()

        spark = configure_spark_with_delta_pip(spark).getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')
        spark.conf.set("spark.sql.shuffle.partitions", 8)

        self.log_message(show=self._SHOW, end=True, sep='=', message='SPARK INITIALIZED')

        return spark

