import pyspark
pyspark_version = pyspark.__version__

from pyspark.sql import SparkSession
from delta.pip_utils import configure_spark_with_delta_pip
from lib.utils.utils import Now, warehouse_dir


class DeltaSpark(Now):

    _SHOW = True

    ####################################################################################################################
    def __init__(self, app_name: str = 'PySparkDelta', warehouse: str = warehouse_dir):
        super().__init__()
        self._appName = app_name
        self._warehouseDir = warehouse

    ####################################################################################################################
    def initialize(self):

        self.log_message(show=self._SHOW, start=True, sep='=', message='STARTING SPARK')

        # --------------------------------------------------------------------------------------------------------------
        spark = (SparkSession
                 .builder
                 .appName(self._appName)
                 .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                 .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                 .config('spark.sql.warehouse.dir', self._warehouseDir)
                 .enableHiveSupport()
                 )
        # --------------------------------------------------------------------------------------------------------------
        spark = configure_spark_with_delta_pip(spark).getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')
        spark.conf.set("spark.sql.shuffle.partitions", 8)
        # --------------------------------------------------------------------------------------------------------------
        delta_version_info = f' Version = 3.1.0 '.center(22, ' ')
        pyspark_version_info_1 = str(pyspark_version)
        pyspark_version_info = f'Version = {pyspark_version_info_1} '.ljust(19, ' ')
        # --------------------------------------------------------------------------------------------------------------
        print('_'*120)
        print(' ' * 120)
        print(f"""{' ' * 9}______      _____                  _      _  ______     _ _          _  ______               _       """)
        print(f"""{' ' * 9}| ___ \\    /  ___|                | |    | | |  _  \\   | | |        | | | ___ \\             | |      """)
        print(f"""{' ' * 9}| |_/ /   _\ `--. _ __   __ _ _ __| | __ | | | | | |___| | |_ __ _  | | | |_/ /___  __ _  __| |_   _ """)
        print(f"""{' ' * 9}|  __/ | | |`--. \\ '_ \ / _` | '__| |/ / | | | | | / _ \\ | __/ _` | | | |    // _ \\/ _` |/ _` | | | |""")
        print(f"""{' ' * 9}| |  | |_| /\__/ / |_) | (_| | |  |   <  | | | |/ /  __/ | || (_| | | | | |\ \  __/ (_| | (_| | |_| |""")
        print(f"""{' ' * 9}\\_|   \\__, \\____/| .__/ \\__,_|_|  |_|\\_\\ | | |___/ \\___|_|\__\\__,_| | | \\_| \\_\\___|\\__,_|\\__,_|\\__, |""")
        print(f"""{' ' * 9}       __/ |     | |                     | |                        | |                         __/ |""")
        print(f"""{' ' * 9}      |___/      |_| {pyspark_version_info} |_| {delta_version_info} |_|                        |___/ """)
        print('_' * 120)
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW, end=True, sep='=', message='SPARK |DELTA | INITIALIZED')

        return spark
