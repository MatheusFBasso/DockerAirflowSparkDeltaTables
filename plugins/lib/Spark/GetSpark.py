from pyspark.sql import SparkSession


from delta.pip_utils import configure_spark_with_delta_pip
from lib.utils.Now import Now
from lib.utils.Brewery.brewery_paths import warehouse_dir


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
        builder = (SparkSession
                 .builder
                 .appName(self._appName)
                 .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
                 .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
                 .config('spark.sql.warehouse.dir', self._warehouseDir)
                 # .enableHiveSupport()
                 )
        # --------------------------------------------------------------------------------------------------------------
        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel('ERROR')
        spark.conf.set("spark.sql.shuffle.partitions", 500)
        # --------------------------------------------------------------------------------------------------------------
        delta_version_info = f' '.ljust(20, ' ')
        pyspark_version_info = f' '.ljust(20, ' ')
        # --------------------------------------------------------------------------------------------------------------
        print(f"┌{'─'*118}┐")
        print(f"│     █████████                            █████      {' ' * 65}│")
        print(f"│    ███░░░░░███                          ░░███       {' ' * 65}│")
        print(f"│   ░███    ░░░ ████████  ██████  ████████ ░███ █████ {' ' * 65}│")
        print(f"│   ░░█████████░░███░░███░░░░░███░░███░░███░███░░███  {' ' * 65}│")
        print(f"│    ░░░░░░░░███░███ ░███ ███████ ░███ ░░░ ░██████░   {' ' * 65}│")
        print(f"│    ███    ░███░███ ░██████░░███ ░███     ░███░░███  {' ' * 65}│")
        print(f"│   ░░█████████ ░███████░░█████████████    ████ █████ {delta_version_info}{' ' * 45}│")
        print(f"│    ░░░░░░░░░  ░███░░░  ░░░░░░░░░░░░░    ░░░░ ░░░░░                      {' ' * 45}│")
        print(f"│               ░███                                                      {' ' * 45}│")
        print(f"│               █████  ██████████░░░░░      ████  █████                   {' ' * 45}│")
        print(f"│                     ░░███░░░░███         ░░███ ░░███                    {' ' * 45}│")
        print(f"│                      ░███   ░░███  ██████ ░███ ███████   ██████         {' ' * 45}│")
        print(f"│                      ░███    ░███ ███░░███░███░░░███░   ░░░░░███        {' ' * 45}│")
        print(f"│                      ░███    ░███░███████ ░███  ░███     ███████        {' ' * 45}│")
        print(f"│                      ░███    ███ ░███░░░  ░███  ░███ ██████░░███        {' ' * 45}│")
        print(f"│                      ██████████  ░░██████ █████ ░░█████░░████████ {pyspark_version_info}{' ' * 31}│")
        print(f"│                     ░░░░░░░░░░    ░░░░░░ ░░░░░   ░░░░░  ░░░░░░░░{' ' * 53}│")
        print(f"└{'─'*118}┘")
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW, end=True, sep='=', message='SPARK | DELTA | INITIALIZED')

        return spark
