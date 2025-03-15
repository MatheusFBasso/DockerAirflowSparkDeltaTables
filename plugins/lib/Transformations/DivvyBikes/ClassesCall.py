from lib.Transformations.DivvyBikes.TableCreation import SetDeltaTables
from lib.APIs.DivvyBikes_api import DivvyBikes
from lib.utils.DivvyBikes.CleanRawData import CleanRawData
from lib.Transformations.DivvyBikes.Bronze import Bronze
from lib.Transformations.DivvyBikes.BronzeRevised import BronzeRevised
from lib.Transformations.DivvyBikes.Silver import Silver
from lib.Transformations.DivvyBikes.Gold import Gold


class DivvyBikesCall:

    def __new__(cls, function: str, *args, **kwargs):

        def func_not_found(func):
            return (func for func in ()).throw(Exception(f"Function {func} not found."))

        map_function = {
            'clean_raw_data': cls.clean_raw_data_aux,
            'bronze_raw_data': cls.bronze_aux,
            'bronze_raw_data_revised_create': cls.bronze_revised_aux,
            'bronze_load_revised': cls.bronze_revised_load_data_aux,
            'divvy_set_delta_tables': cls.set_delta_tables_aux,

            'divvy_get_bike_status': cls.get_bike_status_aux,
            'silver_bike_status': cls.silver_bike_status_aux,
            'gold_bike_status': cls.gold_bike_status_aux,

            'divvy_get_station_information': cls.get_station_info_aux,
            'silver_station_information': cls.silver_station_info_aux,
            'gold_station_information': cls.gold_station_info_aux,

            'divvy_get_station_status': cls.get_station_status_aux,
            'silver_station_status': cls.silver_station_status_aux,
            'gold_station_status': cls.gold_station_status_aux,

            'divvy_get_system_pricing': cls.get_system_pricing_aux,
            'silver_system_pricing': cls.silver_system_pricing_aux,
            'gold_system_pricing': cls.gold_system_pricing_aux,

            'divvy_get_vehicle_types': cls.get_vehicle_types_aux,
            'silver_vehicle_types': cls.silver_vehicle_types_aux,
            'gold_vehicle_types': cls.gold_vehicle_types_aux,
        }

        return map_function.get(function, func_not_found)(args, kwargs)

    @staticmethod
    def clean_raw_data_aux(args, kwargs):
        dict_kwargs = kwargs
        return CleanRawData(sub_folder_path=dict_kwargs.get('sub_folder_path')).execute()

    @staticmethod
    def bronze_aux(args, kwargs):
        bronze = Bronze()
        bronze.load_all_data_to_bronze()
        del bronze
        return True

    @staticmethod
    def bronze_revised_aux(args, kwargs):
        bronze = BronzeRevised()
        bronze.create_raw_tables()
        del bronze

    @staticmethod
    def bronze_revised_load_data_aux(args, kwargs):
        bronze = BronzeRevised()
        dict_kwargs = kwargs
        bronze.load_raw_data_to_bronze(divvy_path=dict_kwargs.get('divvy_path'))
        del bronze, dict_kwargs
        return True

    @staticmethod
    def set_delta_tables_aux(args, kwargs):
        return SetDeltaTables().create_delta_tables()

    @staticmethod
    def get_bike_status_aux(args, kwargs):
        return DivvyBikes().initialize('get_free_bike_status')

    @staticmethod
    def silver_bike_status_aux(args, kwargs):
        return Silver().silver_bike_status()

    @staticmethod
    def gold_bike_status_aux(args, kwargs):
        return Gold().gold_bike_status()

    @staticmethod
    def get_station_info_aux(args, kwargs):
        return DivvyBikes().initialize('get_station_information')

    @staticmethod
    def silver_station_info_aux(args, kwargs):
        return Silver().silver_station_information()

    @staticmethod
    def gold_station_info_aux(args, kwargs):
        return Gold().gold_station_information()

    @staticmethod
    def get_station_status_aux(args, kwargs):
        return DivvyBikes().initialize('get_station_status')

    @staticmethod
    def silver_station_status_aux(args, kwargs):
        return Silver().silver_station_status()

    @staticmethod
    def gold_station_status_aux(args, kwargs):
        return Gold().gold_station_status()

    @staticmethod
    def get_system_pricing_aux(args, kwats):
        return DivvyBikes().initialize('get_system_pricing_plan')

    @staticmethod
    def silver_system_pricing_aux(args, kwats):
        return Silver().silver_system_pricing_plan()

    @staticmethod
    def gold_system_pricing_aux(args, kwats):
        return Gold().gold_system_pricing_plan()

    @staticmethod
    def get_vehicle_types_aux(args, kwargs):
        return DivvyBikes().initialize('get_vehicle_types')

    @staticmethod
    def silver_vehicle_types_aux(args, kwargs):
        return Silver().silver_vehicle_types()

    @staticmethod
    def gold_vehicle_types_aux(args, kwargs):
        return Gold().gold_vehicle_types()
