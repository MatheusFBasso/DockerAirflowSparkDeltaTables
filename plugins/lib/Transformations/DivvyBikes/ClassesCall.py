from lib.Transformations.DivvyBikes.TableCreation import SetDeltaTables
from lib.APIs.DivvyBikes_api import DivvyBikes
from lib.utils.DivvyBikes.CleanRawData import CleanRawData
from lib.Transformations.DivvyBikes.Silver import Silver


class DivvyBikesCall:

    def __new__(cls, function: str, *args, **kwargs):

        def func_not_found(func):
            return (func for func in ()).throw(Exception(f"Function {func} not found."))

        map_function = {
            'clean_raw_data': cls.clean_raw_data_aux,
            'divvy_set_delta_tables': cls.set_delta_tables_aux,

            'divvy_get_bike_status': cls.get_bike_status_aux,
            'silver_bike_status': cls.silver_bike_status_aux,

            'divvy_get_station_information': cls.get_station_info_aux,
            'silver_station_information': cls.silver_station_info_aux,
        }

        return map_function.get(function, func_not_found)(args, kwargs)

    @staticmethod
    def set_delta_tables_aux(args, kwargs):
        return SetDeltaTables().create_delta_tables()

    @staticmethod
    def get_bike_status_aux(args, kwargs):
        return DivvyBikes().initialize('get_free_bike_status')

    @staticmethod
    def clean_raw_data_aux(args, kwargs):
        dict_kwargs = kwargs
        return CleanRawData(sub_folder_path=dict_kwargs.get('sub_folder_path')).execute()

    @staticmethod
    def silver_bike_status_aux(args, kwargs):
        return Silver().silver_bike_status()

    @staticmethod
    def get_station_info_aux(args, kwargs):
        return DivvyBikes().initialize('get_station_information')

    @staticmethod
    def silver_station_info_aux(args, kwargs):
        return Silver().silver_station_information()