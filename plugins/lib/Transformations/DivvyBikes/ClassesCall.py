from lib.Transformations.DivvyBikes.TableCreation import SetDeltaTables
from lib.APIs.DivvyBikes_api import DivvyBikes


class DivvyBikesCall:

    def __new__(cls, function: str, *args, **kwargs):

        def func_not_found(func):
            return (func for func in ()).throw(Exception(f"Function {func} not found."))

        map_function = {
            'divvy_set_delta_tables': cls.set_delta_tables_aux,
            'divvy_get_bike_status': cls.get_bike_status_aux,
        }

        return map_function.get(function, func_not_found)(args, kwargs)

    @staticmethod
    def set_delta_tables_aux(args, kwargs):
        return SetDeltaTables().create_delta_tables()

    @staticmethod
    def get_bike_status_aux(args, kwargs):
        return DivvyBikes().initialize('get_free_bike_status')