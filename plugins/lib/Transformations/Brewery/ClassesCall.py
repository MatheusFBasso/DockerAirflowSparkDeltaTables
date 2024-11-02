from lib.Transformations.Brewery.TableCreation import SetDeltaTables
from lib.APIs.Brewery_api import BreweryAPI
from lib.utils.Brewery.CleanRawData import CleanRawData
from lib.Transformations.Brewery.Silver import BrewerySilver
from lib.Transformations.Brewery.Gold import GoldTables


class BreweryClasses:

    def __new__(cls, function: str, *args, **kwargs):

        def func_not_found(func):
            return (func for func in ()).throw(Exception(f"Function {func} not found."))

        map_function = {
            'brew_set_delta_tables': cls.set_brewery_delta_tables,
            'brew_api_extract': cls.brew_extract_api_aux,
            'brew_clean_raw_data': cls.brew_clean_raw_aux,
            'brew_silver': cls.brew_silver_aux,
            'gold_brewery_types_total': cls.gold_brewery_type_total_aux,
        }

        return map_function.get(function, func_not_found)(args, kwargs)

    @staticmethod
    def set_brewery_delta_tables(args, kwargs):
        return SetDeltaTables().create_delta_tables()


    @staticmethod
    def brew_extract_api_aux(args, kwargs):
        return BreweryAPI().extract_data()


    @staticmethod
    def brew_clean_raw_aux(args, kwargs):
        return CleanRawData().execute()


    @staticmethod
    def brew_silver_aux(args, kwargs):
        return BrewerySilver().execute()

    @staticmethod
    def gold_brewery_type_total_aux(args, kwargs):
        return GoldTables().brewery_type_total()