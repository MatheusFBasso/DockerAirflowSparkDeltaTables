from lib.brewery.delta.clean_raw_data import CleanRawData
from lib.brewery.delta.raw_files_to_silver import BrewerySilver
from lib.brewery.delta.SetDeltaTables import SetDeltaTables
from lib.brewery.delta.silver_to_gold import BreweryGold
from lib.brewery.api_breweries.get_data_breweries import BreweryAPI


class BreweryClasses:

    def __new__(cls, function: str, *args, **kwargs):
        # --------------------------------------------------------------------------------------------------------------
        def func_not_found(func):
            return (func for func in ()).throw(Exception(f"Function {func} not found."))
        # --------------------------------------------------------------------------------------------------------------

        map_function = {
            'brew_extract_api': cls.brew_extract_api_aux,
            'brew_clean_bronze': cls.brew_clean_bronze_aux,
            'brew_set_delta_tables': cls.brew_set_delta_tables_aux,
            'brew_silver': cls.brew_silver_aux,
            'brew_gold': cls.brew_gold_aux,
        }

        return map_function.get(function, func_not_found)(args, kwargs)

    # --------------------------------------------------------------------------------------------------------------
    @staticmethod
    def brew_extract_api_aux(args, kwargs):
        return BreweryAPI().extract_data()

    # --------------------------------------------------------------------------------------------------------------
    @staticmethod
    def brew_silver_aux(args, kwargs):
        return BrewerySilver().execute()

    # --------------------------------------------------------------------------------------------------------------
    @staticmethod
    def brew_gold_aux(args, kwargs):
        return BreweryGold().execute()

    # --------------------------------------------------------------------------------------------------------------
    @staticmethod
    def brew_set_delta_tables_aux(args, kwargs):
        return SetDeltaTables().create_delta_tables()

    # --------------------------------------------------------------------------------------------------------------
    @staticmethod
    def brew_clean_bronze_aux(args, kwargs):
        return CleanRawData().execute()
