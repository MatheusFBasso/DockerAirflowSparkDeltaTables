from lib.utils.DivvyBikes.divvy_bikes_path import bronze_path_raw_data_bkp, bronze_path_raw_data
from lib.utils.Now import Now
from datetime import datetime
import requests
import json
import os


class DivvyBikes(Now):
    _SHOW_LOG = True

    STATION_STATUS = 'https://gbfs.lyft.com/gbfs/2.3/chi/en/station_status.json'
    STATION_INFORMATION = 'https://gbfs.lyft.com/gbfs/2.3/chi/en/station_information.json'
    FREE_BIKE_STATUS = 'https://gbfs.lyft.com/gbfs/2.3/chi/en/free_bike_status.json'
    SYSTEM_PRICING_PLANS = 'https://gbfs.lyft.com/gbfs/2.3/chi/en/system_pricing_plans.json'
    VEHICLE_TYPES = 'https://gbfs.lyft.com/gbfs/2.3/chi/en/vehicle_types.json'

    def __init__(self):
        print(f"┌──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐")
        print(f"│                                                                                                                      │")
        print(f"│                                                                                                                      │")
        print(f"│      ██████████    ███                                        ███████████   ███  █████                               │")
        print(f"│     ░░███░░░░███  ░░░                                        ░░███░░░░░███ ░░░  ░░███                                │")
        print(f"│      ░███   ░░███ ████  █████ █████ █████ █████ █████ ████    ░███    ░███ ████  ░███ █████  ██████   █████          │")
        print(f"│      ░███    ░███░░███ ░░███ ░░███ ░░███ ░░███ ░░███ ░███     ░██████████ ░░███  ░███░░███  ███░░███ ███░░           │")
        print(f"│      ░███    ░███ ░███  ░███  ░███  ░███  ░███  ░███ ░███     ░███░░░░░███ ░███  ░██████░  ░███████ ░░█████          │")
        print(f"│      ░███    ███  ░███  ░░███ ███   ░░███ ███   ░███ ░███     ░███    ░███ ░███  ░███░░███ ░███░░░   ░░░░███         │")
        print(f"│      ██████████   █████  ░░█████     ░░█████    ░░███████     ███████████  █████ ████ █████░░██████  ██████          │")
        print(f"│     ░░░░░░░░░░   ░░░░░    ░░░░░       ░░░░░      ░░░░░███    ░░░░░░░░░░░  ░░░░░ ░░░░ ░░░░░  ░░░░░░  ░░░░░░           │")
        print(f"│                                                  ███ ░███                                                            │")
        print(f"│                                                 ░░██████         █████████   ███████████  █████                      │")
        print(f"│                                                  ░░░░░░         ███░░░░░███ ░░███░░░░░███░░███                       │")
        print(f"│                                                                ░███    ░███  ░███    ░███ ░███                       │")
        print(f"│                                                                ░███████████  ░██████████  ░███                       │")
        print(f"│                                                                ░███░░░░░███  ░███░░░░░░   ░███                       │")
        print(f"│                                                                ░███    ░███  ░███         ░███                       │")
        print(f"│                                                                █████   █████ █████        █████ VERSION 1.0          │")
        print(f"│                                                               ░░░░░   ░░░░░ ░░░░░        ░░░░░                       │")
        print(f"│                                                                                                                      │")
        print(f"└──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘")

    def _save_file(self, save_data: list, path: str = bronze_path_raw_data, file_name: str = 'extracted_at_') -> None:
        """
        Saves the data to the specified path with the date informing Year, month, day ('%Y_%m_%d')

        :param save_data: the list of dicts containing the data to be saved
        :param file_name: the name of the file
        :return: Saves the data to the specified path with the date informing Year, month, day ('%Y_%m_%d')
        """

        self.log_message(show=self._SHOW_LOG, message='BEGINNING TO SAVE THE DATA TO BRONZE LAYER')

        file_name += datetime.today().strftime('%Y_%m_%dT%H_%M')

        if not os.path.exists(f"""{path}/"""):
            os.makedirs(f"""{path}/""")

        with open(f"""{path}/{file_name}.json""", 'w') as output:
            output.write('[' + ',\n'.join(json.dumps(_dict) for _dict in save_data) + ']\n')

        self.log_message(show=self._SHOW_LOG,
                         message="""DATA SAVED SUCCESSFULLY. FILE NAME: {}""".format(file_name))

    @staticmethod
    def all_status_list() -> list:
        return ['get_station_status', 'get_station_information', 'get_free_bike_status', 'get_system_pricing_plan',
                'get_vehicle_types']

    def get_station_status(self) -> None:
        self.log_message(show=self._SHOW_LOG, message='GETTING STATION SATUS', start=True)
        data = requests.get(url=self.STATION_STATUS).json()
        self.log_message(show=self._SHOW_LOG, message='GETTING STATION SATUS | OK', end=True)
        self._save_file(save_data=[data], path=f'{bronze_path_raw_data}/station_status', file_name='extracted_at_')

    def get_station_information(self) -> None:
        self.log_message(show=self._SHOW_LOG, message='GETTING STATION INFORMATION', start=True)
        data = requests.get(url=self.STATION_INFORMATION).json()
        self.log_message(show=self._SHOW_LOG, message='GETTING STATION INFORMATION | OK', end=True)
        self._save_file(save_data=[data], path=f'{bronze_path_raw_data}/station_information', file_name='extracted_at_')

    def get_free_bike_status(self) -> None:
        self.log_message(show=self._SHOW_LOG, message='GETTING STATION FREE BIKE STATUS', start=True)
        data = requests.get(url=self.FREE_BIKE_STATUS).json()
        self.log_message(show=self._SHOW_LOG, message='GETTING STATION FREE BIKE STATUS | OK', end=True)
        self._save_file(save_data=[data], path=f'{bronze_path_raw_data}/free_bike_status', file_name='extracted_at_')

    def get_system_pricing_plan(self) -> None:
        self.log_message(show=self._SHOW_LOG, message='GETTING STATION FREE BIKE STATUS', start=True)
        data = requests.get(url=self.SYSTEM_PRICING_PLANS).json()
        self.log_message(show=self._SHOW_LOG, message='GETTING STATION FREE BIKE STATUS | OK', end=True)
        self._save_file(save_data=[data], path=f'{bronze_path_raw_data}/system_pricing_plan', file_name='extracted_at_')

    def get_vehicle_types(self) -> None:
        self.log_message(show=self._SHOW_LOG, message='GETTING STATION FREE BIKE STATUS', start=True)
        data = requests.get(url=self.VEHICLE_TYPES).json()
        self.log_message(show=self._SHOW_LOG, message='GETTING STATION FREE BIKE STATUS | OK', end=True)
        self._save_file(save_data=[data], path=f'{bronze_path_raw_data}/vehicle_types', file_name='extracted_at_')

    def initialize(self, func: str):
        self.__getattribute__(func)()