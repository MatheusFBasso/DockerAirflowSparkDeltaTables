from lib.utils.utils import Now, bronze_path_raw_data
import requests
import json
from math import ceil
from datetime import datetime
from glob import glob
from collections import Counter


class IncorrectBrewType(Exception):
    pass


class BreweryAPI(Now):

    _TYPES = ['micro', 'nano', 'regional', 'brewpub', 'large', 'planning', 'bar', 'contract', 'proprietor', 'closed']
    _API_HTML = 'https://api.openbrewerydb.org/v1/breweries'
    _SHOW_LOG = True

    ####################################################################################################################
    def __init__(self, per_page: int = 50, brewery_id: str = None, brewery_type: str = None):
        super().__init__()

        if per_page < 40 or per_page > 100: raise ValueError('per_page must be between 40 and 100')
        if 1000 % per_page != 0: raise ValueError('per_page must be a multiple of 100 for saving the data')
        self.per_page = per_page

        if brewery_type is not None:
            if brewery_type.lower() not in self._TYPES:
                raise IncorrectBrewType('Type informed "{}" is not correct, please select one of the following: {}'\
                                        .format(brewery_type, ', '.join(self._TYPES)))
            else:
                self._type = brewery_type.lower()

    ####################################################################################################################
    def _get_request(self, endpoint: str, what_for: str, show_inner_log: bool = True) -> dict:
        """
        Send and analyze the request sent with the endpoint for a safer data import

        :param endpoint: specified endpoint for the request
        :param what_for: name of the request to be shown in the log
        :return: the json object from the request
        """
        if show_inner_log and self._SHOW_LOG:
            self.log_message(
                show=self._SHOW_LOG,
                message="""{} | [EXTRACT] | SENDING REQUEST FOR {}""".format(self.now(), str(what_for).upper()))

        response = requests.get(self._API_HTML + endpoint)

        if show_inner_log and self._SHOW_LOG:
            self.log_message(
                show=self._SHOW_LOG,
                message="""{} | [EXTRACT] | INITIAL ANALYSIS FOR {}""".format(self.now(), str(what_for).upper()))

        if response.status_code != 200:
            raise SystemError("""Return error {}: {}""".format(response.status_code, response.text))

        if not (isinstance(response.json(), dict) or isinstance(response.json(), list)):
            raise TypeError(
                """API returned an unexpected object, expected JSON but got {}""".format(type(response.json)))

        if show_inner_log and self._SHOW_LOG:
            self.log_message(
                show=self._SHOW_LOG,
                message="""[EXTRACT] | INITIAL ANALYSIS COMPLETE FOR {}""".format(str(what_for).upper()))

        return response.json()

    ####################################################################################################################
    def _get_pages_list(self) -> tuple[list[int], int]:
        """
        Create a list of values needed to extract the data from the API

        :return: a list containing the ranges needed for the extraction
        """

        total_numer_of_pages = self._get_request(endpoint='/meta', what_for='TOTAL AVAILABLE BREWERIES')

        self.log_message(show=self._SHOW_LOG, message='[EXTRACT] | CHECKING IF FIELD "total" IS IN THE RESPONSE')

        if 'total' not in total_numer_of_pages.keys(): raise TypeError(""""total" not found""")

        self.log_message(show=self._SHOW_LOG, message='[EXTRACT] | CHECKING IF FIELD "total" IS IN THE RESPONSE | OK')

        return [page + 1 for page in range(ceil(int(total_numer_of_pages.get('total')) / int(self.per_page)))], int(
            total_numer_of_pages.get('total'))

    ####################################################################################################################
    def _save_file(self, save_data: list, file_name: str = 'extracted_at_') -> None:
        """
        Saves the data to the specified path with the date informing Year, month, day ('%Y_%m_%d')

        :param save_data: the list of dicts containing the data to be saved
        :param file_name: the name of the file
        :return: Saves the data to the specified path with the date informing Year, month, day ('%Y_%m_%d')
        """

        self.log_message(show=self._SHOW_LOG, message='[LOAD] | BEGINNING TO SAVE THE DATA TO BRONZE LAYER')

        file_name += datetime.today().strftime('%Y_%m_%d')

        with open(f"""{bronze_path_raw_data}/{file_name}.json""", 'w') as output:
            output.write('[' + ',\n'.join(json.dumps(_dict) for _dict in save_data) + ']\n')

        self.log_message(show=self._SHOW_LOG,
                         message="""[LOAD] | DATA SAVED SUCCESSFULLY. FILE NAME: {}""".format(file_name))

    ####################################################################################################################
    def _files_validation(self, expected_total_responses: int) -> None:
        """Validate the files saved"""

        total_responses = 0
        response_per_file = []

        self.log_message(show=self._SHOW_LOG, message='[LOAD] | STARTING FILE VALIDATION')

        _files = sorted(list(glob(f'''{bronze_path_raw_data}/*''')))

        for _num, _dir in enumerate(_files):
            with open(_dir) as json_file:
                data = json.load(json_file)
                response_per_file.append(len(data))
                total_responses += len(data)

        if total_responses != expected_total_responses:
            raise ValueError(
                'Expected total_responses {} but got {}'.format(expected_total_responses, total_responses))

        response_per_file = Counter(response_per_file)
        response_per_file = [
            '{} {} WITH {} BREWERIES'.format(response_per_file.get(n),
                                             'FILE' if response_per_file.get(n) == 1 else 'FILES', n)
            for n in response_per_file.keys()]

        self.log_message(
            show=self._SHOW_LOG,
            message="""[LOAD] | TOTAL NUMBER OF FILES SAVED: {}\n{}{}\n{}TOTAL NUMBER OF BREWERIES: {}""".format(
                len(_files),
                ' ' * 31,
                f"\n{' ' * 31}".join(response_per_file),
                ' ' * 31,
                total_responses))

        self.log_message(show=self._SHOW_LOG, message='[LOAD] | FILE VALIDATION FINISHED')

    ####################################################################################################################
    def extract_data(self) -> None:
        """
        extract and saves the data from all the Breweries
        """

        print(f'{"░" * 25}░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░░{"░" * 25}')
        print(f'{"░" * 25}░        ░░  ░░░░  ░░        ░░       ░░░░      ░░░░      ░░░        ░{"░" * 25}')
        print(f'{"▒" * 25}▒  ▒▒▒▒▒▒▒▒▒  ▒▒  ▒▒▒▒▒▒  ▒▒▒▒▒  ▒▒▒▒  ▒▒  ▒▒▒▒  ▒▒  ▒▒▒▒  ▒▒▒▒▒  ▒▒▒▒{"▒" * 25}')
        print(f'{"▓" * 25}▓      ▓▓▓▓▓▓    ▓▓▓▓▓▓▓  ▓▓▓▓▓       ▓▓▓  ▓▓▓▓  ▓▓  ▓▓▓▓▓▓▓▓▓▓▓  ▓▓▓▓{"▓" * 25}')
        print(f'{"█" * 25}█  █████████  ██  ██████  █████  ███  ███        ██  ████  █████  ████{"█" * 25}')
        print(f'{"█" * 25}█        ██  ████  █████  █████  ████  ██  ████  ███      ██████  ████{"█" * 25}')
        print(f'{"█" * 25}██████████████████████████████████████████████████████████████████████{"█" * 25}')

        data = []

        self.log_message(show=self._SHOW_LOG, message='[EXTRACT] | STARTING EXTRACTION')

        pages, total = self._get_pages_list()
        report_num = 1

        self.log_message(show=self._SHOW_LOG, message='[EXTRACT] | STARTING PAGE EXTRACTION')

        for num, page in enumerate(pages):
            data += self._get_request(endpoint='?page={}&per_page={}'.format(page, self.per_page), what_for='MAIN',
                                      show_inner_log=False)

            if num % 2 == 0:
                self.log_message(
                    show=self._SHOW_LOG,
                    message="""[EXTRACT] | {:.2f}% [{}]""".format(
                        num / max(pages) * 100,
                        '=' * int(num / max(pages) * 10 - 1) + '>' + ' ' * int(10 - num / max(pages) * 10),
                        end="\r"))

            if num == max(pages) - 1:
                self.log_message(show=self._SHOW_LOG, message='[EXTRACT] | 100% [==========]')

                self.log_message(
                    show=self._SHOW_LOG,
                    message="""[LOAD] | SAVING PAGE {} OF {}""".format(num + 1, max(pages)))

                self._save_file(save_data=data, file_name='PART_{}_AT_'.format(report_num))
                del data
                break

            if len(data) % 1000 == 0:
                self.log_message(
                    show=self._SHOW_LOG,
                    message="""[LOAD] | SAVING PAGE {} OF {}""".format(num + 1, max(pages)))



                self._save_file(save_data=data, file_name='PART_{}_AT_'.format(report_num))
                data = []
                report_num += 1

        self.log_message(
            show=self._SHOW_LOG,
            message="""[EXTRACT] | EXTRACTION COMPLETED. {} BREWERIES IN {} PAGES""".format(
                total, max(pages)))

        self._files_validation(expected_total_responses=total)
