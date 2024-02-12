from lib.utils.utils import Now, bronze_path_raw_data, bronze_path_raw_data_bkp
from datetime import datetime, timedelta
import shutil
import os
from glob import glob


class CleanRawData(Now):

    _SHOW = True
    _TODAY_FORMATED = datetime(datetime.now().year, datetime.now().month, datetime.now().day).strftime("%Y_%m_%d")

    ####################################################################################################################
    def __init__(self, raw_data_path: str = bronze_path_raw_data, raw_data_bkp_path: str = bronze_path_raw_data_bkp):
        self._raw_path = raw_data_path
        self._raw_bkp = raw_data_bkp_path

    ####################################################################################################################
    @staticmethod
    def _string_to_date(date_string: str, format_string: str = '%Y_%m_%d') -> datetime | None:

        """
        Converts a date string to a datetime object.

        Parameters:
        - date_string: The date as a string.
        - format_string: The format of the date string. Defaults to '%Y_%m_%d'.

        Returns:
        - A datetime object.
        """
        # --------------------------------------------------------------------------------------------------------------
        try:
            return datetime.strptime(date_string, format_string)
        except ValueError as e:
            print(f"An error occurred: {e}")
            return None

    ####################################################################################################################
    def _move_all_files(self, pattern: str = 'PART_*.json', subfolder_name: str = None) -> None:
        """
        Moves all files from src_dir to dst_dir that match the given pattern into a new subfolder.

        Parameters:
        - src_dir: The source directory from which to move files.
        - dst_dir: The destination directory to which to move files.
        - pattern: The pattern to match files against. Defaults to 'PART_*.json' which matches all files.
        - subfolder_name: The name of the new subfolder to create in the destination directory.

        Returns:
        - None
        """
        self.log_message(show=self._SHOW, message='CHECKING DIRECTORIES')
        # --------------------------------------------------------------------------------------------------------------
        new_dst_dir = os.path.join(self._raw_bkp, subfolder_name)
        os.makedirs(new_dst_dir, exist_ok=True)
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW, message='CHECKING DIRECTORIES SUCCESS')
        self.log_message(show=self._SHOW, message='STARTING TO MOVE BACKUP FILES')
        # --------------------------------------------------------------------------------------------------------------
        for file_name in glob(os.path.join(self._raw_path, pattern)):
            if os.path.isfile(file_name):
                shutil.copy(file_name, new_dst_dir)
                os.remove(file_name)
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(show=self._SHOW, message='BACKUP FILES MOVED SUCCESSFULLY')

    ####################################################################################################################
    def _delete_folder(self, days: float = 7):
        """
        Deletes a folder and all its contents.

        Parameters:
        - folder_path: The path to the folder to be deleted.

        Returns:
        - None
        """
        # --------------------------------------------------------------------------------------------------------------
        folder_path = [_file for _file in sorted(glob(self._raw_bkp + '/*')) if self._string_to_date(
            _file.replace('\\', '/')
                 .split('/')[-1]
                 .replace('BKP_', ''))
                       <=
                       (datetime(datetime.now().year, datetime.now().month, datetime.now().day) -
                        timedelta(days=days))]
        # --------------------------------------------------------------------------------------------------------------
        self.log_message(
            show=self._SHOW,
            message="""DELETING OLD BACKUP FILES. PARAM DAYS = {}""".format(days))
        # --------------------------------------------------------------------------------------------------------------

        for folder in folder_path:
            print(folder)

            if os.path.exists(folder):
                try:
                    shutil.rmtree(folder)
                    self.log_message(
                        show=self._SHOW,
                        message="""DELETED SUCCESSFULLY. FOLDER: {}""".format(
                            folder.split('/')[-1]))

                except Exception as e:
                    print(f"An error occurred while deleting the folder: {e}")
                    raise e
            else:
                print(f"The folder '{folder}' does not exist.")

    ####################################################################################################################
    def execute(self):

        self.log_message(show=self._SHOW, message="STARTING PROCESS", start=True, end=True, sep='=')

        # --------------------------------------------------------------------------------------------------------------
        self._move_all_files(subfolder_name='BKP_' + self._TODAY_FORMATED)
        self._delete_folder()
        # --------------------------------------------------------------------------------------------------------------

        self.log_message(show=self._SHOW, message="FINISHING PROCESS", start=True, end=True, sep='=')
