from datetime import datetime
import pytz

class Now:

    ####################################################################################################################
    def __init__(self, show: bool = True):
        self._show = show

    ####################################################################################################################
    @staticmethod
    def now(timezone='America/Sao_Paulo') -> str:
        return datetime.now(pytz.timezone(timezone)).strftime('%Y-%m-%dT%H:%M:%S')

    @staticmethod
    def now_datetime(timezone='America/Sao_Paulo'):
        return datetime.now(pytz.timezone(timezone))

    ####################################################################################################################
    def log_message(self, message: str, start: bool = False, end: bool = False, sep: str = '-', line_length: int = 120,
                    show: bool = True) -> None:
        if show:
            length_fill_line = line_length - len(message) - len(self.now()) - 5

            if start:
                print(sep * line_length)

            print(f"""{self.now()} | {message} |{sep * length_fill_line if length_fill_line <= 120 else ""}""")

            if end:
                print(sep * line_length)