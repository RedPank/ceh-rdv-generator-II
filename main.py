import argparse
import ctypes
import logging
import os
import pathlib
import tkinter
import pandas as pd
from core.config import Config
from core.ui import MainWindow

format_str = "%(asctime)s %(levelname)s %(message)s"


class ColoredFormatter(logging.Formatter):
    COLORS = {'DEBUG': '\033[94m', 'INFO': '\033[92m', 'WARNING': '\033[93m',
              'ERROR': '\033[91m', 'CRITICAL': '\033[95m'}

    def format(self, record):
        log_fmt = f"{self.COLORS.get(record.levelname, '')}{format_str}\033[0m"
        formatter = logging.Formatter(log_fmt)
        return formatter.format(record)


program: str = "ceh-rdv-generator-II"
version: str = "1.0"

def main() -> int:

    parser = argparse.ArgumentParser(prog="ceh-rdv-generator-II")
    parser.add_argument(
        "-c", "--config",
        type=str,
        default='generator.yaml',
        help="Файл конфигурации"
    )
    args = parser.parse_args()

    # Файл настройки программы.
    config_name: str = os.path.abspath(args.config)
    Config.load_config(config_name=config_name)

    logging.basicConfig(level=logging.INFO, filename=Config.log_file, filemode="w",
                        format=format_str,
                        encoding='utf-8')

    if Config.colorlog:
        logging.getLogger().handlers[0].setFormatter(ColoredFormatter())

    print("ceh-rdv-generator-II")
    print("Формирование файлов потоков AirFlow для загрузки данных из внешних источников в слой RDV системы ЦЕХ")
    print(f"config={config_name}")
    print(f"log_file={Config.log_file}")

    logging.info('START')
    logging.info(f"config={config_name}")
    logging.info(f"log_file={Config.log_file}")
    logging.info(f'templates_path="{Config.templates_path}"')

    # Настройка pandas
    # Включение режима "копирование при записи"
    pd.options.mode.copy_on_write = True
    # Печатаем все колонки
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', 50)
    pd.set_option('display.width', 255)

    win = MainWindow()

    # Смена иконки программы
    ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("VTB.ceh.ceh-rdv-generator-II.1_0")
    resource_path = os.path.join(pathlib.Path(__file__).parent.resolve(), 'res')
    win.iconbitmap(os.path.join(resource_path, "ceh-icon.ico"))
    image = tkinter.PhotoImage(file=os.path.join(resource_path, "ceh-icon.png"))
    win.iconphoto(True, image)

    win.mainloop()

    logging.info('STOP')
    return 0


if __name__ == "__main__":
    exit_code = main()
    exit(exit_code)
else:
    exit(100)
