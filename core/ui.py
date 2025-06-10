import subprocess
import tkinter as tk
from tkinter import ttk, SOLID
from tkinter.messagebox import showinfo, showerror, showwarning
from tkinter import filedialog
import logging

from jinja2 import TemplateNotFound

from core.map_gen import mapping_generator
import core.exceptions as exp
from core.config import Config


class MainWindow(tk.Tk):
    def __init__(self):
        super().__init__()

        # Скрываем окно, что-бы не было "мелькания" при пере-позиционировании окна
        self.withdraw()

        author: str = Config.author
        self.file_path = tk.StringVar(value=Config.excel_file)

        self.env = Config.env

        self.wm_title("Генератор файлов описания src-rdv потока")

        frame = tk.Frame(
            self,    # Обязательный параметр, который указывает окно для размещения Frame.
            padx=5,  # Задаём отступ по горизонтали.
            pady=5,  # Задаём отступ по вертикали.
            borderwidth=1,
            relief="solid"
        )
        frame.pack(anchor='nw', fill='both', padx=5, pady=10)

        # file_path_text = ttk.Entry(
        #     frame,
        #     textvariable=self.file_path,
        #     font=("Arial", 10),
        #     state='readonly')
        # file_path_text.pack(fill=tk.X, padx=25, pady=10)

        # open_file_dialog_button = ttk.Button(
        #     frame,
        #     text="Выбрать EXCEL-файл с маппингом",
        #     command=self._setup_file_path
        # )
        # open_file_dialog_button.pack(fill=tk.X, padx=25, pady=0)

        # Дополнительная информация
        text_info: list = [f'Маппинг: {self.file_path.get()}\n',
                          '\n',
                          f'Конфиг:  {Config.config_file}\n',
                          f'Шаблоны: {Config.templates_path}\n',
                          '\n',
                          f'Каталог: {Config.out_path}\n',
                          f'Журнал:  {Config.log_file}\n',
                          f'Автор:   {author}\n'
                          ]
        self.info_text = tk.StringVar(value="".join(text_info))
        label_info = tk.Text(frame, font=("Courier New", 11), height=8)
        label_info.insert(index=tk.END, chars=self.info_text.get())
        label_info.configure(state="disabled")
        label_info.pack(fill="x", padx=25, pady=10)

        # Фрейм кнопок
        frame_key = tk.Frame(frame,  # Обязательный параметр, который указывает окно для размещения Frame.
                             padx=5,  # Задаём отступ по горизонтали.
                             pady=5,  # Задаём отступ по вертикали.
                             borderwidth=0,
                             relief="solid"
                             )
        frame_key.pack(anchor='nw', fill='both', padx=5, pady=5)
        frame_key.columnconfigure(0, weight=1)
        frame_key.columnconfigure(1, weight=1)
        frame_key.columnconfigure(2, weight=1)

        start_export_button = tk.Button(
            frame_key,
            text="Формировать",
            command=self._export_mapping
        )
        start_export_button.grid(row=0, column=0, sticky=tk.E, padx=10)

        view_log_button = tk.Button(
            frame_key,
            text="Открыть журнал",
            command=self._view_log
        )
        view_log_button.grid(row=0, column=1, sticky=tk.E, padx=10)

        exit_button = tk.Button(
            frame_key,
            text="Завершить",
            command=self.destroy
        )
        exit_button.grid(row=0, column=2, sticky=tk.E, padx=10)

    def _setup_file_path(self):
        initial_dir: str = Config.out_path
        filetypes = [('Excel files', '*.xls'), ('Excel files', '*.xlsx'), ('All files', '*.*')]
        title = "Выбор файла"

        file_path: str = filedialog.askopenfilename(filetypes=filetypes, initialdir=initial_dir, title=title)
        if file_path:
            self.file_path.set(file_path)

    @staticmethod
    def _view_log():
        subprocess.Popen(args=Config.log_viewer)

    def _export_mapping(self):
        msg: str

        if not self.file_path.get():
            showerror("Ошибка", "EXCEL-файл с описанием данных не определен")
            return

        if not all((
                self.file_path.get(),
                # self.out_path.get(),
                # self.author.get(),
        )):
            showerror("Ошибка", "Проверьте заполнение полей формы")
        else:
            try:

                mapping_generator(
                    file_path=self.file_path.get(),
                    out_path=Config.out_path
                )

                if Config.is_error:
                    msg = ("Во время обработки были ошибки.\n"
                           "Прочитайте описание ошибок (error) "
                           "в журнале работы программы!")
                    showerror("Ошибка", msg)
                    logging.info("Обработка завершена с ошибками")

                elif Config.is_warning:
                    msg = ("Обработка завершена c предупреждениями.\n"
                           "Прочитайте предупреждения (warning) "
                           "в журнале работы программы.")
                    showwarning("Предупреждение", msg)
                    logging.info("Обработка завершена с предупреждениями")

                else:
                    msg = ("Обработка завершена без ошибок.\n"
                           "Для получения подробной информации о сформированных потоках\n"
                           "прочитайте журнал работы программы.")
                    showinfo("Успешно", msg)
                    logging.info('Обработка завершена без ошибок')

            except (exp.IncorrectMappingException, ValueError) as err:
                logging.error(err)
                msg = f"Ошибка: {err}.\nПроверьте журнал работы программы."
                showerror(title="Ошибка", message=msg)

            except TemplateNotFound:
                msg = "Ошибка чтения шаблона.\nПроверьте журнал работы программы."
                logging.exception("Ошибка чтения шаблона")
                showerror(title="Ошибка", message=msg)

            except Exception as err:
                logging.exception("Обработка данных завершилась из-за необрабатываемой ошибки")
                print(f"Unexpected {err=}, {type(err)=}")

                msg = ("Во время обработки были ошибки.\n"
                       "Прочитайте описание ошибок (error) "
                       "в журнале работы программы!")
                showerror("Ошибка", msg)
                logging.info("Обработка завершена с ошибками")

                self.destroy()
                raise
