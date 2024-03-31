"""
Разбор файла логов 1С
"""
from datetime import date, datetime
import logging
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path
import sys
from io import TextIOWrapper
import yaml


def read_message(f: TextIOWrapper) -> list[str] | None:
    """
    Читаем сообщение из файла
    """

    # Получаем первые две строчки сообщения
    message = []
    for _ in range(2):
        try:
            file_line = next(f)
        except StopIteration:
            return None
        message.append(file_line)

    # Оставшиеся строки сообщения пропускаем
    for _ in range(2):
        try:
            file_line = next(f)
        except StopIteration:
            return None

    return message


def read_current_row(curr_dblogpath: str) -> int:
    """
    Получаем номер предпоследней считанной строки в файле
    """

    # Получаем путь к файлу
    workdatapath = Path("workdata.yml")

    # Если файл с текущей считанной строкой не существует, то начинаем читать сначала,
    # т.е. пропускаем две первые строчки
    if not workdatapath.exists():
        return 2

    # Открываем файл c номер последней считанной строки файла
    with open(workdatapath, "r", encoding="utf-8") as workdatafile:
        workdata = yaml.safe_load(workdatafile)
        
    if workdata["curr_log"] == curr_dblogpath:
        # Продолжаем читать текущий файл
        return workdata["curr_row"]
    else:
        # Если у нас новый файл, из которого читаем, то читать его надо сначала
        return 2


def write_current_row(last_read_row: int, curr_dblogpath: str) -> None:
    """
    Загружаем в файл 'workdata' номер предпоследней считанной строки файла
    """

    # Получаем путь к файлу
    workdatapath = Path("workdata.yml")

    workdata = {}
    workdata["curr_row"] = last_read_row
    workdata["curr_log"] = curr_dblogpath

    with open(workdatapath, "w", encoding="utf-8") as workdatafile:
        yaml.dump(workdata, workdatafile)


if __name__ == "__main__":

    # Проверим существование файла с конфигом
    config_file = Path("config.yml")
    if not config_file.exists():
        sys.stderr.write(
            "Отсутствует файл с настройками config.yml. Работа невозможна!")
        sys.exit(2)

    with open('config.yml', 'r', encoding="utf-8") as file:
        settings = yaml.safe_load(file)

    # Логируем текущие результаты обработки
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    # Создаем файл для записи логов работы обработки в полночь
    rotation_logger_handler = TimedRotatingFileHandler(
        filename=settings["logpath"],
        encoding='utf-8',
        when="M",
        interval=1,
        backupCount=5
    )
    rotation_logger_handler.setLevel(logging.DEBUG)

    rotation_logger_handler.suffix = '%Y%m%d'

    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    rotation_logger_handler.setFormatter(formatter)

    logger.addHandler(rotation_logger_handler)

    # Получаем текущую дату
    current_date = date.today().strftime('%Y%m%d')

    # Получаем путь к файлу логов
    dblogpath = Path(settings['dblogpath'], f"{current_date}000000.lgp")
    
    logger.info("Сформирован путь к файлу логов")

    # Проверяем существование файла, из которого будем читать
    if not dblogpath.exists():
        logger.warning("Файл '%s' не существует. Продолжение работы невозможно", dblogpath)
        sys.exit(3)

    # Список сообщений с ошибками
    messages_arr = []

    # Счетчик ошибок в файле
    curr_err_count = 0

    # Читаем с первой строки, дальше будем считывать из файла номер строки, на котором остановились
    # Когда дойдем до конца файла, curr_row будет содержать предпоследнюю строку файла,
    # это обеспечит корректное чтение файла при повторной записи обработки
    curr_row = read_current_row(str(dblogpath))

    logger.info("Загружен номер строки, с которой нужно начинать чтение файла")

    logger.info("Начало чтения файла %s", dblogpath)

    with open(dblogpath, "r", encoding="utf-8") as logfile:

        # Переходим к нужной строке в файле для продолжения чтения
        for _ in range(curr_row):
            next(logfile)

        for row in logfile:

            message_data = read_message(logfile)

            if message_data is None:
                break

            # Проверяем, содержит ли сообщение ошибку. Если ошибка есть, то обрабатываем сообщение,
            # иначе - пропускаем сообщение
            has_message_err = ",17,E," in message_data[1]

            # Увеличиваем счетчик считанных строк
            curr_row += 5

            if has_message_err:

                # Выбираем строку даты и времени возникновения ошибки из первой строки
                message_time = datetime.strptime(
                    message_data[0][1:-4], '%Y%m%d%H%M%S')
                
                # Выбираем индекс начала текста ошибки сообщения
                text_err_idx = message_data[1].find(",17,E,") + 6

                # Выбираем строку текста ошибки из второй строки
                message_text_err = message_data[1][text_err_idx:-3]

                # Массив из даты и текст ошибки сообщения
                message_data = [message_time, message_text_err]

                messages_arr.append(message_data)

                curr_err_count += 1

    logger.info(
        "Файл '%s000000.lgp' прочитан до конца. Количество сообщений с ошибками: %s",
        current_date,
        curr_err_count)

    # Сохраняем номер последней считанной строки в файл 'workdata'
    write_current_row(curr_row, str(dblogpath))

    logger.info("Номер последней считанной строки сохранен в файл 'workdata.yml'")

    # Записываем в файл сообщения с ошибками
    with open(settings["resultfile"], "a+", encoding="utf-8") as resultfile:
        for message_data in messages_arr:
            resultfile.write(f"{message_data[0]} {message_data[1]}\n")

    logger.info("Данные с ошибками сохранены в файл %s", settings["resultfile"])
