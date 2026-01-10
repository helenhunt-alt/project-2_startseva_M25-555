# src/primitive_db/engine.py

import shlex

import prompt
from prettytable import PrettyTable

from src.primitive_db.core import (
    create_table,
    delete,
    drop_table,
    insert,
    list_tables,
    select,
    update,
)
from src.primitive_db.parser import parse_condition, parse_set_clause
from src.primitive_db.utils import (
    load_metadata,
    load_table_data,
    save_metadata,
    save_table_data,
)


def print_help():
    print("\n*** Процесс работы с примитивной БД ***")
    print("\nКоманды управления таблицами:")
    print(
        "create_table <имя_таблицы> <столбец1:тип> <столбец2:тип> ... "
        "- создать таблицу"
    )
    print("list_tables - показать список всех таблиц")
    print("drop_table <имя_таблицы> - удалить таблицу")
    print("info <имя_таблицы> - информация о таблице")

    print("\nКоманды работы с данными:")
    print(
        "insert into <имя_таблицы> values (<значение1>, <значение2>, ...) "
        "- добавить запись"
    )
    print(
        "select from <имя_таблицы> [where <столбец = значение>] "
        "- выбрать записи"
    )
    print(
        "update <имя_таблицы> set <столбец = значение> "
        "where <столбец = значение> - обновить записи"
    )
    print(
        "delete from <имя_таблицы> where <столбец = значение> "
        "- удалить записи"
    )

    print("\nСлужебные команды:")
    print("help - показать справку")
    print("exit - выход из программы\n")


def print_table(data):
    if not data:
        print("Нет записей для отображения.")
        return
    headers = data[0].keys()
    table = PrettyTable()
    table.field_names = headers
    for row in data:
        table.add_row([row[h] for h in headers])
    print(table)


def run():
    print_help()

    while True:
        user_input = prompt.string(">>>Введите команду: ").strip()
        if not user_input:
            continue

        try:
            args = shlex.split(user_input)
        except ValueError:
            print(f"Некорректное значение: {user_input}. Попробуйте снова.")
            continue

        command = args[0].lower()
        metadata = load_metadata()

        if command == "exit":
            print("Выход из программы...")
            break
        elif command == "help":
            print_help()

        elif command == "create_table":
            if len(args) < 2:
                print(
                    "Некорректное значение: отсутствует имя таблицы. "
                    "Попробуйте снова."
                )
                continue
            table_name = args[1]
            columns = args[2:]
            metadata = create_table(metadata, table_name, columns)
            save_metadata(metadata)

        elif command == "drop_table":
            if len(args) != 2:
                print(f"Некорректное значение: {' '.join(args[1:])}. Попробуйте снова.")
                continue
            table_name = args[1]
            metadata_new = drop_table(metadata, table_name)
            if metadata_new is not None:
                metadata = metadata_new
                save_metadata(metadata)

        elif command == "list_tables":
            list_tables(metadata)

        elif command == "insert":
            if (
                len(args) < 4 
                or args[1].lower() != "into" 
                or args[3].lower() != "values"
            ):
                print(
                    "Некорректная команда insert. "
                    "Формат: insert into <table> values (<values>)"
                )
                continue
            table_name = args[2]
            values_str = user_input[user_input.find("(")+1:user_input.rfind(")")]
            values = [v.strip().strip('"').strip("'") for v in values_str.split(",")]
            table_data = load_table_data(table_name)
            table_data = insert(metadata, table_name, table_data, values)
            save_table_data(table_name, table_data)

        elif command == "select":
            if len(args) < 3 or args[1].lower() != "from":
                print(
                    "Некорректная команда select. "
                    "Формат: select from <table> [where <condition>]"
                )
                continue
            table_name = args[2]
            table_data = load_table_data(table_name)
            where_clause = None
            if "where" in args:
                condition_str = user_input.split("where",1)[1].strip()
                where_clause = parse_condition(condition_str)
            result = select(table_data, where_clause)
            if result is not None:
                print_table(result)

        elif command == "update":
            if len(args) < 6 or args[2].lower() != "set" or "where" not in args:
                print(
                    "Некорректная команда update. "
                    "Формат: update <table> set <col=val,...> where <condition>"
                )
                continue
            table_name = args[1]
            set_str = user_input.split("set",1)[1].split("where")[0].strip()
            where_str = user_input.split("where",1)[1].strip()
            set_clause = parse_set_clause(set_str)
            where_clause = parse_condition(where_str)
            table_data = load_table_data(table_name)
            table_data = update(table_data, set_clause, where_clause)
            save_table_data(table_name, table_data)

        elif command == "delete":
            if len(args) < 5 or args[1].lower() != "from" or "where" not in args:
                print(
                "Некорректная команда delete. "
                "Формат: delete from <table> where <condition>"
                )
                continue
            table_name = args[2]
            where_str = user_input.split("where",1)[1].strip()
            where_clause = parse_condition(where_str)
            table_data = load_table_data(table_name)
            if not table_data:  # <-- минимальная проверка
                print("Ошибка валидации: Таблица пуста, удалять нечего.")
                continue
            table_data_new = delete(table_data, where_clause)
            if table_data_new is not None:
                save_table_data(table_name, table_data_new)

        elif command == "info":
            if len(args) != 2:
                print("Некорректная команда info. Формат: info <table>")
                continue
            table_name = args[1]
            if table_name not in metadata:
                print(f'Ошибка: Таблица "{table_name}" не существует.')
                continue
            table_data = load_table_data(table_name)
            columns = metadata[table_name]
            print(f"Таблица: {table_name}")
            print("Столбцы: " + ", ".join(f"{name}:{typ}" for name, typ in columns))
            print(f"Количество записей: {len(table_data)}")

        else:
            print(f"Функции {command} нет. Попробуйте снова.")