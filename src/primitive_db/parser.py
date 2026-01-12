# src/primitive_db/parser.py

import shlex


def parse_condition(condition_str):
    """
    Преобразует строку условия вида 'col=val' в словарь {col: val}.
    Поддерживает int, float, bool и str.
    Возвращает пустой словарь при некорректном формате.
    """
    if not condition_str:
        return {}

    tokens = shlex.split(condition_str)
    if len(tokens) != 3 or tokens[1] != "=":
        print(f'Некорректное условие: {condition_str}')
        return {}

    column = tokens[0]
    value = tokens[2]

    if value.lower() == "true":
        value = True
    elif value.lower() == "false":
        value = False
    else:
        try:
            if "." in value:
                value = float(value)
            else:
                value = int(value)
        except ValueError:
            pass

    return {column: value}


def parse_set_clause(set_str):
    """
    Преобразует строку вида 'col1=val1, col2=val2' в словарь {col1: val1, col2: val2}.
    Использует parse_condition для обработки каждого элемента.
    Возвращает пустой словарь при ошибке формата.
    """
    result = {}
    parts = [p.strip() for p in set_str.split(",")]
    for part in parts:
        condition = parse_condition(part)
        if not condition:
            return {}
        result.update(condition)
    return result