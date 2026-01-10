# src/primitive_db/parser.py

import shlex


def parse_condition(condition_str):
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
    return parse_condition(set_str)