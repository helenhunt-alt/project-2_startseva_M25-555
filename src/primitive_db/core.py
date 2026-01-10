# src/primitive_db/core.py

from src.decorators import (
    confirm_action,
    create_cacher,
    handle_db_errors,
    log_time,
)

VALID_TYPES = {"int", "str", "bool"}

cache_result = create_cacher()


@handle_db_errors
def create_table(metadata, table_name, columns):
    table_name = table_name.strip()
    if table_name in metadata:
        raise KeyError(f'Таблица "{table_name}" уже существует.')

    table_columns = [("ID", "int")] 

    for col in columns:
        if ":" not in col:
            raise ValueError(f'Некорректное значение: {col}. Формат <имя>:<тип>.')
        col_name, col_type = col.split(":", 1)
        col_name = col_name.strip()
        col_type = col_type.strip().lower()
        if col_type not in VALID_TYPES:
            raise ValueError(f'Некорректный тип для столбца {col_name}: {col_type}')
        table_columns.append((col_name, col_type))

    metadata[table_name] = table_columns
    print(f'Таблица "{table_name}" успешно создана со столбцами: ' +
          ", ".join(f"{name}:{typ}" for name, typ in table_columns))
    return metadata


@handle_db_errors
@confirm_action("удаление таблицы")
def drop_table(metadata, table_name):
    table_name = table_name.strip()
    if table_name not in metadata:
        raise KeyError(f'Таблица "{table_name}" не существует.')

    del metadata[table_name]
    print(f'Таблица "{table_name}" успешно удалена.')
    return metadata

def list_tables(metadata):
    if not metadata:
        print("Таблицы отсутствуют.")
    else:
        for table_name in metadata:
            print(f"- {table_name}")


@handle_db_errors
@log_time
def insert(metadata, table_name, table_data, values):
    if table_name not in metadata:
        raise KeyError(f'Таблица "{table_name}" не существует.')

    columns = metadata[table_name]
    if len(values) != len(columns) - 1:
        raise ValueError("Количество значений не совпадает с количеством столбцов.")

    record = {}
    new_id = max((row["ID"] for row in table_data), default=0) + 1
    record["ID"] = new_id

    for i, (col_name, col_type) in enumerate(columns[1:]):
        val = values[i]
        try:
            if col_type == "int":
                val = int(val)
            elif col_type == "bool":
                if isinstance(val, str):
                    val = val.lower()
                    if val in ("true", "1"):
                        val = True
                    elif val in ("false", "0"):
                        val = False
                    else:
                        raise ValueError(
                           f'Некорректное значение для столбца {col_name}: {values[i]}'
                        )
                else:
                    val = bool(val)
            else:
                val = str(val)
        except ValueError as e:
            raise ValueError(
                f'Некорректное значение для столбца {col_name}: {values[i]}'
                ) from e
        record[col_name] = val

    table_data.append(record)
    print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
    return table_data


@handle_db_errors
@log_time
def select(table_data, where_clause=None):
    if table_data is None or not table_data:
        raise ValueError("Таблица пуста, выбирать нечего.")

    key = str(where_clause) if where_clause else "all"

    def compute_result():
        if not where_clause:
            return table_data
        filtered = []
        for row in table_data:
            if all(row.get(k) == v for k, v in where_clause.items()):
                filtered.append(row)
        return filtered

    return cache_result(key, compute_result)


@handle_db_errors
def update(table_data, set_clause, where_clause):
    if table_data is None or not table_data:
        raise ValueError("Таблица пуста, обновлять нечего.")

    updated_count = 0
    for row in table_data:
        if all(row.get(k) == v for k, v in where_clause.items()):
            for k, v in set_clause.items():
                row[k] = v
            updated_count += 1

    if updated_count == 0:
        print("Ошибка валидации: Нет подходящих записей для обновления.")
        return table_data
    print(f'{updated_count} запись(и) успешно обновлены.')
    return table_data


@handle_db_errors
@confirm_action("удаление записей")
def delete(table_data, where_clause):
    if table_data is None or not table_data:
        raise ValueError("Таблица пуста, удалять нечего.")

    new_data = []
    deleted_count = 0
    for row in table_data:
        if all(row.get(k) == v for k, v in where_clause.items()):
            deleted_count += 1
        else:
            new_data.append(row)

    if deleted_count == 0:
        raise ValueError("Нет подходящих записей для удаления.")
    print(f'{deleted_count} запись(и) успешно удалены.')
    return new_data