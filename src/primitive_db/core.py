# src/primitive_db/core.py

VALID_TYPES = {"int", "str", "bool"}

def create_table(metadata, table_name, columns):
    table_name = table_name.strip()
    if table_name in metadata:
        print(f'Ошибка: Таблица "{table_name}" уже существует.')
        return metadata

    table_columns = [("ID", "int")] 

    for col in columns:
        if ":" not in col:
            print(f'Некорректное значение: {col}. Попробуйте снова.')
            return metadata
        col_name, col_type = col.split(":", 1)
        col_name = col_name.strip()
        col_type = col_type.strip().lower()
        if col_type not in VALID_TYPES:
            print(f'Некорректное значение: {col}. Попробуйте снова.')
            return metadata
        table_columns.append((col_name, col_type))

    metadata[table_name] = table_columns
    print(f'Таблица "{table_name}" успешно создана со столбцами: ' +
          ", ".join(f"{name}:{typ}" for name, typ in table_columns))
    return metadata


def drop_table(metadata, table_name):
    table_name = table_name.strip()
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return metadata

    del metadata[table_name]
    print(f'Таблица "{table_name}" успешно удалена.')
    return metadata

def list_tables(metadata):
    if not metadata:
        print("Таблицы отсутствуют.")
    else:
        for table_name in metadata:
            print(f"- {table_name}")

def insert(metadata, table_name, table_data, values):
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return table_data

    columns = metadata[table_name]
    if len(values) != len(columns) - 1:
        print('Ошибка: количество значений не совпадает с количеством столбцов.')
        return table_data

    record = {}
    if table_data:
        new_id = max(row["ID"] for row in table_data) + 1
    else:
        new_id = 1
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
                        raise ValueError
                else:
                    val = bool(val)
            else:  # str
                val = str(val)
        except ValueError:
            print(f'Некорректное значение для столбца {col_name}: {values[i]}')
            return table_data
        record[col_name] = val

    table_data.append(record)
    print(f'Запись с ID={new_id} успешно добавлена в таблицу "{table_name}".')
    return table_data


def select(table_data, where_clause=None):
    if not table_data:
        return []

    if not where_clause:
        return table_data

    filtered = []
    for row in table_data:
        match = True
        for k, v in where_clause.items():
            if row.get(k) != v:
                match = False
                break
        if match:
            filtered.append(row)
    return filtered


def update(table_data, set_clause, where_clause):
    updated_count = 0
    for row in table_data:
        match = True
        for k, v in where_clause.items():
            if row.get(k) != v:
                match = False
                break
        if match:
            for k, v in set_clause.items():
                row[k] = v
            updated_count += 1

    if updated_count:
        print(f'{updated_count} запись(и) успешно обновлены.')
    else:
        print("Нет подходящих записей для обновления.")
    return table_data


def delete(table_data, where_clause):
    new_data = []
    deleted_count = 0
    for row in table_data:
        match = True
        for k, v in where_clause.items():
            if row.get(k) != v:
                match = False
                break
        if match:
            deleted_count += 1
            continue
        new_data.append(row)

    if deleted_count:
        print(f'{deleted_count} запись(и) успешно удалены.')
    else:
        print("Нет подходящих записей для удаления.")
    return new_data