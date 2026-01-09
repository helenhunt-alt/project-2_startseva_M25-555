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