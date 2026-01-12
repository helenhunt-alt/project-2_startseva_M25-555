# src/primitive_db/utils.py

import json
import os

from src.primitive_db.constants import DATA_DIR, META_FILE


def load_metadata():
    """
    Загружает метаданные таблиц из META_FILE.
    Возвращает словарь, пустой если файл не найден.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    try:
        with open(META_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {}


def save_metadata(data):
    """Сохраняет метаданные таблиц в META_FILE."""
    os.makedirs(DATA_DIR, exist_ok=True)
    with open(META_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)


def load_table_data(table_name):
    """
    Загружает данные таблицы table_name из JSON-файла.
    Возвращает пустой список, если файл не найден.
    """
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"{table_name}.json")
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return []


def save_table_data(table_name, data):
    """Сохраняет данные таблицы table_name в JSON-файл."""
    os.makedirs(DATA_DIR, exist_ok=True)
    path = os.path.join(DATA_DIR, f"{table_name}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)