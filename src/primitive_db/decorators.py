# src/decorators.py

import time
from functools import wraps


def handle_db_errors(func):
    """Обрабатывает ошибки базы данных при вызове функции."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except FileNotFoundError:
            print(
                "Ошибка: файл данных не найден. "
                "Возможно, база данных не инициализирована."
            )
        except KeyError as e:
            print(f"Ошибка: таблица или столбец {e} не найден.")
        except ValueError as e:
            print(f"Ошибка валидации: {e}")
        except Exception as e:
            print(f"Произошла непредвиденная ошибка: {e}")
    return wrapper


def confirm_action(action_name):
    """Запрашивает подтверждение пользователя перед выполнением функции."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            answer = input(f'Вы уверены, что хотите выполнить "{action_name}"? [y/n]: ')
            if answer.lower() != 'y':
                print(f'Операция "{action_name}" отменена.')
                if args:
                    return args[0]
                return None
            return func(*args, **kwargs)
        return wrapper
    return decorator


def log_time(func):
    """Выводит время выполнения функции в секундах."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.monotonic()
        result = func(*args, **kwargs)
        end = time.monotonic()
        duration = end - start
        print(f'Функция {func.__name__} выполнилась за {duration:.3f} секунд.')
        return result
    return wrapper


def create_cacher():
    """Создаёт замыкание для кэширования результатов функций."""
    cache = {}

    def cache_result(key, value_func):
        if key in cache:
            return cache[key]
        result = value_func()
        cache[key] = result
        return result
    cache_result.clear = cache.clear
    return cache_result