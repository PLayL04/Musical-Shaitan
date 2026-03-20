#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import shutil
import re
import logging
import configparser
import threading
import time
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import Counter
from functools import partial

# Проверка наличия обязательных библиотек
try:
    import acoustid
except ImportError:
    print("Ошибка: библиотека acoustid не установлена. Установите: pip install pyacoustid")
    sys.exit(1)

try:
    from mutagen import File
except ImportError:
    print("Ошибка: библиотека mutagen не установлена. Установите: pip install mutagen")
    sys.exit(1)

# Конфигурация
CONFIG_FILE = 'config.ini'
DEFAULT_CONFIG = {
    'PATHS': {
        'SOURCE_DIR': r'D:\Music\Source',
        'DEST_DIR': r'D:\Music\Sorted',
        'TRASH_DIR': r'D:\Music\Duplicates'
    },
    'SETTINGS': {
        'MAX_WORKERS': 'auto', # 'auto' или число
        'LOG_FILE': 'music_sorter.log',
        'FPCALC_PATH': 'D:\Libs\fpcalc.exe' # Путь к fpcalc, если не в PATH
    }
}

# Глобальные переменные
FPCALC_PATH = None
DEST_DIR = None
stats_lock = threading.Lock()
stats = Counter()
mkdir_lock = threading.Lock()

def load_config():
    """Загружает конфигурацию из файла или создаёт с значениями по умолчанию."""
    config = configparser.ConfigParser()
    if Path(CONFIG_FILE).exists():
        config.read(CONFIG_FILE, encoding='utf-8')
    else:
        # Создаём конфиг по умолчанию
        for section, options in DEFAULT_CONFIG.items():
            config[section] = options
        with open(CONFIG_FILE, 'w', encoding='utf-8') as f:
            config.write(f)
        print(f"Создан файл конфигурации {CONFIG_FILE}. Отредактируйте его при необходимости.")
    return config

def setup_logging(log_file):
    """Настройка логирования."""
    logging.basicConfig(
        filename=log_file,
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        encoding='utf-8'
    )
    # Добавляем обработчик для ошибок в консоль (только критические)
    console = logging.StreamHandler()
    console.setLevel(logging.ERROR)
    console.setFormatter(logging.Formatter('%(levelname)s: %(message)s'))
    logging.getLogger('').addHandler(console)

def sanitize_name(name):
    """
    Заменяет недопустимые для Windows символы на '_'.
    Гарантирует, что результат не пустой (возвращает 'Unknown' при необходимости).
    """
    if not name:
        return "Unknown"
    safe = re.sub(r'[<>:"/\\|?*]', '_', str(name))
    safe = safe.strip()
    # Если после замены осталась только строка из '_' или пусто, заменяем на 'Unknown'
    if not safe or safe.replace('_', '') == '':
        return "Unknown"
    return safe

def fix_encoding(text):
    """
    Улучшенное исправление кодировки.
    Если исходный текст не содержит русских букв, пробуем перекодировать из latin1 в cp1251.
    Если после перекодировки появляются русские буквы, возвращаем исправленный вариант,
    иначе оставляем исходный.
    """
    if not text:
        return "Unknown"
    text = str(text).strip()
    # Если уже есть русские буквы, считаем, что кодировка правильная
    if re.search(r'[а-яА-ЯёЁ]', text):
        return text
    # Пробуем перекодировать
    try:
        fixed = text.encode('latin1').decode('cp1251')
        if re.search(r'[а-яА-ЯёЁ]', fixed):
            return fixed
    except Exception:
        pass
    return text

def get_quality_score(file_path):
    """Оценка качества файла (битрейт + бонус для lossless)."""
    try:
        audio = File(file_path)
        if not audio or not hasattr(audio.info, 'bitrate'):
            return 0
        bitrate = getattr(audio.info, 'bitrate', 0) or 0
        # Расширенный список lossless форматов
        lossless_ext = {'.flac', '.wav', '.alac', '.ape', '.wv', '.aiff', '.dff', '.dsf'}
        if file_path.suffix.lower() in lossless_ext:
            return 1_000_000 + bitrate
        return bitrate
    except Exception as e:
        logging.error(f"Ошибка при получении качества {file_path}: {e}")
        return 0

def get_fingerprint_data(file_path, fpcalc_path=None):
    """Получение акустического отпечатка с возможностью указания пути к fpcalc."""
    try:
        kwargs = {}
        if fpcalc_path:
            kwargs['fpcalc'] = fpcalc_path
        duration, fingerprint = acoustid.fingerprint_file(str(file_path), **kwargs)
        with stats_lock:
            stats['fingerprint_ok'] += 1
        return {
            'fp': fingerprint,
            'dur': duration,
            'path': file_path,
            'score': get_quality_score(file_path)
        }
    except Exception as e:
        logging.error(f"Ошибка при создании отпечатка для {file_path}: {e}")
        with stats_lock:
            stats['fingerprint_error'] += 1
        return None

def organize_file(file_path, dest_root):
    """
    Копирует файл в целевую структуру папок на основе тегов.
    Возвращает путь к скопированному файлу или None при ошибке.
    """ 
    try:
        audio = File(file_path, easy=True)
        dest = Path(dest_root)

        if not audio:
            # Нет тегов
            target_folder = dest / "!НетДанных"
            artist = album = title = track = None
            new_name = file_path.name
        else:
            # Безопасное извлечение тегов
            artist_list = audio.get('artist')
            artist = fix_encoding(artist_list[0]) if artist_list and len(artist_list) > 0 else "Unknown"

            album_list = audio.get('album')
            album = fix_encoding(album_list[0]) if album_list and len(album_list) > 0 else "Unknown"

            title_list = audio.get('title')
            title = fix_encoding(title_list[0]) if title_list and len(title_list) > 0 else "Unknown"

            # Обработка номера трека (может быть строкой или списком)
            track_raw = audio.get('tracknumber')
            if track_raw:
                if isinstance(track_raw, list) and len(track_raw) > 0:
                    track_str = str(track_raw[0])
                else:
                    track_str = str(track_raw)
                track = track_str.split('/')[0].strip().zfill(2)
            else:
                track = "00"

            target_folder = dest / sanitize_name(album) / sanitize_name(artist)
            new_name = f"{sanitize_name(track)}. {sanitize_name(title)}{file_path.suffix}"

        # Используем глобальный mkdir_lock и оборачиваем в него весь процесс записи
        with mkdir_lock:
            target_folder.mkdir(parents=True, exist_ok=True)

            # Генерация уникального имени внутри блокировки, чтобы избежать конфликтов
            final_path = target_folder / new_name
            counter = 1
            while final_path.exists():
                stem = final_path.stem
                base = re.sub(r'\s\(\d+\)$', '', stem)
                new_stem = f"{base} ({counter})"
                final_path = target_folder / f"{new_stem}{file_path.suffix}"
                counter += 1

            # Копируем файл, пока замок закрыт для других потоков
            shutil.copy2(file_path, final_path)
        
        logging.info(f"Скопирован: {file_path} -> {final_path}")
        with stats_lock:
            stats['organized_ok'] += 1
        return final_path
    except Exception as e:
        logging.error(f"Ошибка при организации {file_path}: {e}")
        with stats_lock:
            stats['organized_error'] += 1
        return None

def move_to_trash(src_path, trash_root):
    """
    Перемещает файл в папку с дубликатами, сохраняя структуру подпапок и избегая конфликтов имён.
    """
    try:
        trash = Path(trash_root)
        relative = src_path.relative_to(Path(DEST_DIR))  # ожидается, что src_path внутри DEST_DIR
        dest_path = trash / relative
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Уникализация имени, если файл уже существует
        if dest_path.exists():
            stem = dest_path.stem
            suffix = dest_path.suffix
            counter = 1
            while dest_path.exists():
                new_stem = f"{stem} ({counter})"
                dest_path = dest_path.with_name(new_stem + suffix)
                counter += 1

        shutil.move(str(src_path), str(dest_path))
        logging.info(f"Перемещён дубликат: {src_path} -> {dest_path}")
        with stats_lock:
            stats['duplicates_moved'] += 1
    except Exception as e:
        logging.error(f"Ошибка при перемещении дубликата {src_path}: {e}")
        with stats_lock:
            stats['duplicate_move_errors'] += 1

def animate_progress(current, total, phase=""):
    """Простая анимация прогресса с затиранием строки."""
    percent = current / total * 100 if total else 0
    bar_length = 30
    filled = int(bar_length * current // total) if total else 0
    bar = '█' * filled + '░' * (bar_length - filled)
    sys.stdout.write(f"\r{phase} [{bar}] {current}/{total} ({percent:.1f}%)")
    sys.stdout.flush()

def main():
    global FPCALC_PATH, DEST_DIR  # для использования в move_to_trash и других функциях

    # Загрузка конфигурации
    config = load_config()
    source_dir = config.get('PATHS', 'SOURCE_DIR', fallback=DEFAULT_CONFIG['PATHS']['SOURCE_DIR'])
    dest_dir = config.get('PATHS', 'DEST_DIR', fallback=DEFAULT_CONFIG['PATHS']['DEST_DIR'])
    trash_dir = config.get('PATHS', 'TRASH_DIR', fallback=DEFAULT_CONFIG['PATHS']['TRASH_DIR'])
    max_workers_setting = config.get('SETTINGS', 'MAX_WORKERS', fallback='auto')
    log_file = config.get('SETTINGS', 'LOG_FILE', fallback='music_sorter.log')
    fpcalc_path_setting = config.get('SETTINGS', 'FPCALC_PATH', fallback='')

    if fpcalc_path_setting:
        try:
            fpcalc_path = Path(fpcalc_path_setting).resolve(strict=True)
            if not fpcalc_path.is_file():
                raise FileNotFoundError(f"Указанный путь не является файлом: {fpcalc_path_setting}")
            # Дополнительная проверка на исполняемость (для Linux/Mac)
            # if not os.access(fpcalc_path, os.X_OK):
            #     raise PermissionError(f"Нет прав на выполнение: {fpcalc_path_setting}")
            FPCALC_PATH = str(fpcalc_path)
        except FileNotFoundError:
            print(f"Ошибка: файл fpcalc не найден по пути {fpcalc_path_setting}")
            sys.exit(1)
        except PermissionError:
            print(f"Ошибка: недостаточно прав для выполнения {fpcalc_path_setting}")
            sys.exit(1)
        except Exception as e:
            print(f"Ошибка при проверке fpcalc: {e}")
            sys.exit(1)

    # Сохраняем путь для работы функции move_to_trash
    DEST_DIR = dest_dir

    # Настройка логирования
    setup_logging(log_file)

    # Определение количества потоков
    if max_workers_setting.lower() == 'auto':
        max_workers = min(os.cpu_count() or 1, 10)
    else:
        try:
            max_workers = int(max_workers_setting)
        except ValueError:
            max_workers = 4
        max_workers = min(max_workers, 10)

    # Проверка существования исходной папки
    source = Path(source_dir)
    if not source.is_dir():
        logging.error(f"Исходная папка не существует: {source_dir}")
        print(f"Ошибка: папка {source_dir} не найдена. Проверьте config.ini")
        sys.exit(1)

    # Сбор всех аудиофайлов
    exts = {'.mp3', '.flac', '.m4a', '.wav', '.ogg', '.wma', '.ape', '.wv', '.aiff', '.dsf', '.dff'}
    all_files = [f for f in source.rglob('*') if f.suffix.lower() in exts]
    total_files = len(all_files)
    print(f"Найдено {total_files} аудиофайлов. Начинаем организацию...")

    # ---- ШАГ 1: Организация файлов (копирование) ----
    organized_paths = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(organize_file, f, dest_dir): f for f in all_files}
        completed = 0
        for future in as_completed(futures):
            result = future.result()
            if result:
                organized_paths.append(result)
            completed += 1
            animate_progress(completed, total_files, "Организация")

    print("\n")  # перевод строки после анимации

    # Если не скопировано ни одного файла, дальше не идём
    if not organized_paths:
        print("Не удалось организовать ни одного файла. Завершение.")
        logging.warning("Нет файлов для дальнейшей обработки.")
        sys.exit(0)

    # ---- ШАГ 2: Получение отпечатков ----
    print("Получение акустических отпечатков...")
    fp_results = []
    # Создаём функцию с фиксированным fpcalc_path
    get_fingerprint_with_path = partial(get_fingerprint_data, fpcalc_path=FPCALC_PATH)
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(get_fingerprint_with_path, p): p for p in organized_paths}
        completed = 0
        for future in as_completed(futures):
            data = future.result()
            if data:
                fp_results.append(data)
            completed += 1
            animate_progress(completed, len(organized_paths), "Отпечатки")

    print("\n")

    # Группировка по отпечатку
    groups = {}
    for item in fp_results:
        fp = item['fp']
        groups.setdefault(fp, []).append(item)

    # ---- ШАГ 3: Удаление дубликатов худшего качества ----
    duplicates_found = sum(1 for files in groups.values() if len(files) > 1)
    if duplicates_found:
        print(f"Найдено {duplicates_found} групп дубликатов. Перемещаем худшие копии в {trash_dir}...")
        trash_root = Path(trash_dir)
        for fp, files in groups.items():
            if len(files) > 1:
                # Сортировка по убыванию качества
                files.sort(key=lambda x: x['score'], reverse=True)
                best = files[0]
                for dup in files[1:]:
                    move_to_trash(dup['path'], trash_root)
        print("Обработка дубликатов завершена.")
    else:
        print("Дубликатов не найдено.")

    # ---- ИТОГОВАЯ СТАТИСТИКА ----
    print("\n" + "="*50)
    print("СТАТИСТИКА ВЫПОЛНЕНИЯ")
    print("="*50)
    with stats_lock:
        print(f"Организовано файлов (успешно/ошибки): {stats['organized_ok']} / {stats['organized_error']}")
        print(f"Отпечатки получены (успешно/ошибки):  {stats['fingerprint_ok']} / {stats['fingerprint_error']}")
        print(f"Дубликаты перемещены (успешно/ошибки): {stats['duplicates_moved']} / {stats['duplicate_move_errors']}")
    print(f"Всего обработано исходных файлов: {total_files}")
    print("="*50)
    print(f"Подробности в лог-файле: {log_file}")
    print("ВСЕ ЗАДАЧИ ВЫПОЛНЕНЫ!")

if __name__ == "__main__":
    main()