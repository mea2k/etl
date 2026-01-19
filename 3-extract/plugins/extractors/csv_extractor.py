"""
CSV Extractor - извлечение данных из локальных CSV файлов

Работает с локальными CSV/TSV файлами на файловой системе Airflow.

Автор: Курс "ETL - Автоматизация подготовки данных"
"""

from typing import Any, Dict, List, Optional, Union
import pandas as pd
import glob
from pathlib import Path
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor

class CSVExtractor(BaseExtractor):
    """
    Extractor для извлечения данных из локальных CSV файлов.
    
    Возможности:
    ------------
    - Чтение CSV, TSV файлов
    - Различные разделители и кодировки
    - Обработка файлов с headers и без
    - Чтение нескольких файлов по маске
    - Инкрементальная загрузка по дате в имени файла
    - Фильтрация строк при чтении
    - Чтение больших файлов чанками
    - Обработка сжатых файлов (gzip, bzip2)
    
    Использование:
    --------------
    1. Connection НЕ требуется (локальные файлы)
       Но можно создать для хранения пути:
       - Admin → Connections → +
       - Connection Id: csv_local
       - Connection Type: File (path)
       - Extra: {"path": "/opt/airflow/data/csv"}
    
    2. Используйте в DAG:
       >>> extractor = CSVExtractor(base_path="/opt/airflow/data/csv")
       >>> df = extractor.extract("users.csv")
    
    Example:
        >>> # Простой CSV
        >>> extractor = CSVExtractor("/opt/airflow/data")
        >>> df = extractor.extract("users.csv")
        >>> 
        >>> # CSV с другим разделителем
        >>> df = extractor.extract("data.csv", delimiter=";")
        >>> 
        >>> # TSV файл
        >>> df = extractor.extract("data.tsv", delimiter="\t")
        >>> 
        >>> # Несколько файлов
        >>> df = extractor.extract_multiple(pattern="sales_*.csv")
        >>> 
        >>> # По дате в имени
        >>> df = extractor.extract_by_date(
        ...     date_str="2025-01-12",
        ...     pattern_template="orders_{date}.csv"
        ... )
    """
    
    def __init__(
        self,
        base_path: str,
        conn_id: str = "csv_local",
        default_encoding: str = "utf-8"
    ):
        """
        Инициализация CSV Extractor.
        
        Args:
            base_path: Базовый путь к директории с CSV файлами
            conn_id: ID Connection (опционально, для совместимости с BaseExtractor)
            default_encoding: Кодировка по умолчанию
        
        Example:
            >>> extractor = CSVExtractor("/opt/airflow/data/csv")
            >>> extractor = CSVExtractor("/data", default_encoding="windows-1251")
        """
        super().__init__(conn_id)
        self.base_path = Path(base_path)
        self.default_encoding = default_encoding
        
        # Проверяем существование директории
        if not self.base_path.exists():
            self.logger.warning(f"⚠️  Base path does not exist: {base_path}")
        else:
            self.logger.info(f"Base path: {self.base_path}")
    
    def extract(
        self,
        filename: str,
        delimiter: str = ",",
        encoding: Optional[str] = None,
        header: Union[int, str, None] = 0,
        skiprows: Optional[int] = None,
        usecols: Optional[List[str]] = None,
        dtype: Optional[Dict[str, Any]] = None,
        **kwargs
    ) -> pd.DataFrame:
        """
        Извлечение данных из CSV файла.
        
        Args:
            filename: Имя файла (относительно base_path) или полный путь
            delimiter: Разделитель колонок (по умолчанию ',')
            encoding: Кодировка файла (по умолчанию из __init__)
            header: Номер строки с заголовками (0 - первая, None - нет headers)
            skiprows: Количество строк для пропуска в начале
            usecols: Список колонок для чтения (None - все)
            dtype: Типы данных для колонок
            **kwargs: Дополнительные параметры для pd.read_csv()
        
        Returns:
            pd.DataFrame: Данные из CSV файла
        
        Raises:
            FileNotFoundError: Если файл не найден
            pd.errors.ParserError: Если ошибка парсинга CSV
        
        Example:
            >>> # Простой CSV
            >>> df = extractor.extract("users.csv")
            >>> 
            >>> # CSV с разделителем ";"
            >>> df = extractor.extract("data.csv", delimiter=";")
            >>> 
            >>> # CSV без заголовков
            >>> df = extractor.extract(
            ...     "data.csv",
            ...     header=None,
            ...     names=['col1', 'col2', 'col3']  # Свои имена колонок
            ... )
            >>> 
            >>> # Только нужные колонки
            >>> df = extractor.extract(
            ...     "large_file.csv",
            ...     usecols=['id', 'name', 'email']
            ... )
            >>> 
            >>> # С типами данных
            >>> df = extractor.extract(
            ...     "data.csv",
            ...     dtype={'id': int, 'amount': float, 'status': str}
            ... )
            >>> 
            >>> # Сжатый файл
            >>> df = extractor.extract("data.csv.gz")  # Автоопределение
        """
        self.logger.info("=" * 60)
        self.logger.info("EXTRACTING FROM CSV")
        self.logger.info("=" * 60)
        
        # Определяем полный путь к файлу
        if Path(filename).is_absolute():
            file_path = Path(filename)
        else:
            file_path = self.base_path / filename
        
        self.logger.info(f"File path: {file_path}")
        self.logger.info(f"Delimiter: '{delimiter}'")
        
        # Проверяем существование файла
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        # Получаем размер файла
        file_size = file_path.stat().st_size
        self.logger.info(f"File size: {file_size} bytes ({file_size / 1024:.2f} KB)")
        
        # Определяем кодировку
        file_encoding = encoding or self.default_encoding
        self.logger.info(f"Encoding: {file_encoding}")
        
        try:
            # Читаем CSV файл
            df = pd.read_csv(
                file_path,
                delimiter=delimiter,
                encoding=file_encoding,
                header=header,
                skiprows=skiprows,
                usecols=usecols,
                dtype=dtype,
                **kwargs
            )
            
            self.logger.info(f"✓ CSV file loaded successfully")
            
            # Логируем статистику
            self.log_extraction_stats(df, {
                'source': 'Local CSV',
                'file_path': str(file_path),
                'file_size_bytes': file_size,
                'delimiter': delimiter,
                'encoding': file_encoding
            })
            
            return df
            
        except pd.errors.ParserError as e:
            self.logger.error(f"✗ CSV parsing error: {e}")
            raise
        except Exception as e:
            self.logger.error(f"✗ Failed to load CSV: {e}")
            raise
    
    def extract_multiple(
        self,
        pattern: str = "*.csv",
        delimiter: str = ",",
        concat: bool = True,
        **kwargs
    ) -> pd.DataFrame:
        """
        Извлечение данных из нескольких CSV файлов по маске.
        
        Args:
            pattern: Glob паттерн для поиска файлов (например, "*.csv", "sales_*.csv")
            delimiter: Разделитель колонок
            concat: Объединить все файлы в один DataFrame
            **kwargs: Параметры для extract()
        
        Returns:
            pd.DataFrame: Объединённые данные из всех файлов
        
        Example:
            >>> # Все CSV файлы
            >>> df = extractor.extract_multiple("*.csv")
            >>> 
            >>> # Файлы по паттерну
            >>> df = extractor.extract_multiple("sales_2025_*.csv")
            >>> 
            >>> # TSV файлы
            >>> df = extractor.extract_multiple("*.tsv", delimiter="\t")
        """
        self.logger.info("=" * 60)
        self.logger.info("MULTIPLE CSV EXTRACTION")
        self.logger.info("=" * 60)
        self.logger.info(f"Pattern: {pattern}")
        
        # Ищем файлы по маске
        search_pattern = str(self.base_path / pattern)
        files = glob.glob(search_pattern)
        
        if not files:
            self.logger.warning(f"No files found matching pattern: {pattern}")
            return pd.DataFrame()
        
        self.logger.info(f"Found {len(files)} files")
        
        # Читаем каждый файл
        dataframes = []
        
        for i, file_path in enumerate(sorted(files), 1):
            filename = Path(file_path).name
            self.logger.info(f"Processing file {i}/{len(files)}: {filename}")
            
            try:
                df = self.extract(file_path, delimiter=delimiter, **kwargs)
                
                # Добавляем метаданные
                df['_source_file'] = filename
                
                dataframes.append(df)
                self.logger.info(f"  ✓ {len(df)} records")
                
            except Exception as e:
                self.logger.error(f"  ✗ Failed to process {filename}: {e}")
                continue
        
        # Объединяем все DataFrame
        if concat and dataframes:
            df_combined = pd.concat(dataframes, ignore_index=True)
            
            self.logger.info("=" * 60)
            self.logger.info("MULTIPLE EXTRACTION COMPLETE")
            self.logger.info(f"Files processed: {len(dataframes)}/{len(files)}")
            self.logger.info(f"Total records: {len(df_combined)}")
            self.logger.info("=" * 60)
            
            return df_combined
        elif dataframes:
            return dataframes[0]
        else:
            return pd.DataFrame()
    
    def extract_by_date(
        self,
        date_str: str,
        pattern_template: str = "data_{date}.csv",
        delimiter: str = ",",
        **kwargs
    ) -> pd.DataFrame:
        """
        Извлечение CSV файла по дате в имени (инкрементальная загрузка).
        
        Args:
            date_str: Дата в формате YYYY-MM-DD или YYYYMMDD
            pattern_template: Шаблон имени файла с {date}
            delimiter: Разделитель колонок
            **kwargs: Параметры для extract()
        
        Returns:
            pd.DataFrame: Данные из файла за указанную дату
        
        Example:
            >>> # Файл orders_2025-01-12.csv
            >>> df = extractor.extract_by_date(
            ...     date_str="2025-01-12",
            ...     pattern_template="orders_{date}.csv"
            ... )
            >>> 
            >>> # Файл sales_20250112.csv
            >>> df = extractor.extract_by_date(
            ...     date_str="20250112",
            ...     pattern_template="sales_{date}.csv"
            ... )
        """
        # Формируем имя файла
        filename = pattern_template.format(date=date_str)
        
        self.logger.info(f"Looking for file with date pattern: {filename}")
        
        return self.extract(filename, delimiter=delimiter, **kwargs)
    
    def extract_in_chunks(
        self,
        filename: str,
        chunk_size: int = 10000,
        delimiter: str = ",",
        **kwargs
    ):
        """
        Чтение большого CSV файла по частям (generator).
        
        Полезно для обработки файлов, которые не помещаются в память.
        
        Args:
            filename: Имя файла
            chunk_size: Размер чанка (количество строк)
            delimiter: Разделитель колонок
            **kwargs: Параметры для pd.read_csv()
        
        Yields:
            pd.DataFrame: Части файла
        
        Example:
            >>> # Обработка большого файла чанками
            >>> for chunk in extractor.extract_in_chunks("huge_file.csv", chunk_size=10000):
            ...     # Обрабатываем каждый чанк
            ...     process_data(chunk)
            ...     # Сохраняем или агрегируем
            ...     save_to_db(chunk)
        """
        file_path = self.base_path / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"CSV file not found: {file_path}")
        
        self.logger.info(f"Reading file in chunks: {filename}")
        self.logger.info(f"Chunk size: {chunk_size} rows")
        
        encoding = kwargs.get('encoding', self.default_encoding)
        
        # Создаём итератор по чанкам
        chunk_iterator = pd.read_csv(
            file_path,
            delimiter=delimiter,
            encoding=encoding,
            chunksize=chunk_size,
            **{k: v for k, v in kwargs.items() if k != 'encoding'}
        )
        
        chunk_num = 0
        for chunk in chunk_iterator:
            chunk_num += 1
            self.logger.info(f"Processing chunk {chunk_num}: {len(chunk)} rows")
            yield chunk
        
        self.logger.info(f"✓ Processed {chunk_num} chunks")
    
    def list_files(
        self,
        pattern: str = "*.csv",
        sort_by: str = "name"
    ) -> List[str]:
        """
        Получить список CSV файлов в директории.
        
        Args:
            pattern: Glob паттерн для поиска файлов
            sort_by: Сортировка ('name', 'size', 'modified')
        
        Returns:
            list: Список путей к файлам
        
        Example:
            >>> # Все CSV файлы
            >>> files = extractor.list_files()
            >>> 
            >>> # Файлы по паттерну
            >>> files = extractor.list_files("sales_*.csv")
            >>> 
            >>> # Сортировка по размеру
            >>> files = extractor.list_files("*.csv", sort_by="size")
        """
        search_pattern = str(self.base_path / pattern)
        files = glob.glob(search_pattern)
        
        # Сортировка
        if sort_by == "name":
            files = sorted(files)
        elif sort_by == "size":
            files = sorted(files, key=lambda x: Path(x).stat().st_size, reverse=True)
        elif sort_by == "modified":
            files = sorted(files, key=lambda x: Path(x).stat().st_mtime, reverse=True)
        
        self.logger.info(f"Found {len(files)} files matching '{pattern}'")
        
        return files
    
    def get_file_info(self, filename: str) -> Dict[str, Any]:
        """
        Получить информацию о CSV файле.
        
        Args:
            filename: Имя файла
        
        Returns:
            dict: Информация о файле
        
        Example:
            >>> info = extractor.get_file_info("users.csv")
            >>> print(f"Size: {info['size_mb']:.2f} MB")
            >>> print(f"Rows: {info['row_count']}")
        """
        file_path = self.base_path / filename
        
        if not file_path.exists():
            raise FileNotFoundError(f"File not found: {file_path}")
        
        # Размер файла
        file_size = file_path.stat().st_size
        
        # Количество строк (быстрый подсчёт)
        with open(file_path, 'r', encoding=self.default_encoding) as f:
            row_count = sum(1 for _ in f) - 1  # -1 для header
        
        # Информация о модификации
        modified_time = file_path.stat().st_mtime
        
        info = {
            'filename': filename,
            'path': str(file_path),
            'size_bytes': file_size,
            'size_kb': file_size / 1024,
            'size_mb': file_size / (1024 ** 2),
            'row_count': row_count,
            'modified_timestamp': modified_time,
            'exists': True
        }
        
        self.logger.info(f"File info for {filename}:")
        self.logger.info(f"  Size: {info['size_mb']:.2f} MB")
        self.logger.info(f"  Rows: {info['row_count']}")
        
        return info
    
    def detect_delimiter(self, filename: str, n_lines: int = 5) -> str:
        """
        Автоопределение разделителя в CSV файле.
        
        Args:
            filename: Имя файла
            n_lines: Количество строк для анализа
        
        Returns:
            str: Определённый разделитель
        
        Example:
            >>> delimiter = extractor.detect_delimiter("unknown.csv")
            >>> print(f"Detected delimiter: '{delimiter}'")
            >>> df = extractor.extract("unknown.csv", delimiter=delimiter)
        """
        import csv
        
        file_path = self.base_path / filename
        
        with open(file_path, 'r', encoding=self.default_encoding) as f:
            # Читаем первые n строк
            sample = ''.join([f.readline() for _ in range(n_lines)])
        
        # Автоопределение через csv.Sniffer
        sniffer = csv.Sniffer()
        delimiter = sniffer.sniff(sample).delimiter
        
        self.logger.info(f"Detected delimiter for {filename}: '{delimiter}'")
        
        return delimiter
    
    def validate_csv(
        self,
        filename: str,
        required_columns: Optional[List[str]] = None,
        delimiter: str = ","
    ) -> Dict[str, Any]:
        """
        Валидация CSV файла.
        
        Args:
            filename: Имя файла
            required_columns: Список обязательных колонок
            delimiter: Разделитель
        
        Returns:
            dict: Результаты валидации
        
        Example:
            >>> result = extractor.validate_csv(
            ...     "users.csv",
            ...     required_columns=['id', 'email', 'name']
            ... )
            >>> if result['valid']:
            ...     print("CSV is valid")
            ... else:
            ...     print(f"Errors: {result['errors']}")
        """
        errors = []
        warnings = []
        
        file_path = self.base_path / filename
        
        # Проверка существования
        if not file_path.exists():
            errors.append(f"File not found: {filename}")
            return {'valid': False, 'errors': errors, 'warnings': warnings}
        
        try:
            # Читаем только первые строки для проверки
            df = pd.read_csv(file_path, delimiter=delimiter, nrows=10)
            
            # Проверка обязательных колонок
            if required_columns:
                missing_cols = set(required_columns) - set(df.columns)
                if missing_cols:
                    errors.append(f"Missing required columns: {missing_cols}")
            
            # Проверка пустого файла
            if df.empty:
                warnings.append("File is empty (no data rows)")
            
            # Проверка дубликатов колонок
            if df.columns.duplicated().any():
                dup_cols = df.columns[df.columns.duplicated()].tolist()
                errors.append(f"Duplicate column names: {dup_cols}")
            
            validation_result = {
                'valid': len(errors) == 0,
                'errors': errors,
                'warnings': warnings,
                'columns': list(df.columns),
                'sample_row_count': len(df)
            }
            
            if validation_result['valid']:
                self.logger.info(f"✓ CSV validation passed for {filename}")
            else:
                self.logger.warning(f"⚠️  CSV validation failed for {filename}")
                for error in errors:
                    self.logger.error(f"  - {error}")
            
            return validation_result
            
        except Exception as e:
            errors.append(f"Failed to read CSV: {e}")
            return {'valid': False, 'errors': errors, 'warnings': warnings}
    
    def test_connection(self) -> bool:
        """
        Проверка доступности базовой директории.
        
        Returns:
            bool: True если директория доступна
        
        Example:
            >>> if extractor.test_connection():
            ...     print("Directory accessible")
            ... else:
            ...     print("Directory not accessible")
        """
        try:
            if not self.base_path.exists():
                self.logger.error(f"✗ Base path does not exist: {self.base_path}")
                return False
            
            if not os.access(self.base_path, os.R_OK):
                self.logger.error(f"✗ Base path not readable: {self.base_path}")
                return False
            
            self.logger.info(f"✓ Base path accessible: {self.base_path}")
            return True
            
        except Exception as e:
            self.logger.error(f"✗ Connection test failed: {e}")
            return False


# =============================================================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ В DAG
# =============================================================================

"""
Пример использования в DAG:

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from extractors.csv_extractor import CSVExtractor


def extract_users(**context):
    '''Извлечение пользователей из CSV'''
    # Создаём Extractor
    extractor = CSVExtractor("/opt/airflow/data/csv")
    
    # Простое чтение CSV
    df = extractor.extract("users.csv")
    
    # Сохраняем результат
    output_path = f"/opt/airflow/data/extracted/users_{context['ds']}.csv"
    extractor.save_to_csv(df, output_path)
    
    return {
        'records': len(df),
        'file': output_path
    }


def extract_daily_sales(**context):
    '''Извлечение ежедневных продаж по дате в имени файла'''
    ds = context['ds']  # 2025-01-12
    ds_nodash = context['ds_nodash']  # 20250112
    
    extractor = CSVExtractor("/opt/airflow/data/sales")
    
    # Файл с датой в имени
    df = extractor.extract_by_date(
        date_str=ds_nodash,
        pattern_template="sales_{date}.csv",
        delimiter=";"
    )
    
    return {'sales_records': len(df)}


def extract_all_reports(**context):
    '''Извлечение всех CSV файлов за месяц'''
    extractor = CSVExtractor("/opt/airflow/data/reports")
    
    # Все CSV файлы по маске
    df = extractor.extract_multiple(
        pattern="report_2025_01_*.csv",
        delimiter=","
    )
    
    # Группируем по файлам
    files_count = df['_source_file'].nunique()
    
    return {
        'total_records': len(df),
        'files_processed': files_count
    }


def extract_semicolon_csv(**context):
    '''Извлечение CSV с разделителем ";"'''
    extractor = CSVExtractor("/opt/airflow/data")
    
    # CSV с другим разделителем
    df = extractor.extract(
        "data.csv",
        delimiter=";",
        encoding="windows-1251"  # Другая кодировка
    )
    
    return {'records': len(df)}


def extract_without_header(**context):
    '''Извлечение CSV без заголовков'''
    extractor = CSVExtractor("/opt/airflow/data")
    
    # CSV без заголовков
    df = extractor.extract(
        "data_no_header.csv",
        header=None,
        names=['id', 'name', 'email', 'created_at']  # Свои имена
    )
    
    return {'records': len(df)}


def process_large_csv(**context):
    '''Обработка большого CSV файла чанками'''
    extractor = CSVExtractor("/opt/airflow/data")
    
    total_processed = 0
    
    # Читаем и обрабатываем по частям
    for chunk in extractor.extract_in_chunks("huge_file.csv", chunk_size=10000):
        # Обрабатываем каждый чанк
        processed = process_chunk(chunk)
        total_processed += processed
    
    return {'total_processed': total_processed}


def validate_csv_file(**context):
    '''Валидация CSV файла перед обработкой'''
    extractor = CSVExtractor("/opt/airflow/data")
    
    # Валидация
    result = extractor.validate_csv(
        "users.csv",
        required_columns=['id', 'email', 'name']
    )
    
    if not result['valid']:
        raise ValueError(f"CSV validation failed: {result['errors']}")
    
    return {'validation': 'passed'}


with DAG(
    dag_id='csv_extract_example',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    extract_users_task = PythonOperator(
        task_id='extract_users',
        python_callable=extract_users,
    )
    
    extract_sales_task = PythonOperator(
        task_id='extract_daily_sales',
        python_callable=extract_daily_sales,
    )
    
    extract_reports_task = PythonOperator(
        task_id='extract_reports',
        python_callable=extract_all_reports,
    )
    
    validate_task = PythonOperator(
        task_id='validate_csv',
        python_callable=validate_csv_file,
    )
    
    # Сначала валидация, потом извлечение
    validate_task >> [extract_users_task, extract_sales_task, extract_reports_task]
```


ТИПИЧНЫЕ ПАТТЕРНЫ CSV ФАЙЛОВ:
==============================

1. Ежедневные файлы:
   sales_20250112.csv
   sales_20250113.csv
   Использование: extract_by_date()

2. Файлы с разделителем даты:
   orders_2025-01-12.csv
   orders_2025-01-13.csv

3. Файлы за период:
   report_2025_01_01.csv
   report_2025_01_02.csv
   ...
   Использование: extract_multiple(pattern="report_2025_01_*.csv")

4. Различные разделители:
   - CSV: delimiter=","
   - TSV: delimiter="\t"
   - Европейский CSV: delimiter=";"
   - Pipe-separated: delimiter="|"

5. Различные кодировки:
   - UTF-8: encoding="utf-8"
   - Windows: encoding="windows-1251"
   - Latin-1: encoding="latin-1"

6. Сжатые файлы (автоопределение):
   - data.csv.gz
   - data.csv.bz2
   - data.csv.zip


BEST PRACTICES:
===============

1. Всегда указывайте кодировку явно:
   df = extractor.extract("file.csv", encoding="utf-8")

2. Для больших файлов используйте чанки:
   for chunk in extractor.extract_in_chunks("large.csv", chunk_size=10000):
       process(chunk)

3. Валидируйте файлы перед обработкой:
   result = extractor.validate_csv("file.csv", required_columns=['id', 'name'])

4. Используйте usecols для больших файлов:
   df = extractor.extract("file.csv", usecols=['id', 'name', 'email'])

5. Указывайте типы данных для оптимизации:
   df = extractor.extract("file.csv", dtype={'id': int, 'amount': float})
"""
