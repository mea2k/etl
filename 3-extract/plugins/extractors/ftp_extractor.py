"""
FTP Extractor - извлечение данных с FTP сервера

Использует FTPHook из Airflow для подключения к FTP серверу
и извлечения файлов различных форматов.

Автор: Курс "ETL - Автоматизация подготовки данных"
"""

import os
import sys
from typing import Any, Dict, List, Optional
from airflow.providers.ftp.hooks.ftp import FTPHook
import pandas as pd
import io
import csv
import json
import zipfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor


class FTPExtractor(BaseExtractor):
    """
    Extractor для извлечения данных с FTP сервера.
    
    Возможности:
    ------------
    - Загрузка CSV, TSV файлов
    - Загрузка JSON файлов
    - Загрузка Excel файлов (xlsx, xls)
    - Обработка ZIP архивов
    - Список файлов в директории
    - Фильтрация по маске имени файла
    - Загрузка нескольких файлов
    - Инкрементальная загрузка (по дате в имени файла)
    
    Использование:
    --------------
    1. Создайте Connection в Airflow UI:
       - Admin → Connections → +
       - Connection Id: ftp_server
       - Connection Type: FTP
       - Host: ftp.example.com
       - Login: ftpuser
       - Password: ftppass
       - Port: 21
       - Extra: {"passive": true}
    
    2. Используйте в DAG:
       >>> extractor = FTPExtractor("ftp_server")
       >>> df = extractor.extract("/data/users.csv", file_type="csv")
    
    Example:
        >>> # CSV файл
        >>> extractor = FTPExtractor("ftp_server")
        >>> df = extractor.extract("/data/sales.csv", file_type="csv")
        >>> 
        >>> # Excel файл
        >>> df = extractor.extract("/reports/monthly_report.xlsx", file_type="excel")
        >>> 
        >>> # JSON файл
        >>> df = extractor.extract("/api_dumps/users.json", file_type="json")
        >>> 
        >>> # Список файлов
        >>> files = extractor.list_files("/data/", pattern="*.csv")
        >>> 
        >>> # Все CSV файлы из директории
        >>> df = extractor.extract_multiple(
        ...     directory="/data/",
        ...     pattern="sales_*.csv"
        ... )
    """
    
    def __init__(self, conn_id: str):
        """
        Инициализация FTP Extractor.
        
        Args:
            conn_id: ID FTP Connection в Airflow
        """
        super().__init__(conn_id)
        self.hook: Optional[FTPHook] = None
    
    def extract(
        self,
        remote_path: str,
        file_type: str = "csv",
        **kwargs
    ) -> pd.DataFrame:
        """
        Извлечение данных из FTP файла.
        
        Args:
            remote_path: Путь к файлу на FTP сервере (например, "/data/file.csv")
            file_type: Тип файла ('csv', 'tsv', 'json', 'excel', 'zip')
            **kwargs: Дополнительные параметры для парсинга:
                     - delimiter: Разделитель для CSV (по умолчанию ',')
                     - encoding: Кодировка файла (по умолчанию 'utf-8')
                     - sheet_name: Лист Excel (по умолчанию 0)
                     - json_orient: Ориентация JSON ('records', 'index', и т.д.)
        
        Returns:
            pd.DataFrame: Данные из файла
        
        Raises:
            FileNotFoundError: Если файл не найден на FTP
            ValueError: Если неизвестный тип файла
        
        Example:
            >>> # CSV с разделителем ";"
            >>> df = extractor.extract("/data/file.csv", file_type="csv", delimiter=";")
            >>> 
            >>> # Excel, конкретный лист
            >>> df = extractor.extract(
            ...     "/reports/data.xlsx",
            ...     file_type="excel",
            ...     sheet_name="Sales"
            ... )
            >>> 
            >>> # JSON файл
            >>> df = extractor.extract("/dumps/users.json", file_type="json")
            >>> 
            >>> # TSV файл
            >>> df = extractor.extract("/data/file.tsv", file_type="tsv")
            >>> 
            >>> # ZIP архив с CSV внутри
            >>> df = extractor.extract(
            ...     "/archives/data.zip",
            ...     file_type="zip",
            ...     csv_filename="data.csv"  # имя файла внутри архива
            ... )
        """
        self.logger.info("=" * 60)
        self.logger.info("EXTRACTING FROM FTP")
        self.logger.info("=" * 60)
        
        # Создаём Hook для FTP
        self.hook = FTPHook(ftp_conn_id=self.conn_id)
        
        self.logger.info(f"Remote path: {remote_path}")
        self.logger.info(f"File type: {file_type}")
        
        try:
            # Получаем подключение к FTP
            ftp_conn = self.hook.get_conn()
            
            # Проверяем существование файла
            try:
                file_size = ftp_conn.size(remote_path)
                self.logger.info(f"File size: {file_size} bytes ({file_size / 1024:.2f} KB)")
            except:
                raise FileNotFoundError(f"File not found on FTP: {remote_path}")
            
            # Загружаем файл в память
            bio = io.BytesIO()
            ftp_conn.retrbinary(f"RETR {remote_path}", bio.write)
            bio.seek(0)
            
            self.logger.info(f"✓ File downloaded from FTP")
            
            # Парсим в зависимости от типа
            if file_type in ["csv", "tsv"]:
                df = self._parse_csv(bio, file_type, **kwargs)
            elif file_type == "json":
                df = self._parse_json(bio, **kwargs)
            elif file_type == "excel":
                df = self._parse_excel(bio, **kwargs)
            elif file_type == "zip":
                df = self._parse_zip(bio, **kwargs)
            else:
                raise ValueError(f"Unknown file type: {file_type}")
            
            # Логируем статистику
            self.log_extraction_stats(df, {
                'source': 'FTP',
                'file_path': remote_path,
                'file_type': file_type,
                'file_size_bytes': file_size
            })
            
            return df
            
        except FileNotFoundError as e:
            self.logger.error(f"✗ File not found: {e}")
            raise
        except Exception as e:
            self.logger.error(f"✗ Extraction failed: {e}")
            raise
        finally:
            # Закрываем соединение
            if self.hook:
                try:
                    self.hook.close_conn()
                except:
                    pass
    
    def _parse_csv(self, bio: io.BytesIO, file_type: str, **kwargs) -> pd.DataFrame:
        """Парсинг CSV/TSV файла."""
        if file_type == "tsv":
            delimiter = kwargs.get("delimiter", "\t")
        else:
            delimiter = kwargs.get("delimiter", ",")
        
        encoding = kwargs.get("encoding", "utf-8")
        self.logger.info(f"Parsing CSV: delimiter='{delimiter}', encoding='{encoding}'")
        
        text_stream = io.TextIOWrapper(bio, encoding=encoding)
        df = pd.read_csv(text_stream, delimiter=delimiter)
        
        self.logger.info(f"✓ CSV parsed successfully")
        return df
    
    def _parse_json(self, bio: io.BytesIO, **kwargs) -> pd.DataFrame:
        """Парсинг JSON файла."""
        encoding = kwargs.get("encoding", "utf-8")
        self.logger.info(f"Parsing JSON")
        
        json_str = bio.read().decode(encoding)
        json_data = json.loads(json_str)
        
        if isinstance(json_data, list):
            df = pd.DataFrame(json_data)
        elif isinstance(json_data, dict):
            df = pd.json_normalize(json_data)
        else:
            raise ValueError(f"Unexpected JSON structure: {type(json_data)}")
        
        self.logger.info(f"✓ JSON parsed successfully")
        return df
    
    def _parse_excel(self, bio: io.BytesIO, **kwargs) -> pd.DataFrame:
        """Парсинг Excel файла."""
        sheet_name = kwargs.get("sheet_name", 0)
        self.logger.info(f"Parsing Excel: sheet='{sheet_name}'")
        
        df = pd.read_excel(bio, sheet_name=sheet_name)
        self.logger.info(f"✓ Excel parsed successfully")
        return df
    
    def _parse_zip(self, bio: io.BytesIO, **kwargs) -> pd.DataFrame:
        """Парсинг ZIP архива."""
        csv_filename = kwargs.get("csv_filename")
        inner_file_type = kwargs.get("inner_file_type", "csv")
        
        self.logger.info(f"Extracting from ZIP archive")
        
        with zipfile.ZipFile(bio, 'r') as zip_ref:
            file_list = zip_ref.namelist()
            self.logger.info(f"Files in archive: {file_list}")
            
            if csv_filename:
                if csv_filename not in file_list:
                    raise FileNotFoundError(f"File '{csv_filename}' not found in archive")
                target_file = csv_filename
            else:
                if not file_list:
                    raise ValueError("ZIP archive is empty")
                target_file = file_list[0]
                self.logger.info(f"Using first file from archive: {target_file}")
            
            with zip_ref.open(target_file) as file:
                file_bio = io.BytesIO(file.read())
                
                if inner_file_type == "csv":
                    df = self._parse_csv(file_bio, "csv", **kwargs)
                elif inner_file_type == "json":
                    df = self._parse_json(file_bio, **kwargs)
                elif inner_file_type == "excel":
                    df = self._parse_excel(file_bio, **kwargs)
                else:
                    raise ValueError(f"Unsupported inner file type: {inner_file_type}")
                
                self.logger.info(f"✓ File extracted from ZIP and parsed")
                return df
    
    def list_files(
        self,
        directory: str = "/",
        pattern: Optional[str] = None,
        file_type: Optional[str] = None
    ) -> List[str]:
        """Получить список файлов в директории FTP."""
        import fnmatch
        
        self.logger.info(f"Listing files in: {directory}")
        self.hook = FTPHook(ftp_conn_id=self.conn_id)
        
        try:
            ftp_conn = self.hook.get_conn()
            ftp_conn.cwd(directory)
            file_list = ftp_conn.nlst()
            
            full_paths = [f"{directory.rstrip('/')}/{f}" for f in file_list]
            
            if pattern:
                full_paths = [f for f in full_paths if fnmatch.fnmatch(Path(f).name, pattern)]
            
            if file_type:
                ext = f".{file_type}"
                full_paths = [f for f in full_paths if f.endswith(ext)]
            
            self.logger.info(f"Found {len(full_paths)} files")
            return full_paths
            
        except Exception as e:
            self.logger.error(f"Failed to list files: {e}")
            raise
        finally:
            if self.hook:
                try:
                    self.hook.close_conn()
                except:
                    pass
    
    def extract_multiple(
        self,
        directory: str,
        pattern: Optional[str] = None,
        file_type: str = "csv",
        concat: bool = True,
        **kwargs
    ) -> pd.DataFrame:
        """Извлечение данных из нескольких файлов."""
        self.logger.info("=" * 60)
        self.logger.info("MULTIPLE FILES EXTRACTION")
        self.logger.info("=" * 60)
        
        files = self.list_files(directory, pattern=pattern)
        
        if not files:
            self.logger.warning("No files found matching criteria")
            return pd.DataFrame()
        
        self.logger.info(f"Processing {len(files)} files...")
        
        dataframes = []
        for i, file_path in enumerate(files, 1):
            self.logger.info(f"Processing file {i}/{len(files)}: {Path(file_path).name}")
            
            try:
                df = self.extract(file_path, file_type=file_type, **kwargs)
                df['_source_file'] = Path(file_path).name
                dataframes.append(df)
                self.logger.info(f"  ✓ {len(df)} records")
            except Exception as e:
                self.logger.error(f"  ✗ Failed: {e}")
                continue
        
        if concat and dataframes:
            df_combined = pd.concat(dataframes, ignore_index=True)
            self.logger.info(f"Total: {len(df_combined)} records from {len(dataframes)} files")
            return df_combined
        elif dataframes:
            return dataframes[0]
        else:
            return pd.DataFrame()
    
    def extract_by_date_pattern(
        self,
        directory: str,
        date_str: str,
        pattern_template: str = "data_{date}.csv",
        file_type: str = "csv",
        **kwargs
    ) -> pd.DataFrame:
        """Извлечение файла по дате в имени (инкрементальная загрузка)."""
        filename = pattern_template.format(date=date_str)
        file_path = f"{directory.rstrip('/')}/{filename}"
        
        self.logger.info(f"Looking for file with date pattern: {file_path}")
        return self.extract(file_path, file_type=file_type, **kwargs)
    
    def test_connection(self) -> bool:
        """Проверка подключения к FTP серверу."""
        try:
            self.hook = FTPHook(ftp_conn_id=self.conn_id)
            ftp_conn = self.hook.get_conn()
            ftp_conn.pwd()
            
            self.logger.info("✓ FTP connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"✗ FTP connection test failed: {e}")
            return False
        finally:
            if self.hook:
                try:
                    self.hook.close_conn()
                except:
                    pass
