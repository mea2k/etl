"""
Экстрактор для извлечения данных из FTP
"""
import os
import sys
import logging
import io
import pandas as pd

from typing import Dict, Any, List, Optional

from airflow.providers.ftp.hooks.ftp import FTPHook

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor

logger = logging.getLogger(__name__)


class FTPExtractor(BaseExtractor):
    """Класс для извлечения данных из FTP сервера"""
    
    def __init__(self, conn_id: str, 
                 config: Optional[Dict[str, Any]] = None):
        super().__init__(conn_id)
        self.hook = None
        logger.info(f"FTPExtractor initialized: {self.conn_id}")
    
    def extract(self, remote_path: str, file_type: str = 'csv', **kwargs) -> List[Dict[str, Any]]:
        """Извлечение данных из FTP файла"""
        try:
            self.hook = FTPHook(ftp_conn_id=self.conn_id)
            
            logger.info(f"Retrieving file from FTP: {remote_path}")
            
            # Используем BytesIO как буфер
            file_buffer = io.BytesIO()
            self.hook.retrieve_file(remote_path, file_buffer)
            # Переходим в начало буфера
            file_buffer.seek(0)
    
            if file_type == 'csv':
                # Читаем как CSV из BytesIO
                df = pd.read_csv(file_buffer, **kwargs)
                result = df.to_dict('records')
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            logger.info(f"Extracted {len(result)} records from FTP: {remote_path}")
            
            return result

        except Exception as e:
            return []
        
        
    def list_remote_files(self, directory: str = "/", pattern: str = "*.csv") -> List[str]:
        """
        Список файлов на FTP сервере, соответствующих паттерну.
        Args:
            directory: путь к директории на FTP (по умолчанию корень "/")
            pattern: glob‑паттерн для фильтрации (например "*.csv", "logs_*.csv")
        Returns:
            список имён файлов, соответствующих паттерну
        Raises:
            Exception при ошибке подключения или доступа
        """
        try:
            self.hook = FTPHook(ftp_conn_id=self.conn_id)
            ftp_conn = self.hook.get_conn()
            # Проверяем существование директории
            ftp_conn.cwd(directory)  # меняем рабочую директорию
            logger.info(f"Successfully changed to FTP directory: {directory}")
        except Exception as cwd_error:
            logger.warning(f"Cannot change to {directory}: {cwd_error}. Listing root instead.")
            ftp_conn.cwd("/")  # пробуем корень
            directory = "/"

        try:

            # используем NLST (стандартная FTP команда)
            file_list_raw = ftp_conn.nlst()
        
            # Очищаем и фильтруем
            file_list = []
            for item in file_list_raw:
                item = item.strip()
                if item and not item.startswith('drwxr'):  # пропускаем директории
                    file_list.append(item)
            
            # Фильтруем по паттерну (простой glob‑матчинг)
            matching_files = []
            for filename in file_list:
                if self._matches_pattern(filename, pattern):
                    matching_files.append(filename)
            
            logger.info(f"Found {len(matching_files)} matching files: {matching_files}")
            return matching_files
            
        except Exception as e:
            logger.error(f"Failed to list FTP directory '{directory}': {e}")
            raise

    def _matches_pattern(self, filename: str, pattern: str) -> bool:
        """
        Простой glob‑матчинг для FTP файлов.
        """
        if pattern == "*":
            return True
        
        # Поддержка базовых паттернов: *.csv, logs_*.csv
        if pattern.startswith("*") and pattern.endswith("*"):
            return pattern[1:-1] in filename
        elif pattern.startswith("*"):
            return filename.endswith(pattern[1:])
        elif pattern.endswith("*"):
            return filename.startswith(pattern[:-1])
        else:
            return filename == pattern