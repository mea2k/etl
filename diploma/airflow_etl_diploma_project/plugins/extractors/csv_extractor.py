"""
Экстрактор данных из CSV файлов.
"""
from datetime import datetime, timedelta
import os
import sys
import pandas as pd

from typing import Optional, List

from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor


class CSVExtractor(BaseExtractor):
    """Экстрактор для извлечения данных из CSV файлов."""
    
    def __init__(self, conn_id: str = "csv_extractor", base_path: str = "/"):
        super().__init__(conn_id)
        self.base_path = Path(base_path)

    def extract(
        self,
        filename: str,
        encoding: str = 'utf-8',
        delimiter: str = ',',
        columns: Optional[List[str]] = None
    ) -> pd.DataFrame:
        """Извлечение данных из CSV файла."""
        try:
            filepath = f"{self.base_path}/{filename}"
            
            if not Path(filepath).exists():
                raise FileNotFoundError(f"File not found: {filepath}")
            
            self.logger.info(f"Reading CSV file: {filepath}")
            
            df = pd.read_csv(
                filepath,
                encoding=encoding,
                delimiter=delimiter,
                usecols=columns
            )
            
            self.log_extraction_stats(df)
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to extract from CSV: {e}")
            raise
    
    def list_files(self, pattern: str = "*.csv") -> List[str]:
        """Список CSV файлов в директории."""
        return [f.name for f in self.base_path.glob(pattern)]


    def extract_latest_files(
        self, 
        days: int = 1, 
        pattern: str = "*.csv",
        limit: Optional[int] = None
    ) -> List[Path]:
        """
        Извлекает список CSV файлов, изменённых за последние N дней.
        Args:
            days: количество дней назад (по дате изменения файла)
            pattern: glob‑паттерн для поиска файлов
            limit: максимальное количество файлов (None = все)
        Returns:
            список путей к файлам Path
        Пример:
            extractor.extract_latest_files(days=2)  # файлы за последние 2 дня
        """
        cutoff_time = datetime.now() - timedelta(days=days)
        
        latest_files = []
        for file_path in self.base_path.glob(pattern):
            if file_path.is_file():
                file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
                if file_mtime >= cutoff_time:
                    latest_files.append(file_path)
        
        # Сортируем по дате изменения (новые первыми)
        latest_files.sort(key=lambda f: f.stat().st_mtime, reverse=True)
        
        # Ограничиваем количество, если нужно
        if limit:
            latest_files = latest_files[:limit]
        
        self.logger.info(
            f"Found {len(latest_files)} CSV files modified in last {days} days: "
            f"{[f.name for f in latest_files]}"
        )
        
        return latest_files
