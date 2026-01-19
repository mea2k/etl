"""
Base Extractor - абстрактный базовый класс для всех Extractors

Этот класс определяет единый интерфейс для извлечения данных из различных
источников (Postgres, MongoDB, HTTP API, FTP, CSV и т.д.)

Все кастомные Extractors ДОЛЖНЫ наследоваться от этого класса и реализовать
метод extract().

"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional
import logging
import pandas as pd
from datetime import datetime


class BaseExtractor(ABC):
    """
    Абстрактный базовый класс для извлечения данных из различных источников.
    
    Концепция:
    ----------
    BaseExtractor обеспечивает единообразный интерфейс для работы со всеми
    источниками данных. Все Extractors должны реализовать метод extract(),
    который возвращает pandas DataFrame.
    
    Преимущества:
    -------------
    - Единый интерфейс для всех источников
    - Централизованное логирование
    - Переиспользование общей логики
    - Легко добавлять новые источники
    - Упрощённое тестирование
    
    Использование:
    --------------
    1. Создайте класс, наследующий BaseExtractor
    2. Реализуйте метод extract()
    3. Используйте в DAG
    
    Example:
        >>> class MyExtractor(BaseExtractor):
        ...     def extract(self, **kwargs) -> pd.DataFrame:
        ...         # Ваша логика извлечения
        ...         return df
        ...
        >>> extractor = MyExtractor("my_conn_id")
        >>> df = extractor.extract(param1="value1")
    """
    
    def __init__(self, conn_id: str):
        """
        Инициализация базового Extractor.    
        Args:
            conn_id: ID Connection в Airflow (создаётся через UI: Admin → Connections)
                    Например: 'postgres_source', 'api_service', 'mongo_db'
        Note:
            Connection должен существовать в Airflow до использования Extractor!
            Проверить можно в UI: Admin → Connections
        """
        self.conn_id = conn_id
        
        # Создаём logger с именем класса для удобной фильтрации логов
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # Логируем инициализацию
        self.logger.info(f"Инициализация {self.__class__.__name__}, conn_id='{conn_id}'")
        
        # Метаданные для отслеживания
        self._extraction_metadata = {
            'conn_id': conn_id,
            'extractor_type': self.__class__.__name__,
            'initialized_at': datetime.now().isoformat(),
        }
    
    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Извлечение данных из источника.
        Этот метод ДОЛЖЕН быть реализован в каждом наследнике.
        Args:
            **kwargs: Параметры, специфичные для каждого источника
                     Например: sql, query, endpoint, filename и т.д.
        Returns:
            pd.DataFrame: Извлечённые данные в виде DataFrame
        Raises:
            NotImplementedError: Если метод не реализован в наследнике
            Exception: При ошибках извлечения данных
        Example:
            >>> df = extractor.extract(table="users", limit=100)
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} must implement extract() method"
        )
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Получить метаданные о Extractor.
        Полезно для логирования и отладки.
        Returns:
            dict: Словарь с метаданными
                - conn_id: ID подключения
                - extractor_type: Тип Extractor
                - initialized_at: Время инициализации
        Example:
            >>> metadata = extractor.get_metadata()
            >>> print(metadata)
            {'conn_id': 'pg_source', 'extractor_type': 'PostgresExtractor', ...}
        """
        return self._extraction_metadata.copy()
    
    def log_extraction_stats(self, df: pd.DataFrame, extra_info: Optional[Dict] = None):
        """
        Логирование статистики извлечённых данных.
        Вызывайте этот метод в конце extract() для детального логирования.
        Args:
            df: DataFrame с извлечёнными данными
            extra_info: Дополнительная информация для логирования
        Example:
            >>> df = self.extract_from_source()
            >>> self.log_extraction_stats(df, {'source_table': 'users'})
        """
        self.logger.info("-" * 50)
        self.logger.info(f"EXTRACTION STATISTICS - {self.__class__.__name__}")
        self.logger.info("-" * 50)
        
        # Основная статистика
        self.logger.info(f"  Извлечено записей: {len(df)}")
        self.logger.info(f"  Столбцы: {list(df.columns)}")
        self.logger.info(f"  Всего столбцов: {len(df.columns)}")
        
        # Размер в памяти
        memory_usage = df.memory_usage(deep=True).sum()
        memory_mb = memory_usage / (1024 ** 2)
        self.logger.info(f"  Использовано памяти: {memory_mb:.2f} MB")
        
        # Типы данных
        dtype_counts = df.dtypes.value_counts().to_dict()
        self.logger.info(f"  Типы данных: {dtype_counts}")
        
        # Пропущенные значения
        null_counts = df.isnull().sum()
        if null_counts.any():
            null_info = null_counts[null_counts > 0].to_dict()
            self.logger.warning(f"  Null-значения: {null_info}")
        else:
            self.logger.info("  Нет Null-значений")
        
        # Дополнительная информация
        if extra_info:
            self.logger.info("Дополнительная инфо:")
            for key, value in extra_info.items():
                self.logger.info(f"  {key}: {value}")
        
        self.logger.info("-" * 50)
    
    def validate_dataframe(self, df: pd.DataFrame, required_columns: Optional[list] = None):
        """
        Валидация извлечённого DataFrame.
        Args:
            df: DataFrame для проверки
            required_columns: Список обязательных колонок
        Raises:
            ValueError: Если валидация не прошла
        Example:
            >>> df = self.extract(...)
            >>> self.validate_dataframe(df, required_columns=['id', 'name', 'email'])
        """
        # Проверка пустоты
        if df.empty:
            self.logger.warning("  DataFrame пустой")
        
        # Проверка обязательных колонок
        if required_columns:
            missing_cols = set(required_columns) - set(df.columns)
            if missing_cols:
                raise ValueError(f"Отсутствуют обяхательные столбцы: {missing_cols}")
            self.logger.info(f" Все обязательные поля присутствуют: {required_columns}")
        
        # Проверка дубликатов индекса
        if df.index.duplicated().any():
            self.logger.warning(f"  Найдено {df.index.duplicated().sum()} дубликатов индекса")
        
        self.logger.info("  Валидация DataFrame успешно прошла")
    
    def save_to_csv(self, df: pd.DataFrame, output_path: str, **kwargs):
        """
        Сохранение DataFrame в CSV файл.
        Вспомогательный метод для быстрого сохранения результатов.
        Args:
            df: DataFrame для сохранения
            output_path: Путь к выходному файлу
            **kwargs: Дополнительные параметры для df.to_csv()
        Example:
            >>> df = extractor.extract(...)
            >>> extractor.save_to_csv(df, '/data/output.csv')
        """
        self.logger.info(f"Сохранение DataFrame в CSV: {output_path}")
        
        # Устанавливаем значения по умолчанию
        kwargs.setdefault('index', False)
        kwargs.setdefault('encoding', 'utf-8')
        
        df.to_csv(output_path, **kwargs)
        
        # Логируем информацию о файле
        import os
        file_size = os.path.getsize(output_path)
        file_size_mb = file_size / (1024 ** 2)
        
        self.logger.info(f"  Файл сохранен успешно")
        self.logger.info(f"  Путь:   {output_path}")
        self.logger.info(f"  Размер: {file_size_mb:.2f} MB")
        self.logger.info(f"  Записей: {len(df)}")
    
    def save_to_json(self, df: pd.DataFrame, output_path: str, orient: str = 'records', **kwargs):
        """
        Сохранение DataFrame в JSON файл.
        Args:
            df: DataFrame для сохранения
            output_path: Путь к выходному файлу
            orient: Формат JSON ('records', 'index', 'columns', и т.д.)
            **kwargs: Дополнительные параметры для df.to_json()
        Example:
            >>> df = extractor.extract(...)
            >>> extractor.save_to_json(df, '/data/output.json')
        """
        self.logger.info(f"Сохранение DataFrame в JSON: {output_path}")
        
        # Устанавливаем значения по умолчанию
        kwargs.setdefault('indent', 2)
        kwargs.setdefault('force_ascii', False)
        
        df.to_json(output_path, orient=orient, indent=2, force_ascii=False, **kwargs)

        self.logger.info(f"  JSON сохранен: {output_path}")

    def __repr__(self) -> str:
        """
        Строковое представление Extractor.
        Returns:
            str: Описание Extractor
        """
        return f"{self.__class__.__name__}(conn_id='{self.conn_id}')"


# =============================================================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ
# =============================================================================

"""
Пример создания кастомного Extractor:

```python
from extractors.base_extractor import BaseExtractor
import pandas as pd

class MyCustomExtractor(BaseExtractor):
    '''Кастомный Extractor для специфичного источника'''
    
    def extract(self, table_name: str, limit: int = 100) -> pd.DataFrame:
        '''
        Извлечение данных из источника.
        
        Args:
            table_name: Название таблицы
            limit: Максимальное количество записей
        
        Returns:
            DataFrame с данными
        '''
        self.logger.info(f"Extracting from table: {table_name}")
        
        # Ваша логика извлечения данных
        # Например, через Hook:
        # hook = SomeHook(conn_id=self.conn_id)
        # data = hook.get_data(table_name, limit=limit)
        
        # Создаём DataFrame
        df = pd.DataFrame(data)
        
        # Логируем статистику
        self.log_extraction_stats(df, {'table': table_name, 'limit': limit})
        
        # Валидация
        self.validate_dataframe(df, required_columns=['id', 'name'])
        
        return df


# Использование в DAG:
def extract_data(**context):
    extractor = MyCustomExtractor("my_conn_id")
    
    df = extractor.extract(table_name="users", limit=1000)
    
    # Сохранение
    output_path = f"/data/users_{context['ds']}.csv"
    extractor.save_to_csv(df, output_path)
    
    return {
        'records': len(df),
        'file': output_path
    }
```
"""