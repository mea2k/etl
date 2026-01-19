"""
Postgres Extractor - извлечение данных из PostgreSQL

Использует PostgresHook из Airflow для подключения к базе данных
и извлечения данных через SQL запросы.
"""

import os
import sys
from typing import Any, List, Optional, Dict
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor



class PostgresExtractor(BaseExtractor):
    """
    Extractor для извлечения данных из PostgreSQL.
    
    Возможности:
    ------------
    - Выполнение произвольных SQL запросов
    - Параметризованные запросы (защита от SQL injection)
    - Инкрементальная загрузка по дате
    - Извлечение конкретных колонок
    - Batch обработка больших таблиц
    
    Использование:
    --------------
    1. Создайте Connection в Airflow UI:
       - Admin → Connections → +
       - Connection Id: pg_source
       - Connection Type: Postgres
       - Host: localhost, Port: 5432
       - Schema: my_database
       - Login/Password: credentials
    
    2. Используйте в DAG:
       >>> extractor = PostgresExtractor("pg_source")
       >>> df = extractor.extract("SELECT * FROM users WHERE active = true")
    
    Example:
        >>> # Простой запрос
        >>> extractor = PostgresExtractor("pg_source")
        >>> df = extractor.extract("SELECT * FROM orders")
        >>> 
        >>> # С параметрами
        >>> df = extractor.extract(
        ...     sql="SELECT * FROM orders WHERE order_date = %(date)s",
        ...     parameters={"date": "2025-01-12"}
        ... )
        >>> 
        >>> # Инкрементальная загрузка
        >>> df = extractor.extract_incremental(
        ...     table_name="orders",
        ...     date_column="created_at",
        ...     start_date="2025-01-01",
        ...     end_date="2025-01-12"
        ... )
    """
    
    def __init__(self, conn_id: str, schema: Optional[str] = None):
        """
        Инициализация Postgres Extractor.   
        Args:
            conn_id: ID Postgres Connection в Airflow
            schema: Имя БД (если не указано в Connections)
        """
        super().__init__(conn_id)
        self.schema = schema
        
        if schema:
            self.logger.info(f"Using schema: {schema}")
    
    def extract(
        self,
        sql: str,
        parameters: Optional[Dict[str, Any]] = None,
    ) -> pd.DataFrame:
        """
        Выполнение SQL запроса и возврат результата в виде DataFrame.
        Args:
            sql: SQL-запрос для выполнения
                Может содержать плейсхолдеры: %(param_name)s
            parameters: Словарь с параметрами для SQL
                Используется для параметризованных запросов
        Returns:
            pd.DataFrame: Результат запроса
        Raises:
            Exception: При ошибках выполнения запроса
        Example:
            >>> # Без параметров
            >>> df = extractor.extract("SELECT * FROM users")
            >>> 
            >>> # С параметрами (рекомендуется!)
            >>> df = extractor.extract(
            ...     sql="SELECT * FROM orders WHERE status = %(status)s AND created_at > %(date)s",
            ...     parameters={"status": "completed", "date": "2025-01-01"}
            ... )
        Note:
            ВСЕГДА используйте параметризованные запросы вместо f-strings
            для защиты от SQL-injection!
        """
        self.logger.info("-" * 50)
        self.logger.info("EXECUTING SQL QUERY")
        self.logger.info("-" * 50)
        
        # Создаём Hook для подключения к Postgres
        hook = PostgresHook(
            postgres_conn_id=self.conn_id,
            schema=self.schema
        )
        
        # Логируем запрос (без параметров для безопасности)
        self.logger.info("SQL Query:")
        self.logger.info(sql)
        
        if parameters:
            # Логируем параметры (осторожно с секретами!)
            safe_params = {k: v for k, v in parameters.items() if not any(
                secret in k.lower() for secret in ['password', 'token', 'secret']
            )}
            if safe_params:
                self.logger.info(f"Parameters: {safe_params}")
        
        try:
            # Выполняем запрос и получаем DataFrame
            df = hook.get_pandas_df(sql=sql, parameters=parameters)
            
            self.logger.info(f"  Query executed successfully")
            
            # Логируем статистику
            self.log_extraction_stats(df, {
                'source': 'PostgreSQL',
                'schema': self.schema,
                'query_type': 'custom_sql'
            })
            
            return df
            
        except Exception as e:
            self.logger.error(f"  Query execution failed: {e}")
            raise
    
    def extract_table(
        self,
        table_name: str,
        columns: Optional[List[str]] = None,
        where_clause: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Извлечение данных из таблицы (упрощённый метод).
        Args:
            table_name: Название таблицы
            columns: Список колонок для выборки (по умолчанию все)
            where_clause: WHERE условие (без слова WHERE)
            limit: Ограничение количества записей
        Returns:
            pd.DataFrame: Данные из таблицы
        Example:
            >>> # Вся таблица
            >>> df = extractor.extract_table("users")
            >>> 
            >>> # Конкретные колонки
            >>> df = extractor.extract_table("users", columns=['id', 'email', 'name'])
            >>> 
            >>> # С фильтром
            >>> df = extractor.extract_table(
            ...     table_name="orders",
            ...     where_clause="status = 'completed' AND amount > 100",
            ...     limit=1000
            ... )
        """
        # Формируем SELECT
        if columns:
            cols_str = ", ".join(columns)
        else:
            cols_str = "*"
        
        # Формируем SQL
        sql = f"SELECT {cols_str} FROM {table_name}"
        if where_clause:
            sql += f" WHERE {where_clause}"
        if limit:
            sql += f" LIMIT {limit}"
        
        return self.extract(sql=sql)
    
    def extract_incremental(
        self,
        table_name: str,
        date_column: str,
        start_date: str,
        end_date: str,
        columns: Optional[List[str]] = None,
        additional_where: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Инкрементальная выборка по диапазону дат.
        Используется для извлечения только новых/изменённых записей
        вместо полной перезагрузки таблицы.
        Args:
            table_name: Имя таблицы
            date_column: Колонка с датой/временем для фильтрации
            start_date: Начальная дата (включительно), формат: YYYY-MM-DD
            end_date: Конечная дата (включительно), формат: YYYY-MM-DD
            columns: Список колонок (опционально)
            additional_where: Дополнительные условия WHERE
        Returns:
            pd.DataFrame: Данные за указанный период
        Example:
            >>> # Заказы за один день
            >>> df = extractor.extract_incremental(
            ...     table_name="orders",
            ...     date_column="created_at",
            ...     start_date="2025-01-12",
            ...     end_date="2025-01-12"
            ... )
            >>> 
            >>> # Заказы за неделю с фильтром
            >>> df = extractor.extract_incremental(
            ...     table_name="orders",
            ...     date_column="created_at",
            ...     start_date="2025-01-01",
            ...     end_date="2025-01-07",
            ...     columns=['id', 'customer_id', 'amount'],
            ...     additional_where="status = 'completed'"
            ... )
        Note:
            Этот метод оптимален для больших таблиц - извлекает только нужный период.
        """
        self.logger.info(f"Incremental extract: {table_name} from {start_date} to {end_date}")
        
        # Формируем SELECT
        cols = ", ".join(columns) if columns else "*"

        # Формируем WHERE
        where_parts = [f"{date_column} BETWEEN %(start_date)s AND %(end_date)s"]        
        if additional_where:
            where_parts.append(additional_where)
        where_clause = " AND ".join(where_parts)
        
        # SQL запрос
        sql = f"""
            SELECT {cols}
            FROM {table_name}
            WHERE {where_clause}
            ORDER BY {date_column}
        """
        # Параметры
        params = {
            "start_date": start_date,
            "end_date": end_date
        }
        
        return self.extract(sql=sql, parameters=params)
    
    def extract_count(self, table_name: str, where_clause: Optional[str] = None) -> int:
        """
        Получить количество записей в таблице.
        Полезно для проверки перед извлечением больших объёмов данных.
        Args:
            table_name: Имя таблицы
            where_clause: WHERE условие (опционально)
        Returns:
            int: Количество записей
        Example:
            >>> count = extractor.extract_count("orders")
            >>> print(f"Total orders: {count}")
            >>> 
            >>> # С фильтром
            >>> count = extractor.extract_count("orders", "status = 'completed'")
        """
        sql = f"SELECT COUNT(*) as count FROM {table_name}"
        
        if where_clause:
            sql += f" WHERE {where_clause}"
        
        df = self.extract(sql)
        return int(df.iloc[0]['count'])
    
    def test_connection(self) -> bool:
        """
        Проверка подключения к базе данных.
        Returns:
            bool: True если подключение успешно
        Example:
            >>> if extractor.test_connection():
            ...     print("Connection OK")
            ... else:
            ...     print("Connection FAILED")
        """
        try:
            hook = PostgresHook(postgres_conn_id=self.conn_id, schema=self.schema)
            hook.get_conn()
            self.logger.info("  Connection test successful")
            return True
        except Exception as e:
            self.logger.error(f"  Connection test failed: {e}")
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
from extractors.postgres_extractor import PostgresExtractor


def extract_orders(**context):
    '''Извлечение заказов за текущий день'''
    ds = context['ds']  # Дата выполнения: 2025-01-12
    
    # Создаём Extractor
    extractor = PostgresExtractor("pg_source", schema="sales")
    
    # Инкрементальная загрузка
    df = extractor.extract_incremental(
        table_name="orders",
        date_column="order_date",
        start_date=ds,
        end_date=ds,
        columns=['order_id', 'customer_id', 'amount', 'status']
    )
    
    # Сохраняем результат
    output_path = f"/opt/airflow/data/extracted/orders_{ds}.csv"
    extractor.save_to_csv(df, output_path)
    
    return {
        'records': len(df),
        'file': output_path
    }


def extract_users(**context):
    '''Извлечение активных пользователей'''
    extractor = PostgresExtractor("pg_source")
    
    # Кастомный SQL
    sql = '''
        SELECT 
            u.id,
            u.email,
            u.name,
            COUNT(o.id) as order_count
        FROM users u
        LEFT JOIN orders o ON u.id = o.customer_id
        WHERE u.active = true
        GROUP BY u.id, u.email, u.name
        HAVING COUNT(o.id) > 0
    '''
    
    df = extractor.extract(sql)
    
    return {'total_users': len(df)}


with DAG(
    dag_id='postgres_extract_example',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    extract_orders_task = PythonOperator(
        task_id='extract_orders',
        python_callable=extract_orders,
    )
    
    extract_users_task = PythonOperator(
        task_id='extract_users',
        python_callable=extract_users,
    )
    
    # Параллельное выполнение
    [extract_orders_task, extract_users_task]
```
"""
