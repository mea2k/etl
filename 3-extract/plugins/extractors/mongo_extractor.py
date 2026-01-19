"""
Mongo Extractor - извлечение данных из MongoDB

Использует MongoHook из Airflow для подключения к MongoDB
и извлечения данных через query запросы.

"""

import os
import sys
from datetime import datetime
from bson import ObjectId, Decimal128, Binary
import base64
from typing import Any, Dict, List, Optional

import pandas as pd

from airflow.providers.mongo.hooks.mongo import MongoHook

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor



class MongoExtractor(BaseExtractor):
    """
    Extractor для извлечения данных из MongoDB.
    
    Возможности:
    ------------
    - Извлечение документов по query (фильтрация)
    - Projection (выбор конкретных полей)
    - Сортировка результатов
    - Ограничение количества документов
    - Фильтрация по дате
    - Агрегационные запросы
    
    Использование:
    --------------
    1. Создайте Connection в Airflow UI:
       - Admin → Connections → +
       - Connection Id: mongo_source
       - Connection Type: MongoDB
       - Host: mongodb://localhost:27017
       - или URI: mongodb://user:password@host:port/database
    
    2. Используйте в DAG:
       >>> extractor = MongoExtractor("mongo_source", database="analytics_db")
       >>> df = extractor.extract(collection="events", query={"status": "active"})
    
    Example:
        >>> # Все документы коллекции
        >>> extractor = MongoExtractor("mongo_source", "my_database")
        >>> df = extractor.extract(collection="users")
        >>> 
        >>> # С фильтром
        >>> df = extractor.extract(
        ...     collection="events",
        ...     query={"event_type": "purchase", "amount": {"$gt": 100}}
        ... )
        >>> 
        >>> # Только нужные поля
        >>> df = extractor.extract(
        ...     collection="users",
        ...     projection={"_id": 0, "email": 1, "name": 1}
        ... )
        >>> 
        >>> # По дате
        >>> df = extractor.extract_by_date(
        ...     collection="events",
        ...     date_field="created_at",
        ...     date_str="2025-01-12"
        ... )
    """
    
    def __init__(self, conn_id: str, database: str):
        """
        Инициализация Mongo Extractor. 
        Args:
            conn_id: ID MongoDB Connection в Airflow
            database: Название базы данных MongoDB
        Note:
            В отличие от Postgres, база данных указывается при инициализации,
            а коллекция - при вызове extract()
        """
        super().__init__(conn_id)
        self.database = database
        
        self.logger.info(f"Using database: {database}")


    def _convert_bson_to_json(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Конвертирует все BSON типы MongoDB в JSON-совместимые типы.
        Обрабатывает:
        - ObjectId → str
        - datetime → ISO string  
        - Decimal128 → float
        - Binary → base64 string
        - Рекурсивно обрабатывает вложенные структуры
        """
        def convert_value(value):
            if isinstance(value, ObjectId):
                return str(value)
            elif isinstance(value, datetime):
                return value.isoformat()
            elif isinstance(value, Decimal128):
                return float(str(value))
            elif isinstance(value, Binary):
                return base64.b64encode(value).decode('utf-8')
            elif isinstance(value, dict):
                return {k: convert_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [convert_value(item) for item in value]
            else:
                return value
        
        return [convert_value(doc) for doc in docs]

    def extract(
        self,
        collection: str,
        query: Optional[Dict[str, Any]] = None,
        projection: Optional[Dict[str, int]] = None,
        sort: Optional[List[tuple]] = None,
        limit: Optional[int] = None,
    ) -> pd.DataFrame:
        """
        Извлечение документов из коллекции MongoDB.
        Args:
            collection: Название коллекции
            query: Фильтр для выборки документов (MongoDB query)
                  Например: {"status": "active", "amount": {"$gt": 100}}
            projection: Выбор полей (1 - включить, 0 - исключить)
                       Например: {"_id": 0, "name": 1, "email": 1}
            sort: Сортировка [(field, direction)]
                 Например: [("created_at", -1)] для DESC
            limit: Максимальное количество документов
        Returns:
            pd.DataFrame: Документы в виде DataFrame
        Raises:
            Exception: При ошибках подключения или выполнения запроса
        Example:
            >>> # Простой запрос
            >>> df = extractor.extract(collection="users")
            >>> 
            >>> # С фильтром и проекцией
            >>> df = extractor.extract(
            ...     collection="orders",
            ...     query={"status": "completed", "total": {"$gte": 100}},
            ...     projection={"_id": 0, "order_id": 1, "total": 1, "customer": 1},
            ...     sort=[("created_at", -1)],
            ...     limit=1000
            ... )
            >>> 
            >>> # Операторы MongoDB:
            >>> # $gt, $gte, $lt, $lte - сравнение
            >>> # $in, $nin - в списке / не в списке
            >>> # $and, $or, $not - логические операторы
            >>> # $exists - проверка существования поля
            >>> # $regex - регулярные выражения
        Note:
            MongoDB query синтаксис отличается от SQL!
            Примеры:
            - SQL: WHERE status = 'active' AND amount > 100
            - Mongo: {"status": "active", "amount": {"$gt": 100}}
        """
        self.logger.info("-" * 50)
        self.logger.info("EXECUTING MONGODB QUERY")
        self.logger.info("-" * 50)
        
        # Создаём Hook для подключения к MongoDB
        hook = MongoHook(mongo_conn_id=self.conn_id)
        
        # Логируем параметры запроса
        self.logger.info(f"Database: {self.database}")
        self.logger.info(f"Collection: {collection}")
        
        # Установка значений по умолчанию
        query = query or {}
        
        # Логируем query (без секретов)
        safe_query = {k: v for k, v in query.items() if not any(
            secret in str(k).lower() for secret in ['password', 'token', 'secret', 'key']
        )}
        if safe_query:
            self.logger.info(f"  Query: {safe_query}")
        
        if projection:
            self.logger.info(f"  Projection: {projection}")
        if sort:
            self.logger.info(f"  Sort: {sort}")
        if limit:
            self.logger.info(f"  Limit: {limit}")
        
        try:
            # Получаем подключение к MongoDB
            client = hook.get_conn()
            
            # Выбираем коллекцию
            coll = client[self.database][collection]
            
            # Выполняем запрос
            cursor = coll.find(query, projection)
            
            # Применяем сортировку
            if sort:
                cursor = cursor.sort(sort)
            
            # Применяем лимит
            if limit:
                cursor = cursor.limit(limit)
            
            # Получаем документы
            docs: List[Dict[str, Any]] = list(cursor)
            
            # Конвертируем BSON типы в JSON-совместимые
            docs = self._convert_bson_to_json(docs)

            self.logger.info(f"  Query executed successfully")
            self.logger.info(f"  Documents retrieved: {len(docs)}")
            
            # Преобразуем в DataFrame
            if docs:
                df = pd.DataFrame(docs)
            else:
                # Пустой DataFrame если нет документов
                df = pd.DataFrame()
                self.logger.warning("  No documents found")
            
            # Логируем статистику
            self.log_extraction_stats(df, {
                'source': 'MongoDB',
                'database': self.database,
                'collection': collection,
                'query_type': 'find'
            })
            
            return df
            
        except Exception as e:
            self.logger.error(f"  Query execution failed: {e}")
            raise
    
    def extract_by_date(
        self,
        collection: str,
        date_field: str,
        date_str: str,
        projection: Optional[Dict[str, int]] = None,
    ) -> pd.DataFrame:
        """
        Фильтрация документов по полю даты (строковое значение).        
        Упрощённый метод для частого случая - фильтрация по дате.        
        Args:
            collection: Название коллекции
            date_field: Название поля с датой
            date_str: Значение даты (строка)
            projection: Выбор полей (опционально)       
        Returns:
            pd.DataFrame: Документы за указанную дату        
        Example:
            >>> # События за конкретную дату
            >>> df = extractor.extract_by_date(
            ...     collection="events",
            ...     date_field="event_date",
            ...     date_str="2025-01-12"
            ... )
            >>> 
            >>> # С выбором полей
            >>> df = extractor.extract_by_date(
            ...     collection="logs",
            ...     date_field="log_date",
            ...     date_str="2025-01-12",
            ...     projection={"_id": 0, "level": 1, "message": 1}
            ... )       
        Note:
            Этот метод работает с датами в строковом формате.
            Для дат в формате ISODate используйте extract_by_date_range().
        """
        self.logger.info(f"Filtering by date: {date_field} = {date_str}")
        
        query = {date_field: date_str}
        
        return self.extract(
            collection=collection,
            query=query,
            projection=projection
        )
    
    def extract_by_date_range(
        self,
        collection: str,
        date_field: str,
        start_date: str,
        end_date: str,
        projection: Optional[Dict[str, int]] = None,
    ) -> pd.DataFrame:
        """
        Извлечение документов за диапазон дат (инкрементальная загрузка).        
        Args:
            collection: Название коллекции
            date_field: Название поля с датой (должно быть ISODate)
            start_date: Начальная дата (YYYY-MM-DD)
            end_date: Конечная дата (YYYY-MM-DD)
            projection: Выбор полей (опционально)
        Returns:
            pd.DataFrame: Документы за указанный период
        Example:
            >>> # События за неделю
            >>> df = extractor.extract_by_date_range(
            ...     collection="events",
            ...     date_field="created_at",
            ...     start_date="2025-01-01",
            ...     end_date="2025-01-07"
            ... )
        Note:
            Этот метод работает с датами в формате ISODate.
            Даты автоматически преобразуются в datetime объекты.
        """        
        self.logger.info(f"Date range: {start_date} to {end_date}")
        
        # Преобразуем строки в datetime
        start_dt = datetime.fromisoformat(start_date)
        end_dt = datetime.fromisoformat(end_date)
        
        # MongoDB query для диапазона дат
        query = {
            date_field: {
                "$gte": start_dt,
                "$lte": end_dt
            }
        }
        
        return self.extract(
            collection=collection,
            query=query,
            projection=projection,
            sort=[(date_field, 1)]  # Сортируем по дате (ASC)
        )
    
    def extract_with_aggregation(
        self,
        collection: str,
        pipeline: List[Dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Выполнение агрегационного запроса (aggregation pipeline).        
        Aggregation pipeline позволяет выполнять сложные трансформации:
        - Группировка ($group)
        - Фильтрация ($match)
        - Сортировка ($sort)
        - Проекция ($project)
        - Лимиты ($limit)
        - Объединения ($lookup)
        Args:
            collection: Название коллекции
            pipeline: Список стадий агрегации
        Returns:
            pd.DataFrame: Результат агрегации
        Example:
            >>> # Подсчёт событий по типам
            >>> pipeline = [
            ...     {"$match": {"status": "active"}},
            ...     {"$group": {
            ...         "_id": "$event_type",
            ...         "count": {"$sum": 1},
            ...         "total_amount": {"$sum": "$amount"}
            ...     }},
            ...     {"$sort": {"count": -1}}
            ... ]
            >>> df = extractor.extract_with_aggregation("events", pipeline)
            >>> 
            >>> # Группировка по дате
            >>> pipeline = [
            ...     {"$group": {
            ...         "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
            ...         "daily_count": {"$sum": 1}
            ...     }},
            ...     {"$sort": {"_id": 1}}
            ... ]
            >>> df = extractor.extract_with_aggregation("orders", pipeline)
        """
        self.logger.info("-" * 50)
        self.logger.info("EXECUTING MONGODB AGGREGATION")
        self.logger.info("-" * 50)
        
        hook = MongoHook(conn_id=self.conn_id)
        client = hook.get_conn()
        coll = client[self.database][collection]
        
        self.logger.info(f"Collection: {collection}")
        self.logger.info(f"Pipeline stages: {len(pipeline)}")
        
        try:
            # Выполняем aggregation
            result = list(coll.aggregate(pipeline))
            
            self.logger.info(f"  Aggregation completed")
            self.logger.info(f"  Results: {len(result)} documents")
            
            # Преобразуем в DataFrame
            if result:
                df = pd.DataFrame(result)
            else:
                df = pd.DataFrame()
                self.logger.warning("  No results from aggregation")
            
            # Логируем статистику
            self.log_extraction_stats(df, {
                'source': 'MongoDB',
                'database': self.database,
                'collection': collection,
                'query_type': 'aggregation',
                'pipeline_stages': len(pipeline)
            })
            
            return df
            
        except Exception as e:
            self.logger.error(f"  Aggregation failed: {e}")
            raise
    
    def extract_count(
        self,
        collection: str,
        query: Optional[Dict[str, Any]] = None,
    ) -> int:
        """
        Получить количество документов в коллекции.
        Полезно для проверки перед извлечением больших объёмов данных.
        Args:
            collection: Название коллекции
            query: Фильтр (опционально)
        Returns:
            int: Количество документов
        Example:
            >>> # Всего документов
            >>> count = extractor.extract_count("events")
            >>> print(f"Total events: {count}")
            >>> 
            >>> # С фильтром
            >>> count = extractor.extract_count(
            ...     collection="orders",
            ...     query={"status": "completed"}
            ... )
            >>> print(f"Completed orders: {count}")
        """
        hook = MongoHook(conn_id=self.conn_id)
        client = hook.get_conn()
        coll = client[self.database][collection]
        
        query = query or {}
        count = coll.count_documents(query)
        
        self.logger.info(f"Count in {collection}: {count} documents")
        return count
    
    def list_collections(self) -> List[str]:
        """
        Получить список всех коллекций в базе данных.
        Returns:
            list: Список названий коллекций
        Example:
            >>> collections = extractor.list_collections()
            >>> print("Available collections:", collections)
            >>> # ['users', 'events', 'orders', 'products']
        """
        hook = MongoHook(conn_id=self.conn_id)
        client = hook.get_conn()
        db = client[self.database]
        collections = db.list_collection_names()
        self.logger.info(f"Collections in {self.database}: {collections}")
        return collections
    
    def get_collection_stats(self, collection: str) -> Dict[str, Any]:
        """
        Получить статистику о коллекции.
        Args:
            collection: Название коллекции
        Returns:
            dict: Статистика (размер, количество документов, индексы)
        Example:
            >>> stats = extractor.get_collection_stats("events")
            >>> print(f"Documents: {stats.get('count', 0)}")
            >>> print(f"Size: {stats.get('size', 0)} bytes")
        """
        hook = MongoHook(conn_id=self.conn_id)
        client = hook.get_conn()
        db = client[self.database]
        
        stats = db.command("collStats", collection)
        
        self.logger.info(f"Collection {collection} stats:")
        self.logger.info(f"  Documents: {stats.get('count', 0)}")
        self.logger.info(f"  Size: {stats.get('size', 0)} bytes")
        self.logger.info(f"  Indexes: {stats.get('nindexes', 0)}")
        
        return stats
    
    def test_connection(self) -> bool:
        """
        Проверка подключения к MongoDB.
        Returns:
            bool: True если подключение успешно
        Example:
            >>> if extractor.test_connection():
            ...     print("MongoDB connection OK")
            ... else:
            ...     print("MongoDB connection FAILED")
        """
        try:
            hook = MongoHook(conn_id=self.conn_id)
            client = hook.get_conn()
            
            # Проверяем подключение
            client.server_info()
            
            # Проверяем доступ к базе
            db = client[self.database]
            db.list_collection_names()
            
            self.logger.info("  MongoDB connection test successful")
            return True
            
        except Exception as e:
            self.logger.error(f"  MongoDB connection test failed: {e}")
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
from extractors.mongo_extractor import MongoExtractor


def extract_events(**context):
    '''Извлечение событий за текущий день'''
    ds = context['ds']  # 2025-01-12
    
    # Создаём Extractor
    extractor = MongoExtractor("mongo_source", database="analytics_db")
    
    # Фильтрация по дате
    df = extractor.extract_by_date(
        collection="events",
        date_field="event_date",
        date_str=ds
    )
    
    # Сохраняем результат
    output_path = f"/opt/airflow/data/extracted/events_{ds}.csv"
    extractor.save_to_csv(df, output_path)
    
    return {
        'records': len(df),
        'file': output_path
    }


def extract_user_activity(**context):
    '''Агрегация активности пользователей'''
    extractor = MongoExtractor("mongo_source", "analytics_db")
    
    # Aggregation pipeline
    pipeline = [
        # Фильтруем активных пользователей
        {"$match": {"status": "active"}},
        
        # Группируем по типу события
        {"$group": {
            "_id": "$event_type",
            "count": {"$sum": 1},
            "unique_users": {"$addToSet": "$user_id"}
        }},
        
        # Подсчитываем уникальных пользователей
        {"$project": {
            "event_type": "$_id",
            "count": 1,
            "unique_users_count": {"$size": "$unique_users"}
        }},
        
        # Сортируем по количеству
        {"$sort": {"count": -1}}
    ]
    
    df = extractor.extract_with_aggregation(
        collection="events",
        pipeline=pipeline
    )
    
    return {'total_event_types': len(df)}


def extract_feedback(**context):
    '''Извлечение отзывов с фильтрацией'''
    extractor = MongoExtractor("mongo_source", "feedback_db")
    
    # Сложный query
    df = extractor.extract(
        collection="feedback",
        query={
            "rating": {"$gte": 4},  # Рейтинг >= 4
            "status": "published",
            "created_at": {
                "$gte": datetime(2025, 1, 1),
                "$lte": datetime(2025, 1, 31)
            }
        },
        projection={
            "_id": 0,
            "user_id": 1,
            "rating": 1,
            "comment": 1,
            "created_at": 1
        },
        sort=[("rating", -1), ("created_at", -1)],
        limit=1000
    )
    
    return {'positive_feedback_count': len(df)}


with DAG(
    dag_id='mongo_extract_example',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    extract_events_task = PythonOperator(
        task_id='extract_events',
        python_callable=extract_events,
    )
    
    extract_activity_task = PythonOperator(
        task_id='extract_user_activity',
        python_callable=extract_user_activity,
    )
    
    extract_feedback_task = PythonOperator(
        task_id='extract_feedback',
        python_callable=extract_feedback,
    )
    
    # Параллельное выполнение
    [extract_events_task, extract_activity_task, extract_feedback_task]
```


ДОПОЛНИТЕЛЬНЫЕ ПРИМЕРЫ MONGODB QUERIES:
========================================

1. Простая фильтрация:
   query = {"status": "active"}

2. Сравнение чисел:
   query = {"price": {"$gt": 100, "$lt": 500}}

3. Списки:
   query = {"category": {"$in": ["Electronics", "Books"]}}

4. Логические операторы:
   query = {
       "$and": [
           {"price": {"$gte": 100}},
           {"stock": {"$gt": 0}}
       ]
   }

5. Регулярные выражения:
   query = {"email": {"$regex": "@gmail.com$"}}

6. Существование поля:
   query = {"discount": {"$exists": True}}

7. Вложенные объекты:
   query = {"address.city": "Moscow"}

8. Массивы:
   query = {"tags": "electronics"}  # Содержит элемент
   query = {"tags": {"$all": ["new", "sale"]}}  # Содержит все

9. Даты:
   from datetime import datetime
   query = {
       "created_at": {
           "$gte": datetime(2025, 1, 1),
           "$lt": datetime(2025, 2, 1)
       }
   }

10. Сложные условия:
    query = {
        "$or": [
            {"status": "premium"},
            {"$and": [
                {"status": "regular"},
                {"orders_count": {"$gte": 10}}
            ]}
        ]
    }
"""
