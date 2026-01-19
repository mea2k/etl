"""
HTTP Extractor - извлечение данных из REST API

Использует HttpHook из Airflow для подключения к HTTP API
и извлечения данных через REST запросы.

"""

import os
import sys
from typing import Any, Dict, List, Optional
from airflow.providers.http.hooks.http import HttpHook
import pandas as pd
import requests
import json
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from base_extractor import BaseExtractor


class HttpExtractor(BaseExtractor):
    """
    Extractor для извлечения данных из REST API.
    
    Возможности:
    ------------
    - GET и POST запросы
    - Параметры запроса (query parameters)
    - Пользовательские headers
    - Авторизация (Bearer, Basic, API Key)
    - Пагинация (offset, cursor, page-based)
    - Обработка rate limits
    - Retry механизм
    - Парсинг различных форматов ответов
    
    Использование:
    --------------
    1. Создайте Connection в Airflow UI:
       - Admin → Connections → +
       - Connection Id: api_service
       - Connection Type: HTTP
       - Host: api.example.com (БЕЗ https://)
       - Schema: https
       - Extra: {"timeout": 30, "api_key": "your_key"}
    
    2. Используйте в DAG:
       >>> extractor = HttpExtractor("api_service")
       >>> df = extractor.extract("/api/v1/users", params={"limit": 100})
    
    Example:
        >>> # Простой GET запрос
        >>> extractor = HttpExtractor("api_service")
        >>> df = extractor.extract("/api/v1/posts")
        >>> 
        >>> # С параметрами
        >>> df = extractor.extract(
        ...     endpoint="/api/v1/users",
        ...     params={"status": "active", "limit": 100, "offset": 0}
        ... )
        >>> 
        >>> # POST запрос
        >>> df = extractor.extract(
        ...     endpoint="/api/v1/search",
        ...     method="POST",
        ...     data={"query": "airflow", "filters": {"category": "tech"}}
        ... )
        >>> 
        >>> # С пагинацией
        >>> df = extractor.extract_with_pagination(
        ...     endpoint="/api/v1/items",
        ...     limit_per_page=100,
        ...     max_pages=10
        ... )
    """
    
    def __init__(self, conn_id: str, timeout: Optional[int] = None):
        """
        Инициализация HTTP Extractor.        
        Args:
            conn_id: ID HTTP Connection в Airflow
            timeout: Таймаут запросов в секундах (по умолчанию из Connection)
        """
        super().__init__(conn_id)
        self.timeout = timeout
        
        if timeout:
            self.logger.info(f"Using custom timeout: {timeout}s")
    
    def extract(
        self,
        endpoint: str,
        method: str = "GET",
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
        response_key: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Выполнение HTTP запроса и возврат результата в виде DataFrame.
        Args:
            endpoint: Путь API endpoint (например, '/api/v1/users')
            method: HTTP метод ('GET', 'POST', 'PUT', 'DELETE')
            params: Query параметры для GET запроса
                   Например: {"limit": 100, "offset": 0}
            data: Данные для POST/PUT запроса (будут отправлены как JSON)
            headers: Дополнительные HTTP headers
            response_key: Ключ в JSON ответе где находятся данные
                         Например: "results", "data", "items"
        Returns:
            pd.DataFrame: Данные из API
        Raises:
            requests.HTTPError: При HTTP ошибках (4xx, 5xx)
            requests.RequestException: При ошибках сети
        Example:
            >>> # GET с параметрами
            >>> df = extractor.extract(
            ...     endpoint="/api/v1/users",
            ...     params={"status": "active", "limit": 50}
            ... )
            >>> 
            >>> # POST с данными
            >>> df = extractor.extract(
            ...     endpoint="/api/v1/search",
            ...     method="POST",
            ...     data={"query": "python", "category": "programming"}
            ... )
            >>> 
            >>> # С кастомными headers
            >>> df = extractor.extract(
            ...     endpoint="/api/v1/data",
            ...     headers={"X-API-Version": "2.0", "Accept-Language": "en"}
            ... )
            >>> 
            >>> # Когда данные в ключе "results"
            >>> df = extractor.extract(
            ...     endpoint="/api/v1/items",
            ...     response_key="results"  # JSON: {"results": [...], "count": 100}
            ... )
        Note:
            - Для авторизации используйте Connection Extra
            - Rate limits обрабатываются автоматически (retry)
        """
        self.logger.info("-" * 50)
        self.logger.info(f"EXECUTING HTTP {method} REQUEST")
        self.logger.info("-" * 50)
        
        # Создаём Hook для HTTP запросов
        hook = HttpHook(
            http_conn_id=self.conn_id,
            method=method
        )
        
        # Логируем параметры запроса
        self.logger.info(f"Endpoint: {endpoint}")
        self.logger.info(f"Method: {method}")
        
        if params:
            # Логируем параметры (без секретов)
            safe_params = {k: v for k, v in params.items() if not any(
                secret in str(k).lower() for secret in ['password', 'token', 'secret', 'key']
            )}
            if safe_params:
                self.logger.info(f"Params: {safe_params}")
        
        # Подготовка headers
        request_headers = {"Accept": "application/json"}
        if headers:
            request_headers.update(headers)
        
        # Получаем таймаут
        timeout = self.timeout or 30
        
        try:
            # Выполняем запрос
            if method.upper() == "GET":
                response = hook.run(
                    endpoint=endpoint,
                    data=params,
                    headers=request_headers,
                    extra_options={"timeout": timeout}
                )
            else:  # POST, PUT, DELETE
                response = hook.run(
                    endpoint=endpoint,
                    data=json.dumps(data) if data else None,
                    headers={**request_headers, "Content-Type": "application/json"},
                    extra_options={"timeout": timeout}
                )
            
            # Проверяем статус код
            response.raise_for_status()
            
            self.logger.info(f"Request successful")
            self.logger.info(f"  Status code: {response.status_code}")
            self.logger.info(f"  Response size: {len(response.content)} bytes")
            
            # Парсим JSON ответ
            try:
                json_data = response.json()
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to parse JSON response: {e}")
                self.logger.error(f"Response text: {response.text[:500]}")
                raise ValueError(f"Invalid JSON response: {e}")
            
            # Извлекаем данные по ключу если указан
            if response_key:
                if response_key in json_data:
                    json_data = json_data[response_key]
                    self.logger.info(f"Extracted data from key: {response_key}")
                else:
                    self.logger.warning(f"  Key '{response_key}' not found in response")
            
            # Преобразуем в DataFrame
            df = self._json_to_dataframe(json_data)
            
            # Логируем статистику
            self.log_extraction_stats(df, {
                'source': 'HTTP API',
                'endpoint': endpoint,
                'method': method,
                'status_code': response.status_code
            })
            
            return df
            
        except requests.HTTPError as e:
            status_code = e.response.status_code if e.response else 'unknown'
            self.logger.error(f"✗ HTTP Error {status_code}: {e}")
            
            # Логируем тело ошибки
            if e.response:
                try:
                    error_body = e.response.json()
                    self.logger.error(f"Error response: {error_body}")
                except:
                    self.logger.error(f"Error response: {e.response.text[:500]}")
            
            raise
            
        except requests.RequestException as e:
            self.logger.error(f"  Request failed: {e}")
            raise
    
    def extract_with_pagination(
        self,
        endpoint: str,
        limit_per_page: int = 100,
        max_pages: Optional[int] = None,
        pagination_type: str = "offset",
        params: Optional[Dict[str, Any]] = None,
        response_key: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Извлечение данных с автоматической пагинацией.
        Поддерживаемые типы пагинации:
        - offset: ?limit=100&offset=0, ?limit=100&offset=100, ...
        - page: ?per_page=100&page=1, ?per_page=100&page=2, ...
        - cursor: ?limit=100&cursor=next_token (требует response_key для cursor)
        Args:
            endpoint: API endpoint
            limit_per_page: Количество записей на странице
            max_pages: Максимальное количество страниц (None = все)
            pagination_type: Тип пагинации ('offset', 'page', 'cursor')
            params: Дополнительные параметры запроса
            response_key: Ключ где находятся данные в ответе
        Returns:
            pd.DataFrame: Все данные со всех страниц
        Example:
            >>> # Offset пагинация
            >>> df = extractor.extract_with_pagination(
            ...     endpoint="/api/v1/users",
            ...     limit_per_page=100,
            ...     max_pages=5,
            ...     pagination_type="offset"
            ... )
            >>> 
            >>> # Page пагинация
            >>> df = extractor.extract_with_pagination(
            ...     endpoint="/api/v1/posts",
            ...     limit_per_page=50,
            ...     pagination_type="page",
            ...     response_key="data"
            ... )
            >>> 
            >>> # Cursor пагинация (до конца)
            >>> df = extractor.extract_with_pagination(
            ...     endpoint="/api/v1/items",
            ...     limit_per_page=100,
            ...     pagination_type="cursor",
            ...     response_key="items"
            ... )
        """
        self.logger.info("-" * 50)
        self.logger.info("PAGINATION EXTRACTION")
        self.logger.info("-" * 50)
        self.logger.info(f"Type: {pagination_type}")
        self.logger.info(f"Limit per page: {limit_per_page}")
        self.logger.info(f"Max pages: {max_pages or 'unlimited'}")
        
        all_data = []
        page_num = 0
        
        if pagination_type == "offset":
            offset = 0
            
            while True:
                page_num += 1
                self.logger.info(f"Fetching page {page_num} (offset={offset})...")
                
                # Параметры для offset пагинации
                page_params = {**(params or {}), "limit": limit_per_page, "offset": offset}
                
                # Запрос страницы
                df_page = self.extract(
                    endpoint=endpoint,
                    params=page_params,
                    response_key=response_key
                )
                
                # Если пусто - конец
                if df_page.empty:
                    self.logger.info("Empty page - stopping pagination")
                    break
                
                all_data.append(df_page)
                self.logger.info(f"  Page {page_num}: {len(df_page)} records")
                
                # Проверка лимита страниц
                if max_pages and page_num >= max_pages:
                    self.logger.info(f"Reached max pages limit: {max_pages}")
                    break
                
                # Если записей меньше лимита - это последняя страница
                if len(df_page) < limit_per_page:
                    self.logger.info("Last page (less than limit)")
                    break
                
                offset += limit_per_page
        
        elif pagination_type == "page":
            page = 1
            
            while True:
                page_num += 1
                self.logger.info(f"Fetching page {page}...")
                
                # Параметры для page пагинации
                page_params = {**(params or {}), "per_page": limit_per_page, "page": page}
                
                # Запрос страницы
                df_page = self.extract(
                    endpoint=endpoint,
                    params=page_params,
                    response_key=response_key
                )
                
                if df_page.empty:
                    break
                
                all_data.append(df_page)
                self.logger.info(f"  Page {page}: {len(df_page)} records")
                
                if max_pages and page_num >= max_pages:
                    break
                
                if len(df_page) < limit_per_page:
                    break
                
                page += 1
        
        elif pagination_type == "cursor":
            cursor = None
            
            while True:
                page_num += 1
                self.logger.info(f"Fetching page {page_num} (cursor={cursor})...")
                
                # Параметры для cursor пагинации
                page_params = {**(params or {}), "limit": limit_per_page}
                if cursor:
                    page_params["cursor"] = cursor
                
                # Запрос страницы (получаем полный ответ)
                hook = HttpHook(http_conn_id=self.conn_id, method="GET")
                response = hook.run(
                    endpoint=endpoint,
                    data=page_params,
                    headers={"Accept": "application/json"}
                )
                response.raise_for_status()
                json_data = response.json()
                
                # Извлекаем данные
                items = json_data.get(response_key, json_data) if response_key else json_data
                
                if not items or (isinstance(items, list) and len(items) == 0):
                    break
                
                df_page = self._json_to_dataframe(items)
                all_data.append(df_page)
                self.logger.info(f"  Page {page_num}: {len(df_page)} records")
                
                # Получаем следующий cursor
                next_cursor = json_data.get("next_cursor") or json_data.get("next")
                if not next_cursor:
                    self.logger.info("No next cursor - end of pagination")
                    break
                
                cursor = next_cursor
                
                if max_pages and page_num >= max_pages:
                    break
        
        else:
            raise ValueError(f"Unknown pagination type: {pagination_type}")
        
        # Объединяем все страницы
        if all_data:
            df_combined = pd.concat(all_data, ignore_index=True)
            self.logger.info("-" * 50)
            self.logger.info(f"PAGINATION COMPLETE")
            self.logger.info(f"Total pages: {page_num}")
            self.logger.info(f"Total records: {len(df_combined)}")
            self.logger.info("-" * 50)
            return df_combined
        else:
            self.logger.warning("No data retrieved")
            return pd.DataFrame()
    
    def extract_with_retry(
        self,
        endpoint: str,
        max_retries: int = 3,
        retry_delay: int = 5,
        **kwargs
    ) -> pd.DataFrame:
        """
        Извлечение данных с автоматическим retry при ошибках.
        Полезно для API с временными сбоями или rate limits.
        Args:
            endpoint: API endpoint
            max_retries: Максимальное количество попыток
            retry_delay: Задержка между попытками (секунды)
            **kwargs: Параметры для extract()
        Returns:
            pd.DataFrame: Данные из API
        Example:
            >>> df = extractor.extract_with_retry(
            ...     endpoint="/api/v1/data",
            ...     max_retries=5,
            ...     retry_delay=10,
            ...     params={"date": "2025-01-12"}
            ... )
        """
        self.logger.info(f"Extract with retry: max_retries={max_retries}, delay={retry_delay}s")
        
        for attempt in range(1, max_retries + 1):
            try:
                self.logger.info(f"Attempt {attempt}/{max_retries}")
                df = self.extract(endpoint=endpoint, **kwargs)
                return df
                
            except (requests.HTTPError, requests.RequestException) as e:
                if attempt == max_retries:
                    self.logger.error(f"All {max_retries} attempts failed")
                    raise
                
                # Проверяем код ошибки
                if isinstance(e, requests.HTTPError):
                    status_code = e.response.status_code if e.response else 0
                    
                    # Для 429 (Rate Limit) увеличиваем задержку
                    if status_code == 429:
                        wait_time = retry_delay * (attempt * 2)  # Экспоненциальная задержка
                        self.logger.warning(f"Rate limit hit, waiting {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    
                    # Для 5xx (Server Error) повторяем
                    if 500 <= status_code < 600:
                        self.logger.warning(f"Server error {status_code}, retrying...")
                        time.sleep(retry_delay)
                        continue
                    
                    # Для 4xx (Client Error) не повторяем
                    if 400 <= status_code < 500:
                        self.logger.error(f"Client error {status_code}, not retrying")
                        raise
                
                # Для других ошибок повторяем
                self.logger.warning(f"Request failed: {e}, retrying in {retry_delay}s...")
                time.sleep(retry_delay)
    
    def _json_to_dataframe(self, json_data: Any) -> pd.DataFrame:
        """
        Преобразование JSON данных в DataFrame.
        Обрабатывает различные форматы:
        - Список объектов: [{"id": 1}, {"id": 2}]
        - Одиночный объект: {"id": 1, "name": "test"}
        - Вложенные структуры
        Args:
            json_data: JSON данные
        Returns:
            pd.DataFrame: DataFrame с данными
        """
        if isinstance(json_data, list):
            # Список объектов - стандартный случай
            if not json_data:
                return pd.DataFrame()
            return pd.DataFrame(json_data)
        elif isinstance(json_data, dict):
            # Одиночный объект - преобразуем в список
            return pd.DataFrame([json_data])
        else:
            # Другие типы - пытаемся обработать
            self.logger.warning(f"Unexpected JSON type: {type(json_data)}")
            try:
                return pd.DataFrame([{"data": json_data}])
            except:
                return pd.DataFrame()
    
    def test_connection(self) -> bool:
        """
        Проверка подключения к API.
        Пытается выполнить простой запрос для проверки доступности.
        Returns:
            bool: True если подключение успешно
        Example:
            >>> if extractor.test_connection():
            ...     print("API connection OK")
            ... else:
            ...     print("API connection FAILED")
        """
        try:
            hook = HttpHook(http_conn_id=self.conn_id, method="GET")
            
            # Пробуем базовый endpoint (обычно "/" или "/health")
            for test_endpoint in ["/", "/health", "/api", "/api/v1"]:
                try:
                    response = hook.run(
                        endpoint=test_endpoint,
                        extra_options={"timeout": 10}
                    )
                    if response.status_code < 500:  # Любой не серверный ответ - OK
                        self.logger.info(f"  API connection test successful ({test_endpoint})")
                        return True   
                except:
                    continue
            
            # Если ни один endpoint не ответил
            self.logger.warning("    No test endpoint responded, but connection might still work")
            return False
            
        except Exception as e:
            self.logger.error(f"  API connection test failed: {e}")
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
from extractors.http_extractor import HttpExtractor


def extract_api_users(**context):
    '''Извлечение пользователей из API'''
    # Создаём Extractor
    extractor = HttpExtractor("api_service")
    
    # Простой GET запрос
    df = extractor.extract(
        endpoint="/users",
        params={"status": "active", "limit": 100}
    )
    
    # Сохраняем результат
    output_path = f"/opt/airflow/data/extracted/api_users_{context['ds']}.csv"
    extractor.save_to_csv(df, output_path)
    
    return {
        'records': len(df),
        'file': output_path
    }


def extract_posts_with_pagination(**context):
    '''Извлечение всех постов с пагинацией'''
    extractor = HttpExtractor("jsonplaceholder_api")
    
    # Автоматическая пагинация
    df = extractor.extract_with_pagination(
        endpoint="/posts",
        limit_per_page=20,
        max_pages=5,  # Макс 5 страниц
        pagination_type="offset"
    )
    
    return {'total_posts': len(df)}


def search_items(**context):
    '''Поиск через POST запрос'''
    extractor = HttpExtractor("api_service")
    
    # POST запрос с данными
    df = extractor.extract(
        endpoint="/api/v1/search",
        method="POST",
        data={
            "query": "airflow tutorial",
            "filters": {
                "category": "technology",
                "date_from": "2025-01-01"
            }
        },
        response_key="results"  # Данные в ключе "results"
    )
    
    return {'search_results': len(df)}


def extract_github_repos(**context):
    '''Извлечение репозиториев с GitHub API'''
    extractor = HttpExtractor("github_api")
    
    # С пагинацией и retry
    df = extractor.extract_with_retry(
        endpoint="/search/repositories",
        params={
            "q": "language:python topic:etl",
            "sort": "stars",
            "order": "desc"
        },
        max_retries=5,
        retry_delay=10,
        response_key="items"
    )
    
    return {'repos_found': len(df)}


def extract_with_auth(**context):
    '''Извлечение с авторизацией'''
    extractor = HttpExtractor("api_with_auth")
    
    # Headers с токеном (токен должен быть в Connection Extra)
    df = extractor.extract(
        endpoint="/api/v1/protected/data",
        headers={
            "X-API-Version": "2.0",
            "Accept-Language": "en-US"
        }
    )
    
    return {'protected_data_count': len(df)}


with DAG(
    dag_id='http_extract_example',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    extract_users_task = PythonOperator(
        task_id='extract_users',
        python_callable=extract_api_users,
    )
    
    extract_posts_task = PythonOperator(
        task_id='extract_posts',
        python_callable=extract_posts_with_pagination,
    )
    
    search_task = PythonOperator(
        task_id='search_items',
        python_callable=search_items,
    )
    
    github_task = PythonOperator(
        task_id='extract_github',
        python_callable=extract_github_repos,
    )
    
    auth_task = PythonOperator(
        task_id='extract_protected',
        python_callable=extract_with_auth,
    )
    
    # Параллельное выполнение
    [extract_users_task, extract_posts_task, search_task, github_task, auth_task]
```


ПРИМЕРЫ CONNECTION НАСТРОЕК:
=============================

1. Базовый HTTP Connection:
   Connection Id: api_service
   Type: HTTP
   Host: api.example.com
   Schema: https
   Extra: {"timeout": 30}

2. С API ключом в Extra:
   Connection Id: api_with_key
   Type: HTTP
   Host: api.example.com
   Schema: https
   Extra: {"api_key": "your_api_key_here", "timeout": 60}

3. С Bearer Token:
   Connection Id: api_bearer
   Type: HTTP
   Host: api.example.com
   Schema: https
   Extra: {"bearer_token": "your_token_here"}

4. GitHub API:
   Connection Id: github_api
   Type: HTTP
   Host: api.github.com
   Schema: https
   Extra: {"Authorization": "token YOUR_GITHUB_TOKEN"}

5. JSONPlaceholder (для тестов):
   Connection Id: jsonplaceholder
   Type: HTTP
   Host: jsonplaceholder.typicode.com
   Schema: https
   Extra: {"timeout": 30}


ТИПИЧНЫЕ ПАТТЕРНЫ API:
======================

1. REST API с JSON:
   GET /api/v1/users
   Response: [{"id": 1, "name": "Alice"}, ...]

2. API с вложенными данными:
   GET /api/v1/data
   Response: {"status": "ok", "data": [...], "count": 100}
   Используйте: response_key="data"

3. API с пагинацией (offset):
   GET /api/items?limit=100&offset=0
   GET /api/items?limit=100&offset=100

4. API с пагинацией (page):
   GET /api/items?per_page=50&page=1
   GET /api/items?per_page=50&page=2

5. API с cursor пагинацией:
   GET /api/items?limit=100
   Response: {"items": [...], "next_cursor": "abc123"}
   GET /api/items?limit=100&cursor=abc123

6. POST для поиска:
   POST /api/search
   Body: {"query": "test", "filters": {...}}

7. API с rate limits:
   429 Too Many Requests
   Используйте: extract_with_retry()

8. API с авторизацией:
   Header: Authorization: Bearer <token>
   Или: X-API-Key: <key>
"""
