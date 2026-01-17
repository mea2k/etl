"""
Simple HTTP Hook - упрощённый Hook для работы с HTTP API

Этот Hook демонстрирует базовые принципы создания кастомных Hooks:
1. Получение Connection из Airflow
2. Построение URL на основе параметров Connection
3. Выполнение HTTP-запросов с авторизацией
4. Безопасное логирование (без секретов)

"""

from __future__ import annotations

import json
from typing import Any, Dict, Optional

from airflow.hooks.base import BaseHook
import requests


class SimpleHTTPHook(BaseHook):
    """
    Простой Hook для работы с HTTP API.
    
    Концепция Hook:
    ---------------
    Hook - это класс, который:
    1. Получает параметры подключения из Connection
    2. Предоставляет удобные методы для работы с внешней системой
    3. Скрывает детали реализации от Operator
    
    Преимущества:
    -------------
    - Переиспользование логики между DAG
    - Централизованное управление credentials
    - Легкое тестирование
    - Стандартизация работы с API
    
    Использование:
    --------------
    >>> hook = SimpleHTTPHook(http_conn_id='my_api_conn')
    >>> data = hook.get('/api/v1/users', params={'limit': 10})
    >>> print(data)
    
    Attributes:
        conn_name_attr: Имя атрибута для connection_id
        default_conn_name: Connection ID по умолчанию
        conn_type: Тип подключения (для регистрации в Airflow)
        hook_name: Название Hook (отображается в UI)
    """
    
    # Метаданные Hook для регистрации в Airflow
    conn_name_attr = "http_conn_id"
    default_conn_name = "simple_http_default"
    conn_type = "http"
    hook_name = "Simple HTTP Hook"
    
    def __init__(self, http_conn_id: str = default_conn_name) -> None:
        """
        Инициализация Hook.
        Args:
            http_conn_id: ID Connection в Airflow
                         (создаётся через UI: Admin → Connections)
        """
        super().__init__()
        self.http_conn_id = http_conn_id
        self.log.info(f"Initialized SimpleHTTPHook with conn_id={http_conn_id}")
    
    def get_conn_config(self) -> Dict[str, Any]:
        """
        Получает конфигурацию подключения из Airflow Connection.
        Connection содержит:
            - host: адрес сервера (например, api.example.com)
            - schema: протокол (http или https)
            - login: имя пользователя (опционально)
            - password: пароль (опционально)
            - port: порт (опционально)
            - extra: дополнительные параметры в JSON формате
        Returns:
            dict: Словарь с параметрами подключения
        Example:
            >>> config = hook.get_conn_config()
            >>> print(config['host'])
            'api.example.com'
        """
        # BaseHook.get_connection() - метод из базового класса
        # Он получает Connection из БД Airflow по conn_id
        conn = self.get_connection(self.http_conn_id)
        
        # extra_dejson - десериализует JSON из поля Extra
        extra = conn.extra_dejson or {}
        
        config = {
            "host": conn.host,                      # Адрес сервера
            "schema": conn.schema or "https",       # Протокол (по умолчанию https)
            "login": conn.login,                    # Логин (если нужна auth)
            "password": conn.password,              # Пароль (если нужна auth)
            "port": conn.port,                      # Порт (если не стандартный)
            "extra": extra,                         # Дополнительные параметры
        }
        
        # ВАЖНО: Логируем только безопасные данные!
        # НЕ логируем password, api_key и другие секреты
        self.log.info(f"Connection config: host={config['host']}, schema={config['schema']}")
        
        return config
    
    def build_url(self, endpoint: str) -> str:
        """
        Строит полный URL на основе Connection и endpoint.
        Args:
            endpoint: Путь API (например, '/api/v1/users')
        Returns:
            str: Полный URL (например, 'https://api.example.com/api/v1/users')
        Example:
            >>> url = hook.build_url('/api/v1/posts')
            >>> print(url)
            'https://jsonplaceholder.typicode.com/api/v1/posts'
        """
        cfg = self.get_conn_config()
        
        # Базовый URL: schema://host
        base = f"{cfg['schema']}://{cfg['host']}"
        
        # Добавляем порт если указан
        if cfg["port"]:
            base = f"{base}:{cfg['port']}"
        
        # Добавляем endpoint
        if endpoint.startswith("/"):
            url = base + endpoint
        else:
            url = f"{base}/{endpoint}"
        
        self.log.info(f"Built URL: {url}")
        return url
    
    def _get_auth_headers(self) -> Dict[str, str]:
        """
        Создаёт заголовки авторизации на основе Connection.
        Поддерживаемые типы авторизации:
            1. API Token (из extra.api_token)
            2. Bearer Token (из extra.bearer_token)
            3. Basic Auth (из login/password)
        Returns:
            dict: Словарь с заголовками
        Note:
            Это приватный метод (начинается с _), используется внутри класса
        """
        cfg = self.get_conn_config()
        headers = {}
        
        # Способ 1: API Token из extra
        api_token = cfg["extra"].get("api_token")
        if api_token:
            headers["Authorization"] = f"Bearer {api_token}"
            self.log.info("Using API token from extra")
            return headers
        
        # Способ 2: Bearer Token из extra
        bearer_token = cfg["extra"].get("bearer_token")
        if bearer_token:
            headers["Authorization"] = f"Bearer {bearer_token}"
            self.log.info("Using Bearer token from extra")
            return headers
        
        # Способ 3: Basic Auth из login/password
        # (requests.get сам добавит Basic Auth если передать auth=(login, password))
        
        return headers
    
    def get(
        self, 
        endpoint: str, 
        params: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Выполняет GET-запрос к API.
        Args:
            endpoint: Путь API (например, '/posts')
            params: Query параметры (например, {'limit': 10, 'page': 1})
            headers: Дополнительные HTTP заголовки
        Returns:
            dict: Ответ API в виде словаря
        Raises:
            requests.HTTPError: Если статус код не 2xx
            requests.RequestException: Если произошла ошибка сети
        Example:
            >>> data = hook.get('/posts', params={'_limit': 5})
            >>> print(len(data))
            5
        """
        # Строим URL
        url = self.build_url(endpoint)
        
        # Получаем заголовки авторизации
        auth_headers = self._get_auth_headers()
        
        # Объединяем заголовки (переданные имеют приоритет)
        final_headers = {**auth_headers, **(headers or {})}
        
        # Получаем timeout из extra или используем по умолчанию
        cfg = self.get_conn_config()
        timeout = cfg["extra"].get("timeout", 30)
        
        # ВАЖНО: Логируем запрос (без секретов в заголовках!)
        self.log.info(f"GET {url}")
        self.log.info(f"Params: {params}")
        self.log.info(f"Timeout: {timeout}s")
        
        try:
            # Выполняем запрос
            response = requests.get(
                url,
                params=params,
                headers=final_headers,
                timeout=timeout,
            )
            
            # Проверяем статус код (вызовет исключение если не 2xx)
            response.raise_for_status()
            
            self.log.info(f"Response status: {response.status_code}")
            self.log.info(f"Response size: {len(response.content)} bytes")
            
            # Пытаемся распарсить JSON
            try:
                data = response.json()
                self.log.info(f"Parsed JSON response")
                return data
            except json.JSONDecodeError:
                # Если не JSON, возвращаем текст
                self.log.warning("Response is not JSON, returning raw text")
                return {"raw_text": response.text}
                
        except requests.HTTPError as e:
            # HTTP ошибка (4xx, 5xx)
            self.log.error(f"HTTP Error: {e}")
            self.log.error(f"Response: {e.response.text if e.response else 'No response'}")
            raise
            
        except requests.RequestException as e:
            # Ошибка сети
            self.log.error(f"Request failed: {e}")
            raise
    
    def post(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Выполняет POST-запрос к API.
        Args:
            endpoint: Путь API
            data: Данные в формате form-data
            json_data: Данные в формате JSON
            headers: Дополнительные HTTP заголовки
        Returns:
            dict: Ответ API
        Example:
            >>> result = hook.post('/posts', json_data={
            ...     'title': 'Test',
            ...     'body': 'Content',
            ...     'userId': 1
            ... })
        """
        url = self.build_url(endpoint)
        auth_headers = self._get_auth_headers()
        final_headers = {**auth_headers, **(headers or {})}
        
        cfg = self.get_conn_config()
        timeout = cfg["extra"].get("timeout", 30)
        
        self.log.info(f"POST {url}")
        
        try:
            response = requests.post(
                url,
                data=data,
                json=json_data,
                headers=final_headers,
                timeout=timeout,
            )
            
            response.raise_for_status()
            self.log.info(f"Response status: {response.status_code}")
            
            try:
                return response.json()
            except json.JSONDecodeError:
                return {"raw_text": response.text}
                
        except requests.HTTPError as e:
            self.log.error(f"HTTP Error: {e}")
            raise
        except requests.RequestException as e:
            self.log.error(f"Request failed: {e}")
            raise


# =============================================================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ
# =============================================================================

"""
Как использовать этот Hook:

1. Создайте Connection в Airflow UI:
   - Admin → Connections → +
   - Connection Id: my_api_conn
   - Connection Type: HTTP
   - Host: jsonplaceholder.typicode.com
   - Schema: https
   - Extra: {"timeout": 30}

2. Используйте в коде:
   
   from custom_hooks.simple_http_hook import SimpleHTTPHook
   # Создать Hook
   hook = SimpleHTTPHook(http_conn_id='my_api_conn')
   # GET запрос
   posts = hook.get('/posts', params={'_limit': 10})
   # POST запрос
   new_post = hook.post('/posts', json_data={
       'title': 'My Title',
       'body': 'Content',
       'userId': 1
   })

3. Использовать в Operator:
   
   class MyOperator(BaseOperator):
       def execute(self, context):
           hook = SimpleHTTPHook(self.http_conn_id)
           data = hook.get(self.endpoint)
           # обработка данных
           return data
"""
