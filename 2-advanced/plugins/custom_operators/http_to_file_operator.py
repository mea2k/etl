"""
HTTP to File Operator - кастомный оператор для загрузки данных из HTTP API в файл

Этот Operator демонстрирует:
1. Как создавать кастомные операторы
2. Использование Hooks внутри Operator
3. Поддержку Jinja-шаблонов
4. Обработку ошибок и валидацию

"""

from __future__ import annotations

import json
from multiprocessing import context
import os
from typing import Any, Dict, Optional

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Импортируем наш кастомный Hook
# ВАЖНО: путь импорта должен соответствовать структуре проекта
from custom_hooks.simple_http_hook import SimpleHTTPHook


class HttpToFileOperator(BaseOperator):
    """
    Оператор для загрузки данных из HTTP API и сохранения в файл.
    
    Концепция Operator:
    -------------------
    Operator - это класс, который:
    1. Описывает ЧТО делает задача (Task)
    2. Использует Hook для работы с внешней системой
    3. Поддерживает параметризацию через Jinja-шаблоны
    4. Может быть переиспользован в разных DAG
    
    Преимущества кастомного Operator:
    ---------------------------------
    - Инкапсуляция сложной логики
    - Переиспользование между DAG
    - Стандартизация задач
    - Легкое тестирование
    
    Использование в DAG:
    --------------------
    >>> download_task = HttpToFileOperator(
    ...     task_id='download_data',
    ...     http_conn_id='my_api_conn',
    ...     endpoint='/api/v1/users',
    ...     output_path='/data/users_{{ ds }}.json',
    ...     params={'limit': 100},
    ... )
    
    Attributes:
        template_fields: Поля, поддерживающие Jinja-шаблоны ({{ ds }}, {{ var.value.my_var }})
        ui_color: Цвет задачи в Graph View (для визуальной идентификации)
    """
    
    # Поля, которые поддерживают Jinja-шаблоны
    # Например, endpoint='/posts/{{ ds }}' → endpoint='/posts/2025-01-12'
    template_fields = ['endpoint', 'output_path']
    
    # Цвет задачи в Graph View (помогает визуально отличать типы задач)
    ui_color = '#4CAF50'  # Зелёный для загрузки данных
    
    def __init__(
        self,
        *,
        http_conn_id: str,
        endpoint: str,
        output_path: str,
        params: Optional[Dict[str, Any]] = None,
        method: str = 'GET',
        data: Optional[Dict[str, Any]] = None,
        create_dirs: bool = True,
        overwrite: bool = True,
        **kwargs,
    ) -> None:
        """
        Инициализация оператора.
        Args:
            http_conn_id: ID Connection в Airflow (Admin → Connections)
            endpoint: Путь API (например, '/api/v1/posts')
                     Поддерживает шаблоны: '/posts/{{ ds }}'
            output_path: Путь для сохранения файла
                        Поддерживает шаблоны: '/data/posts_{{ ds }}.json'
            params: Query параметры для GET-запроса
                   Поддерживает шаблоны: {'limit': '{{ var.value.limit }}'}
            method: HTTP метод ('GET' или 'POST')
            data: Данные для POST-запроса
            create_dirs: Создавать директории если не существуют
            overwrite: Перезаписывать файл если существует
            **kwargs: Дополнительные параметры BaseOperator
                     (task_id, dag, retries, и т.д.)
        Example:
            >>> op = HttpToFileOperator(
            ...     task_id='download_posts',
            ...     http_conn_id='jsonplaceholder',
            ...     endpoint='/posts',
            ...     output_path='/opt/airflow/data/posts.json',
            ...     params={'_limit': 10},
            ... )
        """
        super().__init__(**kwargs)
        
        # Сохраняем параметры как атрибуты
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.output_path = output_path
        self.request_params = params or {}
        self.method = method.upper()
        self.data = data
        self.create_dirs = create_dirs
        self.overwrite = overwrite
        
        # Валидация параметров
        if self.method not in ['GET', 'POST']:
            raise ValueError(f"Unsupported HTTP method: {self.method}")
        
        self.log.info(f"Initialized HttpToFileOperator:")
        self.log.info(f"  conn_id: {self.http_conn_id}")
        self.log.info(f"  endpoint: {self.endpoint}")
        self.log.info(f"  output: {self.output_path}")
    
    def execute(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        Основной метод выполнения задачи.
        Вызывается Airflow при запуске Task.
        Args:
            context: Контекст выполнения Airflow
                    Содержит: ds, execution_date, dag, task, и т.д.
        Returns:
            dict: Результат выполнения (сохраняется в XCom)
                 {'status': 'success', 'file_path': '...', 'records_count': 10}
        Raises:
            Exception: Если произошла ошибка загрузки или сохранения
        Note:
            Этот метод должен быть идемпотентным - повторный запуск
            должен давать тот же результат.
        """
        self.log.info("-" * 50)
        self.log.info("НАЧАЛО ВЫПОЛНЕНИЯ HttpToFileOperator")
        self.log.info("-" * 50)
        
        # Шаг 1: Создаём Hook для работы с API
        self.log.info(f"Step 1: Creating HTTP Hook (conn_id={self.http_conn_id})")
        hook = SimpleHTTPHook(http_conn_id=self.http_conn_id)
        
        # Шаг 2: Подготовка параметров
        # В этот момент Jinja-шаблоны уже обработаны Airflow
        self.log.info(f"Step 2: Preparing request parameters")
        self.log.info(f"  Endpoint: {self.endpoint}")
        self.log.info(f"  Method: {self.method}")
        self.log.info(f"  Params: {self.params}")
        
        # Можно добавить параметры из контекста
        # Например, добавим дату выполнения в params
        enriched_params = dict(self.request_params)
        enriched_params.setdefault("execution_date", context["ds"])
        self.log.info(f"  Enriched Params: {enriched_params}")
        
        # Шаг 3: Выполнение HTTP-запроса
        self.log.info(f"Step 3: Making HTTP request")
        try:
            if self.method == 'GET':
                response_data = hook.get(
                    endpoint=self.endpoint,
                    params=enriched_params,
                )
            elif self.method == 'POST':
                response_data = hook.post(
                    endpoint=self.endpoint,
                    json_data=self.data,
                )
            else:
                raise ValueError(f"Unsupported method: {self.method}")
            
            self.log.info(f"HTTP request successful")
            
        except Exception as e:
            self.log.error(f"HTTP request failed: {e}")
            raise
        
        # Шаг 4: Валидация данных
        self.log.info(f"Step 4: Validating response data")
        if not response_data:
            self.log.warning("  Response data is empty")
        
        # Подсчёт количества записей
        if isinstance(response_data, list):
            records_count = len(response_data)
        elif isinstance(response_data, dict):
            # Если это объект с массивом внутри
            if 'data' in response_data and isinstance(response_data['data'], list):
                records_count = len(response_data['data'])
            else:
                records_count = 1
        else:
            records_count = 0
        
        self.log.info(f"  Records count: {records_count}")
        
        # Шаг 5: Создание директорий
        if self.create_dirs:
            output_dir = os.path.dirname(self.output_path)
            if output_dir and not os.path.exists(output_dir):
                self.log.info(f"Step 5: Creating directory: {output_dir}")
                os.makedirs(output_dir, exist_ok=True)
        
        # Шаг 6: Проверка существования файла
        if os.path.exists(self.output_path):
            if self.overwrite:
                self.log.warning(f"  File exists and will be overwritten: {self.output_path}")
            else:
                self.log.info(f"  File already exists, skipping: {self.output_path}")
                return {
                    'status': 'skipped',
                    'reason': 'file_exists',
                    'file_path': self.output_path,
                }
        
        # Шаг 7: Сохранение данных в файл
        self.log.info(f"Step 6: Saving data to file")
        self.log.info(f"  Output path: {self.output_path}")
        
        try:
            # Атомарная запись: сначала во временный файл
            temp_path = self.output_path + '.tmp'
            
            with open(temp_path, 'w', encoding='utf-8') as f:
                json.dump(response_data, f, indent=2, ensure_ascii=False)
            
            # Переименовываем в финальный файл (атомарная операция)
            os.rename(temp_path, self.output_path)
            
            file_size = os.path.getsize(self.output_path)
            self.log.info(f"  File saved successfully")
            self.log.info(f"  File size: {file_size} bytes")
            
        except Exception as e:
            self.log.error(f"  Failed to save file: {e}")
            # Удаляем временный файл если есть
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise
        
        # Шаг 8: Формирование результата
        result = {
            'status': 'success',
            'file_path': self.output_path,
            'file_size_bytes': file_size,
            'records_count': records_count,
            'execution_date': context['ds'],
            'task_id': context['task'].task_id,
        }
        
        self.log.info("-" * 50)
        self.log.info("ЗАВЕРШЕНИЕ ВЫПОЛНЕНИЯ")
        self.log.info(f"Result: {result}")
        self.log.info("-" * 50)
        
        # Результат сохраняется в XCom автоматически
        return result


# =============================================================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ
# =============================================================================

"""
1. Базовое использование в DAG:

from custom_operators.http_to_file_operator import HttpToFileOperator

download_task = HttpToFileOperator(
    task_id='download_posts',
    http_conn_id='jsonplaceholder',
    endpoint='/posts',
    output_path='/opt/airflow/data/posts_{{ ds }}.json',
    params={'_limit': 10},
)


2. С параметрами из Variables:

download_task = HttpToFileOperator(
    task_id='download_users',
    http_conn_id='my_api',
    endpoint='/users',
    output_path='/data/users.json',
    params={'limit': '{{ var.value.api_limit }}'},  # Из Variable
)


3. POST-запрос:

create_task = HttpToFileOperator(
    task_id='create_post',
    http_conn_id='my_api',
    endpoint='/posts',
    method='POST',
    data={
        'title': 'New Post',
        'body': 'Content',
        'userId': 1,
    },
    output_path='/data/created_post.json',
)


4. В цепочке задач:

extract = HttpToFileOperator(...)
process = PythonOperator(...)
load = PostgresOperator(...)

extract >> process >> load
"""
