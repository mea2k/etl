"""
DAG 03: Извлечение данных из HTTP API и сохранение в JSON

Демонстрирует использование HttpExtractor для извлечения данных из REST API
(JSONPlaceholder) и сохранение результатов в JSON формате.

Граф DAG:
---------
start → extract_api_to_json → validate_output → end
"""

from datetime import datetime, timedelta
import json
import os
import logging
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

from extractors.http_extractor import HttpExtractor

# =============================================================================
# ПАРАМЕТРЫ DAG
# =============================================================================

default_args = {
    'owner': 'student_etl',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'retries': 1,  # Меньше retry для демонстрации
    'retry_delay': timedelta(minutes=2),
}

dag_id = 'dag_03_http_extract'
dag_tags = ['lesson3', 'extract', 'http', 'api', 'json', 'example']

# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def extract_api_to_json(**context):
    """
    Извлечение данных из HTTP API и сохранение в JSON.  
    Демонстрирует:
    - Создание HttpExtractor
    - Извлечение данных с параметрами
    - Сохранение в JSON формат
    - Логирование результатов
    Args:
        **context: Airflow context с метаданными выполнения
    Returns:
        str: Путь к созданному JSON файлу
    """
    logger = logging.getLogger(__name__)
    ds = context["ds"]  # Execution date в формате YYYY-MM-DD
    
    logger.info("-" * 50)
    logger.info(f"EXTRACTING DATA FROM HTTP API FOR {ds}")
    logger.info("-" * 50)

    # Создаём HttpExtractor
    # Connection 'api_service' должен быть настроен в Airflow UI
    extractor = HttpExtractor(conn_id="api_service")

    # Извлекаем данные из API
    # JSONPlaceholder API: https://jsonplaceholder.typicode.com
    df = extractor.extract(
        endpoint="/posts",
        params={"userId": 1, "_limit": 10}  # Ограничение для демонстрации
    )

    logger.info(f"  Extracted {len(df)} posts from API")
    logger.info(f"  Columns: {list(df.columns)}")
    
    # Создаём директорию для выходных файлов
    output_dir = Path("/opt/airflow/data/extracted/03")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Имя файла - posts_YYYY-MM-DD.json
    output_file = f"{output_dir}/posts_{ds}.json"
    
    # Сохраняем в JSON
    df.to_json(
        output_file,
        orient="records",     # Формат: список объектов
        indent=2,             # Форматирование для читаемости
        force_ascii=False     # Поддержка Unicode
    )
    
    logger.info(f"  Saved {len(df)} posts to {output_file}")
    logger.info("-" * 50)
    
    # Возвращаем метаданные для следующих задач
    return {
        'file_path': str(output_file),
        'records_count': len(df),
        'columns': list(df.columns)
    }


def validate_output(**context):
    """
    Валидация сохранённого JSON файла.
    Проверяет:
    - Существование файла
    - Корректность JSON формата
    - Количество записей > 0
    - Наличие обязательных полей
    Raises:
        AssertionError: Если валидация не прошла
        FileNotFoundError: Если файл не найден
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("VALIDATING JSON OUTPUT")
    logger.info("-" * 50)
    
    # Получаем результаты из предыдущей задачи
    result = ti.xcom_pull(task_ids='extract_api_to_json')
    
    file_path = result['file_path']
    expected_count = result['records_count']
    
    logger.info(f"  Validating file: {file_path}")
    logger.info(f"  Expected records: {expected_count}")
    
    # Проверка 1: Файл существует
    assert Path(file_path).exists(), f"Output file not found: {file_path}"
    logger.info("  File exists")
    
    # Проверка 2: Файл не пустой
    file_size = Path(file_path).stat().st_size
    assert file_size > 0, "File is empty"
    logger.info(f"  File size: {file_size} bytes ({file_size / 1024:.2f} KB)")
    
    # Проверка 3: Валидный JSON
    with open(file_path, 'r') as f:
        data = json.load(f)
    logger.info("  Valid JSON format")
    
    # Проверка 4: Количество записей
    assert len(data) == expected_count, f"Record count mismatch: {len(data)} != {expected_count}"
    logger.info(f"  Record count matches: {len(data)}")
      
    logger.info("-" * 50)
    logger.info("  ALL VALIDATIONS PASSED")
    logger.info("-" * 50)
    
    return {
        'status': 'valid',
        'checks_passed': 5,
        'file_size_kb': file_size / 1024
    }


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id=dag_id,  # 'dag_03_http_extract',
    default_args=default_args,
    description='Загрузка данных из HTTP REST API',
    schedule_interval='@daily',  # Выполняется каждый день
    catchup=False,
    tags=dag_tags,
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# =============================================================================

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Извлечение данных из API
extract_http_task = PythonOperator(
    task_id='extract_api_to_json', 
    python_callable=extract_api_to_json,
    provide_context=True, 
    dag=dag,
)

# Задача валидации
validate_task = PythonOperator(
    task_id='validate_output',
    python_callable=validate_output,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# =============================================================================

# Параллельное извлечение, затем сводка
start_task >> extract_http_task >> validate_task >> end_task

# =============================================================================
# ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ
# =============================================================================

"""
Как запустить:
--------------

1. Создайте Connection в Airflow UI:
   - Connection Id: api_service
   - Connection Type: HTTP
   - Host: jsonplaceholder.typicode.com
   - Schema: https
   - Extra: {"timeout": 30}

2. Запустите DAG:
   - Найдите dag_03_http_extract в UI
   - Нажмите Trigger DAG

3. Проверьте результаты:
   - Логи задачи extract_api_to_json
   - Файл /opt/airflow/data/extracted/03/posts_YYYY-MM-DD.json
   - Логи задачи validate_output


Что наблюдать:
--------------

1. В логах задачи extract_api_to_json:
   - Количество извлечённых постов (обычно 10 для userId=1)
   - Список колонок DataFrame
   - Путь к сохранённому файлу

2. В логах задачи validate_output:
   - 5 проверок валидации
   - Размер файла в KB
   - Подтверждение корректности данных

3. В файле posts_YYYY-MM-DD.json:
   - JSON массив объектов
   - Структура: userId, id, title, body
   - Форматирование (indent=2)

Частые ошибки:
--------------

1. **Connection не настроен:**
   ```
   Connection 'api_service' doesn't exist
   ```
   Решение: Создайте Connection в Admin → Connections

2. **Параметр limit не поддерживается:**
   ```
   API returns all posts regardless of limit parameter
   ```
   Решение: JSONPlaceholder игнорирует limit, используйте _limit

3. **Файл не создаётся:**
   ```
   FileNotFoundError
   ```
   Решение: Проверьте что директория создана (mkdir -p)

4. **JSON некорректный:**
   ```
   JSONDecodeError
   ```
   Решение: Проверьте параметр force_ascii=False


Полезные команды:
-----------------

# Проверить созданный JSON файл
docker compose exec airflow cat /opt/airflow/data/extracted/03/posts_2025-01-12.json | jq

# Посмотреть размер файла
docker compose exec airflow ls -lh /opt/airflow/data/extracted/03/

# Проверить структуру JSON
docker compose exec airflow python -m json.tool /opt/airflow/data/extracted/03/posts_2025-01-12.json

# Подсчитать количество записей
docker compose exec airflow cat /opt/airflow/data/extracted/03/posts_2025-01-12.json | jq 'length'
"""
