"""
DAG 01: Простое извлечение из Postgres

Демонстрирует базовое использование PostgresExtractor

Граф DAG:
---------
start → extract_users → validate → end
"""

from datetime import datetime, timedelta
import os
import sys
import pandas as pd
import logging
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


# TODO: Когда будет готов extractors, раскомментируйте
# from extractors.postgres_extractor import PostgresExtractor


# =============================================================================
# ПАРАМЕТРЫ DAG
# =============================================================================

default_args = {
    'owner': 'student_etl',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag_id = 'dag_01_postgres_extract'
dag_tags = ['lesson1', 'extract', 'postgres', 'example']

# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def extract_users(**context):
    """
    Извлечение активных пользователей из Postgres.
    Демонстрирует:
    - Создание Extractor
    - Простой SQL запрос
    - Сохранение в CSV
    - Возврат метаданных через XCom
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("EXTRACTING USERS FROM POSTGRES")
    logger.info("-" * 50)
    
    # TODO: Раскомментируйте когда extractor будет готов
    # Дополнительно необходимо создать Connection в Airflow
    # с id 'pg_source', указывающий на вашу Postgres базу.
    
    # extractor = PostgresExtractor("pg_source")
    # df = extractor.extract("SELECT * FROM users WHERE active = true")
    
    # Заглушка для демонстрации
    # Закомментируйте когда extractor будет готов
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'email': ['alice@example.com', 'bob@example.com', 'charlie@example.com'],
        'name': ['Alice', 'Bob', 'Charlie']
    })
    
    logger.info(f"  Extracted {len(df)} users")
    
    # Сохранение
    # Создание директорий для выходных файлов (если нет)
    output_dir = Path("/opt/airflow/data/extracted/01")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Имя файла - posts_YYYY-MM-DD.json
    output_path = f"{output_dir}/users_{context['ds']}.csv"

    # TODO: Поменять использование df.save_to_csv() на extractor.save_to_csv()
    df.to_csv(output_path, index=False)
    # ИЛИ
    #extractor.save_to_csv(df, output_path)

    logger.info(f"  Saved to: {output_path}")
    logger.info("-" * 50)
    
    return {
        'records': len(df),
        'file': output_path,
        'columns': list(df.columns)
    }


def validate_extraction(**context):
    """
    Валидация результатов извлечения.
    
    Проверяет:
    - Наличие выходного файла
    - Количество записей > 0
    - Корректность структуры данных
    """
    logger = logging.getLogger(__name__)

    logger.info("-" * 50)
    logger.info("VALIDATING EXTRACTION RESULTS")
    logger.info("-" * 50)

    # Получаем результаты из предыдущей задачи
    ti = context['task_instance']
    result = ti.xcom_pull(task_ids='extract_users')
    
    logger.info(f"  Records extracted: {result['records']}")
    logger.info(f"  File: {result['file']}")
    logger.info(f"  Columns: {result['columns']}")
    
    # Проверка на существование файла
    assert os.path.exists(result['file']), f"Output file not found: {result['file']}"
    logger.info("  Output file exists")

    # Проверка на непустой файл (наличие записей)
    assert result['records'] > 0, "No records extracted"
    logger.info(f"  Records count is valid: {result['records']}")
    
    logger.info("  Validation passed")
    
    return {"status": "valid"}


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id=dag_id,  #'dag_01_postgres_extract',
    default_args=default_args,
    description='Простое извлечение из Postgres',
    schedule_interval='@daily',
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

extract_task = PythonOperator(
    task_id='extract_users',
    python_callable=extract_users,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate',
    python_callable=validate_extraction,
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

start_task >> extract_task >> validate_task >> end_task


# =============================================================================
# ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ
# =============================================================================

"""
Как запустить:
--------------

1. Создайте Connection:
   - Admin → Connections → +
   - Connection Id: pg_source
   - Connection Type: Postgres
   - Host: postgres
   - Schema: source_db
   - Login: airflow
   - Password: airflow

2. Запустите DAG:
   - Найдите dag_01_postgres_extract в списке
   - Нажмите Trigger DAG

3. Проверьте результаты:
   - Логи задачи extract_users
   - Файл /opt/airflow/data/extracted/01/users_YYYY-MM-DD.csv


Упражнения:
-----------

1. Модифицируйте SQL запрос:
   - Добавьте фильтр по дате
   - Выберите только нужные колонки
   - Добавьте сортировку

2. Добавьте обработку ошибок:
   - try-except блок
   - Логирование ошибок
   - Retry логику

3. Добавьте статистику:
   - Количество записей
   - Размер файла
   - Время выполнения
"""
