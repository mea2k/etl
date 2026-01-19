"""
DAG Homework Template: Unified User Catalog

Шаблон для выполнения домашнего задания - создание единого каталога
пользователей из разных источников (Postgres, HTTP API, CSV).

ЗАДАНИЕ:
--------
Реализовать ETL pipeline который:
1. Извлекает пользователей из Postgres, HTTP API и CSV
2. Объединяет их с дедупликацией по email
3. Сохраняет результат в файл
4. Валидирует итоговый каталог

ГРАФ:
-----
start → [extract_pg, extract_http, extract_csv] → merge → save → validate → end
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import json
import pandas as pd

# TODO: Импортируйте ваши Extractors
# from extractors.postgres_extractor import PostgresExtractor
# from extractors.http_extractor import HttpExtractor
# from extractors.csv_extractor import CSVExtractor


# =============================================================================
# ПАРАМЕТРЫ DAG
# =============================================================================

default_args = {
    'owner': 'student',  # TODO: Укажите ваше имя
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 12),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

dag_id = 'dag_homework_user_catalog'
dag_tags = ['extract', 'users', 'multi-source', 'homework']


# =============================================================================
# TODO: РЕАЛИЗУЙТЕ ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def extract_postgres_users(**context):
    """
    TODO: Извлечение пользователей из Postgres.
    
    Требования:
    - Использовать PostgresExtractor
    - SQL: SELECT id as user_id, email, full_name FROM users
    - Добавить колонку 'source' = 'postgres'
    - Вернуть список словарей (to_dict('records'))
    
    Returns:
        list: [{'user_id': 1, 'email': '...', 'full_name': '...', 'source': 'postgres'}, ...]
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("EXTRACTING USERS FROM POSTGRES")
    logger.info("=" * 60)
    
    # TODO: Ваш код здесь
    # extractor = PostgresExtractor("pg_source")
    # sql = "..."
    # df = extractor.extract(sql)
    # df['source'] = 'postgres'
    # return df.to_dict('records')
    
    # Заглушка для примера
    return [
        {'user_id': 1, 'email': 'alice@example.com', 'full_name': 'Alice Smith', 'source': 'postgres'},
        {'user_id': 2, 'email': 'bob@example.com', 'full_name': 'Bob Johnson', 'source': 'postgres'},
    ]


def extract_http_users(**context):
    """
    TODO: Извлечение пользователей из HTTP API.
    
    Требования:
    - Использовать HttpExtractor
    - Endpoint: /users (JSONPlaceholder API)
    - Преобразовать поля: id → user_id, name → full_name
    - Добавить колонку 'source' = 'api'
    - Вернуть список словарей
    
    Returns:
        list: [{'user_id': ..., 'email': '...', 'full_name': '...', 'source': 'api'}, ...]
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("EXTRACTING USERS FROM HTTP API")
    logger.info("=" * 60)
    
    # TODO: Ваш код здесь
    # extractor = HttpExtractor("api_service")
    # df = extractor.extract("/users")
    # df = df.rename(columns={'id': 'user_id', 'name': 'full_name'})
    # df = df[['user_id', 'email', 'full_name']]
    # df['source'] = 'api'
    # return df.to_dict('records')
    
    # Заглушка для примера
    return [
        {'user_id': 101, 'email': 'david@example.com', 'full_name': 'David Lee', 'source': 'api'},
        {'user_id': 102, 'email': 'alice@example.com', 'full_name': 'Alice Williams', 'source': 'api'},  # Дубликат!
    ]


def extract_csv_users(**context):
    """
    TODO: Извлечение пользователей из CSV файла.
    
    Требования:
    - Использовать CSVExtractor
    - Файл: users.csv в /opt/airflow/data/csv/
    - Добавить колонку 'source' = 'csv'
    - Вернуть список словарей
    
    Returns:
        list: [{'user_id': ..., 'email': '...', 'full_name': '...', 'source': 'csv'}, ...]
    """
    logger = logging.getLogger(__name__)
    
    logger.info("=" * 60)
    logger.info("EXTRACTING USERS FROM CSV")
    logger.info("=" * 60)
    
    # TODO: Ваш код здесь
    # extractor = CSVExtractor("/opt/airflow/data/csv")
    # df = extractor.extract("users.csv")
    # df['source'] = 'csv'
    # return df.to_dict('records')
    
    # Заглушка для примера
    return [
        {'user_id': 201, 'email': 'emma@example.com', 'full_name': 'Emma Davis', 'source': 'csv'},
    ]


def merge_users(**context):
    """
    TODO: Объединение пользователей с дедупликацией по email.
    
    Правила:
    - Приоритет источников: Postgres > API > CSV
    - Если email повторяется - берём первого (по приоритету)
    - Сохранить информацию о дубликатах в логах
    
    Returns:
        list: Уникальные пользователи
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("=" * 60)
    logger.info("MERGING USERS FROM ALL SOURCES")
    logger.info("=" * 60)
    
    # TODO: Получите данные из предыдущих задач
    pg_users = ti.xcom_pull(task_ids='extract_pg_users')
    api_users = ti.xcom_pull(task_ids='extract_http_users')
    csv_users = ti.xcom_pull(task_ids='extract_csv_users')
    
    # TODO: Объедините всех пользователей
    # all_users = pg_users + api_users + csv_users
    
    # TODO: Реализуйте дедупликацию по email
    # seen_emails = set()
    # unique_users = []
    # duplicates_count = 0
    # 
    # for user in all_users:
    #     email = user['email']
    #     if email not in seen_emails:
    #         seen_emails.add(email)
    #         unique_users.append(user)
    #     else:
    #         duplicates_count += 1
    
    # Заглушка для примера
    all_users = pg_users + api_users + csv_users
    
    seen_emails = set()
    unique_users = []
    duplicates_count = 0
    
    for user in all_users:
        email = user['email']
        if email not in seen_emails:
            seen_emails.add(email)
            unique_users.append(user)
        else:
            duplicates_count += 1
    
    logger.info(f"Total users from all sources: {len(all_users)}")
    logger.info(f"Unique users after deduplication: {len(unique_users)}")
    logger.info(f"Duplicates removed: {duplicates_count}")
    
    logger.info("=" * 60)
    
    return unique_users


def save_catalog(**context):
    """
    TODO: Сохранение каталога пользователей в файлы.
    
    Требования:
    - Сохранить в JSON: /opt/airflow/data/extracted/user_catalog.json
    - Сохранить в CSV: /opt/airflow/data/extracted/user_catalog.csv
    - Вернуть метаданные (количество, пути к файлам)
    
    Returns:
        dict: {'total_users': ..., 'json_file': '...', 'csv_file': '...'}
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("=" * 60)
    logger.info("SAVING USER CATALOG")
    logger.info("=" * 60)
    
    # TODO: Получите объединённых пользователей
    users = ti.xcom_pull(task_ids='merge_users')
    
    # TODO: Сохраните в JSON
    json_path = "/opt/airflow/data/extracted/user_catalog.json"
    # with open(json_path, 'w') as f:
    #     json.dump(users, f, indent=2)
    
    # TODO: Сохраните в CSV
    csv_path = "/opt/airflow/data/extracted/user_catalog.csv"
    # df = pd.DataFrame(users)
    # df.to_csv(csv_path, index=False)
    
    # Заглушка для примера
    with open(json_path, 'w') as f:
        json.dump(users, f, indent=2)
    
    df = pd.DataFrame(users)
    df.to_csv(csv_path, index=False)
    
    logger.info(f"✓ JSON saved to: {json_path}")
    logger.info(f"✓ CSV saved to: {csv_path}")
    logger.info(f"Total users in catalog: {len(users)}")
    logger.info("=" * 60)
    
    return {
        'total_users': len(users),
        'json_file': json_path,
        'csv_file': csv_path
    }


def validate_catalog(**context):
    """
    TODO: Валидация каталога пользователей.
    
    Проверки:
    - Файлы существуют
    - Есть записи (> 0)
    - Все обязательные колонки присутствуют
    - Email уникальны
    
    Raises:
        AssertionError: Если валидация не прошла
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("=" * 60)
    logger.info("VALIDATING USER CATALOG")
    logger.info("=" * 60)
    
    # TODO: Получите метаданные из предыдущей задачи
    result = ti.xcom_pull(task_ids='save_catalog')
    
    # TODO: Проверьте файлы
    import os
    assert os.path.exists(result['json_file']), "JSON file not found"
    assert os.path.exists(result['csv_file']), "CSV file not found"
    logger.info("✓ Files exist")
    
    # TODO: Проверьте количество
    assert result['total_users'] > 0, "No users in catalog"
    logger.info(f"✓ Users count is valid: {result['total_users']}")
    
    # TODO: Проверьте структуру CSV
    df = pd.read_csv(result['csv_file'])
    
    # Обязательные колонки
    required_cols = ['user_id', 'email', 'full_name', 'source']
    for col in required_cols:
        assert col in df.columns, f"Missing column: {col}"
    logger.info(f"✓ All required columns present: {required_cols}")
    
    # Уникальность email
    assert df['email'].is_unique, "Duplicate emails found!"
    logger.info("✓ All emails are unique")
    
    logger.info("=" * 60)
    logger.info("✓ ALL VALIDATIONS PASSED!")
    logger.info("=" * 60)
    
    return {"status": "valid", "checks_passed": 4}


# =============================================================================
# BONUS ЗАДАНИЕ 1: Статистика по источникам (+2 балла)
# =============================================================================

def create_statistics(**context):
    """
    BONUS: Создание статистики по источникам.
    
    Создайте:
    - Количество пользователей по источникам
    - Топ-5 email доменов
    - Распределение по источникам (проценты)
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    # TODO: Получите пользователей
    users = ti.xcom_pull(task_ids='merge_users')
    df = pd.DataFrame(users)
    
    # TODO: Статистика по источникам
    stats = {
        'total_users': len(df),
        'by_source': df.groupby('source').size().to_dict(),
        'email_domains': df['email'].str.split('@').str[1].value_counts().head(5).to_dict()
    }
    
    logger.info("Statistics:")
    logger.info(json.dumps(stats, indent=2))
    
    # Сохраните в файл
    stats_path = "/opt/airflow/data/extracted/user_statistics.json"
    with open(stats_path, 'w') as f:
        json.dump(stats, f, indent=2)
    
    return stats


# =============================================================================
# BONUS ЗАДАНИЕ 2: Обработка ошибок (+2 балла)
# =============================================================================

def extract_with_error_handling(extract_func, source_name, **context):
    """
    BONUS: Wrapper для обработки ошибок при извлечении.
    
    Если извлечение падает - логируем ошибку и возвращаем пустой список
    вместо остановки всего DAG.
    """
    logger = logging.getLogger(__name__)
    
    try:
        result = extract_func(**context)
        logger.info(f"✓ {source_name}: extracted {len(result)} users")
        return result
    except Exception as e:
        logger.error(f"✗ {source_name} extraction failed: {e}")
        logger.warning(f"Continuing without {source_name} data")
        return []


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id=dag_id,  # 'dag_homework_user_catalog',
    default_args=default_args,
    description='Homework: Unified User Catalog from multiple sources',
    schedule_interval='@daily',
    catchup=False,
    tags=dag_tags,
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# =============================================================================

start_task = DummyOperator(task_id='start', dag=dag)

# TODO: Создайте задачи извлечения
extract_pg_task = PythonOperator(
    task_id='extract_pg_users',
    python_callable=extract_postgres_users,
    dag=dag,
)

extract_http_task = PythonOperator(
    task_id='extract_http_users',
    python_callable=extract_http_users,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id='extract_csv_users',
    python_callable=extract_csv_users,
    dag=dag,
)

# TODO: Создайте задачи обработки
merge_task = PythonOperator(
    task_id='merge_users',
    python_callable=merge_users,
    dag=dag,
)

save_task = PythonOperator(
    task_id='save_catalog',
    python_callable=save_catalog,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_catalog',
    python_callable=validate_catalog,
    dag=dag,
)

# BONUS: Задача статистики (раскомментируйте если делаете)
# stats_task = PythonOperator(
#     task_id='create_statistics',
#     python_callable=create_statistics,
#     dag=dag,
# )

end_task = DummyOperator(task_id='end', dag=dag)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# =============================================================================

# TODO: Постройте граф зависимостей
start_task >> [extract_pg_task, extract_http_task, extract_csv_task]
[extract_pg_task, extract_http_task, extract_csv_task] >> merge_task
merge_task >> save_task >> validate_task >> end_task

# BONUS: Добавьте статистику после валидации
# validate_task >> stats_task >> end_task


# =============================================================================
# ИНСТРУКЦИИ ПО ВЫПОЛНЕНИЮ
# =============================================================================

"""
ШАГ 1: Подготовка
-----------------
1. Создайте Connections:
   - pg_source (Postgres)
   - api_service (HTTP) - jsonplaceholder.typicode.com
   
2. Создайте тестовые данные:
   - Postgres: таблица users (автоматически создаётся init.sql)
   - CSV: файл /opt/airflow/data/csv/users.csv
   
   Пример users.csv:
   user_id,email,full_name
   201,emma@example.com,Emma Davis
   202,frank@example.com,Frank Miller
   203,alice@example.com,Alice Wilson


ШАГ 2: Реализация
-----------------
1. Реализуйте extract_postgres_users():
   - Создайте PostgresExtractor
   - Выполните SQL запрос
   - Добавьте source = 'postgres'
   - Верните список словарей

2. Реализуйте extract_http_users():
   - Создайте HttpExtractor
   - Извлеките данные с /users
   - Преобразуйте колонки
   - Добавьте source = 'api'

3. Реализуйте extract_csv_users():
   - Создайте CSVExtractor
   - Прочитайте users.csv
   - Добавьте source = 'csv'

4. Реализуйте merge_users():
   - Объедините все списки
   - Удалите дубликаты по email
   - Логируйте статистику

5. Реализуйте save_catalog():
   - Сохраните в JSON
   - Сохраните в CSV
   - Верните метаданные

6. Проверьте validate_catalog():
   - Убедитесь что все проверки работают


ШАГ 3: Тестирование
-------------------
1. Запустите DAG
2. Проверьте логи каждой задачи
3. Убедитесь что все задачи зелёные
4. Проверьте выходные файлы:
   - user_catalog.json
   - user_catalog.csv


ШАГ 4: Сдача
------------
Сдайте:
1. Код DAG (этот файл с реализацией)
2. Скриншоты:
   - Graph View (все задачи зелёные)
   - Логи validate_catalog
   - Содержимое user_catalog.csv
3. Документация HOMEWORK.md:
   - Как настроить Connections
   - Как создать тестовые данные
   - Примеры запуска
   - Результаты выполнения


BONUS ЗАДАНИЯ:
--------------
1. Статистика (+2 балла):
   - Реализуйте create_statistics()
   - Добавьте в граф DAG

2. Обработка ошибок (+2 балла):
   - Используйте extract_with_error_handling
   - Покажите что DAG работает даже если один источник падает
"""
