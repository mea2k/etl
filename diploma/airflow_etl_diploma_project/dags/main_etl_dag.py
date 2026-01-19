"""
Основной ETL DAG для бизнес-аналитики
Запуск: ежедневно в 9:00
"""
from datetime import datetime, timedelta
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator

# --------------------------------------------------
# ОПРЕДЕЛЕНИЕ КОНВЕЙЕРА (DAG)
# --------------------------------------------------

# Параметры по умолчанию
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Определение DAG
dag = DAG(
    'main_etl',
    default_args=default_args,
    description='ETL pipeline для бизнес-аналитики',
    schedule_interval='0 9 * * *',  # Ежедневно в 9:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['etl', 'analytics', 'daily'],
)


# --------------------------------------------------
# ОПРЕДЕЛЕНИЕ ФУНКЦИЙ ДЛЯ ЗАДАЧ КОНВЕЙЕРА
# --------------------------------------------------

def empty_callable(**context):
    """Пустая функция-заглушка"""
    pass

def extract_from_postgres(**context):
    """Извлечение данных из PostgreSQL"""
    from extractors.postgres_extractor import PostgresExtractor

    execution_date = context['logical_date']
    extractor = PostgresExtractor('postgres_source', 'source_db')
    
    # Извлечение заказов
    orders = extractor.extract_incremental(
        table_name='orders',
        date_column='order_date',
        start_date=(execution_date - timedelta(days=1)).strftime('%Y-%m-%d'),
        end_date=execution_date.strftime('%Y-%m-%d')
    )
    
    # Сохранение в XCom для следующих задач
    return {'orders': orders, 'count': len(orders)}


def extract_from_mongo(**context):
    """Извлечение данных из MongoDB"""
    from extractors.mongo_extractor import MongoExtractor
    
    execution_date = context['logical_date']
    extractor = MongoExtractor(conn_id='mongo_source', database='feedback_db')
    
    feedback = extractor.extract_by_date(
        'feedback',
        (execution_date - timedelta(days=1)).strftime('%Y-%m-%d'),
        execution_date.strftime('%Y-%m-%d')
    )
    
    return {'feedback': feedback, 'count': len(feedback)}


def extract_from_csv(**context):
    """Извлечение данных из CSV"""
    from extractors.csv_extractor import CSVExtractor
    
    extractor = CSVExtractor(base_path='/opt/airflow/data/csv')
    
    execution_date = context['logical_date']

    # Берём файлы за последние 7 дней, максимум 5 файлов
    latest_paths = extractor.extract_latest_files(days=7, limit=5)
    
    all_data = []
    for path in latest_paths:
        print(path.name)  # Печатаем имя файла
        df = extractor.extract(
            filename=path.name,
            encoding='utf-8',
            delimiter=','
        )
        all_data.append(df)
    
    # Объединяем все DataFrame
    combined_df = pd.concat(all_data, ignore_index=True) if all_data else pd.DataFrame()
    
    ti = context['ti']
    ti.xcom_push(key='latest_files', value=[str(p) for p in latest_paths])
    ti.xcom_push(key='extracted_count', value=len(combined_df))
    
    return {'products': combined_df, 'count': len(combined_df)}


def extract_from_api(**context):
    """Извлечение данных из REST API"""
    from extractors.api_extractor import APIExtractor
    
    extractor = APIExtractor('api_service', 'https://api.example.com')
    analytics = extractor.extract('/api/v1/analytics/daily-stats')
    
    return {'analytics': analytics, 'count': len(analytics)}


def extract_from_ftp(**context):
    """Извлечение данных из FTP"""
    from extractors.ftp_extractor import FTPExtractor

    execution_date = context['logical_date']
    date_str = (execution_date - timedelta(days=1)).strftime('%Y%m%d')
    
    extractor = FTPExtractor('ftp_server')
    
    # 1. Получаем список файлов
    files = extractor.list_remote_files(
        directory="/", 
        pattern="*.csv"  # или "delivery_logs_*.csv"
    )
    
    print(f"Available CSV files on FTP: {files}")
    
    if not files:
        print("No CSV files found on FTP. Skipping.")
        return {'logs': [], 'count': 0, 'files_found': []}
    
    # 2. Берём самый свежий файл (по имени или первый)
    target_file = files[0]  # первый файл
    remote_path = f"/{target_file}"
    
    # 3. Извлекаем данные
    logs = extractor.extract(remote_path, file_type='csv')
    
    return {
        'logs': logs, 
        'count': len(logs), 
        'file_used': target_file,
        'files_found': files
    }    
    

def validate_data(**context):
    """Валидация извлеченных данных"""
    ti = context['task_instance']
    
    # Получение данных из предыдущих задач
    postgres_data = ti.xcom_pull(task_ids='extract_from_postgres')
    mongo_data = ti.xcom_pull(task_ids='extract_from_mongo')
    csv_data = ti.xcom_pull(task_ids='extract_from_csv')

    print(f"Postgres records: {postgres_data['count'] if postgres_data is not None else 0}")
    print(f"MongoDB records: {mongo_data['count'] if mongo_data is not None else 0}")
    print(f"CSV records: {csv_data['count'] if csv_data is not None else 0}")
    
    return {'status': 'validated'}


def transform_data(**context):
    """Трансформация данных"""
    # TODO: Реализация трансформации
    # 1. Очистка данных
    # 2. Валидация
    # 3. Нормализация
    # 4. Обогащение

    return {'status': 'transformed'}


def load_to_analytics(**context):
    """Загрузка в аналитическую БД"""
    # TODO: Реализовать загрузку
    return {'status': 'loaded'}


def load_to_dwh(**context):
    """Загрузка в DWH с SCD Type 2"""
    # TODO: Реализовать загрузку в DWH
    return {'status': 'loaded_to_dwh'}


# --------------------------------------------------
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# --------------------------------------------------

# Начало
start = EmptyOperator(task_id='start', dag=dag)

extract_postgres = PythonOperator(
    task_id='extract_from_postgres',
    python_callable=extract_from_postgres,
    dag=dag
)

extract_mongo = PythonOperator(
    task_id='extract_from_mongo',
    python_callable=extract_from_mongo,
    dag=dag
)

extract_csv = PythonOperator(
    task_id='extract_from_csv',
    python_callable=extract_from_csv,
    dag=dag
)

extract_api = PythonOperator(
    task_id='extract_from_api',
    python_callable=extract_from_api, #empty_callable, 
    dag=dag
)

extract_ftp = PythonOperator(
    task_id='extract_from_ftp',
    python_callable=extract_from_ftp,
    dag=dag
)

validate = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag
)

load_analytics = PythonOperator(
    task_id='load_to_analytics',
    python_callable=load_to_analytics,
    dag=dag
)

load_dwh = PythonOperator(
    task_id='load_to_dwh',
    python_callable=load_to_dwh,
    dag=dag
)

# Конец
end = EmptyOperator(task_id='end', dag=dag)

# --------------------------------------------------
# ОПРЕДЕЛЕНИЕ ПОРЯДКА ВЫПОЛНЕНИЯ ЗАДАЧ
# --------------------------------------------------
# Зависимости задач
start >> [extract_postgres, extract_mongo, extract_csv, extract_api, extract_ftp]
[extract_postgres, extract_mongo, extract_csv, extract_api, extract_ftp] >> validate
validate >> transform
transform >> [load_analytics, load_dwh]
[load_analytics, load_dwh] >> end
