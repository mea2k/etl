"""
DAG 03: Параллельное извлечение из множества источников

Демонстрирует извлечение данных из 5 различных источников параллельно:
- PostgreSQL (реляционная БД)
- MongoDB (NoSQL документная БД)
- HTTP API (REST API)
- FTP (файловый сервер)
- CSV (локальные файлы)

Граф DAG:
---------
                    ┌─ extract_postgres ─┐
                    ├─ extract_mongo ────┤
start → check_env →─┤─ extract_http ─────├→ merge_data → analyze → save_report → end
                    ├─ extract_ftp ──────┤
                    └─ extract_csv ──────┘
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import json
import pandas as pd

from extractors.postgres_extractor import PostgresExtractor
from extractors.mongo_extractor import MongoExtractor
from extractors.http_extractor import HttpExtractor
from extractors.ftp_extractor import FTPExtractor
from extractors.csv_extractor import CSVExtractor


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

dag_id = 'dag_04_multi_source_extract'
dag_tags = ['lesson4', 'extract', 'multi_source', 'advanced']


# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def check_environment(**context):
    """
    Проверка доступности всех источников данных.
    
    Тестирует подключения ко всем источникам перед началом извлечения.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("CHECKING DATA SOURCES AVAILABILITY")
    logger.info("-" * 50)
    
    results = {}
    
    # Проверка Postgres
    try:
        pg_extractor = PostgresExtractor("pg_source")
        pg_ok = pg_extractor.test_connection()
        results['postgres'] = 'OK' if pg_ok else 'FAILED'
    except Exception as e:
        logger.error(f"Postgres check failed: {e}")
        results['postgres'] = 'FAILED'

    # Проверка Mongo
    try:
        mongo_extractor = MongoExtractor("mongo_source", database="analytics_db")
        mongo_ok = mongo_extractor.test_connection()
        results['mongo'] = 'OK' if mongo_ok else 'FAILED'
    except Exception as e:
        logger.error(f"Mongo check failed: {e}")
        results['mongo'] = 'FAILED'

    # Проверка HTTP
    try:
        http_extractor = HttpExtractor("api_service")
        http_ok = http_extractor.test_connection()
        results['http'] = 'OK' if http_ok else 'FAILED'
    except Exception as e:
        logger.error(f"HTTP check failed: {e}")
        results['http'] = 'FAILED'
    
    # Проверка CSV
    try:
        csv_extractor = CSVExtractor("/opt/airflow/data/csv")
        csv_ok = csv_extractor.test_connection()
        results['csv'] = 'OK' if csv_ok else 'FAILED'
    except Exception as e:
        logger.error(f"CSV check failed: {e}")
        results['csv'] = 'FAILED'
    
    # FTP - опциональные
    results['ftp'] = 'OPTIONAL'
    
    logger.info("Environment check results:")
    for source, status in results.items():
        logger.info(f"  {source.upper()}: {status}")
    
    logger.info("-" * 50)
    
    return results


def extract_from_postgres(**context):
    """Извлечение пользователей из PostgreSQL."""
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("EXTRACTING FROM POSTGRES")
    logger.info("-" * 50)
    
    try:
        extractor = PostgresExtractor("pg_source")
        
        # Извлекаем активных пользователей
        df = extractor.extract(
            sql="SELECT id, email, full_name FROM users WHERE active = true",
        )
        
        logger.info(f"  Extracted {len(df)} users from Postgres")
        
        # Добавляем метаданные источника
        df['source'] = 'postgres'
        df['extracted_at'] = datetime.now().isoformat()
        
        
        
        # Сохраняем промежуточный результат
        # Создаём директорию для выходных файлов
        output_dir = Path("/opt/airflow/data/extracted/04")
        output_dir.mkdir(parents=True, exist_ok=True)
        # Имя файла - temp_postgres_YYYY-MM-DD.csv
        temp_path = f"{output_dir}/temp_postgres_{context['ds']}.csv"

        extractor.save_to_csv(df, temp_path)
        
        # Возвращаем для объединения
        return {
            'source': 'postgres',
            'records': len(df),
            'data': df.to_dict('records'),
            'file': temp_path
        }
        
    except Exception as e:
        logger.error(f"  Postgres extraction failed: {e}")
        return {'source': 'postgres', 'records': 0, 'data': [], 'error': str(e)}


def extract_from_mongo(**context):
    """Извлечение событий из MongoDB."""
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("EXTRACTING FROM MONGODB")
    logger.info("-" * 50)
    
    try:
        extractor = MongoExtractor("mongo_source", database="analytics_db")
        
        # Извлекаем события за текущий день
        ds = context['ds']
        df = extractor.extract_by_date(
            collection="events",
            date_field="event_date",
            date_str=ds
        )
        
        logger.info(f"  Extracted {len(df)} events from MongoDB")
        
        # Добавляем метаданные
        df['source'] = 'mongodb'
        df['extracted_at'] = datetime.now().isoformat()
        
        return {
            'source': 'mongodb',
            'records': len(df),
            'data': df.to_dict('records')[:100],  # Ограничиваем для XCom
        }
        
    except Exception as e:
        logger.error(f"  MongoDB extraction failed: {e}")
        logger.warning("MongoDB is optional - continuing without it")
        return {'source': 'mongodb', 'records': 0, 'data': [], 'error': str(e)}


def extract_from_http(**context):
    """Извлечение данных из HTTP API."""
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("EXTRACTING FROM HTTP API")
    logger.info("-" * 50)
    
    try:
        extractor = HttpExtractor("api_service")
        
        # Извлекаем посты из JSONPlaceholder
        df = extractor.extract(
            endpoint="/posts",
            params={"_limit": 20}  # Ограничиваем количество
        )
        
        logger.info(f"  Extracted {len(df)} posts from API")
        
        # Добавляем метаданные
        df['source'] = 'http_api'
        df['extracted_at'] = datetime.now().isoformat()
        
        return {
            'source': 'http_api',
            'records': len(df),
            'data': df.to_dict('records'),
        }
        
    except Exception as e:
        logger.error(f"  HTTP extraction failed: {e}")
        logger.warning("HTTP is optional - continuing without it")
        return {'source': 'http_api', 'records': 0, 'data': [], 'error': str(e)}


def extract_from_ftp(**context):
    """Извлечение файлов с FTP сервера."""
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("EXTRACTING FROM FTP")
    logger.info("-" * 50)
    
    try:
        extractor = FTPExtractor("ftp_server")
        
        # Пытаемся извлечь файл
        ds = context['ds_nodash']  # 20250112
        df = extractor.extract_by_date_pattern(
            directory="/data/",
            date_str=ds,
            pattern_template="sales_{date}.csv",
            file_type="csv"
        )
        
        logger.info(f"  Extracted {len(df)} records from FTP")
        
        # Добавляем метаданные
        df['source'] = 'ftp'
        df['extracted_at'] = datetime.now().isoformat()
        
        return {
            'source': 'ftp',
            'records': len(df),
            'data': df.to_dict('records')[:100],
        }
        
    except Exception as e:
        logger.error(f"  FTP extraction failed: {e}")
        logger.warning("FTP is optional - continuing without it")
        return {'source': 'ftp', 'records': 0, 'data': [], 'error': str(e)}


def extract_from_csv(**context):
    """Извлечение из локальных CSV файлов."""
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("EXTRACTING FROM LOCAL CSV")
    logger.info("-" * 50)
    
    try:
        extractor = CSVExtractor("/opt/airflow/data/csv")
        
        # Извлекаем все CSV файлы
        df = extractor.extract_multiple(
            pattern="*.csv",
            delimiter=","
        )
        
        logger.info(f"  Extracted {len(df)} records from CSV files")
        
        # Добавляем метаданные
        df['source'] = 'csv_files'
        df['extracted_at'] = datetime.now().isoformat()
        
        return {
            'source': 'csv_files',
            'records': len(df),
            'data': df.to_dict('records')[:100],
        }
        
    except Exception as e:
        logger.error(f"  CSV extraction failed: {e}")
        return {'source': 'csv_files', 'records': 0, 'data': [], 'error': str(e)}


def merge_all_data(**context):
    """
    Объединение данных из всех источников.
    
    Собирает результаты из всех задач извлечения и объединяет их.
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("MERGING DATA FROM ALL SOURCES")
    logger.info("-" * 50)
    
    # Получаем результаты из всех источников
    sources = [
        'extract_postgres',
        'extract_mongo',
        'extract_http',
        'extract_ftp',
        'extract_csv'
    ]
    
    all_results = []
    summary = {
        'total_records': 0,
        'sources': {},
        'errors': []
    }
    
    for task_id in sources:
        result = ti.xcom_pull(task_ids=task_id)
        
        if result:
            source_name = result['source']
            records_count = result['records']
            
            summary['sources'][source_name] = {
                'records': records_count,
                'status': 'success' if records_count > 0 else 'no_data'
            }
            
            if 'error' in result:
                summary['errors'].append({
                    'source': source_name,
                    'error': result['error']
                })
                summary['sources'][source_name]['status'] = 'error'
            
            summary['total_records'] += records_count
            
            # Собираем данные (если есть)
            if 'data' in result and result['data']:
                all_results.extend(result['data'])
    
    logger.info("Merge summary:")
    logger.info(f"  Total records: {summary['total_records']}")
    logger.info("  By source:")
    for source, info in summary['sources'].items():
        logger.info(f"    {source}: {info['records']} records ({info['status']})")
    
    if summary['errors']:
        logger.warning(f"  Errors: {len(summary['errors'])}")
        for err in summary['errors']:
            logger.warning(f"    {err['source']}: {err['error']}")
    
    logger.info("-" * 50)
    
    return {
        'summary': summary,
        'merged_data': all_results[:1000],  # Ограничиваем для XCom
    }


def analyze_merged_data(**context):
    """
    Анализ объединённых данных.
    Создаёт статистику и метрики по всем извлечённым данным.
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("ANALYZING MERGED DATA")
    logger.info("-" * 50)
    
    merge_result = ti.xcom_pull(task_ids='merge_data')
    summary = merge_result['summary']
    merged_data = merge_result['merged_data']
    
    # Анализ
    analysis = {
        'extraction_date': context['ds'],
        'extraction_timestamp': datetime.now().isoformat(),
        'total_records': summary['total_records'],
        'sources_count': len(summary['sources']),
        'successful_sources': sum(1 for s in summary['sources'].values() if s['status'] == 'success'),
        'failed_sources': len(summary['errors']),
        'details': summary
    }
    
    # Если есть данные, создаём DataFrame для анализа
    if merged_data:
        df = pd.DataFrame(merged_data)
        
        # Подсчёт по источникам
        if 'source' in df.columns:
            source_counts = df['source'].value_counts().to_dict()
            analysis['records_by_source'] = source_counts
        
        # Колонки
        analysis['total_columns'] = len(df.columns)
        analysis['column_names'] = list(df.columns)
    
    logger.info("Analysis results:")
    logger.info(f"  Total records analyzed: {analysis['total_records']}")
    logger.info(f"  Successful sources: {analysis['successful_sources']}/{analysis['sources_count']}")
    
    if analysis.get('records_by_source'):
        logger.info("  Records by source:")
        for source, count in analysis['records_by_source'].items():
            logger.info(f"    {source}: {count}")
    
    logger.info("-" * 50)
    
    return analysis


def save_final_report(**context):
    """
    Сохранение финального отчёта.
    Создаёт JSON отчёт со всей статистикой извлечения.
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    ds = context['ds']
    
    logger.info("-" * 50)
    logger.info("SAVING FINAL REPORT")
    logger.info("-" * 50)
    
    # Получаем результаты анализа
    analysis = ti.xcom_pull(task_ids='analyze_data')
    
    # Формируем финальный отчёт
    report = {
        'dag_id': context['dag'].dag_id,
        'execution_date': ds,
        'execution_timestamp': datetime.now().isoformat(),
        'status': 'completed',
        'analysis': analysis
    }
    

    # Сохраняем отчет в JSON
    # Создаём директорию для выходных файлов
    output_dir = Path("/opt/airflow/data/extracted/04")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Имя файла - multi_source_report_YYYY-MM-DD.json
    report_path = f"{output_dir}/multi_source_report_{ds}.json"

    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    logger.info(f"  Report saved to: {report_path}")
    logger.info("-" * 50)
    logger.info("MULTI-SOURCE EXTRACTION COMPLETE!")
    logger.info("-" * 50)
    
    return {
        'report_path': report_path,
        'status': 'success'
    }


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id=dag_id,  # 'dag_04_multi_source_extract',
    default_args=default_args,
    description='Параллельное извлечение из множества источников',
    schedule_interval='@daily',
    catchup=False,
    tags=dag_tags,
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# =============================================================================

start_task = DummyOperator(task_id='start', dag=dag)

check_env_task = PythonOperator(
    task_id='check_environment',
    python_callable=check_environment,
    dag=dag,
)

# Задачи извлечения (выполняются параллельно)
extract_pg_task = PythonOperator(
    task_id='extract_postgres',
    python_callable=extract_from_postgres,
    dag=dag,
)

extract_mongo_task = PythonOperator(
    task_id='extract_mongo',
    python_callable=extract_from_mongo,
    dag=dag,
)

extract_http_task = PythonOperator(
    task_id='extract_http',
    python_callable=extract_from_http,
    dag=dag,
)

extract_ftp_task = PythonOperator(
    task_id='extract_ftp',
    python_callable=extract_from_ftp,
    dag=dag,
)

extract_csv_task = PythonOperator(
    task_id='extract_csv',
    python_callable=extract_from_csv,
    dag=dag,
)

# Задачи обработки
merge_task = PythonOperator(
    task_id='merge_data',
    python_callable=merge_all_data,
    dag=dag,
)

analyze_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_merged_data,
    dag=dag,
)

report_task = PythonOperator(
    task_id='save_report',
    python_callable=save_final_report,
    dag=dag,
)

end_task = DummyOperator(task_id='end', dag=dag)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# =============================================================================

# Граф выполнения
start_task >> check_env_task

# Параллельное извлечение из всех источников
check_env_task >> [
    extract_pg_task,
    extract_mongo_task,
    extract_http_task,
    extract_ftp_task,
    extract_csv_task
]

# Последовательная обработка результатов
[
    extract_pg_task,
    extract_mongo_task,
    extract_http_task,
    extract_ftp_task,
    extract_csv_task
] >> merge_task >> analyze_task >> report_task >> end_task


# =============================================================================
# ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ
# =============================================================================

"""
Как запустить:
--------------

1. Настройте Connections для всех источников:
   
   Обязательные:
    - pg_source (Postgres)
    - mongo_source (MongoDB) 
   Опциональные:
   - api_service (HTTP) - jsonplaceholder.typicode.com
   - ftp_server (FTP)

2. Подготовьте тестовые данные:
   - CSV файлы в /opt/airflow/data/csv/
   - Таблицы в Postgres (автоматически через postgres_init.sql)
   - Коллекции в MongoDB (автоматически через init_mongo.js)

3. Запустите DAG:
   - Trigger DAG вручную
   - Наблюдайте параллельное выполнение в Graph View

4. Проверьте результаты:
   - Файл multi_source_report_YYYY-MM-DD.json
   - Логи каждой задачи извлечения


Что наблюдать:
--------------

1. В Graph View:
   - 5 задач извлечения выполняются параллельно
   - Визуализация потока данных

2. В логах check_environment:
   - Статус каждого источника
   - Какие источники доступны

3. В логах задач extract_*:
   - Количество извлечённых записей
   - Время выполнения
   - Ошибки (если есть)

4. В логах merge_data:
   - Общее количество записей
   - Распределение по источникам
   - Список ошибок

5. В файле отчёта:
   - Полная статистика
   - Метаданные извлечения
   - Детали по каждому источнику


Особенности:
------------

1. Отказоустойчивость:
   - Если один источник недоступен - продолжаем с другими
   - MongoDB и FTP помечены как опциональные
   - Ошибки логируются, но не останавливают DAG

2. Параллелизм:
   - Все источники опрашиваются одновременно
   - Экономия времени выполнения

3. Метаданные:
   - Каждая запись помечена источником
   - Время извлечения
   - Отслеживаемость данных
"""
