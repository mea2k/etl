"""
DAG 02: Инкрементальная загрузка из Postgres

Демонстрирует инкрементальную загрузку данных - извлечение только новых/
изменённых записей за текущий день вместо полной загрузки таблицы.

Граф DAG:
---------
start → extract_orders → extract_products → create_summary → end
"""

from datetime import datetime, timedelta
from pathlib import Path
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import json

from extractors.postgres_extractor import PostgresExtractor


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

dag_id = 'dag_02_incremental_extract'
dag_tags = ['lesson2', 'extract', 'postgres', 'incremental', 'example']


# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def extract_orders_incremental(**context):
    """
    Инкрементальная загрузка заказов за текущий день.
    Извлекает только заказы, созданные в день выполнения DAG.
    Это гораздо эффективнее чем загружать всю таблицу каждый раз.
    """
    logger = logging.getLogger(__name__)
    ds = context['ds']  # Execution date (now): YYYY-MM-DD
    
    logger.info("-" * 50)
    logger.info(f"INCREMENTAL EXTRACTION: ORDERS FOR {ds}")
    logger.info("-" * 50)
    
    # Создаём Extractor
    extractor = PostgresExtractor("pg_source")
    
    # Инкрементальная загрузка по дате
    df = extractor.extract_incremental(
        table_name="orders",
        date_column="order_date",  # Колонка с датой
        start_date=ds,             # Начало периода (текущий день)
        end_date=ds,               # Конец периода (текущий день)
        columns=['id', 'user_id', 'order_date', 'amount', 'status']
    )
    
    logger.info(f"  Extracted {len(df)} orders for {ds}")
    
    # Сохранение в CSV
    # Создание директорий для выходных файлов (если нет)
    output_dir = Path("/opt/airflow/data/extracted/02")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Имя файла - orders_YYYY-MM-DD.csv
    output_path = f"{output_dir}/orders_{ds}.csv"

    extractor.save_to_csv(df, output_path)
    
    # Статистика
    if not df.empty:
        total_amount = df['amount'].sum()
        avg_amount = df['amount'].mean()
        logger.info(f"  Total amount: ${total_amount:.2f}")
        logger.info(f"  Average amount: ${avg_amount:.2f}")
    else:
        total_amount = 0
        avg_amount = 0
        logger.warning("  No orders for this date")
    
    logger.info("-" * 50)
    
    return {
        'date': ds,
        'records': len(df),
        'file': output_path,
        'total_amount': float(total_amount),
        'avg_amount': float(avg_amount)
    }


def extract_products_updated(**context):
    """
    Извлечение продуктов с обновлениями за последние 7 дней.
    Демонстрирует фильтрацию по диапазону дат.
    """
    logger = logging.getLogger(__name__)
    ds = context['ds']
    
    # Дата 7 дней назад
    execution_date = datetime.strptime(ds, '%Y-%m-%d')
    week_ago = (execution_date - timedelta(days=7)).strftime('%Y-%m-%d')
    
    logger.info("-" * 50)
    logger.info(f"EXTRACTING PRODUCTS: {week_ago} to {ds}")
    logger.info("-" * 50)
    
    extractor = PostgresExtractor("pg_source")
    
    # Продукты за последнюю неделю
    df = extractor.extract_incremental(
        table_name="products",
        date_column="created_at",
        start_date=week_ago,
        end_date=ds
    )
    
    logger.info(f"  Extracted {len(df)} products updated in last 7 days")
    
    # Сохранение в CSV
    # Создание директорий для выходных файлов (если нет)
    output_dir = Path("/opt/airflow/data/extracted/02")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Имя файла - products_weekly_YYYY-MM-DD.csv
    output_path = f"{output_dir}/products_weekly_{ds}.csv"

    extractor.save_to_csv(df, output_path)
    
    # Группировка по категориям
    if not df.empty and 'category' in df.columns:
        category_counts = df['category'].value_counts().to_dict()
        logger.info(f"  Products by category: {category_counts}")
    else:
        category_counts = {}
    
    logger.info("-" * 50)
    
    return {
        'date_from': week_ago,
        'date_to': ds,
        'records': len(df),
        'file': output_path,
        'categories': category_counts
    }


def create_daily_summary(**context):
    """
    Создание сводки по извлечённым данным.
    Объединяет результаты из всех задач извлечения.
    """
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    ds = context['ds']
    
    logger.info("-" * 50)
    logger.info(f"CREATING DAILY SUMMARY FOR {ds}")
    logger.info("-" * 50)
    
    # Получаем результаты из предыдущих задач
    orders_result = ti.xcom_pull(task_ids='extract_orders')
    products_result = ti.xcom_pull(task_ids='extract_products')
    
    # Формируем сводку
    summary = {
        'date': ds,
        'extraction_timestamp': datetime.now().isoformat(),
        'orders': {
            'count': orders_result['records'],
            'total_amount': orders_result['total_amount'],
            'avg_amount': orders_result['avg_amount'],
            'file': orders_result['file']
        },
        'products': {
            'count': products_result['records'],
            'date_range': f"{products_result['date_from']} to {products_result['date_to']}",
            'categories': products_result['categories'],
            'file': products_result['file']
        },
        'total_records_extracted': orders_result['records'] + products_result['records']
    }
    
    # Сохранение сводки в JSON
    # Создание директорий для выходных файлов (если нет)
    output_dir = Path("/opt/airflow/data/extracted/02")
    output_dir.mkdir(parents=True, exist_ok=True)
    # Имя файла - summary_YYYY-MM-DD.json
    summary_path = f"{output_dir}/summary_{ds}.json"
 
    with open(summary_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    logger.info("Summary:")
    logger.info(f"  Orders extracted: {summary['orders']['count']}")
    logger.info(f"  Total order amount: ${summary['orders']['total_amount']:.2f}")
    logger.info(f"  Products extracted: {summary['products']['count']}")
    logger.info(f"  Total records: {summary['total_records_extracted']}")
    logger.info(f"  Summary saved to: {summary_path}")
    
    logger.info("-" * 50)
    
    return summary


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id=dag_id,  #'dag_02_incremental_extract',
    default_args=default_args,
    description='Инкрементальная загрузка данных из Postgres',
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

# Извлечение заказов за день
extract_orders_task = PythonOperator(
    task_id='extract_orders',
    python_callable=extract_orders_incremental,
    provide_context=True,
    dag=dag,
)

# Извлечение продуктов за неделю
extract_products_task = PythonOperator(
    task_id='extract_products',
    python_callable=extract_products_updated,
    provide_context=True,
    dag=dag,
)

# Создание сводки
summary_task = PythonOperator(
    task_id='create_summary',
    python_callable=create_daily_summary,
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
start_task >> extract_orders_task >> extract_products_task >> summary_task >> end_task


# =============================================================================
# ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ
# =============================================================================

"""
Как запустить:
--------------

1. Убедитесь что Connection настроен:
   - Connection Id: pg_source
   - Type: Postgres
   - Host: postgres
   - Schema: source_db
   - Login: airflow
   - Password: airflow

2. Запустите DAG:
   - Найдите dag_02_incremental_extract
   - Нажмите Trigger DAG

3. Проверьте результаты:
   - Логи задач extract_orders и extract_products
   - Файлы в /opt/airflow/data/extracted/02/:
     * orders_YYYY-MM-DD.csv
     * products_weekly_YYYY-MM-DD.csv
     * summary_YYYY-MM-DD.json


Что наблюдать:
--------------

1. В логах задачи extract_orders:
   - Количество заказов за день
   - Общая сумма заказов
   - Средний чек

2. В логах задачи extract_products:
   - Количество продуктов за неделю
   - Распределение по категориям

3. В файле summary_YYYY-MM-DD.json:
   - Полная статистика по всем извлечениям
   - Пути к выходным файлам
   - Метрики
"""
