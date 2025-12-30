"""
DAG для демонстрации загрузки данных с SCD Type 2
Реализует полный ETL процесс с оптимизацией производительности
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import logging
from faker import Faker
import random

# Конфигурация
POSTGRES_CONN_ID = 'warehouse_postgres'
STAGING_SCHEMA = 'staging'
DIM_SCHEMA = 'dim'
FACT_SCHEMA = 'fact'
BATCH_SIZE = 10000

fake = Faker()
logger = logging.getLogger(__name__)


def generate_customer_data(num_records=50000, **context):
    """
    Генерирует тестовые данные клиентов
    """
    logger.info(f"Generating {num_records} customer records...")
    
    customers = []
    for i in range(num_records):
        customers.append({
            'customer_id': i + 1,
            'customer_name': fake.name(),
            'email': fake.email(),
            'phone': fake.phone_number(),
            'address': fake.street_address(),
            'city': fake.city(),
            'country': fake.country()
        })
    
    df = pd.DataFrame(customers)
    
    # Сохраняем в CSV для демонстрации
    output_path = '/opt/airflow/data/customers_raw.csv'
    df.to_csv(output_path, index=False)
    
    logger.info(f"Generated {len(df)} records and saved to {output_path}")
    
    return output_path


def load_to_staging(**context):
    """
    Загружает данные в staging таблицу с использованием bulk insert
    """
    logger.info("Loading data to staging table...")
    
    # Читаем данные
    df = pd.read_csv('/opt/airflow/data/customers_raw.csv')
    
    # Подключение к БД
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    
    # Очищаем staging
    with engine.begin() as conn:
        conn.execute(f"TRUNCATE TABLE {STAGING_SCHEMA}.customers_staging")
    
    # Bulk insert с использованием to_sql
    start_time = datetime.now()
    
    df.to_sql(
        'customers_staging',
        engine,
        schema=STAGING_SCHEMA,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=BATCH_SIZE
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"Loaded {len(df)} records to staging in {duration:.2f} seconds")
    logger.info(f"Performance: {len(df)/duration:.2f} records/second")
    
    return len(df)


def apply_scd_type2(**context):
    """
    Применяет SCD Type 2 логику к dimension таблице
    """
    logger.info("Applying SCD Type 2 logic...")
    
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    scd_query = f"""
    -- 1. Обновляем существующие записи, которые изменились
    UPDATE {DIM_SCHEMA}.customers c
    SET 
        valid_to = CURRENT_TIMESTAMP,
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP
    WHERE c.is_current = TRUE
    AND EXISTS (
        SELECT 1 
        FROM {STAGING_SCHEMA}.customers_staging s
        WHERE s.customer_id = c.customer_id
        AND (
            s.customer_name != c.customer_name OR
            s.email != c.email OR
            s.phone != c.phone OR
            s.address != c.address OR
            s.city != c.city OR
            s.country != c.country
        )
    );
    
    -- 2. Вставляем новые версии измененных записей
    INSERT INTO {DIM_SCHEMA}.customers (
        customer_id, customer_name, email, phone, 
        address, city, country,
        valid_from, valid_to, is_current, created_at, updated_at
    )
    SELECT 
        s.customer_id,
        s.customer_name,
        s.email,
        s.phone,
        s.address,
        s.city,
        s.country,
        CURRENT_TIMESTAMP as valid_from,
        NULL as valid_to,
        TRUE as is_current,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at
    FROM {STAGING_SCHEMA}.customers_staging s
    WHERE EXISTS (
        SELECT 1 
        FROM {DIM_SCHEMA}.customers c
        WHERE c.customer_id = s.customer_id
        AND c.is_current = FALSE
        AND c.valid_to = (
            SELECT MAX(valid_to)
            FROM {DIM_SCHEMA}.customers
            WHERE customer_id = s.customer_id
        )
    );
    
    -- 3. Вставляем полностью новые записи
    INSERT INTO {DIM_SCHEMA}.customers (
        customer_id, customer_name, email, phone,
        address, city, country,
        valid_from, valid_to, is_current, created_at, updated_at
    )
    SELECT 
        s.customer_id,
        s.customer_name,
        s.email,
        s.phone,
        s.address,
        s.city,
        s.country,
        CURRENT_TIMESTAMP as valid_from,
        NULL as valid_to,
        TRUE as is_current,
        CURRENT_TIMESTAMP as created_at,
        CURRENT_TIMESTAMP as updated_at
    FROM {STAGING_SCHEMA}.customers_staging s
    WHERE NOT EXISTS (
        SELECT 1 
        FROM {DIM_SCHEMA}.customers c
        WHERE c.customer_id = s.customer_id
    );
    """
    
    start_time = datetime.now()
    hook.run(scd_query)
    end_time = datetime.now()
    
    duration = (end_time - start_time).total_seconds()
    logger.info(f"SCD Type 2 applied in {duration:.2f} seconds")


def generate_orders_data(num_records=100000, **context):
    """
    Генерирует данные заказов для партиционированной таблицы
    """
    logger.info(f"Generating {num_records} order records...")
    
    # Получаем список customer_id из dimension таблицы
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    customer_ids_query = f"SELECT DISTINCT customer_id FROM {DIM_SCHEMA}.customers WHERE is_current = TRUE"
    customer_ids = hook.get_pandas_df(customer_ids_query)['customer_id'].tolist()
    
    if not customer_ids:
        logger.warning("No customers found, using generated IDs")
        customer_ids = list(range(1, 1001))
    
    orders = []
    statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
    
    for i in range(num_records):
        order_date = fake.date_between(start_date='-11M', end_date='today')
        orders.append({
            'customer_id': random.choice(customer_ids),
            'order_date': order_date,
            'order_amount': round(random.uniform(10.0, 1000.0), 2),
            'product_id': random.randint(1, 500),
            'quantity': random.randint(1, 10),
            'status': random.choice(statuses)
        })
    
    df = pd.DataFrame(orders)
    output_path = '/opt/airflow/data/orders_raw.csv'
    df.to_csv(output_path, index=False)
    
    logger.info(f"Generated {len(df)} order records and saved to {output_path}")
    
    return output_path


def load_orders_bulk(**context):
    """
    Массовая загрузка заказов в партиционированную таблицу
    """
    logger.info("Loading orders with bulk insert...")
    
    df = pd.read_csv('/opt/airflow/data/orders_raw.csv')
    df['order_date'] = pd.to_datetime(df['order_date'])
    
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    engine = hook.get_sqlalchemy_engine()
    
    start_time = datetime.now()
    
    # Bulk insert в партиционированную таблицу
    df.to_sql(
        'orders',
        engine,
        schema=FACT_SCHEMA,
        if_exists='append',
        index=False,
        method='multi',
        chunksize=BATCH_SIZE
    )
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    logger.info(f"Loaded {len(df)} orders in {duration:.2f} seconds")
    logger.info(f"Performance: {len(df)/duration:.2f} records/second")
    
    # Записываем статистику производительности
    with engine.begin() as conn:
        conn.execute(f"""
            INSERT INTO public.load_performance_log 
            (table_name, records_loaded, load_duration_seconds, records_per_second)
            VALUES (
                'fact.orders',
                {len(df)},
                {duration},
                {len(df)/duration}
            )
        """)


def analyze_performance(**context):
    """
    Анализирует производительность загрузки
    """
    logger.info("Analyzing load performance...")
    
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    
    stats_query = """
    SELECT 
        table_name,
        AVG(records_per_second) as avg_records_per_sec,
        MAX(records_per_second) as max_records_per_sec,
        MIN(records_per_second) as min_records_per_sec,
        COUNT(*) as load_count
    FROM public.load_performance_log
    WHERE load_timestamp > CURRENT_TIMESTAMP - INTERVAL '24 hours'
    GROUP BY table_name
    """
    
    stats = hook.get_pandas_df(stats_query)
    logger.info("Performance Statistics:\n" + stats.to_string())
    
    return stats.to_dict()


# Определение DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'etl_data_warehouse_scd',
    default_args=default_args,
    description='ETL pipeline with SCD Type 2 and partitioning',
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'warehouse', 'scd', 'lesson4'],
)

# Задачи DAG
task_generate_customers = PythonOperator(
    task_id='generate_customer_data',
    python_callable=generate_customer_data,
    op_kwargs={'num_records': 50000},
    dag=dag,
)

task_load_staging = PythonOperator(
    task_id='load_to_staging',
    python_callable=load_to_staging,
    dag=dag,
)

task_apply_scd = PythonOperator(
    task_id='apply_scd_type2',
    python_callable=apply_scd_type2,
    dag=dag,
)

task_generate_orders = PythonOperator(
    task_id='generate_orders_data',
    python_callable=generate_orders_data,
    op_kwargs={'num_records': 100000},
    dag=dag,
)

task_load_orders = PythonOperator(
    task_id='load_orders_bulk',
    python_callable=load_orders_bulk,
    dag=dag,
)

task_analyze = PythonOperator(
    task_id='analyze_performance',
    python_callable=analyze_performance,
    dag=dag,
)

# Определение зависимостей
task_generate_customers >> task_load_staging >> task_apply_scd
task_apply_scd >> task_generate_orders >> task_load_orders >> task_analyze
