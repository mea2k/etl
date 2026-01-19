"""
Скрипт для автоматической настройки Airflow Connections.
Использует переменные окружения из .env файла.
"""
import json
import os
from airflow import settings
from airflow.models import Connection


def _envToJson(env_var: str):
    result = None
    src = os.getenv(env_var)
    if src and len(src) > 0:
        try:
            result = json.loads(src)
        except json.JSONDecodeError as e:
            result = None
    return result


def create_connection(conn_id, conn_type, host, schema, login, password, port=None, extra=None):
    """Создание или обновление подключения."""
    conn = Connection(
        conn_id=conn_id,
        conn_type=conn_type,
        host=host,
        schema=schema,
        login=login,
        password=password,
        port=port,
        extra=extra
    )
    
    session = settings.Session()
    
    # Удаление существующего подключения
    existing_conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
    if existing_conn:
        session.delete(existing_conn)
        session.commit()
    
    # Создание нового подключения
    session.add(conn)
    session.commit()
    session.close()
    
    print(f"Connection '{conn_id}' created successfully")


def setup_all_connections():
    """Настройка всех подключений."""
    
    print("Setting up Airflow Connections...")
    print("-" * 50)
    
    total_count = 0

    # PostgreSQL Source
    if os.getenv('POSTGRES_SOURCE_HOST'):
        create_connection(
            conn_id='postgres_source',
            conn_type='postgres',
            host=os.getenv('POSTGRES_SOURCE_HOST', 'postgres-source'),
            schema=os.getenv('POSTGRES_SOURCE_DB', 'source_db'),
            login=os.getenv('POSTGRES_SOURCE_USER', 'source_user'),
            password=os.getenv('POSTGRES_SOURCE_PASSWORD'),
            port=5432,
            extra = _envToJson('POSTGRES_SOURCE_EXTRA')
        )
        total_count += 1
    
    # PostgreSQL Analytics
    if os.getenv('POSTGRES_ANALYTICS_HOST'):
        create_connection(
            conn_id='postgres_analytics',
            conn_type='postgres',
            host=os.getenv('POSTGRES_ANALYTICS_HOST', 'postgres-analytics'),
            schema=os.getenv('POSTGRES_ANALYTICS_DB', 'analytics_db'),
            login=os.getenv('POSTGRES_ANALYTICS_USER', 'analytics_user'),
            password=os.getenv('POSTGRES_ANALYTICS_PASSWORD'),
            port=5432,
            extra = _envToJson('POSTGRES_ANALYTICS_EXTRA')
        )
        total_count += 1

    # MongoDB
    if os.getenv('MONGO_HOST'):
        mongo_uri = f"mongodb://{os.getenv('MONGO_USER')}:{os.getenv('MONGO_PASSWORD')}@{os.getenv('MONGO_HOST')}:{os.getenv('MONGO_PORT')}"
        create_connection(
            conn_id='mongo_source',
            conn_type='mongo',
            host=os.getenv('MONGO_HOST', 'mongodb'),
            schema=os.getenv('MONGO_DB', 'feedback_db'),
            login=os.getenv('MONGO_USER', 'mongo_user'),
            password=os.getenv('MONGO_PASSWORD'),
            port=int(os.getenv('MONGO_PORT', 27017)),
            extra=json.dumps({"authSource": os.getenv('MONGO_AUTH_SOURCE', 'admin')})
        )
        total_count += 1

    # FTP Server
    if os.getenv('FTP_HOST'):
        # Парсим дополнительные параметры
        path = os.getenv('FTP_PATH')
        extra = _envToJson('FTP_EXTRA')

        # Добавляем стандартные параметры FTP
        if extra is None:
            extra = {}
        if not extra.get('passive_mode'):
            extra['passive_mode'] = True  # По умолчанию используем passive mode для безопасности
        if not extra.get('encoding'):
            extra['encoding'] = 'utf-8'  # Кодировка по умолчанию
        
        # Если указан путь, добавляем его в extra
        if path:
            extra['path'] = path

        create_connection(
            conn_id='ftp_server',
            conn_type='ftp',
            host=os.getenv('FTP_HOST', 'ftp-server'),
            schema=None,
            login=os.getenv('FTP_USER', 'ftp_user'),
            password=os.getenv('FTP_PASSWORD'),
            port=int(os.getenv('FTP_PORT', 21)),
            extra = extra
        )
        total_count += 1
    
    # API Service
    if os.getenv('API_URL'):
        base_url = os.getenv('API_URL')
        api_key = os.getenv('API_KEY') or os.getenv('API_TOKEN')
        extra = _envToJson('API_EXTRA')
        
        # Добавляем API key в extra если есть
        if api_key:
            extra['api_key'] = api_key

        create_connection(
            conn_id='api_service',
            conn_type='http',
            host=base_url,
            schema=None,
            login=None,
            password=None,
            port=None,
            extra=extra
        )
        total_count += 1

    # Итоговое сообщение
    print("-" * 50)
    print(f"All connections setup completed! Total connections: {total_count}")
    print("Verify connections in Airflow UI: 'Admin → Connections'")


# Main function to setup all connections
if __name__ == '__main__':
    setup_all_connections()
