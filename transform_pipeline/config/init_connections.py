"""
Автоматическая инициализация Airflow Connections
Используя переменные окружения и Python скрипт
"""

import os
import json
from airflow.models import Connection
from airflow.settings import Session
from airflow import settings
import logging
from typing import Dict, Optional, List

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# =============================================================================
# ФУНКЦИИ ПО РАБОТЕ С CONNECTION
# =============================================================================

"""
CREATE_OR_UPDATE_CONNECTION
Создает или обновляет Airflow Connection
Args:
    conn_id: Уникальный ID соединения
    conn_type: Тип соединения (postgres, mongo, http и т.д.)
    host: Хост БД/сервиса
    schema: Имя базы данных
    login: Логин
    password: Пароль
    port: Порт
    extra: Дополнительные параметры (JSON dict)
    uri: Connection URI (альтернатива отдельным параметрам)
    description: Описание соединения
Return:
    None
"""
def create_or_update_connection(
    conn_id: str,
    conn_type: str,
    host: Optional[str] = None,
    schema: Optional[str] = None,
    login: Optional[str] = None,
    password: Optional[str] = None,
    port: Optional[int] = None,
    extra: Optional[Dict] = None,
    uri: Optional[str] = None,
    description: Optional[str] = None
) -> None:

    session = Session()
    
    try:
        # Проверяем, существует ли connection
        existing_conn = session.query(Connection).filter(
            Connection.conn_id == conn_id
        ).first()
        
        if existing_conn:
            logger.info(f"Connection '{conn_id}' уже существует. Обновляем...")
            conn = existing_conn
        else:
            logger.info(f"Создаем новый connection '{conn_id}'...")
            conn = Connection(conn_id=conn_id)
        
        # Устанавливаем параметры
        if uri:
            # Если передан URI, парсим его
            conn.parse_from_uri(uri)
        else:
            # Иначе устанавливаем параметры по отдельности
            conn.conn_type = conn_type
            conn.host = host
            conn.schema = schema
            conn.login = login
            conn.password = password
            conn.port = port
            
            if extra:
                conn.extra = json.dumps(extra)
        
        if description:
            conn.description = description
        
        # Сохраняем или обновляем connection
        session.add(conn)
        session.commit()
        logger.info(f"Connection '{conn_id}' успешно {'обновлен' if existing_conn else 'создан'}")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка при создании connection '{conn_id}': {e}")
        raise
    
    finally:
        session.close()

"""
DELETE_CONNECTION
Удаляет connection по ID
Args:
    conn_id: Уникальный ID соединения
Return:
    True - удалено соединение с ID conn_id
    False - соединение с conn_id НЕ найдено
"""
def delete_connection(conn_id: str) -> None:

    session = Session()
    
    try:
        conn = session.query(Connection).filter(
            Connection.conn_id == conn_id
        ).first()
        
        if conn:
            session.delete(conn)
            session.commit()
            logger.info(f"Connection '{conn_id}' удален")
            return True
        else:
            logger.warning(f"Connection '{conn_id}' не найден")
            return False
    
    except Exception as e:
        session.rollback()
        logger.error(f"Ошибка при удалении connection '{conn_id}': {e}")
        raise
    
    finally:
        session.close()

"""
LIST_ALL_CONNECTIONS
Возвращает список всех Connections
Args:
    None
Return:
    List[Dict] - список объектов Connections (Dict)
"""
def list_all_connections() -> List[Dict]:
    session = Session()
    try:
        connections = session.query(Connection).all()
        
        result = []
        for conn in connections:
            result.append({
                'conn_id': conn.conn_id,
                'conn_type': conn.conn_type,
                'host': conn.host,
                'schema': conn.schema,
                'login': conn.login,
                'port': conn.port,
                'description': conn.description
            })
        
        return result
    
    finally:
        session.close()


# =============================================================================
# СОЗДАНИЕ CONNECTION (Postgres, Mongo, RESTAPI) ИЗ ENV ПЕРЕМЕННЫХ
# =============================================================================

"""
INIT_POSTGRES_FROM_ENV
Инициализация PostgreSQL connection из ENV переменных
Args:
    conn_id: Уникальный ID соединения, который создастся
    env_prefix: Префикс переменных окружения (одинаковый для всех переменных)
                Ожидаемые переменные окружения:
                    {PREFIX}_HOST
                    {PREFIX}_PORT
                    {PREFIX}_DATABASE
                    {PREFIX}_USER
                    {PREFIX}_PASSWORD
                    {PREFIX}_EXTRA (опционально, JSON строка)

                Пример:
                    SOURCE_POSTGRES_HOST=db.example.com
                    SOURCE_POSTGRES_PORT=5432
                    SOURCE_POSTGRES_DATABASE=mydb
                    SOURCE_POSTGRES_USER=myuser
                    SOURCE_POSTGRES_PASSWORD=secret
                    SOURCE_POSTGRES_EXTRA='{"sslmode": "require"}'
Return:
    None
"""
def init_postgres_from_env(conn_id: str, env_prefix: str) -> None:
    host = os.getenv(f"{env_prefix}_HOST")
    port = os.getenv(f"{env_prefix}_PORT", "5432")
    database = os.getenv(f"{env_prefix}_DATABASE")
    user = os.getenv(f"{env_prefix}_USER")
    password = os.getenv(f"{env_prefix}_PASSWORD")
    extra_json = os.getenv(f"{env_prefix}_EXTRA")
    
    # Валидация обязательных параметров
    if not all([host, database, user, password]):
        missing = []
        if not host: missing.append(f"{env_prefix}_HOST")
        if not database: missing.append(f"{env_prefix}_DATABASE")
        if not user: missing.append(f"{env_prefix}_USER")
        if not password: missing.append(f"{env_prefix}_PASSWORD")
        
        raise ValueError(f"Отсутствуют обязательные переменные: {', '.join(missing)}")
    
    # Парсим extra параметры
    extra = None
    if extra_json:
        try:
            extra = json.loads(extra_json)
        except json.JSONDecodeError as e:
            logger.warning(f"Не удалось распарсить {env_prefix}_EXTRA: {e}")
    
    # Создаем connection
    create_or_update_connection(
        conn_id=conn_id,
        conn_type='postgres',
        host=host,
        port=int(port),
        schema=database,
        login=user,
        password=password,
        extra=extra,
        description=f"PostgreSQL connection initialized from {env_prefix}_* env vars"
    )

"""
INIT_MONGO_FROM_ENV
Инициализация MongoDB connection из ENV переменных
Args:
    conn_id: Уникальный ID соединения, который создастся
    env_prefix: Префикс переменных окружения (одинаковый для всех переменных)
                Ожидаемые переменные окружения:
                    {PREFIX}_HOST (или {PREFIX}_URI для полного URI)
                    {PREFIX}_PORT
                    {PREFIX}_DATABASE
                    {PREFIX}_USER
                    {PREFIX}_PASSWORD
                    {PREFIX}_EXTRA (опционально)
    
                Пример с отдельными параметрами:
                    SOURCE_MONGO_HOST=mongo.example.com
                    SOURCE_MONGO_PORT=27017
                    SOURCE_MONGO_DATABASE=analytics
                    SOURCE_MONGO_USER=mongouser
                    SOURCE_MONGO_PASSWORD=secret
                    SOURCE_MONGO_EXTRA='{"authSource": "admin", "replicaSet": "rs0"}'
                
                Пример с URI:
                    MONGO_PROD_URI=mongodb://user:pass@host1:27017,host2:27017/dbname?replicaSet=rs0
Return:
    None
"""
def init_mongo_from_env(conn_id: str, env_prefix: str) -> None:
     # Проверяем, есть ли готовый URI
    uri = os.getenv(f"{env_prefix}_URI")
    
    if uri:
        # Используем URI
        create_or_update_connection(
            conn_id=conn_id,
            conn_type='mongo',
            uri=uri,
            description=f"MongoDB connection initialized from {env_prefix}_URI"
        )
    else:
        # Используем отдельные параметры
        host = os.getenv(f"{env_prefix}_HOST")
        port = os.getenv(f"{env_prefix}_PORT", "27017")
        database = os.getenv(f"{env_prefix}_DATABASE")
        user = os.getenv(f"{env_prefix}_USER")
        password = os.getenv(f"{env_prefix}_PASSWORD")
        extra_json = os.getenv(f"{env_prefix}_EXTRA")
        
        if not all([host, database, user, password]):
            missing = []
            if not host: missing.append(f"{env_prefix}_HOST")
            if not database: missing.append(f"{env_prefix}_DATABASE")
            if not user: missing.append(f"{env_prefix}_USER")
            if not password: missing.append(f"{env_prefix}_PASSWORD")
            
            raise ValueError(f"Отсутствуют обязательные переменные: {', '.join(missing)}")
        
        extra = None
        if extra_json:
            try:
                extra = json.loads(extra_json)
            except json.JSONDecodeError as e:
                logger.warning(f"Не удалось распарсить {env_prefix}_EXTRA: {e}")
        
        create_or_update_connection(
            conn_id=conn_id,
            conn_type='mongo',
            host=host,
            port=int(port),
            schema=database,
            login=user,
            password=password,
            extra=extra,
            description=f"MongoDB connection initialized from {env_prefix}_* env vars"
        )

"""
INIT_FTP_FROM_ENV
Инициализация FTP connection из ENV переменных
Args:
    conn_id: Уникальный ID соединения, который создастся
    env_prefix: Префикс переменных окружения (одинаковый для всех переменных)
                Ожидаемые переменные окружения:
                    {PREFIX}_HOST - FTP сервер (обязательно)
                    {PREFIX}_PORT - Порт (по умолчанию 21) (опционально)
                    {PREFIX}_USER - Имя пользователя (опционально, может быть анонимный доступ)
                    {PREFIX}_PASSWORD - Пароль (опционально)
                    {PREFIX}_PATH - Начальный путь на FTP сервере (опционально)
                    {PREFIX}_EXTRA - Дополнительные параметры в формате JSON (опционально)
    
                Пример:
                    FTP_SOURCE_HOST=ftp.example.com
                    FTP_SOURCE_PORT=21
                    FTP_SOURCE_USER=etl_user
                    FTP_SOURCE_PASSWORD=secure_pass123
                    FTP_SOURCE_PATH=/data/exports
                    FTP_SOURCE_EXTRA='{"passive_mode": true, "encoding": "utf-8"}'
Return:
    None
"""
def init_ftp_from_env(conn_id: str, env_prefix: str) -> None:
    # Получаем обязательные параметры
    host = os.getenv(f"{env_prefix}_HOST")


    logger.info(f"Создание FTP соединения с {host}")

    if not host:
        raise ValueError(f"Отсутствует обязательная переменная {env_prefix}_HOST")
    
    # Получаем опциональные параметры
    port = os.getenv(f"{env_prefix}_PORT", "21")
    login = os.getenv(f"{env_prefix}_USER")
    password = os.getenv(f"{env_prefix}_PASSWORD")
    path = os.getenv(f"{env_prefix}_PATH")
    extra_json = os.getenv(f"{env_prefix}_EXTRA")
    
    # Преобразуем порт в число
    try:
        port = int(port)
    except ValueError:
        logger.warning(f"Некорректный порт {port}, используется значение по умолчанию 21")
        port = 21
    
    # Парсим дополнительные параметры
    extra = {}
    if extra_json and extra_json != "" and len(extra_json) > 0:
        try:
            extra = json.loads(extra_json)
        except json.JSONDecodeError as e:
            logger.warning(f"Не удалось распарсить {env_prefix}_EXTRA: {e}")
    
    # Добавляем стандартные параметры FTP
    if not extra or not extra.get('passive_mode'):
        extra['passive_mode'] = True  # По умолчанию используем passive mode для безопасности
    
    if not extra or not extra.get('encoding'):
        extra['encoding'] = 'utf-8'  # Кодировка по умолчанию
    
    # Если указан путь, добавляем его в extra
    if path:
        extra['path'] = path

    # Создаем или обновляем соединение
    create_or_update_connection(
        conn_id=conn_id,
        conn_type='ftp',
        host=host,
        port=port,
        login=login, 
        password=password,
        extra=json.dumps(extra) if extra else None,  # Сериализуем extra в JSON строку
        description=f"FTP connection initialized from {env_prefix}_* env vars"
    )
    
    logger.info(f"FTP соединение '{conn_id}' успешно инициализировано из переменных окружения")


"""
INIT_API_FROM_ENV
Инициализация HTTP/API connection из ENV переменных
Args:
    conn_id: Уникальный ID соединения, который создастся
    env_prefix: Префикс переменных окружения (одинаковый для всех переменных)
                Ожидаемые переменные окружения:
                    {PREFIX}_URL - Base URL API
                    {PREFIX}_API_KEY (или {PREFIX}_TOKEN)
                    {PREFIX}_EXTRA (опционально)
    
                Пример:
                    SOURCE_API_URL=https://api.competitor.com/v1
                    SOURCE_API_KEY=your_api_key_here
                    SOURCE_API_EXTRA='{"timeout": 30}'
Return:
    None
"""
def init_api_from_env(conn_id: str, env_prefix: str) -> None:

    base_url = os.getenv(f"{env_prefix}_URL")
    api_key = os.getenv(f"{env_prefix}_API_KEY") or os.getenv(f"{env_prefix}_TOKEN")
    extra_json = os.getenv(f"{env_prefix}_EXTRA")
    
    if not base_url:
        raise ValueError(f"Отсутствует {env_prefix}_URL")
    
    extra = {}
    if extra_json:
        try:
            extra = json.loads(extra_json)
        except json.JSONDecodeError as e:
            logger.warning(f"Не удалось распарсить {env_prefix}_EXTRA: {e}")
    
    # Добавляем API key в extra если есть
    if api_key:
        extra['api_key'] = api_key
    
    create_or_update_connection(
        conn_id=conn_id,
        conn_type='http',
        host=base_url,
        extra=extra,
        description=f"HTTP API connection initialized from {env_prefix}_* env vars"
    )



# =============================================================================
# ИНИЦИАЛИЗАЦИЯ CONNECTIONS ИЗ КОНФИГУРАЦИОННОГО СЛОВАРЯ
# =============================================================================


"""
INIT_CONNECTIONS_FROM_CONFIG
Инициализация множественных connections из конфига
Args:
    config: Словарь с описанием connections
    Пример config:
        {
            'postgres_prod': {
                'type': 'postgres',
                'env_prefix': 'POSTGRES_PROD'
            },
            'mongo_analytics': {
                'type': 'mongo',
                'env_prefix': 'MONGO_ANALYTICS'
            },
            'api_external': {
                'type': 'http',
                'env_prefix': 'API_EXTERNAL'
            },
            'ftp_external': {
                'type': 'ftp',
                'env_prefix': 'FTP_EXTERNAL'
            }
        }
Return:
    None
"""
def init_connections_from_config(config: Dict) -> None:
     for conn_id, conn_config in config.items():
        conn_type = conn_config.get('type')
        env_prefix = conn_config.get('env_prefix')
        
        if not conn_type or not env_prefix:
            logger.warning(f"Пропускаем {conn_id}: отсутствует type или env_prefix")
            continue
        
        try:
            if conn_type == 'postgres':
                init_postgres_from_env(conn_id, env_prefix)
            elif conn_type == 'mongo':
                init_mongo_from_env(conn_id, env_prefix)
            elif conn_type == 'ftp':
                init_ftp_from_env(conn_id, env_prefix)
            elif conn_type == 'http':
                init_api_from_env(conn_id, env_prefix)
            else:
                logger.warning(f"Неизвестный тип connection: {conn_type}")
        
        except Exception as e:
            logger.error(f"Ошибка при инициализации {conn_id}: {e}")
            # Продолжаем со следующим connection


# =============================================================================
# ИНИЦИАЛИЗАЦИЯ CONNECTIONS ИЗ ENV ФАЙЛА
# =============================================================================

"""
LOAD_ENV_FILE
Загружает переменные из .env файла в окружение
Args:
    filepath: Путь и имя файла с переменными окружения (по умолчанию '.env')
    Формат .env файла:
        # PostgreSQL Production
        SOURCE_POSTGRES_HOST=db.example.com
        SOURCE_POSTGRES_PORT=5432
        SOURCE_POSTGRES_DATABASE=mydb
        SOURCE_POSTGRES_USER=myuser
        SOURCE_POSTGRES_PASSWORD=secret123
Return:
    None
"""
def load_env_file(filepath: str = '.env') -> None:

    if not os.path.exists(filepath):
        logger.warning(f"Файл {filepath} не найден")
        return
    
    with open(filepath, 'r') as f:
        for line in f:
            line = line.strip()
            
            # Пропускаем комментарии и пустые строки
            if not line or line.startswith('#'):
                continue
            
            # Парсим KEY=VALUE
            if '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()
                logger.debug(f"Загружена переменная: {key.strip()}")


# =============================================================================
# ГЛАВНЫЙ СКРИПТ ИНИЦИАЛИЗАЦИИ
# =============================================================================


"""
INITIALIZE_ALL_CONNECTIONS
Главная функция инициализации всех connections
Вызывайте её при старте Airflow или в отдельном init скрипте
Args:
    None
Return:
    None
"""
def initialize_all_connections():

    logger.info("-" * 40)
    logger.info("ИНИЦИАЛИЗАЦИЯ AIRFLOW CONNECTIONS")
    logger.info("-" * 40)
    
  
    # Определяем конфигурацию connections
    connections_config = {
        # PostgreSQL connections
        'source_postgres': {
            'type': 'postgres',
            'env_prefix': 'SOURCE_POSTGRES'
        },
        'target_postgres': {
            'type': 'postgres',
            'env_prefix': 'TARGET_POSTGRES'
        },
    }
    
    # Инициализируем connections из ENV
    logger.info(f"\nИнициализация {len(connections_config)} connections...")
    init_connections_from_config(connections_config)
    
   
    # Выводим список созданных connections
    logger.info("\n" + "-" * 40)
    logger.info("СОЗДАННЫЕ CONNECTIONS:")
    logger.info("-" * 40)
    
    connections = list_all_connections()
    for conn in connections:
        logger.info(f"  • {conn['conn_id']:30} | {conn['conn_type']:10} | {conn['host'] or 'N/A'}")
    
    logger.info("-" * 40)
    logger.info(f"Всего connections: {len(connections)}")
    logger.info("-" * 40)




# =============================================================================
# ЗАПУСК ГЛАВНОГО СКРИПТА ИНИЦИАЛИЗАЦИИ
# =============================================================================
if __name__ == "__main__":
    initialize_all_connections()