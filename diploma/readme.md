# Задание на дипломную работу

## Тема: "Дашборд аналитика бизнес-процессов"

### Цель проекта

Разработать ETL-pipeline на базе Apache Airflow для автоматизированного сбора, обработки и визуализации данных бизнес-процессов. Система должна ежедневно (в 9:00) собирать данные из различных источников, трансформировать их и загружать в аналитическую БД и хранилище данных (Data Warehouse) для последующей визуализации на дашборде.

---

## 1. Описание предметной области

Выберите одну из предметных областей или предложите свою:

### Варианты предметных областей

**A. Интернет-магазин**

- Бизнес-процессы: заказы, платежи, доставка, отзывы
- Метрики: количество заказов, средний чек, конверсия, популярные товары, география

**B. Служба доставки**

- Бизнес-процессы: прием заказов, маршрутизация, доставка
- Метрики: количество доставок, время доставки, загрузка курьеров, процент успеха

**C. Образовательная платформа**

- Бизнес-процессы: регистрация, курсы, выполнение заданий, подписки
- Метрики: активные пользователи, завершение курсов, выручка, оценки курсов

**Требования**: Опишите выбранную область, бизнес-процессы, ключевые метрики и обоснуйте выбор источников данных.

---

## 2. Описание архитектуры проекта

Создайте архитектурную диаграмму системы, включающую:

### Компоненты

1. **Источники данных**: PostgreSQL, MongoDB, CSV/FTP, REST API _(минимум 3)_
2. **Слой извлечения (Extract)**: классы Extractors под разные типы источников
3. **Слой трансформации (Transform)**: очистка, валидация, агрегация
4. **Слой загрузки (Load)**: в аналитическую БД и DWH
5. **Apache Airflow**: оркестрация DAG
6. **Хранилища**: аналитическая БД, Data Warehouse
7. **Визуализация**: Grafana/Metabase/Superset

### Технологический стек

- Python 3.10+
- Apache Airflow 2.10+
- PostgreSQL / MongoDB
- Docker & Docker Compose
- pandas, psycopg2, pymongo

Опишите потоки данных, расписание и зависимости задач.

---

## 3. Описание структуры источников данных

### 3.1 PostgreSQL - Транзакционная база

```sql
-- Таблица заказов
CREATE TABLE orders (
    order_id SERIAL PRIMARY KEY,
    customer_id INTEGER NOT NULL,
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_amount DECIMAL(10, 2),
    status VARCHAR(50),  -- pending, processing, shipped, delivered, cancelled
    payment_method VARCHAR(50),
    shipping_address TEXT
);

-- Таблица клиентов
CREATE TABLE customers (
    customer_id SERIAL PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255) UNIQUE,
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100)
);
```

### 3.2 MongoDB - Коллекция отзывов

```json
{
  "_id": "ObjectId",
  "customer_id": 12345,
  "order_id": 67890,
  "rating": 4.5,
  "comment": "Отличный сервис!",
  "feedback_date": "2024-12-30T10:30:00Z",
  "category": "delivery"
}
```

### 3.3 CSV - Справочник продуктов

```csv
product_id,product_name,category,price,stock_quantity
1,Laptop Dell,Electronics,1299.99,50
2,iPhone 15,Electronics,1199.99,120
```

### 3.4 REST API - Веб-аналитика

```http
GET /api/v1/analytics/daily-stats

Response:
{
  "date": "2024-12-30",
  "page_views": 15234,
  "unique_visitors": 8456,
  "conversion_rate": 0.034
}
```

### 3.5 FTP - Логи доставки

```csv
delivery_id,order_id,courier_id,pickup_time,delivery_time,status
1,1001,25,2024-12-30 09:00:00,2024-12-30 10:30:00,delivered
```

---

## 4. Структура конечной таблицы для аналитика

```sql
CREATE TABLE daily_business_analytics (
    id SERIAL PRIMARY KEY,
    analytics_date DATE NOT NULL UNIQUE,
    
    -- Метрики заказов
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    avg_order_value DECIMAL(10, 2) DEFAULT 0,
    
    -- Статусы заказов
    orders_pending INTEGER DEFAULT 0,
    orders_processing INTEGER DEFAULT 0,
    orders_shipped INTEGER DEFAULT 0,
    orders_delivered INTEGER DEFAULT 0,
    orders_cancelled INTEGER DEFAULT 0,
    
    -- Клиенты
    unique_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    avg_customer_rating DECIMAL(3, 2) DEFAULT 0,
    
    -- Продукты
    total_products_sold INTEGER DEFAULT 0,
    top_category VARCHAR(100),
    
    -- Веб-метрики
    total_page_views INTEGER DEFAULT 0,
    unique_visitors INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5, 4) DEFAULT 0,
    
    -- Доставка
    total_deliveries INTEGER DEFAULT 0,
    avg_delivery_time_minutes INTEGER DEFAULT 0,
    successful_delivery_rate DECIMAL(5, 4) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

---

## 5. Структура Data Warehouse (схема "звезда")

### Важно: Стратегия SCD Type 2

**Slowly Changing Dimensions (SCD) Type 2** - это метод отслеживания исторических изменений в измерениях. При изменении атрибута объекта (например, клиент переехал в другой город) создается новая версия записи с новым surrogate key, а старая версия помечается как неактуальная.

**Преимущества SCD Type 2:**

- Полная история изменений атрибутов
- Возможность анализа "как было" на любую дату
- Сохранение точности исторических отчетов
- Аудит изменений данных

**Ключевые поля для SCD Type 2:**

- `effective_date` - дата начала действия записи
- `expiration_date` - дата окончания действия (9999-12-31 для текущей)
- `is_current` - флаг текущей версии (TRUE/FALSE)

**Пример изменения:**

```
Клиент ID=123 переехал из Москвы в Санкт-Петербург 2024-06-15

До изменения:
customer_key | customer_id | city            | effective_date | expiration_date | is_current
1001        | 123         | Москва          | 2024-01-01    | 9999-12-31     | TRUE

После изменения:
customer_key | customer_id | city            | effective_date | expiration_date | is_current
1001        | 123         | Москва          | 2024-01-01    | 2024-06-14     | FALSE
1125        | 123         | Санкт-Петербург | 2024-06-15    | 9999-12-31     | TRUE
```

### 5.1 Fact Table - Факты заказов

```sql
CREATE TABLE fact_orders (
    fact_id BIGSERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    product_key INTEGER REFERENCES dim_products(product_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    
    -- Метрики
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(12, 2),
    order_status VARCHAR(50),
    
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### 5.2 Dimension Tables

```sql
-- Измерение: Клиенты (SCD Type 2)
CREATE TABLE dim_customers (
    customer_key SERIAL PRIMARY KEY,        -- Surrogate key (генерируется автоматически)
    customer_id INTEGER NOT NULL,           -- Natural key (ID из источника)
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    city VARCHAR(100),
    customer_segment VARCHAR(50),
    
    -- Поля для SCD Type 2
    effective_date DATE NOT NULL,           -- Дата начала действия версии
    expiration_date DATE DEFAULT '9999-12-31',  -- Дата окончания (9999-12-31 = активна)
    is_current BOOLEAN DEFAULT TRUE,        -- TRUE = текущая версия, FALSE = историческая
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для эффективного поиска текущих версий
CREATE INDEX idx_dim_customers_current ON dim_customers(customer_id, is_current);
CREATE INDEX idx_dim_customers_dates ON dim_customers(effective_date, expiration_date);

-- Измерение: Продукты (SCD Type 2)
CREATE TABLE dim_products (
    product_key SERIAL PRIMARY KEY,         -- Surrogate key
    product_id INTEGER NOT NULL,            -- Natural key
    product_name VARCHAR(255),
    category VARCHAR(100),
    brand VARCHAR(100),
    
    -- Поля для SCD Type 2
    effective_date DATE NOT NULL,
    expiration_date DATE DEFAULT '9999-12-31',
    is_current BOOLEAN DEFAULT TRUE,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_dim_products_current ON dim_products(product_id, is_current);

-- Измерение: Дата
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,  -- YYYYMMDD
    full_date DATE NOT NULL,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    month_number INTEGER,
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN
);

-- Измерение: Время
CREATE TABLE dim_time (
    time_key INTEGER PRIMARY KEY,  -- HHMMSS
    hour INTEGER,
    minute INTEGER,
    time_of_day VARCHAR(20),  -- Morning, Afternoon, Evening, Night
    is_business_hours BOOLEAN
);
```

---

## 6. Описание источников данных

| Источник | Назначение | Частота обновления | Подключение |
|----------|------------|-------------------|-------------|
| **PostgreSQL** | Транзакционные данные (заказы, клиенты) | Реального времени | `postgres:5432/production_db` |
| **MongoDB** | Неструктурированные данные (отзывы) | Реального времени | `mongodb:27017/feedback_db` |
| **CSV файлы** | Справочные данные (продукты) | Ежедневно в 02:00 | `/data/csv/products_YYYYMMDD.csv` |
| **FTP сервер** | Логи доставки | Ежедневно в 01:00 | `ftp://ftp-server/logs/` |
| **REST API** | Веб-аналитика | По запросу | `https://api.example.com/v1/stats` |

### Стратегия извлечения

- Инкрементальная загрузка (только новые/измененные данные)
- Временной диапазон: вчерашний день (00:00 - 23:59)
- Обработка ошибок с повторными попытками
- Логирование всех операций

---

## 7. Этапы "Загрузка данных" (Extract)

### Процесс извлечения

1. **Инициализация подключений**
   - Проверка доступности источников
   - Валидация учетных данных
   - Установка соединений

2. **Определение временного диапазона**

   ```python
   execution_date = context['execution_date']
   start_time = execution_date.replace(hour=0, minute=0, second=0)
   end_time = start_time + timedelta(days=1)
   ```

3. **Извлечение из PostgreSQL**
   - SQL-запрос с фильтром по дате
   - Пагинация для больших объемов
   - Сохранение в staging

4. **Извлечение из MongoDB**
   - Запрос с фильтром по `feedback_date`
   - Обработка курсора
   - Конвертация BSON → dict

5. **Загрузка CSV файлов**
   - Поиск файла по маске `products_YYYYMMDD.csv`
   - Парсинг CSV
   - Обработка кодировки UTF-8

6. **Получение с FTP**
   - Подключение к FTP
   - Скачивание файлов по маске
   - Парсинг содержимого

7. **Запрос к REST API**
   - HTTP GET с параметрами даты
   - Обработка пагинации
   - Парсинг JSON

8. **Сохранение raw данных**
   - Staging область
   - Метаданные загрузки
   - Логирование

### Обработка ошибок

- ConnectionError → повторная попытка
- DataValidationError → логирование и пропуск
- Exception → остановка pipeline

---

## 8. Этапы "Трансформация данных" (Transform)

### 8.1 Очистка данных

**Удаление дубликатов:**

```python
df = df.drop_duplicates(subset=['order_id'], keep='last')
```

**Обработка пропусков:**

```python
df['total_amount'].fillna(0, inplace=True)
df = df.dropna(subset=['order_id', 'customer_id'])
```

**Очистка текста:**

```python
df['status'] = df['status'].str.strip().str.lower()
status_mapping = {'в обработке': 'processing', 'доставлен': 'delivered'}
df['status'] = df['status'].replace(status_mapping)
```

### 8.2 Валидация данных

**Проверка типов:**

```python
df['order_id'] = df['order_id'].astype('int64')
df['order_date'] = pd.to_datetime(df['order_date'])
df['total_amount'] = pd.to_numeric(df['total_amount'])
```

**Проверка диапазонов:**

```python
df = df[df['total_amount'] >= 0]
df = df[df['order_date'] <= datetime.now()]
```

**Бизнес-правила:**

```python
# Delivered заказы должны иметь дату доставки
invalid = df[(df['status'] == 'delivered') & (df['delivery_date'].isna())]
df.loc[invalid.index, 'status'] = 'shipped'
```

### 8.3 Нормализация

```python
# Даты к единому формату
df['order_date'] = pd.to_datetime(df['order_date']).dt.tz_localize('UTC')

# Округление сумм
df['total_amount'] = df['total_amount'].round(2)

# Конвертация валют (при необходимости)
df['total_amount_usd'] = df.apply(lambda row: convert_currency(row), axis=1)
```

### 8.4 Обогащение данных

```python
# Добавление вычисляемых полей
df['day_of_week'] = df['order_date'].dt.dayofweek
df['is_weekend'] = df['day_of_week'].isin([5, 6])
df['hour_of_day'] = df['order_date'].dt.hour

# Категоризация сумм
df['amount_category'] = pd.cut(
    df['total_amount'],
    bins=[0, 50, 200, 1000, float('inf')],
    labels=['small', 'medium', 'large', 'extra_large']
)

# Объединение со справочниками
df = df.merge(df_customers[['customer_id', 'city']], on='customer_id', how='left')
```

### 8.5 Агрегация метрик

```python
def calculate_daily_metrics(df, analytics_date):
    return {
        'analytics_date': analytics_date,
        'total_orders': len(df),
        'total_revenue': df['total_amount'].sum(),
        'avg_order_value': df['total_amount'].mean(),
        'unique_customers': df['customer_id'].nunique(),
        'orders_delivered': len(df[df['status'] == 'delivered']),
        'top_category': df['category'].mode()[0] if not df.empty else None
    }
```

---

## 9. Этапы "Загрузка данных" (Load)

### 9.1 Загрузка в аналитическую БД

**Upsert в daily_business_analytics**

```python
def upsert_analytics(df_analytics, conn):
    for _, row in df_analytics.iterrows():
        if exists := check_existing(conn, row['analytics_date']):
            conn.execute("""
                UPDATE daily_business_analytics
                SET total_orders = %s, total_revenue = %s, updated_at = NOW()
                WHERE analytics_date = %s
            """, (row['total_orders'], row['total_revenue'], row['analytics_date']))
        else:
            conn.execute("""
                INSERT INTO daily_business_analytics 
                (analytics_date, total_orders, total_revenue)
                VALUES (%s, %s, %s)
            """, (row['analytics_date'], row['total_orders'], row['total_revenue']))
    conn.commit()
```

### 9.2 Загрузка в Data Warehouse

**Таблицы измерений (SCD Type 2):**

```python
def load_dim_customers(df_customers, conn):
    for _, customer in df_customers.iterrows():
        current = get_current_record(conn, customer['customer_id'])
        
        if current is None:
            # Новый клиент
            insert_new_customer(conn, customer)
        elif has_changes(current, customer):
            # Закрываем старую версию
            conn.execute("""
                UPDATE dim_customers 
                SET expiration_date = CURRENT_DATE, is_current = FALSE
                WHERE customer_key = %s
            """, (current['customer_key'],))
            # Создаем новую версию
            insert_new_customer(conn, customer)
    conn.commit()
```

**Таблица фактов:**

```python
def load_fact_orders(df_facts, conn):
    # Обогащение surrogate keys
    df_facts = enrich_with_dimension_keys(df_facts, conn)
    
    # Batch insert
    values = [(row['order_id'], row['customer_key'], 
               row['product_key'], row['total_amount']) 
              for _, row in df_facts.iterrows()]
    
    execute_values(conn.cursor(), """
        INSERT INTO fact_orders 
        (order_id, customer_key, product_key, total_amount)
        VALUES %s
    """, values)
    conn.commit()
```

### 9.3 Обновление агрегированных таблиц

```python
def update_aggregates(analytics_date, conn):
    # Удаление старых данных
    conn.execute("DELETE FROM agg_daily_sales WHERE date_key = %s", 
                (int(analytics_date.strftime('%Y%m%d')),))
    
    # Вставка новых агрегатов
    conn.execute("""
        INSERT INTO agg_daily_sales
        SELECT date_key, product_key, customer_segment,
               COUNT(*) as total_orders, SUM(total_amount) as total_revenue
        FROM fact_orders f
        JOIN dim_customers c ON f.customer_key = c.customer_key
        WHERE f.date_key = %s
        GROUP BY date_key, product_key, customer_segment
    """, (int(analytics_date.strftime('%Y%m%d')),))
    conn.commit()
```

### 9.4 Валидация загруженных данных

```python
def validate_loaded_data(analytics_date, conn):
    validations = []
    
    # Проверка наличия записи в аналитике
    result = conn.execute("""
        SELECT total_orders FROM daily_business_analytics
        WHERE analytics_date = %s
    """, (analytics_date,)).fetchone()
    validations.append({'check': 'analytics_exists', 'passed': result is not None})
    
    # Проверка количества фактов
    fact_count = conn.execute("""
        SELECT COUNT(*) FROM fact_orders WHERE date_key = %s
    """, (int(analytics_date.strftime('%Y%m%d')),)).fetchone()[0]
    validations.append({'check': 'facts_loaded', 'passed': fact_count > 0})
    
    # Проверка отсутствия NULL в критичных полях
    null_count = conn.execute("""
        SELECT COUNT(*) FROM fact_orders
        WHERE date_key = %s AND (customer_key IS NULL OR product_key IS NULL)
    """, (int(analytics_date.strftime('%Y%m%d')),)).fetchone()[0]
    validations.append({'check': 'no_nulls', 'passed': null_count == 0})
    
    for v in validations:
        if not v['passed']:
            raise ValidationError(f"Validation failed: {v['check']}")
    
    return True
```

---

## 10. Docker Compose для инфраструктуры

```yaml
version: '3.8'

services:
  # Airflow Database
  postgres-airflow:
    image: postgres:15
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-airflow-data:/var/lib/postgresql/data
    ports:
      - "5433:5432"

  # Airflow Webserver
  airflow-webserver:
    image: apache/airflow:2.8.1-python3.10
    depends_on:
      - postgres-airflow
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
    ports:
      - "8080:8080"
    command: webserver

  # Airflow Scheduler
  airflow-scheduler:
    image: apache/airflow:2.8.1-python3.11
    depends_on:
      - postgres-airflow
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres-airflow/airflow
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./data:/opt/airflow/data
    command: scheduler

  # Source PostgreSQL
  postgres-source:
    image: postgres:15
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
      POSTGRES_DB: production_db
    volumes:
      - postgres-source-data:/var/lib/postgresql/data
      - ./init-scripts/init-source-db.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5432:5432"

  # Analytics PostgreSQL & DWH
  postgres-analytics:
    image: postgres:15
    environment:
      POSTGRES_USER: analytics
      POSTGRES_PASSWORD: analytics
      POSTGRES_DB: analytics_db
    volumes:
      - postgres-analytics-data:/var/lib/postgresql/data
      - ./init-scripts/init-analytics-db.sql:/docker-entrypoint-initdb.d/init.sql
    ports:
      - "5434:5432"

  # MongoDB
  mongodb:
    image: mongo:7
    environment:
      MONGO_INITDB_ROOT_USERNAME: mongo
      MONGO_INITDB_ROOT_PASSWORD: mongo
      MONGO_INITDB_DATABASE: feedback_db
    volumes:
      - mongodb-data:/data/db
      - ./init-scripts/init-mongo.js:/docker-entrypoint-initdb.d/init.js
    ports:
      - "27017:27017"

  # FTP Server
  ftp-server:
    image: fauria/vsftpd
    environment:
      FTP_USER: ftpuser
      FTP_PASS: ftppass
    volumes:
      - ./data/ftp:/home/vsftpd/ftpuser
    ports:
      - "21:21"
      - "21100-21110:21100-21110"

  # Mock REST API
  mock-api:
    image: mockserver/mockserver:latest
    environment:
      MOCKSERVER_INITIALIZATION_JSON_PATH: /config/initializerJson.json
    volumes:
      - ./mock-api/initializerJson.json:/config/initializerJson.json
    ports:
      - "1080:1080"

  # Grafana
  grafana:
    image: grafana/grafana:latest
    depends_on:
      - postgres-analytics
    environment:
      GF_SECURITY_ADMIN_USER: admin
      GF_SECURITY_ADMIN_PASSWORD: admin
    volumes:
      - grafana-data:/var/lib/grafana
      - ./grafana/dashboards:/var/lib/grafana/dashboards
    ports:
      - "3000:3000"

volumes:
  postgres-airflow-data:
  postgres-source-data:
  postgres-analytics-data:
  mongodb-data:
  grafana-data:

networks:
  default:
    name: etl-network
```

### Запуск

```bash
# Запуск всей инфраструктуры
docker-compose up -d

# Просмотр логов
docker-compose logs -f airflow-scheduler

# Остановка
docker-compose down

# Очистка данных
docker-compose down -v
```

---

## 11. Классы Extractors

### Базовый класс

```python
# extractors/base_extractor.py

from abc import ABC, abstractmethod
from typing import Any, Dict, List
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class BaseExtractor(ABC):
    """Базовый класс для всех экстракторов"""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.connection = None
        
    @abstractmethod
    def connect(self):
        """Установка соединения"""
        pass
    
    @abstractmethod
    def disconnect(self):
        """Закрытие соединения"""
        pass
    
    @abstractmethod
    def extract(self, start_date: datetime, end_date: datetime) -> List[Dict]:
        """Извлечение данных за период"""
        pass
    
    def __enter__(self):
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
```

### PostgreSQL Extractor

```python
# extractors/postgres_extractor.py

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd
from .base_extractor import BaseExtractor

class PostgresExtractor(BaseExtractor):
    """Экстрактор для PostgreSQL"""
    
    def connect(self):
        self.connection = psycopg2.connect(
            host=self.config['host'],
            port=self.config['port'],
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password']
        )
        logger.info(f"Connected to PostgreSQL: {self.config['database']}")
    
    def disconnect(self):
        if self.connection:
            self.connection.close()
    
    def extract(self, start_date, end_date):
        query = """
            SELECT o.*, c.first_name, c.last_name, c.email, c.city
            FROM orders o
            LEFT JOIN customers c ON o.customer_id = c.customer_id
            WHERE o.order_date >= %s AND o.order_date < %s
        """
        with self.connection.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, (start_date, end_date))
            data = [dict(row) for row in cursor.fetchall()]
        logger.info(f"Extracted {len(data)} orders")
        return data
```

### MongoDB Extractor

```python
# extractors/mongodb_extractor.py

from pymongo import MongoClient
from .base_extractor import BaseExtractor

class MongoDBExtractor(BaseExtractor):
    """Экстрактор для MongoDB"""
    
    def connect(self):
        connection_string = (
            f"mongodb://{self.config['username']}:{self.config['password']}"
            f"@{self.config['host']}:{self.config['port']}"
        )
        self.client = MongoClient(connection_string)
        self.db = self.client[self.config['database']]
        self.collection = self.db[self.config['collection']]
        logger.info(f"Connected to MongoDB: {self.config['database']}")
    
    def disconnect(self):
        if self.client:
            self.client.close()
    
    def extract(self, start_date, end_date):
        query = {"feedback_date": {"$gte": start_date, "$lt": end_date}}
        data = []
        for doc in self.collection.find(query):
            doc['_id'] = str(doc['_id'])
            data.append(doc)
        logger.info(f"Extracted {len(data)} feedback documents")
        return data
```

### CSV Extractor

```python
# extractors/csv_extractor.py

import pandas as pd
import glob
import os
from .base_extractor import BaseExtractor

class CSVExtractor(BaseExtractor):
    """Экстрактор для CSV файлов"""
    
    def connect(self):
        directory = self.config.get('directory', '/data/csv')
        if not os.path.exists(directory):
            raise FileNotFoundError(f"Directory not found: {directory}")
    
    def disconnect(self):
        pass
    
    def extract(self, start_date, end_date):
        directory = self.config.get('directory', '/data/csv')
        pattern = self.config.get('file_pattern', 'products_*.csv')
        
        files = glob.glob(os.path.join(directory, pattern))
        all_data = []
        
        for file_path in files:
            df = pd.read_csv(file_path, encoding='utf-8')
            all_data.extend(df.to_dict('records'))
            logger.info(f"Extracted {len(df)} records from {os.path.basename(file_path)}")
        
        return all_data
```

### FTP Extractor

```python
# extractors/ftp_extractor.py

from ftplib import FTP
import pandas as pd
import io
from .base_extractor import BaseExtractor

class FTPExtractor(BaseExtractor):
    """Экстрактор для FTP"""
    
    def connect(self):
        self.ftp = FTP()
        self.ftp.connect(self.config['host'], self.config.get('port', 21))
        self.ftp.login(self.config['username'], self.config['password'])
        if 'path' in self.config:
            self.ftp.cwd(self.config['path'])
        logger.info(f"Connected to FTP: {self.config['host']}")
    
    def disconnect(self):
        if hasattr(self, 'ftp'):
            self.ftp.quit()
    
    def extract(self, start_date, end_date):
        files = self.ftp.nlst()
        all_data = []
        
        for filename in files:
            if filename.endswith('.csv'):
                data = io.BytesIO()
                self.ftp.retrbinary(f'RETR {filename}', data.write)
                content = data.getvalue().decode('utf-8')
                df = pd.read_csv(io.StringIO(content))
                all_data.extend(df.to_dict('records'))
                logger.info(f"Extracted {len(df)} records from {filename}")
        
        return all_data
```

### REST API Extractor

```python
# extractors/api_extractor.py

import requests
import time
from .base_extractor import BaseExtractor

class APIExtractor(BaseExtractor):
    """Экстрактор для REST API"""
    
    def connect(self):
        base_url = self.config['base_url']
        response = requests.get(f"{base_url}/health", timeout=30)
        response.raise_for_status()
        logger.info(f"API accessible: {base_url}")
    
    def disconnect(self):
        pass
    
    def extract(self, start_date, end_date):
        url = f"{self.config['base_url']}{self.config.get('endpoint', '/api/v1/stats')}"
        headers = {'Content-Type': 'application/json'}
        
        if token := self.config.get('auth_token'):
            headers['Authorization'] = f'Bearer {token}'
        
        params = {
            'start_date': start_date.strftime('%Y-%m-%d'),
            'end_date': end_date.strftime('%Y-%m-%d')
        }
        
        response = self._make_request_with_retry(url, headers, params)
        data = response.json()
        result = [data] if isinstance(data, dict) else data
        logger.info(f"Extracted {len(result)} records from API")
        return result
    
    def _make_request_with_retry(self, url, headers, params, max_retries=3):
        for attempt in range(max_retries):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                if response.status_code == 429:
                    time.sleep(int(response.headers.get('Retry-After', 60)))
                    continue
                response.raise_for_status()
                return response
            except requests.exceptions.RequestException as e:
                if attempt == max_retries - 1:
                    raise
                time.sleep(2 ** attempt)
```

---

## 12. Функции очистки и валидации

```python
# transformers/validators.py

import pandas as pd
import numpy as np
import re
import logging

logger = logging.getLogger(__name__)

class DataValidator:
    """Класс для валидации данных"""
    
    @staticmethod
    def validate_data_types(df, schema):
        """Валидация и приведение типов"""
        errors = []
        for column, expected_type in schema.items():
            if column not in df.columns:
                errors.append(f"Missing column: {column}")
                continue
            try:
                if expected_type == 'int64':
                    df[column] = pd.to_numeric(df[column], errors='coerce').astype('Int64')
                elif expected_type == 'float64':
                    df[column] = pd.to_numeric(df[column], errors='coerce')
                elif expected_type == 'datetime64':
                    df[column] = pd.to_datetime(df[column], errors='coerce')
            except Exception as e:
                errors.append(f"Error converting {column}: {e}")
        return df, errors
    
    @staticmethod
    def validate_not_null(df, required_columns, drop_invalid=True):
        """Проверка обязательных полей"""
        initial_count = len(df)
        if drop_invalid:
            df = df.dropna(subset=required_columns)
        removed = initial_count - len(df)
        logger.info(f"Removed {removed} rows with NULL values")
        return df, removed
    
    @staticmethod
    def validate_ranges(df, range_rules):
        """Валидация диапазонов"""
        violations = []
        for column, rules in range_rules.items():
            if column not in df.columns:
                continue
            if min_val := rules.get('min'):
                invalid = df[df[column] < min_val]
                if len(invalid) > 0:
                    violations.append(f"{column}: {len(invalid)} below {min_val}")
                    df = df[df[column] >= min_val]
            if max_val := rules.get('max'):
                invalid = df[df[column] > max_val]
                if len(invalid) > 0:
                    violations.append(f"{column}: {len(invalid)} above {max_val}")
                    df = df[df[column] <= max_val]
        return df, violations
    
    @staticmethod
    def validate_business_rules(df):
        """Проверка бизнес-правил"""
        violations = []
        
        # Delivered заказы должны иметь дату доставки
        if 'status' in df.columns and 'delivery_date' in df.columns:
            invalid = df[(df['status'] == 'delivered') & (df['delivery_date'].isna())]
            if not invalid.empty:
                violations.append(f"{len(invalid)} delivered without delivery_date")
                df.loc[invalid.index, 'status'] = 'shipped'
        
        return df, violations


class DataCleaner:
    """Класс для очистки данных"""
    
    @staticmethod
    def remove_duplicates(df, subset=None, keep='last'):
        """Удаление дубликатов"""
        initial = len(df)
        df = df.drop_duplicates(subset=subset, keep=keep)
        removed = initial - len(df)
        logger.info(f"Removed {removed} duplicates")
        return df, removed
    
    @staticmethod
    def clean_text_columns(df, columns):
        """Очистка текстовых полей"""
        for col in columns:
            if col in df.columns:
                df[col] = df[col].str.strip()
                df[col] = df[col].str.replace(r'\s+', ' ', regex=True)
        return df
    
    @staticmethod
    def standardize_values(df, column, mapping):
        """Стандартизация значений"""
        if column in df.columns:
            df[column] = df[column].replace(mapping)
        return df
    
    @staticmethod
    def fill_missing_values(df, fill_strategy):
        """Заполнение пропусков"""
        for column, strategy in fill_strategy.items():
            if column not in df.columns:
                continue
            if strategy == 'mean':
                df[column].fillna(df[column].mean(), inplace=True)
            elif strategy == 'median':
                df[column].fillna(df[column].median(), inplace=True)
            elif strategy == 'mode':
                mode = df[column].mode()
                if not mode.empty:
                    df[column].fillna(mode[0], inplace=True)
            else:
                df[column].fillna(strategy, inplace=True)
        return df

# Пример использования
def validate_and_clean_orders(df):
    """Полный цикл валидации и очистки"""
    logger.info(f"Initial rows: {len(df)}")
    
    # Удаление дубликатов
    df, _ = DataCleaner.remove_duplicates(df, subset=['order_id'])
    
    # Валидация типов
    schema = {
        'order_id': 'int64',
        'total_amount': 'float64',
        'order_date': 'datetime64'
    }
    df, _ = DataValidator.validate_data_types(df, schema)
    
    # Проверка обязательных полей
    df, _ = DataValidator.validate_not_null(df, ['order_id', 'customer_id'])
    
    # Валидация диапазонов
    range_rules = {'total_amount': {'min': 0, 'max': 1000000}}
    df, _ = DataValidator.validate_ranges(df, range_rules)
    
    # Очистка текста
    df = DataCleaner.clean_text_columns(df, ['status', 'payment_method'])
    
    # Стандартизация статусов
    mapping = {'в обработке': 'processing', 'доставлен': 'delivered'}
    df = DataCleaner.standardize_values(df, 'status', mapping)
    
    # Бизнес-правила
    df, _ = DataValidator.validate_business_rules(df)
    
    logger.info(f"Final rows: {len(df)}")
    return df
```

---

## 13. Функции загрузки данных

```python
# loaders/warehouse_loader.py

import logging
from psycopg2.extras import execute_values

logger = logging.getLogger(__name__)

class WarehouseLoader:
    """Класс для загрузки данных в хранилище"""
    
    def __init__(self, connection):
        self.conn = connection
    
    def upsert_analytics(self, df_analytics):
        """Вставка/обновление дневной аналитики"""
        for _, row in df_analytics.iterrows():
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT id FROM daily_business_analytics 
                WHERE analytics_date = %s
            """, (row['analytics_date'],))
            
            if cursor.fetchone():
                # UPDATE
                cursor.execute("""
                    UPDATE daily_business_analytics
                    SET total_orders = %s,
                        total_revenue = %s,
                        avg_order_value = %s,
                        updated_at = NOW()
                    WHERE analytics_date = %s
                """, (row['total_orders'], row['total_revenue'], 
                      row['avg_order_value'], row['analytics_date']))
            else:
                # INSERT
                cursor.execute("""
                    INSERT INTO daily_business_analytics
                    (analytics_date, total_orders, total_revenue, avg_order_value)
                    VALUES (%s, %s, %s, %s)
                """, (row['analytics_date'], row['total_orders'], 
                      row['total_revenue'], row['avg_order_value']))
            
            self.conn.commit()
        logger.info("Analytics data loaded successfully")
    
    def load_dim_customers(self, df_customers):
        """
        Загрузка измерения клиентов с использованием SCD Type 2
        
        Алгоритм SCD Type 2:
        1. Для каждого клиента ищем текущую активную версию (is_current=TRUE)
        2. Если клиента нет - создаем новую запись (INSERT)
        3. Если клиент существует - сравниваем отслеживаемые атрибуты:
           a. Без изменений - ничего не делаем
           b. Есть изменения:
              - Закрываем текущую версию (UPDATE: is_current=FALSE, expiration_date=вчера)
              - Создаем новую версию (INSERT: новый customer_key, is_current=TRUE)
        
        Результат: полная история изменений атрибутов клиента
        """
        cursor = self.conn.cursor()
        
        for _, customer in df_customers.iterrows():
            # Шаг 1: Проверяем наличие текущей активной версии
            cursor.execute("""
                SELECT customer_key, email, city, customer_segment
                FROM dim_customers
                WHERE customer_id = %s AND is_current = TRUE
            """, (customer['customer_id'],))
            
            current = cursor.fetchone()
            
            if current is None:
                # Шаг 2: Новый клиент - создаем первую версию
                cursor.execute("""
                    INSERT INTO dim_customers
                    (customer_id, first_name, last_name, email, city, customer_segment,
                     effective_date, expiration_date, is_current)
                    VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE, '9999-12-31', TRUE)
                """, (customer['customer_id'], customer['first_name'],
                      customer['last_name'], customer['email'], customer['city'],
                      customer.get('customer_segment', 'Regular')))
                logger.info(f"Создан новый клиент: {customer['customer_id']}")
            else:
                # Шаг 3: Проверяем изменения отслеживаемых атрибутов
                current_key, current_email, current_city, current_segment = current
                
                has_changes = (
                    current_email != customer['email'] or 
                    current_city != customer['city'] or
                    current_segment != customer.get('customer_segment')
                )
                
                if has_changes:
                    # Есть изменения - применяем SCD Type 2
                    
                    # Закрываем текущую версию (истекает вчера)
                    cursor.execute("""
                        UPDATE dim_customers
                        SET expiration_date = CURRENT_DATE - INTERVAL '1 day',
                            is_current = FALSE
                        WHERE customer_key = %s
                    """, (current_key,))
                    
                    # Создаем новую версию (начинается сегодня)
                    cursor.execute("""
                        INSERT INTO dim_customers
                        (customer_id, first_name, last_name, email, city, customer_segment,
                         effective_date, expiration_date, is_current)
                        VALUES (%s, %s, %s, %s, %s, %s, CURRENT_DATE, '9999-12-31', TRUE)
                    """, (customer['customer_id'], customer['first_name'],
                          customer['last_name'], customer['email'], customer['city'],
                          customer.get('customer_segment', 'Regular')))
                    
                    logger.info(f"SCD Type 2: Новая версия клиента {customer['customer_id']}")
                    logger.debug(f"   Старый key={current_key} закрыт, создан новый key")
                else:
                    logger.debug(f"   Клиент {customer['customer_id']} без изменений")
        
        self.conn.commit()
        logger.info(f"Обработано {len(df_customers)} клиентов с SCD Type 2")
    
    def get_customer_version_by_date(self, customer_id, as_of_date):
        """
        ДЕМОНСТРАЦИЯ: Получение версии клиента на определенную дату
        
        Это показывает главное преимущество SCD Type 2 - возможность
        "вернуться в прошлое" и увидеть, какими были данные на любую дату
        
        Args:
            customer_id: ID клиента
            as_of_date: Дата, на которую нужны данные
            
        Returns:
            Версия клиента, действующая на указанную дату
        
        Пример:
            # Клиент переехал 15 июня 2024
            customer_may = loader.get_customer_version_by_date(123, '2024-05-01')
            # -> city='Москва'
            
            customer_july = loader.get_customer_version_by_date(123, '2024-07-01')
            # -> city='Санкт-Петербург'
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT 
                customer_key,
                customer_id,
                first_name,
                last_name,
                email,
                city,
                customer_segment,
                effective_date,
                expiration_date
            FROM dim_customers
            WHERE customer_id = %s 
              AND effective_date <= %s 
              AND expiration_date >= %s
        """, (customer_id, as_of_date, as_of_date))
        
        result = cursor.fetchone()
        
        if result:
            logger.info(f"Найдена версия клиента {customer_id} на дату {as_of_date}")
            logger.debug(f"  Key={result[0]}, City={result[5]}, "
                        f"Valid from {result[7]} to {result[8]}")
        else:
            logger.warning(f"Версия клиента {customer_id} на {as_of_date} не найдена")
        
        return result


    def load_fact_orders(self, df_facts):
        """Загрузка фактов заказов"""
        # Обогащение surrogate keys
        df_facts = self._enrich_with_keys(df_facts)
        
        # Batch insert
        values = [
            (row['order_id'], row['customer_key'], row['product_key'],
             row['date_key'], row['total_amount'], row['order_status'])
            for _, row in df_facts.iterrows()
        ]
        
        cursor = self.conn.cursor()
        execute_values(cursor, """
            INSERT INTO fact_orders
            (order_id, customer_key, product_key, date_key, 
             total_amount, order_status)
            VALUES %s
        """, values)
        
        self.conn.commit()
        logger.info(f"Loaded {len(df_facts)} facts")
    
    def _enrich_with_keys(self, df_facts):
        """Добавление surrogate keys"""
        import pandas as pd
        
        # Получение customer_key
        customer_keys = pd.read_sql("""
            SELECT customer_id, customer_key
            FROM dim_customers WHERE is_current = TRUE
        """, self.conn)
        df_facts = df_facts.merge(customer_keys, on='customer_id', how='left')
        
        # Получение product_key
        product_keys = pd.read_sql("""
            SELECT product_id, product_key
            FROM dim_products WHERE is_current = TRUE
        """, self.conn)
        df_facts = df_facts.merge(product_keys, on='product_id', how='left')
        
        # Добавление date_key
        df_facts['date_key'] = df_facts['order_date'].dt.strftime('%Y%m%d').astype(int)
        
        return df_facts
    
    def update_aggregates(self, analytics_date):
        """Обновление агрегированных таблиц"""
        date_key = int(analytics_date.strftime('%Y%m%d'))
        cursor = self.conn.cursor()
        
        # Удаление старых данных
        cursor.execute("""
            DELETE FROM agg_daily_sales WHERE date_key = %s
        """, (date_key,))
        
        # Вставка новых агрегатов
        cursor.execute("""
            INSERT INTO agg_daily_sales
            (date_key, product_key, customer_segment,
             total_orders, total_revenue, avg_order_value)
            SELECT 
                f.date_key,
                f.product_key,
                c.customer_segment,
                COUNT(DISTINCT f.order_id),
                SUM(f.total_amount),
                AVG(f.total_amount)
            FROM fact_orders f
            JOIN dim_customers c ON f.customer_key = c.customer_key
            WHERE f.date_key = %s
            GROUP BY f.date_key, f.product_key, c.customer_segment
        """, (date_key,))
        
        self.conn.commit()
        logger.info(f"Aggregates updated for {analytics_date}")
    
    def validate_load(self, analytics_date):
        """Валидация загруженных данных"""
        cursor = self.conn.cursor()
        validations = []
        
        # Проверка наличия аналитики
        cursor.execute("""
            SELECT total_orders FROM daily_business_analytics
            WHERE analytics_date = %s
        """, (analytics_date,))
        validations.append({'check': 'analytics_exists', 'passed': cursor.fetchone() is not None})
        
        # Проверка фактов
        date_key = int(analytics_date.strftime('%Y%m%d'))
        cursor.execute("SELECT COUNT(*) FROM fact_orders WHERE date_key = %s", (date_key,))
        fact_count = cursor.fetchone()[0]
        validations.append({'check': 'facts_loaded', 'passed': fact_count > 0})
        
        # Проверка NULL
        cursor.execute("""
            SELECT COUNT(*) FROM fact_orders
            WHERE date_key = %s AND (customer_key IS NULL OR product_key IS NULL)
        """, (date_key,))
        null_count = cursor.fetchone()[0]
        validations.append({'check': 'no_nulls', 'passed': null_count == 0})
        
        for v in validations:
            if not v['passed']:
                raise Exception(f"Validation failed: {v['check']}")
        
        logger.info("All validations passed")
        return True
```

---

## 14. Конвейеры генерации тестовых данных

```python
# dags/generate_test_data_dag.py

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random
import pandas as pd
import psycopg2
from pymongo import MongoClient

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 12, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def generate_customers():
    """Генерация тестовых клиентов"""
    conn = psycopg2.connect(
        host='postgres-source',
        database='production_db',
        user='postgres',
        password='postgres'
    )
    cursor = conn.cursor()
    
    first_names = ['Иван', 'Петр', 'Мария', 'Анна', 'Алексей']
    last_names = ['Иванов', 'Петров', 'Сидоров', 'Смирнов', 'Кузнецов']
    cities = ['Москва', 'Санкт-Петербург', 'Новосибирск', 'Екатеринбург']
    
    for i in range(100):
        cursor.execute("""
            INSERT INTO customers (first_name, last_name, email, city, country)
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (email) DO NOTHING
        """, (
            random.choice(first_names),
            random.choice(last_names),
            f'customer{i}@example.com',
            random.choice(cities),
            'Россия'
        ))
    
    conn.commit()
    conn.close()
    print("Generated 100 test customers")

def generate_orders(**context):
    """Генерация тестовых заказов"""
    execution_date = context['execution_date']
    
    conn = psycopg2.connect(
        host='postgres-source',
        database='production_db',
        user='postgres',
        password='postgres'
    )
    cursor = conn.cursor()
    
    statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
    payment_methods = ['card', 'cash', 'online']
    
    # Генерация 50-100 заказов за день
    num_orders = random.randint(50, 100)
    
    for i in range(num_orders):
        customer_id = random.randint(1, 100)
        order_date = execution_date + timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )
        total_amount = round(random.uniform(10, 5000), 2)
        
        cursor.execute("""
            INSERT INTO orders 
            (customer_id, order_date, total_amount, status, payment_method)
            VALUES (%s, %s, %s, %s, %s)
        """, (customer_id, order_date, total_amount, 
              random.choice(statuses), random.choice(payment_methods)))
    
    conn.commit()
    conn.close()
    print(f"Generated {num_orders} test orders for {execution_date.date()}")

def generate_feedback(**context):
    """Генерация тестовых отзывов"""
    execution_date = context['execution_date']
    
    client = MongoClient('mongodb://mongo:mongo@mongodb:27017/')
    db = client['feedback_db']
    collection = db['customer_feedback']
    
    comments = [
        'Отличный сервис!',
        'Быстрая доставка',
        'Товар соответствует описанию',
        'Есть замечания к упаковке',
        'Долго ждал доставку'
    ]
    
    # Генерация 20-50 отзывов
    num_feedback = random.randint(20, 50)
    
    for i in range(num_feedback):
        feedback = {
            'customer_id': random.randint(1, 100),
            'order_id': random.randint(1, 1000),
            'rating': round(random.uniform(2.0, 5.0), 1),
            'comment': random.choice(comments),
            'feedback_date': execution_date + timedelta(
                hours=random.randint(0, 23),
                minutes=random.randint(0, 59)
            ),
            'category': random.choice(['delivery', 'product', 'service'])
        }
        collection.insert_one(feedback)
    
    client.close()
    print(f"Generated {num_feedback} test feedback for {execution_date.date()}")

def generate_csv_products():
    """Генерация CSV с продуктами"""
    today = datetime.now().strftime('%Y%m%d')
    
    products = pd.DataFrame({
        'product_id': range(1, 51),
        'product_name': [f'Product {i}' for i in range(1, 51)],
        'category': [random.choice(['Electronics', 'Clothing', 'Books', 'Home']) 
                     for _ in range(50)],
        'price': [round(random.uniform(10, 1000), 2) for _ in range(50)],
        'stock_quantity': [random.randint(0, 500) for _ in range(50)]
    })
    
    filepath = f'/opt/airflow/data/csv/products_{today}.csv'
    products.to_csv(filepath, index=False)
    print(f"Generated CSV: {filepath}")

def generate_ftp_delivery_logs(**context):
    """Генерация логов доставки для FTP"""
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y%m%d')
    
    num_deliveries = random.randint(30, 80)
    
    logs = pd.DataFrame({
        'delivery_id': range(1, num_deliveries + 1),
        'order_id': [random.randint(1, 1000) for _ in range(num_deliveries)],
        'courier_id': [random.randint(1, 20) for _ in range(num_deliveries)],
        'pickup_time': [
            execution_date + timedelta(hours=random.randint(8, 12))
            for _ in range(num_deliveries)
        ],
        'delivery_time': [
            execution_date + timedelta(hours=random.randint(13, 20))
            for _ in range(num_deliveries)
        ],
        'status': [random.choice(['delivered', 'failed', 'returned']) 
                   for _ in range(num_deliveries)]
    })
    
    filepath = f'/opt/airflow/data/ftp/delivery_logs_{date_str}.csv'
    logs.to_csv(filepath, index=False)
    print(f"Generated FTP logs: {filepath}")

# Определение DAG
dag = DAG(
    'generate_test_data',
    default_args=default_args,
    description='Generate test data for all sources',
    schedule_interval='@daily',
    catchup=False
)

# Задачи
task_customers = PythonOperator(
    task_id='generate_customers',
    python_callable=generate_customers,
    dag=dag
)

task_orders = PythonOperator(
    task_id='generate_orders',
    python_callable=generate_orders,
    dag=dag
)

task_feedback = PythonOperator(
    task_id='generate_feedback',
    python_callable=generate_feedback,
    dag=dag
)

task_csv = PythonOperator(
    task_id='generate_csv_products',
    python_callable=generate_csv_products,
    dag=dag
)

task_ftp = PythonOperator(
    task_id='generate_ftp_logs',
    python_callable=generate_ftp_delivery_logs,
    dag=dag
)

# Зависимости
task_customers >> task_orders >> [task_feedback, task_ftp]
task_csv
```

---

## Критерии оценки дипломной работы

### 1. Полнота реализации (40 баллов)

- Реализация всех 3+ источников данных (10 баллов)
- Корректная работа Extract-Transform-Load (15 баллов)
- Настроенный Data Warehouse (10 баллов)
- Работающий дашборд (5 баллов)

### 2. Качество кода (30 баллов)

- Архитектура и структура проекта (10 баллов)
- Обработка ошибок и логирование (10 баллов)
- Документация и комментарии (10 баллов)

### 3. Настройка Airflow (20 баллов)

- Корректная структура DAG (10 баллов)
- Зависимости и расписание задач (5 баллов)
- Мониторинг и алерты (5 баллов)

### 4. Документация (10 баллов)

- README с инструкциями по запуску (5 баллов)
- Описание архитектуры и решений (5 баллов)

---

## Рекомендации по выполнению

### Этап 1: Подготовка (1 неделя)

1. Выбор предметной области
2. Проектирование архитектуры
3. Настройка Docker окружения
4. Инициализация баз данных

### Этап 2: Разработка Extract (1-2 недели)

1. Создание базовых классов Extractors
2. Реализация экстракторов для каждого источника
3. Тестирование извлечения данных
4. Генерация тестовых данных

### Этап 3: Разработка Transform (1 неделя)

1. Реализация валидаторов
2. Реализация очистки данных
3. Создание трансформеров
4. Тестирование обработки

### Этап 4: Разработка Load (1 неделя)

1. Создание структуры DWH
2. Реализация загрузчиков
3. Тестирование загрузки
4. Валидация данных

### Этап 5: Airflow DAG (1 неделя)

1. Создание основного DAG
2. Настройка зависимостей задач
3. Тестирование пайплайна
4. Настройка расписания

### Этап 6: Визуализация (1 неделя)

1. Настройка Grafana/Metabase
2. Создание дашборда
3. Настройка обновления данных
4. Финальное тестирование

### Этап 7: Документация (3-5 дней)

1. Написание README
2. Документирование API
3. Создание презентации
4. Подготовка к защите

---

## Дополнительные материалы

### Полезные ссылки

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Pandas Documentation](https://pandas.pydata.org/docs/)
- [PostgreSQL Tutorial](https://www.postgresql.org/docs/)
- [MongoDB Manual](https://docs.mongodb.com/)
- [Grafana Documentation](https://grafana.com/docs/)

### Рекомендуемая литература

- "Data Pipelines with Apache Airflow" - Bas Harenslak, Julian de Ruiter
- "The Data Warehouse Toolkit" - Ralph Kimball
- "Designing Data-Intensive Applications" - Martin Kleppmann

---

**Удачи в выполнении дипломной работы! 🚀**
