# Задание на дипломную работу

## Тема: "Дашборд аналитика бизнес-процессов"

### Цель проекта

Разработать полноценный ETL-pipeline на базе Apache Airflow для автоматизированного сбора, обработки и визуализации данных бизнес-процессов. Система должна ежедневно (в 9:00) собирать данные из различных источников, трансформировать их и загружать в аналитическую БД и хранилище данных (Data Warehouse) для последующей визуализации на дашборде.

## Ключевые требования

### 1. Data Warehouse с SCD Type 2

Хранилище данных (Data Warehouse) должно использовать стратегию **Slowly Changing Dimensions (SCD) Type 2** для отслеживания исторических изменений в таблицах измерений (dimensions). Это позволит сохранять полную историю изменений атрибутов и анализировать данные в контексте их исторического состояния.

**Что такое SCD Type 2?**

**SCD Type 2** - это метод отслеживания изменений в измерениях, при котором:

- При изменении атрибута создается НОВАЯ версия записи
- Старая версия сохраняется и помечается как неактуальная
- Каждая версия имеет период действия (effective_date, expiration_date)
- Текущая версия имеет флаг is_current = TRUE

**Пример:**

```text
Клиент переехал из Москвы в Санкт-Петербург 15.06.2025

До изменения:
customer_key | customer_id | city   | effective_date | expiration_date | is_current
1001         | 123         | Москва | 2025-01-01     | 9999-12-31      | TRUE

После изменения:
customer_key | customer_id | city             | effective_date | expiration_date | is_current
1001         | 123         | Москва           | 2025-01-01     | 2025-06-14      | FALSE  ← закрыта
1125         | 123         | Санкт-Петербург  | 2025-06-15     | 9999-12-31      | TRUE   ← новая
```

**Преимущества:**

- Полная история изменений
- Анализ "как было" на любую дату
- Точность исторических отчетов
- Аудит изменений

### 2. Безопасность подключений

**Безопасность подключений**: Все подключения к источникам данных должны осуществляться через **Airflow Connections**, а аутентификационные данные (логины, пароли, токены) должны передаваться через **переменные окружения (.env файл)**. Жесткое кодирование учетных данных в коде запрещено.

```python
# НЕПРАВИЛЬНО - НИКОГДА ТАК НЕ ДЕЛАЙТЕ!
conn = psycopg2.connect(
    host='postgres',
    user='user',
    password='password123'  # ЖЕСТКИЙ КОД ПАРОЛЯ!
)

# ПРАВИЛЬНО
from airflow.providers.postgres.hooks.postgres import PostgresHook

hook = PostgresHook(postgres_conn_id='postgres_source')
conn = hook.get_conn()
```

**Требования:**

1. Все пароли в файле `.env`
2. `.env` добавлен в `.gitignore`
3. Все подключения через Airflow Connections
4. Использование Hooks (PostgresHook, MongoHook и т.д.)

### 3. Минимум 3 источника данных

Обязательные источники:

1. **PostgreSQL** - транзакционная БД (заказы, клиенты)
2. **MongoDB** - документо-ориентированная БД (отзывы)
3. **CSV или FTP** - файловые источники (продукты, логи доставки)
4. **REST API** - опционально (веб-аналитика)

---

## Описание предметной области

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

## Архитектура проекта

```text
┌─────────────────────────────────────────────────────────────────┐
│                    ИСТОЧНИКИ ДАННЫХ                              │
├──────────────┬──────────────┬──────────────┬────────────────────┤
│  PostgreSQL  │   MongoDB    │   CSV/FTP    │     REST API       │
│   (orders,   │  (feedback)  │  (products,  │ (web analytics)    │
│  customers)  │              │  deliveries) │  [опционально]     │
└──────┬───────┴──────┬───────┴──────┬───────┴────────┬───────────┘
       │              │              │                │
       └──────────────┴──────────────┴────────────────┘
                              ↓
       ┌────────────────────────────────────────────────────────┐
       │           AIRFLOW DAG (запуск в 9:00 AM)               │
       ├────────────────────────────────────────────────────────┤
       │  EXTRACT  →  TRANSFORM  →  LOAD                        │
       │  (извлечь)  (преобразовать)  (загрузить)               │
       └──────────────────────┬─────────────────────────────────┘
                              ↓
       ┌────────────────────────────────────────────────────────┐
       │               ЦЕЛЕВЫЕ ХРАНИЛИЩА                        │
       ├──────────────────────┬─────────────────────────────────┤
       │  Analytics DB        │    Data Warehouse               │
       │  (агрегаты)          │    (детализация + SCD Type 2)   │
       └──────────┬───────────┴─────────────┬───────────────────┘
                  │                         │
                  └───────────┬─────────────┘
                              ↓
                    ┌──────────────────────┐
                    │    ВИЗУАЛИЗАЦИЯ      │
                    │  Grafana / Superset  │
                    └──────────────────────┘
```

---

## Структура проекта

```text
airflow_etl_diploma_project/
│
├── dags/                             # DAG-файлы Apache Airflow
│   ├── main_etl_dag.py               # Основной ETL DAG (9:00 AM)
│   └── generate_test_data_dag.py     # Генерация тестовых данных
│
├── plugins/                          # Плагины и компоненты ETL
│   │
│   ├── extractors/                   # Извлечение данных
│   │   ├── base_extractor.py         # Базовый класс
│   │   ├── postgres_extractor.py     # PostgreSQL
│   │   ├── mongo_extractor.py        # MongoDB
│   │   ├── csv_extractor.py          # CSV файлы
│   │   ├── ftp_extractor.py          # FTP сервер
│   │   └── api_extractor.py          # REST API
│   │
│   ├── transformers/                 # Трансформация данных
│   │   ├── base_transformer.py       # Базовый класс
│   │   ├── data_cleaner.py           # Очистка (дубликаты, null)
│   │   ├── data_validator.py         # Валидация (схема, диапазоны)
│   │   ├── data_normalizer.py        # Нормализация (форматы)
│   │   └── data_enricher.py          # Обогащение (доп. данные)
│   │
│   ├── loaders/                      # Загрузка данных
│   │   ├── base_loader.py            # Базовый класс
│   │   ├── analytics_loader.py       # В аналитическую БД
│   │   ├── dwh_loader.py             # В DWH
│   │   └── scd_type2_handler.py      # Обработчик SCD Type 2
│   │
│   ├── validators/                   # Валидаторы
│   │   ├── schema_validator.py       # Проверка схемы
│   │   └── quality_checker.py        # Качество данных
│   │
│   └── utils/                        # Утилиты
│       ├── db_helpers.py
│       ├── logger_config.py
│       └── constants.py
│
├── init/                             # Скрипты инициализации БД
│   ├── init_source_db.sql            # Инициализация источников
│   ├── init_analytics_db.sql         # Аналитическая БД
│   ├── init_dwh.sql                  # Data Warehouse (с SCD Type 2)
│   └── create_views.sql              # Представления для отчетов
│
├── scripts/                          # Вспомогательные скрипты
│   ├── setup_connections.py          # Настройка Airflow Connections
│   ├── generate_sample_data.py       # Генерация тестовых данных
│   └── check_data_quality.py         # Проверка качества
│
├── data/                             # Директории для данных
│   ├── csv/                          # CSV файлы
│   ├── ftp/                          # FTP файлы
│   └── api/                          # Кеш API
│
├── config/                           # Конфигурация
│   └── logging.conf
│
├── docker-compose.yml                # Docker Compose
├── Dockerfile                        # Dockerfile для Airflow
├── requirements.txt                  # Python зависимости для Dockerfile
├── .env.example                      # Пример переменных окружения
├── .gitignore                        # Git ignore
└── README.md                         # Документация
```

---

## Технологический стек

- Python 3.10+
- Apache Airflow 2.10+
- PostgreSQL / MongoDB
- Docker & Docker Compose
- pandas, psycopg2, pymongo

Опишите потоки данных, расписание и зависимости задач.

---

## Практическая часть

### Шаг 1: Настройка переменных окружения

1. Скопируйте файл с примером:

    ```bash
    cp .env.example .env
    ```

2. Отредактируйте `.env` и заполните **ВСЕ** переменные:

    ```env
    # PostgreSQL Source
    POSTGRES_SOURCE_HOST=postgres-source
    POSTGRES_SOURCE_PORT=5433
    POSTGRES_SOURCE_DB=source_db
    POSTGRES_SOURCE_USER=source_user
    POSTGRES_SOURCE_PASSWORD=source_password

    # PostgreSQL Analytics
    POSTGRES_ANALYTICS_HOST=postgres-analytics
    POSTGRES_ANALYTICS_PORT=5434
    POSTGRES_ANALYTICS_DB=analytics_db
    POSTGRES_ANALYTICS_USER=analytics_user
    POSTGRES_ANALYTICS_PASSWORD=analytics_password

    # MongoDB
    MONGO_HOST=mongodb
    MONGO_PORT=27017
    MONGO_DB=source_mongo_db
    MONGO_USER=mongo_user
    MONGO_PASSWORD=mongo_password

    # ... и т.д.
    ```

**ВАЖНО:**

- НЕ коммитить `.env` в Git
- Убедитесь, что `.env` в `.gitignore`

### Шаг 2: Запуск Docker контейнеров

```bash
# Запуск всех сервисов
docker compose up -d

# Проверка статуса
docker compose ps

# Просмотр логов
docker compose logs -f airflow
```

**Доступные сервисы:**

- Airflow web-интерфейс: http://localhost:8080 (admin / admin)
- PostgreSQL Source: localhost:5433
- PostgreSQL Analytics: localhost:5434
- PgAdmin4: http://localhost:5000
- MongoDB: localhost:27017
- Grafana: http://localhost:3000 (admin / admin)

### Шаг 3: Инициализация баз данных

Базы данных инициализируются автоматически при первом запуске через скрипты в `init/`:

- `init_source_db.sql` - создает таблицы orders, customers
- `init_analytics_db.sql` - создает daily_business_analytics
- `init_dwh.sql` - создает DWH с SCD Type 2

### Шаг 4: Настройка Airflow Connections

**Автоматически:**

```bash
docker compose exec -i airflow python /opt/airflow/scripts/setup_connections.py
```

**Вручную через UI:**

1. Откройте http://localhost:8080
2. Admin → Connections
3. Добавьте подключения:

    **postgres_source:**
    - Conn Id: `postgres_source`
    - Conn Type: `Postgres`
    - Host: `postgres-source`
    - Schema: `source_db`
    - Login: `source_user`
    - Password: `<из .env>`
    - Port: `5432`

    **mongodb_conn:**
    - Conn Id: `mongodb_conn`
    - Conn Type: `MongoDB`
    - Host: `mongodb`
    - Schema: `feedback_db`
    - Login: `mongo_user`
    - Password: `<из .env>`
    - Port: `27017`

    **другие (при необходимости)**

### Шаг 5: Запуск DAG

1. Откройте Airflow UI: http://localhost:8080
2. Найдите DAG `main_etl_pipeline`
3. Включите DAG (toggle в позицию ON)
4. Нажмите `"Trigger DAG"` для ручного запуска

DAG будет автоматически запускаться каждый день в 9:00 AM.

---

## ETL Pipeline - Детальное описание

### Фаза 1: EXTRACT (Извлечение)

#### 1.1 PostgreSQL

**Таблицы:**

```sql
-- orders: заказы с датой, суммой, статусом
-- customers: клиенты с контактами и адресами
-- order_items: позиции в заказах с товарами
```

**Стратегия извлечения:**

```python
# Инкрементальная загрузка по дате
extractor = PostgresExtractor(conn_id='postgres_source')

orders_df = extractor.extract_incremental(
    table_name='orders',
    date_column='order_date',
    start_date='{{ ds }}',  # Airflow macro: execution date
    end_date='{{ tomorrow_ds }}'  # Следующий день
)
```

**Почему инкрементальная загрузка?**

- Загружаем только новые данные
- Экономим ресурсы
- Быстрее выполняется

#### 1.2 MongoDB

**Коллекция: customer_feedback**

```json
{
  "_id": "ObjectId(...)",
  "customer_id": 123,
  "order_id": 456,
  "rating": 4.5,
  "comment": "Отличный сервис!",
  "feedback_date": "2025-01-13T14:30:00Z",
  "category": "delivery"
}
```

**Стратегия извлечения:**

```python
extractor = MongoExtractor(
    conn_id='mongodb_conn',
    database='feedback_db'
)

feedback_df = extractor.extract_by_date(
    collection='customer_feedback',
    date_field='feedback_date',
    start_date=execution_date,
    end_date=execution_date + timedelta(days=1)
)
```

#### 1.3 CSV

**Файл: products_YYYYMMDD.csv**

```csv
product_id,product_name,category,price,stock_quantity
1,Laptop Dell,Electronics,1299.99,50
2,iPhone 15,Electronics,1199.99,120
```

**Стратегия:**

```python
extractor = CSVExtractor(base_path='/opt/airflow/data/csv')

products_df = extractor.extract(
    filename=f'products_{execution_date.strftime("%Y%m%d")}.csv'
)
```

#### 1.4 FTP

**Файл: delivery_logs_YYYYMMDD.csv**

```csv
delivery_id,order_id,courier_id,pickup_time,delivery_time,status
1,1001,25,2025-01-13 09:00:00,2025-01-13 10:30:00,delivered
```

### Фаза 2: TRANSFORM (Трансформация)

#### 2.1 Очистка данных (Data Cleaning)

**Задачи:**

- Удаление дубликатов
- Обработка пропущенных значений
- Удаление некорректных записей
- Удаление выбросов

<details>

**<summary>Пример кода CLEAN</summary>**

```python
class DataCleaner(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Удаление дубликатов
        df = df.drop_duplicates(subset=['order_id'], keep='last')
        
        # Обработка null значений
        df['phone'] = df['phone'].fillna('Unknown')
        df['shipping_address'] = df['shipping_address'].fillna('N/A')
        
        # Удаление некорректных записей
        df = df[df['total_amount'] > 0]
        df = df[df['quantity'] > 0]
        
        # Удаление выбросов (сумма заказа > 1 млн - подозрительно)
        df = df[df['total_amount'] < 1000000]
        
        return df
```

</details>

#### 2.2 Валидация (Data Validation)

**Задачи:**

- Проверка схемы данных
- Проверка типов данных
- Проверка диапазонов значений
- Проверка форматов

<details>

**<summary>Пример кода VALIDATION</summary>**

```python
class DataValidator:
    def validate_orders(self, df: pd.DataFrame) -> tuple:
        """Возвращает (valid_df, invalid_df, errors)"""
        errors = []
        
        # Проверка обязательных полей
        required_fields = ['order_id', 'customer_id', 'total_amount']
        for field in required_fields:
            if field not in df.columns:
                errors.append(f"Missing required field: {field}")
        
        # Проверка типов
        if not pd.api.types.is_numeric_dtype(df['total_amount']):
            errors.append("total_amount must be numeric")
        
        # Проверка диапазонов
        invalid_amount = df[
            (df['total_amount'] < 0) | (df['total_amount'] > 1000000)
        ]
        if len(invalid_amount) > 0:
            errors.append(f"Found {len(invalid_amount)} records with invalid amount")
        
        # Проверка статусов
        valid_statuses = ['pending', 'processing', 'shipped', 'delivered', 'cancelled']
        invalid_status = df[~df['status'].isin(valid_statuses)]
        
        # Разделение на валидные и невалидные
        valid_mask = (
            df['total_amount'].between(0, 1000000) &
            df['status'].isin(valid_statuses)
        )
        
        valid_df = df[valid_mask]
        invalid_df = df[~valid_mask]
        
        return valid_df, invalid_df, errors
```

</details>

#### 2.3 Нормализация (Data Normalization)

**Задачи:**

- Приведение к единому формату
- Стандартизация дат
- Нормализация строк
- Приведение числовых значений

<details>

**<summary>Пример кода NORMALIZAION</summary>**


```python
class DataNormalizer(BaseTransformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        # Нормализация строк
        df['country'] = df['country'].str.upper().str.strip()
        df['city'] = df['city'].str.title().str.strip()
        df['email'] = df['email'].str.lower().str.strip()
        
        # Нормализация телефонов (убрать все кроме цифр)
        df['phone'] = df['phone'].str.replace(r'\D', '', regex=True)
        
        # Стандартизация дат
        df['order_date'] = pd.to_datetime(df['order_date'])
        df['order_date'] = df['order_date'].dt.tz_localize(None)
        
        # Округление цен
        df['total_amount'] = df['total_amount'].round(2)
        
        return df
```

</details>

#### 2.4 Обогащение (Data Enrichment)

**Задачи:**

- Добавление вычисляемых полей
- Обогащение из справочников
- Категоризация
- Агрегации

<details>

**<summary>Пример кода ENRICH</summary>**

```python
class DataEnricher(BaseTransformer):
    def transform(self, df: pd.DataFrame, products_df: pd.DataFrame = None) -> pd.DataFrame:
        # Добавление временных характеристик
        df['order_hour'] = df['order_date'].dt.hour
        df['order_day_of_week'] = df['order_date'].dt.dayofweek
        df['is_weekend'] = df['order_day_of_week'].isin([5, 6])
        df['time_of_day'] = pd.cut(
            df['order_hour'],
            bins=[0, 6, 12, 18, 24],
            labels=['Night', 'Morning', 'Afternoon', 'Evening']
        )
        
        # Категоризация клиентов по сумме заказа
        df['order_category'] = pd.cut(
            df['total_amount'],
            bins=[0, 1000, 5000, 10000, float('inf')],
            labels=['Small', 'Medium', 'Large', 'VIP']
        )
        
        # Обогащение из справочника продуктов
        if products_df is not None:
            df = df.merge(
                products_df[['product_id', 'category', 'price']],
                on='product_id',
                how='left'
            )
        
        # Расчет скидки
        df['discount'] = (df['original_price'] - df['final_price']) / df['original_price']
        df['discount_pct'] = (df['discount'] * 100).round(2)
        
        return df
```

</details>

#### 2.5 Оценка качества (Data Quality Assessment)

**Метрики качества:**

<details>

**<summary>Пример кода DATA_QUALITY</summary>**

```python
def assess_data_quality(df: pd.DataFrame) -> dict:
    """Оценка качества данных."""
    
    total_records = len(df)
    
    quality_metrics = {
        'total_records': total_records,
        
        # Completeness (полнота)
        'completeness': {
            field: (df[field].notna().sum() / total_records * 100)
            for field in df.columns
        },
        
        # Uniqueness (уникальность)
        'uniqueness': {
            'order_id': (df['order_id'].nunique() / total_records * 100),
            'customer_id': (df['customer_id'].nunique() / total_records * 100)
        },
        
        # Validity (валидность)
        'validity': {
            'valid_emails': (df['email'].str.match(r'^[\w\.-]+@[\w\.-]+\.\w+$').sum() / total_records * 100),
            'valid_amounts': ((df['total_amount'] > 0).sum() / total_records * 100)
        },
        
        # Consistency (консистентность)
        'duplicates': df.duplicated(subset=['order_id']).sum(),
        'null_values': df.isnull().sum().to_dict()
    }
    
    return quality_metrics
```

</details>

### Фаза 3: LOAD (Загрузка)

#### 3.1 Загрузка в аналитическую БД

**Таблица: daily_business_analytics**

Эта таблица содержит АГРЕГИРОВАННЫЕ метрики за день.

**Стратегия: UPSERT**

<details>

**<summary>Пример кода LOAD-UPSERT</summary>**

```python
def load_daily_analytics(df: pd.DataFrame, execution_date: date):
    """Загрузка дневных метрик."""
    
    # Агрегация метрик за день
    analytics = {
        'analytics_date': execution_date,
        'total_orders': len(df),
        'total_revenue': df['total_amount'].sum(),
        'avg_order_value': df['total_amount'].mean(),
        
        # По статусам
        'orders_pending': len(df[df['status'] == 'pending']),
        'orders_processing': len(df[df['status'] == 'processing']),
        'orders_delivered': len(df[df['status'] == 'delivered']),
        
        # Клиенты
        'unique_customers': df['customer_id'].nunique(),
        'new_customers': calculate_new_customers(df, execution_date),
        
        # Средний рейтинг из feedback
        'avg_customer_rating': calculate_avg_rating(execution_date)
    }
    
    # UPSERT (INSERT ... ON CONFLICT UPDATE)
    query = """
    INSERT INTO daily_business_analytics (
        analytics_date, total_orders, total_revenue, avg_order_value,
        orders_pending, orders_processing, orders_delivered,
        unique_customers, new_customers, avg_customer_rating
    )
    VALUES (
        %(analytics_date)s, %(total_orders)s, %(total_revenue)s, %(avg_order_value)s,
        %(orders_pending)s, %(orders_processing)s, %(orders_delivered)s,
        %(unique_customers)s, %(new_customers)s, %(avg_customer_rating)s
    )
    ON CONFLICT (analytics_date) DO UPDATE SET
        total_orders = EXCLUDED.total_orders,
        total_revenue = EXCLUDED.total_revenue,
        avg_order_value = EXCLUDED.avg_order_value,
        orders_pending = EXCLUDED.orders_pending,
        orders_processing = EXCLUDED.orders_processing,
        orders_delivered = EXCLUDED.orders_delivered,
        unique_customers = EXCLUDED.unique_customers,
        new_customers = EXCLUDED.new_customers,
        avg_customer_rating = EXCLUDED.avg_customer_rating,
        updated_at = CURRENT_TIMESTAMP
    """
    
    cursor.execute(query, analytics)
    conn.commit()
```

</details>

#### 3.2 Загрузка в Data Warehouse с SCD Type 2

_Самая важная и сложная часть проекта!_

##### Шаг 1: Загрузка измерения dim_customers (SCD Type 2)

<details>

**<summary>Пример кода</summary>**

```python
def load_dim_customers_scd2(customers_df: pd.DataFrame, effective_date: date):
    """
    Загрузка измерения клиентов с SCD Type 2.
    
    Логика:
    1. Для каждого клиента получаем текущую версию (is_current=TRUE)
    2. Сравниваем отслеживаемые атрибуты
    3. Если изменились - закрываем старую версию и создаем новую
    4. Если не изменились - ничего не делаем
    5. Если клиент новый - создаем первую версию
    """
    tracked_attributes = ['city', 'country', 'email', 'phone']
    
    for _, customer in customers_df.iterrows():
        customer_id = customer['customer_id']
        
        # Получение текущей версии
        current_version = get_current_customer_version(customer_id)
        
        if current_version is None:
            # НОВЫЙ КЛИЕНТ - создаем первую версию
            insert_customer_version(
                customer_id=customer_id,
                attributes=customer,
                effective_date=effective_date,
                expiration_date=date(9999, 12, 31),
                is_current=True
            )
            logger.info(f"Inserted NEW customer: {customer_id}")
            
        else:
            # СУЩЕСТВУЮЩИЙ КЛИЕНТ - проверяем изменения
            attributes_changed = False
            
            for attr in tracked_attributes:
                if str(current_version[attr]) != str(customer[attr]):
                    attributes_changed = True
                    logger.info(
                        f"Customer {customer_id}: {attr} changed "
                        f"from '{current_version[attr]}' to '{customer[attr]}'"
                    )
                    break
            
            if attributes_changed:
                # ИЗМЕНЕНИЯ ЕСТЬ - применяем SCD Type 2
                
                # 1. Закрываем текущую версию
                close_customer_version(
                    customer_key=current_version['customer_key'],
                    expiration_date=effective_date - timedelta(days=1)
                )
                
                # 2. Создаем новую версию
                insert_customer_version(
                    customer_id=customer_id,
                    attributes=customer,
                    effective_date=effective_date,
                    expiration_date=date(9999, 12, 31),
                    is_current=True
                )
                
                logger.info(f"Applied SCD Type 2 for customer: {customer_id}")
            else:
                # НЕТ ИЗМЕНЕНИЙ - ничего не делаем
                logger.debug(f"No changes for customer: {customer_id}")

def get_current_customer_version(customer_id: int) -> dict:
    """Получение текущей активной версии клиента."""
    query = """
    SELECT *
    FROM dim_customers
    WHERE customer_id = %s AND is_current = TRUE
    LIMIT 1
    """
    cursor.execute(query, (customer_id,))
    row = cursor.fetchone()
    
    if row:
        columns = [desc[0] for desc in cursor.description]
        return dict(zip(columns, row))
    return None

def close_customer_version(customer_key: int, expiration_date: date):
    """Закрытие текущей версии клиента."""
    query = """
    UPDATE dim_customers
    SET 
        expiration_date = %s,
        is_current = FALSE,
        updated_at = CURRENT_TIMESTAMP
    WHERE customer_key = %s
    """
    cursor.execute(query, (expiration_date, customer_key))

def insert_customer_version(
    customer_id: int,
    attributes: dict,
    effective_date: date,
    expiration_date: date,
    is_current: bool
):
    """Вставка новой версии клиента."""
    query = """
    INSERT INTO dim_customers (
        customer_id, first_name, last_name, email, phone,
        city, country, customer_segment,
        effective_date, expiration_date, is_current
    )
    VALUES (
        %(customer_id)s, %(first_name)s, %(last_name)s, %(email)s, %(phone)s,
        %(city)s, %(country)s, %(customer_segment)s,
        %(effective_date)s, %(expiration_date)s, %(is_current)s
    )
    """
    
    params = {
        'customer_id': customer_id,
        'first_name': attributes['first_name'],
        'last_name': attributes['last_name'],
        'email': attributes['email'],
        'phone': attributes['phone'],
        'city': attributes['city'],
        'country': attributes['country'],
        'customer_segment': attributes.get('customer_segment', 'Regular'),
        'effective_date': effective_date,
        'expiration_date': expiration_date,
        'is_current': is_current
    }
    
    cursor.execute(query, params)
```

</details>

##### Шаг 2: Загрузка фактов fact_orders

**КРИТИЧЕСКИ ВАЖНО:** При загрузке фактов нужно получить правильный surrogate key измерения для ДАТЫ ЗАКАЗА.

<details>

**<summary>Пример кода</summary>**

```python
def load_fact_orders(orders_df: pd.DataFrame):
    """
    Загрузка фактов заказов.
    Важно: используем customer_key который был активен на дату заказа!
    """
    for _, order in orders_df.iterrows():
        order_date = order['order_date'].date()
        
        # Получение customer_key для ДАТЫ ЗАКАЗА
        customer_key = get_customer_key_for_date(
            customer_id=order['customer_id'],
            as_of_date=order_date
        )
        
        # Получение product_key для ДАТЫ ЗАКАЗА
        product_key = get_product_key_for_date(
            product_id=order['product_id'],
            as_of_date=order_date
        )
        
        # Получение date_key
        date_key = int(order_date.strftime('%Y%m%d'))
        
        # Вставка факта
        insert_fact_order(
            order_id=order['order_id'],
            customer_key=customer_key,
            product_key=product_key,
            date_key=date_key,
            quantity=order['quantity'],
            unit_price=order['unit_price'],
            total_amount=order['total_amount'],
            order_status=order['status']
        )

def get_customer_key_for_date(customer_id: int, as_of_date: date) -> int:
    """
    Получение customer_key для конкретной даты.    
    Возвращает тот customer_key, который был активен на указанную дату.
    Это КЛЮЧЕВАЯ функция для корректной работы SCD Type 2!
    """
    query = """
    SELECT customer_key
    FROM dim_customers
    WHERE customer_id = %s
      AND effective_date <= %s
      AND expiration_date >= %s
    LIMIT 1
    """
    cursor.execute(query, (customer_id, as_of_date, as_of_date))
    result = cursor.fetchone()
    if result:
        return result[0]
    else:
        logger.error(
            f"No customer_key found for customer_id={customer_id} on {as_of_date}"
        )
        raise ValueError(f"Customer key not found")
```

</details>

**Пример работы SCD Type 2 при загрузке фактов:**

```text
Ситуация:
- Клиент ID=123 сделал заказ 01.06.2025
- Клиент переехал 15.06.2025
- Клиент сделал еще один заказ 20.06.2025

dim_customers:
customer_key | customer_id | city   | effective_date | expiration_date | is_current
1001         | 123         | Москва | 2025-01-01     | 2025-06-14      | FALSE
1125         | 123         | СПб    | 2025-06-15     | 9999-12-31      | TRUE

fact_orders:
fact_id | order_id | customer_key | order_date | ...
100     | 5001     | 1001         | 2025-06-01 | ...  ← Привязан к Москве
101     | 5002     | 1125         | 2025-06-20 | ...  ← Привязан к СПб

Результат:
- Первый заказ правильно показывает, что клиент был из Москвы
- Второй заказ правильно показывает, что клиент уже из СПб
- История сохранена корректно!
```

---


## 📈 Data Warehouse - Схема "Звезда"

### Структура таблиц

```
                    ┌──────────────┐
                    │   dim_date   │
                    │  (dimension) │
                    └──────┬───────┘
                           │
       ┌──────────────┐    │    ┌──────────────┐
       │dim_customers │────┼────│ dim_products │
       │(SCD Type 2)  │    │    │ (SCD Type 2) │
       └──────┬───────┘    │    └──────┬───────┘
              │            │           │
              └────────────┼───────────┘
                           │
                    ┌──────▼───────┐
                    │ fact_orders  │
                    │    (fact)    │
                    └──────────────┘
```

### Таблицы фактов (Fact Tables)

**fact_orders** - факты заказов

```sql
CREATE TABLE fact_orders (
    fact_id BIGSERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    
    -- Ссылки на измерения (SCD Type 2!)
    customer_key INTEGER REFERENCES dim_customers(customer_key),
    product_key INTEGER REFERENCES dim_products(product_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    time_key INTEGER REFERENCES dim_time(time_key),
    
    -- Метрики (всегда числовые, аддитивные)
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(12, 2),
    
    -- Дегенерированные измерения
    order_status VARCHAR(50),
    payment_method VARCHAR(50),
    
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Таблицы измерений (Dimension Tables)

**dim_customers** (SCD Type 2)

```sql
CREATE TABLE dim_customers (
    customer_key SERIAL PRIMARY KEY,        -- Surrogate key
    customer_id INTEGER NOT NULL,           -- Natural key
    
    -- Атрибуты клиента
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    email VARCHAR(255),
    phone VARCHAR(20),
    city VARCHAR(100),
    country VARCHAR(100),
    customer_segment VARCHAR(50),
    
    -- SCD Type 2 поля
    effective_date DATE NOT NULL,           -- Дата начала действия
    expiration_date DATE DEFAULT '9999-12-31',  -- Дата окончания
    is_current BOOLEAN DEFAULT TRUE,        -- Флаг текущей версии
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индексы для быстрого поиска
CREATE INDEX idx_dim_customers_current 
    ON dim_customers(customer_id, is_current);
CREATE INDEX idx_dim_customers_dates 
    ON dim_customers(effective_date, expiration_date);
```

**dim_date** (статическое измерение)

```sql
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,           -- YYYYMMDD
    full_date DATE NOT NULL UNIQUE,
    day_of_week INTEGER,
    day_name VARCHAR(10),
    month INTEGER,
    month_name VARCHAR(10),
    quarter INTEGER,
    year INTEGER,
    is_weekend BOOLEAN,
    is_holiday BOOLEAN
);
```

### Запросы для аналитики

**Пример 1: Выручка по клиентам с учетом истории**

```sql
-- Выручка по городам, где жили клиенты на момент заказа
SELECT 
    c.city,
    COUNT(DISTINCT f.order_id) as total_orders,
    SUM(f.total_amount) as total_revenue,
    AVG(f.total_amount) as avg_order_value
FROM fact_orders f
JOIN dim_customers c ON f.customer_key = c.customer_key
JOIN dim_date d ON f.date_key = d.date_key
WHERE d.year = 2025
GROUP BY c.city
ORDER BY total_revenue DESC;
```

**Пример 2: Анализ изменений клиентов**

```sql
-- Клиенты, которые меняли город
SELECT 
    customer_id,
    city,
    effective_date,
    expiration_date,
    is_current,
    CASE 
        WHEN is_current = FALSE THEN 'Historical'
        ELSE 'Current'
    END as version_status
FROM dim_customers
WHERE customer_id IN (
    SELECT customer_id
    FROM dim_customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
)
ORDER BY customer_id, effective_date;
```

---

## Визуализация - Dashboard в Grafana

### Настройка Grafana

1. Откройте Grafana: http://localhost:3000
2. Логин: admin / admin
3. Add data source → PostgreSQL
4. Настройте подключение к `postgres-analytics`

### Рекомендуемые панели для дашборда

**1. Обзор продаж (Sales Overview)**

- Total Orders (общее количество заказов)
- Total Revenue (общая выручка)
- Average Order Value (средний чек)
- Revenue Trend (тренд выручки по дням)

**SQL для Total Revenue:**

```sql
SELECT 
    SUM(total_revenue) as total_revenue
FROM daily_business_analytics
WHERE analytics_date >= NOW() - INTERVAL '30 days';
```

**2. География продаж (Sales by Geography)**

- Map: продажи по городам
- Top 10 Cities (топ-10 городов по выручке)
- Orders by Country (заказы по странам)

**SQL для топ-10 городов:**

```sql
SELECT 
    c.city,
    COUNT(DISTINCT f.order_id) as orders,
    SUM(f.total_amount) as revenue
FROM fact_orders f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE c.is_current = TRUE
GROUP BY c.city
ORDER BY revenue DESC
LIMIT 10;
```

**3. Анализ клиентов (Customer Analysis)**

- New vs Returning Customers
- Customer Segments Distribution
- Average Customer Rating
- Top Customers by Revenue

**SQL для сегментов:**

```sql
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) as customers,
    SUM(f.total_amount) as revenue
FROM fact_orders f
JOIN dim_customers c ON f.customer_key = c.customer_key
GROUP BY c.customer_segment;
```

**4. Качество сервиса (Service Quality)**

- Average Rating (средний рейтинг)
- Rating Distribution (распределение оценок)
- Delivery Success Rate (процент успешных доставок)
- Average Delivery Time (среднее время доставки)

**SQL для рейтинга:**

```sql
SELECT 
    DATE(analytics_date) as date,
    avg_customer_rating
FROM daily_business_analytics
WHERE analytics_date >= NOW() - INTERVAL '30 days'
ORDER BY date;
```

**5. Товары (Products)**

- Top 10 Products (топ-10 товаров)
- Sales by Category (продажи по категориям)
- Low Stock Alert (товары с низким остатком)

---

## Мониторинг и алерты

### Настройка алертов в Airflow

**В DAG файле:**

```python
default_args = {
    'owner': 'student',
    'email': ['student@example.com'],
    'email_on_failure': True,       # Уведомление при ошибке
    'email_on_retry': False,         # Не уведомлять при retry
    'email_on_success': False,       # Не уведомлять при успехе
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}
```

### Метрики для мониторинга

**1. Производительность:**

- Время выполнения DAG
- Время выполнения каждой задачи
- Использование памяти
- Использование CPU

**2. Качество данных:**

- Количество обработанных записей
- Количество ошибок валидации
- Процент дубликатов
- Процент null значений

**3. Доступность:**

- Статус подключений к источникам
- Количество неудачных попыток
- Задержки в расписании

**Пример логирования метрик:**

```python
def log_data_quality_metrics(**context):
    """Логирование метрик качества в БД."""
    execution_date = context['execution_date']
    
    # Получение метрик из XCom
    orders_count = context['task_instance'].xcom_pull(
        key='orders_count', 
        task_ids='extract_postgres_orders'
    )
    
    # Сохранение в data_quality_metrics
    insert_quality_metrics(
        run_date=execution_date,
        dag_id=context['dag'].dag_id,
        task_id=context['task'].task_id,
        source_name='postgres_orders',
        total_records=orders_count,
        # ... другие метрики
    )
```

---

## Рекомендации по выполнению

### Этап 1: Подготовка (1-2 дня)

1. Выбор предметной области
2. Проектирование архитектуры
3. Настройка Docker окружения
4. Инициализация баз данных

### Этап 2: Разработка Extract (2-3 дня)

1. Создание базовых классов Extractors
2. Реализация экстракторов для каждого источника
3. Тестирование извлечения данных
4. Генерация тестовых данных

### Этап 3: Разработка Transform (2-3 дня)

1. Реализация валидаторов
2. Реализация очистки данных
3. Создание трансформеров
4. Тестирование обработки

### Этап 4: Разработка Load (2-3 дня)

1. Создание структуры DWH
2. Реализация загрузчиков
3. Тестирование загрузки
4. Валидация данных

### Этап 5: Airflow DAG (2-3 дня)

1. Создание основного DAG
2. Настройка зависимостей задач
3. Тестирование пайплайна
4. Настройка расписания

### Этап 6: Визуализация (1-2 дня)

1. Настройка Grafana/Metabase
2. Создание дашборда
3. Настройка обновления данных
4. Финальное тестирование

### Этап 7: Документация (1-2 дня)

1. Написание README
2. Документирование API
3. Создание презентации
4. Подготовка к защите

---

## Критерии оценки дипломной работы

### 1. Полнота реализации (35 баллов)

- Реализация всех 3+ источников данных (8 баллов)
- Корректная работа Extract-Transform-Load (10 баллов)
- Настроенный Data Warehouse с **SCD Type 2** (12 баллов)
  - Правильная реализация версионирования (effective_date, expiration_date, is_current)
  - Корректное закрытие старых версий
  - Создание новых версий при изменениях
  - Привязка фактов к правильным версиям измерений
- Работающий дашборд (5 баллов)

### 2. Безопасность и управление подключениями (15 баллов)

- Использование `.env` файла для учетных данных (5 баллов)
- `.env` добавлен в `.gitignore`, нет хардкод паролей в коде (3 балла)
- Настроены **Airflow Connections** для всех источников (5 баллов)
- Код использует Airflow Hooks (`PostgresHook`, `MongoHook`) (2 балла)

### 3. Качество кода (25 баллов)

- Архитектура и структура проекта (10 баллов)
- Обработка ошибок и логирование (8 баллов)
- Документация и комментарии (7 баллов)

### 4. Настройка Airflow (15 баллов)

- Корректная структура DAG (8 баллов)
- Зависимости и расписание задач (4 балла)
- Мониторинг и алерты (3 балла)

### 5. Документация (10 баллов)

- README с инструкциями по запуску и настройке .env (5 баллов)
- Описание архитектуры и решений, включая обоснование использования SCD Type 2 (5 баллов)

---

## Полезные ресурсы

### Документация

**Apache Airflow:**
- [Официальная документация Apache Airflow](https://airflow.apache.org/docs/apache-airflow/2.11.0/)
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Managing Connections](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Pandas Documentation](https://pandas.pydata.org/docs/)

**PostgreSQL:**
- [PostgreSQL 15 Documentation](https://www.postgresql.org/docs/15/)
- [PostgreSQL Performance Tips](https://wiki.postgresql.org/wiki/Performance_Optimization)

**MongoDB:**
- [MongoDB Manual](https://docs.mongodb.com/manual/)
- [PyMongo Documentation](https://pymongo.readthedocs.io/)

**Data Warehouse:**
- [Kimball Group - Star Schema](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/)
- [SCD Type 2 Explanation](https://en.wikipedia.org/wiki/Slowly_changing_dimension)

**Grafana:**
- [Grafana Documentation](https://grafana.com/docs/grafana/latest/)
- [PostgreSQL Data Source](https://grafana.com/docs/grafana/latest/datasources/postgres/)

### Книги

1. **"Data Pipelines with Apache Airflow"** - Bas Harenslak, Julian de Ruiter
   - Лучшая книга по Airflow
   - Практические примеры
   - Best practices

2. **"The Data Warehouse Toolkit"** - Ralph Kimball, Margy Ross
   - Библия проектирования DWH
   - Подробно про SCD
   - Схемы "звезда" и "снежинка"

3. **"Designing Data-Intensive Applications"** - Martin Kleppmann
   - Архитектура data систем
   - Паттерны и анти-паттерны
   - Масштабирование

### Статьи и блоги

- [Apache Airflow Blog](https://airflow.apache.org/blog/)
- [Kimball Group Blog](https://www.kimballgroup.com/blog/)
- [dbt Blog - Analytics Engineering](https://blog.getdbt.com/)

### Инструменты для разработки

- **DBeaver** - GUI для работы с БД
- **Postman** - тестирование API
- **Docker Desktop** - управление контейнерами
- **VS Code** - IDE с плагинами для Python

---
