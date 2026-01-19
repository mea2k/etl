# Тема 3 – Извлечение данных из различных источников (Extract в ETL)

## Цель занятия

После этого занятия вы сможете:

1. **Понимать паттерн Extract** - первый шаг ETL pipeline
2. **Создавать Extractors** - классы для единообразного извлечения данных
3. **Работать с разными источниками** - Postgres, MongoDB, HTTP API, FTP, CSV
4. Использовать **Hooks** через собственные классы-экстракторы для единообразного доступа к данным.
5. **Строить параллельные pipeline** - извлечение из нескольких источников одновременно
6. **Применять инкрементальную загрузку** - извлекать только новые/изменённые данные
7. Подготовить основу для последующих шагов: Transform и Load.

## Необходимые знания

- Тема 1 (основы Airflow, DAG, операторы)
- Тема 2 (Hooks, Operators, Connections)
- Базовое знание SQL
- Понимание REST API и HTTP
- Опыт работы с pandas

---

## Теоретическая часть

### Концепция Extract

**Extract (извлечение)** — первый этап ETL — заключается в извлечении всех доступных данных в исходном ("сыром") виде **без изменений** из различных источников (БД, API, файловая система, FTP и т.п.).
​
```
Источники данных → Extract → Raw Data → Transform → Load
```

### Ключевые принципы Extract

- **Минимальная инвазивность** - не создавать нагрузку на основную систему  
- **Инкрементальность** - забирать только изменения, не всё каждый раз  
- **Единообразие** - одинаковый интерфейс для всех источников
- **Надёжность** - обработка ошибок и retry  
- **Логирование** - отслеживание выполнения и отслеживание извлечённых данных  

### Архитектура Extract в Airflow

```
┌─────────────┐
│ Connection  │ (Credentials в Airflow UI)
└──────┬──────┘
       ↓
┌─────────────┐
│    Hook     │ (Airflow: PostgresHook, HttpHook, ...)
└──────┬──────┘
       ↓
┌─────────────┐
│  Extractor  │ (Наш класс с бизнес-логикой)
└──────┬──────┘
       ↓
┌─────────────┐
│  DAG Task   │ (Задача в DAG)
└─────────────┘
```

**Connections** — все аутентификационные данные, необходимые для подключения к источникам, должны храниться **только в Connections**!

**Hooks** — используются для упрощения проекта - готовые инструменты Airflow для различных источника (SQL, HTTP, FTP).

**Extractors** — классы-обёртки с бизнес-логикой извлечения из конкретных источников и преобразования данных в единых формат, подходящий для последующей обработки (DataFrame/списки).

**Почему используем Extractors, а не Hooks напрямую?**

| Подход | Преимущества | Недостатки |
|--------|-------------|------------|
| **Напрямую Hook** | Просто | Дублирование кода, нет единого интерфейса |
| **Через Extractor** | Переиспользование, единый интерфейс, бизнес-логика | Дополнительный слой |


### Базовый класс Extractor

Для универсальности, предлагается согдать базовый абстрактный класс `BaseExtractor`, в котором явно задать абстрактные методы, реализуемые наследниками. Таким образом, любой Extractor будет вызывать одинаковые методы, но реализованы они будут под каждый источник индивидуально.

Пример кода базового класса приведён в файле [base_extractor.py](plugins/extractors/base_extractor.py)

Интерфейс класса:

```python
class BaseExtractor(ABC):
    """
    Абстрактный базовый класс для извлечения данных.
    Все экстракторы наследуются от этого класса и реализуют метод extract().
    """
    def __init__(self, conn_id: str):
        """
        Инициализация экстрактора.
        Args:
            conn_id: Airflow Connection ID (должен существовать в UI).
        """
        self.conn_id = conn_id
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info(f"Initialized {self.__class__.__name__} for connection: {conn_id}")

    @abstractmethod
    def extract(self, **kwargs) -> pd.DataFrame:
        """
        Извлечение данных из источника.
        Должен вернуть pandas.DataFrame.
        """
        raise NotImplementedError

```

**Преимущества:**

- Единообразный код в DAG
- Легко добавлять новые источники
- Централизованное логирование
- Переиспользование логики

### Примеры Extractor разных типов

**1. PostgresExtractor для SQL БД (Postgres)**

**Возможности:**

- Выполнение произвольного SQL
- Инкрементальная загрузка по дате
- Параметризованные запросы

Полный код содержится в файле [postgres_extractor.py](plugins/extractors/postgres_extractor.py)

**Пример использования:**

```python
# Создано Connections c conn_id="pg_source"
extractor = PostgresExtractor("pg_source")

# Простой запрос
df = extractor.extract("SELECT * FROM users WHERE active = true")

# С параметрами
df = extractor.extract(
    sql="SELECT * FROM orders WHERE order_date = %(date)s",
    parameters={"date": "2025-12-01"}
)

# Инкрементальная загрузка
df = extractor.extract_incremental(
    table_name="orders",
    date_column="created_at",
    start_date="2025-12-01",
    end_date="2025-12-12"
)
```

**2. MongoExtractor - для NoSQL СУБД MongoDB**

**Возможности:**

- Извлечение документов по query
- Projection (выбор полей)
- Фильтрация по дате

Полный код содержится в файле [mongo_extractor.py](plugins/extractors/mongo_extractor.py)

**Пример использования:**

```python
# Создано Connections c conn_id="mongo_source"
extractor = MongoExtractor("mongo_source", database="analytics_db")

# Все документы коллекции
df = extractor.extract(collection="events")

# С фильтром
df = extractor.extract(
    collection="events",
    query={"event_type": "purchase", "amount": {"$gt": 100}}
)

# По дате
df = extractor.extract_by_date(
    collection="events",
    date_field="created_at",
    date_str="2025-12-01"
)
```

**3. HttpExtractor - для HTTP REST API**

**Возможности:**

- GET/POST запросы
- Параметры и headers
- Обработка JSON ответов

Полный код содержится в файле [http_extractor.py](plugins/extractors/http_extractor.py)

**Пример использования:**

```python
# Создано Connections c conn_id="api_service"
extractor = HttpExtractor("api_service")

# GET запрос
df = extractor.extract(
    endpoint="/api/v1/users",
    params={"limit": 100, "offset": 0}
)

# С headers
df = extractor.extract(
    endpoint="/api/v1/data",
    headers={"X-API-Version": "2.0"}
)
```

**4. FTPExtractor - для получения файлов с FTP-сервера**

**Возможности:**

- Загрузка CSV, JSON, Excel
- Список файлов
- Обработка архивов

Полный код содержится в файле [ftp_extractor.py](plugins/extractors/ftp_extractor.py)

**Пример использования:**

```python
# Создано Connections c conn_id="ftp_server"
extractor = FTPExtractor("ftp_server")

# CSV файл
df = extractor.extract("/data/users.csv", file_type="csv")

# Excel файл
df = extractor.extract("/reports/sales.xlsx", file_type="excel")

# Список файлов
files = extractor.list_files("/data/")
```

**5. CSVExtractor - локальные файлы CSV**

**Возможности:**

- Чтение CSV/TSV
- Разные разделители
- Обработка кодировок

Полный код содержится в файле [csv_extractor.py](plugins/extractors/csv_extractor.py)

**Пример использования:**

```python
extractor = CSVExtractor(base_path="/opt/airflow/data/csv")

# CSV файл
df = extractor.extract("users.csv")

# TSV файл
df = extractor.extract("data.tsv", delimiter="\t")
```

---

## Практическая часть

### Структура проекта

```text
lesson3_extract/
├── dags/
│   ├── dag_01_postgres.py              # Пример 1: Postgres → CSV
│   ├── dag_02_incremental.py           # Пример 2: Incremental Extract
│   ├── dag_03_http.py                  # Пример 2: HTTP → JSON
│   ├── dag_04_multi_source.py          # Пример 3: 5 источников параллельно
│   └── dag_homework_template.py        # Шаблон для ДЗ
├── plugins/extractors/
│   ├── base_extractor.py               # Базовый класс
│   ├── postgres_extractor.py           # PostgresExtractor
│   ├── mongo_extractor.py              # MongoExtractor
│   ├── http_extractor.py               # HttpExtractor
│   ├── ftp_extractor.py                # FTPExtractor
│   └── csv_extractor.py                # CSVExtractor
├── init/
│   ├── postgres_init.sql               # Создание БД и таблиц
│   └── mongo_init.js                   # Инициализация MongoDB
├── data/
│   ├── csv/                            # Тестовые CSV файлы
│   └── extracted/                      # Сохраненные результаты
├── docker-compose.yml                  # Конфигурация Docker-контейнеров
└── README.md                           # Настоящее руководство
```

### Шаг 1: Запуск инфраструктуры

```bash
# 1. Запуск Docker Compose
docker compose up -d

# 2. Проверка статуса
docker compose ps

# Должны быть запущены:
# - airflow (Airflow webserver + scheduler)
# - postgres (база данных)
# - mongodb (NoSQL база)
# - ftp (FTP сервер, опционально)

# 3. Открыть Airflow
# http://localhost:8080
# admin / admin
```

### Шаг 2: Создание Connections

Создайте следующие Connections через UI (**Admin → Connections → +**):

| Conn ID | Type | Host/URL | Доп. параметры |
|---------|------|----------|---------------|
| `pg_source` | Postgres | localhost:5432 | Login:airflow, Password:airflow, Schema:source_db |
| `mongo_source` | MongoDB | mongodb://localhost:27017 | Login:airflow, Password:airflow, Schema:analytics_db, Extra: `{"authSource": "admin"}` |
| `api_service` | HTTP | https://jsonplaceholder.typicode.com | Extra: `{"timeout": 30}` |
| `ftp_server` | FTP | ftp.localhost | Login:ftp_user, Password:ftp_pass |
| `csv_local` | Generic | /opt/airflow/data/csv | |

#### 1. PostgreSQL Connection

- **Connection Id**: `pg_source`
- **Connection Type**: `Postgres`
- **Host**: `postgres`
- **Database/Schema**: `source_db`
- **Login**: `airflow`
- **Password**: `airflow`
- **Port**: `5432`

#### 2. MongoDB Connection

- **Connection Id**: `mongo_source`
- **Connection Type**: `MongoDB`
- **Host**: `mongodb`
- **Default DB/Schema**: `analytics_db`
- **Username**: `airflow`
- **Password**: `airflow`
- **Port**: `27017`
- **Extra**: `{"authSource": "admin"}`

#### 3. HTTP Connection

- **Connection Id**: `api_service`
- **Connection Type**: `HTTP`
- **Host**: `jsonplaceholder.typicode.com`
- **Schema**: `https`
- **Extra**: `{"timeout": 30}`

#### 4. FTP Connection (опционально)

- **Connection Id**: `ftp_server`
- **Connection Type**: `FTP`
- **Host**: `ftp`
- **Login**: `ftpuser`
- **Password**: `ftppass`
- **Port**: `21`

#### 5. CSV Local (для логики)

- **Connection Id**: `csv_local`
- **Connection Type**: `File (path)`
- **Extra**: `{"path": "/opt/airflow/data/csv"}`

### Шаг 3: Подготовка тестовых данных

Тестовые данные создаются автоматически при запуске Docker Compose:

**Postgres:** Таблицы `users`, `orders`, `products`  
**MongoDB:** Коллекции `events`, `feedback`, `products_viewed` в БД `analytics_db`  
**CSV:** Файлы в `/data/csv/`  

Для проверки:

```bash
# Проверка Postgres
docker compose exec -i postgres psql -U airflow -d source_db -c "SELECT COUNT(*) FROM users;"

# Проверка MongoDB
docker compose exec -i mongodb mongosh --eval "db.events.count()"

# Проверка CSV
docker compose exec -i airflow ls -la /opt/airflow/data/csv/
```

### Примеры DAG

#### Пример 1: Простое извлечение из Postgres (Postgres → CSV)

**DAG:** [dag_o1_postgres.py](dags/dag_01_postgres.py)

1. Получение данных из Postgres (Airflow PostgresHook).
2. Сохранение данных в локальный CSV.
3. Логирование количества строк и столбцов.

```python
def extract_users(**context):
    """Извлечение пользователей"""
    extractor = PostgresExtractor("pg_source")
    df = extractor.extract("SELECT * FROM users WHERE active = true")
    
    # Сохраняем в CSV
    output_path = f"/opt/airflow/data/extracted/users_{context['ds']}.csv"
    df.to_csv(output_path, index=False)
    
    return {"records": len(df), "file": output_path}
```

**Что наблюдать:**

1. Логи задачи - количество извлечённых записей
2. Файл `/opt/airflow/data/extracted/users_YYYY-MM-DD.csv`
3. Время выполнения в Gantt Chart

#### Пример 2: Инкрементальная загрузка

**DAG:** [dag_02_incremental.py](dags/dag_02_incremental.py)

```python
def extract_orders_incremental(**context):
    """Инкрементальная загрузка заказов"""
    ds = context['ds']  # текущая дата в формате YYYY-MM-DD
    
    extractor = PostgresExtractor("pg_source")
    df = extractor.extract_incremental(
        table_name="orders",
        date_column="created_at",
        start_date=ds,
        end_date=ds,  # Только за текущий день
    )
    
    return {"records": len(df), "date": ds}
```

#### Пример 3: Извлечение данных из HTTP

**DAG:** [dag_03_http.py](dags/dag_03_http.py)

1. Запрос к API с параметром `execution_date`.
2. Сохранение исходного (raw) ответа в файл/таблицу.
3. Простейшая проверка ответа (HTTP-код, размер).

```python
from extractors.http_extractor import HttpExtractor

def extract_api_to_json(**context):
    ds = context["ds"]
    extractor = HttpExtractor(conn_id="api_service")

    df = extractor.extract(
        endpoint="/posts",
        params={"userId": 1, "limit": 100},
    )

    output_dir = Path("/opt/airflow/data/http")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / f"items_{ds}.json"

    df.to_json(output_file, orient="records", indent=2, force_ascii=False)
    print(f"Saved {len(df)} items to {output_file}")
    return str(output_file)


with DAG(...) as dag:
    start = DummyOperator(task_id='start')
    http_task = PythonOperator(
        task_id="extract_api_to_json",
        python_callable=_extract_api_to_json,
    )
    end = DummyOperator(task_id='end')

    start >> http_task >> end
```

#### Пример 4: Параллельное извлечение

**DAG:** [dag_04_multi_source.py](dags/dag_04_multi_source.py)

1. Источники: Postgres + HTTP или CSV + Mongo _(извлечение осуществляется параллельно)_.
2. объединение данных по ключу в PythonOperator.
3. Запись объединённого результата в CSV-файл.

```text
start → [extract_postgres, extract_mongo, extract_http] → merge → save
```

```python
def extract_postgres(**context):
    extractor = PostgresExtractor("pg_source")
    df = extractor.extract("SELECT * FROM users")
    return df.to_dict('records')

def extract_mongo(**context):
    extractor = MongoExtractor("mongo_source", "analytics_db")
    df = extractor.extract(collection="events")
    return df.to_dict('records')

def extract_http(**context):
    extractor = HttpExtractor("api_service")
    df = extractor.extract("/posts")
    return df.to_dict('records')

def merge_data(**context):
    """Объединение данных из всех источников"""
    ti = context['task_instance']
    
    pg_data = ti.xcom_pull(task_ids='extract_postgres')
    mongo_data = ti.xcom_pull(task_ids='extract_mongo')
    http_data = ti.xcom_pull(task_ids='extract_http')
    
    total = len(pg_data) + len(mongo_data) + len(http_data)
    print(f"Total records: {total}")
    
    return {"total": total}

with DAG(...) as dag:
    start = DummyOperator(task_id='start')
    
    pg_task = PythonOperator(task_id='extract_postgres', ...)
    mongo_task = PythonOperator(task_id='extract_mongo', ...)
    http_task = PythonOperator(task_id='extract_http', ...)
    
    merge_task = PythonOperator(task_id='merge', ...)

    end = DummyOperator(task_id='end')

    start >> [pg_task, mongo_task, http_task] >> merge_task >> end
```

### Порядок действий

#### Пример 1: Postgres → CSV (DAG 1)

1. Изучить код DAG [dag_01_postgres.py](dags/dag_01_postgres.py)
2. Найти все места с пометкой `TODO:` и исправить.
3. Убедиться, что Connection `pg_source` настроен.
4. Запустить DAG вручную.
5. Проверить:
    - логи всех задач
    - наличие файла `/opt/airflow/data/extracted/01/users_YYYY-MM-DD.csv`

**Вопросы для обсуждения:**

1. Чем отличается `get_pandas_df()` от `get_records()` в PostgresHook?
2. Как реализовать инкрементальность по `order_date`?

**Упражнение:**

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

#### Пример 2. Инкрементальная загрузка (DAG 2)

1. Изучить код DAG [dag_02_incremental.py](dags/dag_02_incremental.py)
2. Убедиться, что Connection `pg_source` настроен.
3. Запустить DAG вручную.
4. Проверить:
    - логи всех задач
    - наличие файлов `/opt/airflow/data/extracted/02/*.csv` и файла статистики `summary_YYYY-MM-DD.json`
5. В логах задачи `extract_orders`:
   - Количество заказов за день
   - Общая сумма заказов
   - Средний чек
6. В логах задачи `extract_products`:
   - Количество продуктов за неделю
   - Распределение по категориям
7. В файле `summary_YYYY-MM-DD.json`:
   - Полная статистика по всем извлечениям
   - Пути к выходным файлам
   - Метрики

**Вопросы для обсуждения:**

1. Можно ли какие-нибудь задачи из этого DAG запускать параллельно? Если да, исправьте код DAG и запустите его повторно. Результат изучите в разделе `Graph`.

**Упражнение:**

1. Модифицируйте период для заказов:
   - Вместо 1 дня извлекайте за неделю
   - Используйте Airflow Variable для периода

2. Добавьте фильтрацию:
   - Извлекайте только заказы со статусом `completed`
   - Добавьте фильтр по сумме (`amount > 100`)

3. Добавьте агрегацию:
   - Подсчитайте количество заказов по статусам
   - Найдите топ-5 пользователей по сумме заказов

4. Добавьте обработку ошибок:
   - Если нет данных - не выходить в ошибку, а логировать
   - Создавать пустой файл с меткой

#### Пример 3. HTTP API → JSON (DAG 3)

1. Изучить код DAG [dag_03_http.py](dags/dag_03_http.py)
2. Убедиться, что Connection `api_service` настроен.
3. Запустить DAG вручную.
4. Проверить:
    - логи всех задач
    - наличие файлов `/opt/airflow/data/extracted/03/posts_YYYY-MM-DD.json`

**Упражнение:**

1. Модифицируйте функцию `extract_api_to_json`:
    - извлекайте посты для userId=2 вместо userId=1
    - измените endpoint на "/comments"
    - добавьте параметр `"postId": 1`

    <details>

    <summary>Подсказка:</summary>

    ```python
    df = extractor.extract(
        endpoint="/comments",
        params={"postId": 1}
    )
    ```

    </details>

2. Используйте Airflow Variables для передачи идентификатора пользователя `userId`.

3. Добавьте фильтрацию извлечённых данных. Добавьте функцию `extract_api_to_json`, которая осуществляет фильтрацию DataFrame после извлечения данных:
    - оставьте только посты, где title содержит слово "sunt"
    - сохраните результат в `posts_filtered_YYYY-MM-DD.json`

    <details>

    <summary>Подсказка:</summary>

    ```python
    df_filtered = df[df['title'].str.contains('sunt', case=False, na=False)]
    ```

    </details>

4. Добавьте обработку ошибок с retry:
    - Используйте метод `extract_with_retry()`
    - Установите `max_retries=5`, `retry_delay=10`
    - Добавьте `try-except` в функцию
    - Логируйте ошибки

    <details>

    <summary>Подсказка:</summary>

    ```python
    try:
        df = extractor.extract_with_retry(
            endpoint="/posts",
            max_retries=5,
            retry_delay=10,
            params={"userId": 1}
        )
    except Exception as e:
        logger.error(f"Failed to extract: {e}")
        raise
    ```
    </details>

5. Параллельное извлечение. Создайте несколько задач, которые будут извлекать данные параллельно:
   - извлекайте posts, comments, users параллельно
   - объедините результаты в один json

### Пример 4. Мульти‑источник (DAG 4)

1. Изучить код DAG [dag_04_multi_source.py](dags/dag_04_multi_source.py)
2. Убедиться, что созданы и настроены необходимые Connections:
    - `pg_source`
    - `mongo_source`
    - `api_service`

3. Убедиться, что файл CSV создан по пути `/opt/airflow/data/csv` (создаются автоматически через [postgres_init.sql](init/postgres_init.sql)).
4. Убедиться, что коллекции в MongoDB созданы ((создаются автоматически через [mongo_init.js](init/mongo_init.js)))
5. Запустить DAG вручную.
6. Проверить:
    - логи всех задач
    - наличие файлов `/opt/airflow/data/extracted/04/multi_source_report_YYYY-MM-DD.json`

7. В Graph View:
   - 5 задач извлечения выполняются параллельно
   - Визуализация потока данных

8. В логах check_environment:
   - Статус каждого источника
   - Какие источники доступны

9. В логах задач extract_*:
   - Количество извлечённых записей
   - Время выполнения
   - Ошибки (если есть)

10. В логах merge_data:
    - Общее количество записей
    - Распределение по источникам
    - Список ошибок

11. В файле отчёта:
    - Полная статистика
    - Метаданные извлечения
    - Детали по каждому источнику

**Особенности:**

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

**Вопросы:**

1. Параллелизм извлечения: когда выгоден, а когда вреден?
2. Как в дальнейшем объединять данные из разных источников (join по ключам)?
3. Какие поля должны быть согласованы (id, даты, timezone)?

**Упражнения:**

1. Добавьте новый источник:
   - Создайте задачу `extract_api2`
   - Используйте другой API
   - Добавьте в граф зависимостей

2. Добавьте фильтрацию:
   - Извлекайте только данные за последние 7 дней
   - Фильтруйте по условиям

3. Добавьте валидацию:
   - Проверяйте обязательные поля
   - Отбрасывайте некорректные записи

4. Добавьте дедупликацию:
   - Удаляйте дубликаты по ключевым полям
   - Логируйте количество удалённых дубликатов

---

## Домашнее задание

### Задание 1: Еще один Extractor

1. Реализовать Extractor для извлечения данных из локальных JSON-файлов.
2. Создать DAG, демонстрирующий работу созданного JsonExtractor-а.
3. Создать тестовый JSON-файл.
4. Продемонстрировать корректную работу JsonExtractor. Извлеченные из файла данные должны быть преобразованы в DataFrame и сохраняться в файле `/opt/airflow/data/extracted/json_<ds>.csv`.

**Требования:**

1. **Файл:** `plugins/extractors/json_extractor.py`

```python
class JSONExtractor(BaseExtractor):
    def __init__(self, base_path: str):
        """
        Args:
            base_path: Базовый путь к JSON файлам
        """
        pass
    
    def extract(self, filename: str) -> pd.DataFrame:
        """
        Извлечение данных из JSON файла.        
        Args:
            filename: Имя файла (например, "data.json")
        Returns:
            DataFrame с данными
        """
        pass
```

2. **DAG:** `dags/dag_homework_json.py`

```python
def extract_json(**context):
    extractor = JSONExtractor("/opt/airflow/data/json")
    df = extractor.extract("products.json")
    # Сохранить в extracted/
    output_path = f"/opt/airflow/data/extracted/json_{context['ds']}.csv"
    df.to_csv(output_path, index=False)
    
    return {"records": len(df), "file": output_path}
```

3. **Тестовый файл:** `data/json/products.json`

```json
[
    {"id": 1, "name": "Product A", "price": 100, "category": "Electronics"},
    {"id": 2, "name": "Product B", "price": 50, "category": "Books"},
    {"id": 3, "name": "Product C", "price": 75, "category": "Electronics"}
]
```

### Задание 2: "Каталог пользователей из разных источников" (Unified User Catalog)

Необходимо реализовать ETL pipeline, который собирает "единый каталог пользователей" из нескольких источников.

**Архитектура:**

```text
┌─────────────┐
│   Postgres  │ → extract_pg_users ──┐
└─────────────┘                       │
                                      ├→ merge_users → save_catalog → validate
┌─────────────┐                       │
│  HTTP API   │ → extract_http_users ─┤
└─────────────┘                       │
                                      │
┌─────────────┐                       │
│     CSV     │ → extract_csv_users ──┘
└─────────────┘
```

**Требования:**

1. Создать Python‑модуль `custom_extractors.py` с минимум тремя классами:

    - `PostgresUserExtractor` — возвращает пользователей из таблицы users (поля id, email, full_name)

    - `HTTPUserExtractor` — получает JSON с пользователями из REST API (можно использовать публичный mock‑сервис или локальный сервис-заглушку).

    - `CSVUserExtractor` — читает пользователей из локального CSV-файла.

2. Реализовать интерфейс: у всех классов есть метод extract() -> list[dict].

3. DAG `dag_homework_users.py`:

    - Задача `extract_pg_users` — использует PostgresUserExtractor

    - Задача `extract_http_users` — использует HTTPUserExtractor

    - Задача `extract_csv_users` — использует CSVUserExtractor

    - Задача `merge_users`:

        - Получает три списка пользователей через XCom

        - Объединяет их по email (простое правило: "первый источник главный", остальные только дополняют отсутствующие поля)

    - Задача `save_merged`:

        - Сохраняет результат в локальный JSON или CSV-файл `/opt/airflow/data/merged_users.json`

<details>

<summary>Пример кода задач</summary>

```python
def extract_pg_users(**context):
    extractor = PostgresUserExtractor("pg_source")
    df = extractor.extract_users()
    return df.to_dict('records')

def extract_http_users(**context):
    extractor = HTTPUserExtractor("api_service")
    df = extractor.extract_users()
    return df.to_dict('records')

def extract_csv_users(**context):
    extractor = CSVUserExtractor("/opt/airflow/data/csv")
    df = extractor.extract_users("users.csv")
    return df.to_dict('records')

def merge_users(**context):
    """
    Объединение пользователей по email.
    Правила:
    - Если email повторяется - берём первого
    - Приоритет: Postgres > API > CSV
    """
    ti = context['task_instance']
    
    pg_users = ti.xcom_pull(task_ids='extract_pg_users')
    api_users = ti.xcom_pull(task_ids='extract_http_users')
    csv_users = ti.xcom_pull(task_ids='extract_csv_users')
    
    # Объединение
    all_users = pg_users + api_users + csv_users
    
    # Дедупликация по email
    seen_emails = set()
    unique_users = []
    for user in all_users:
        email = user['email']
        if email not in seen_emails:
            seen_emails.add(email)
            unique_users.append(user)
    
    logger.info(f"Total users: {len(all_users)}")
    logger.info(f"Unique users: {len(unique_users)}")
    logger.info(f"Duplicates removed: {len(all_users) - len(unique_users)}")
    
    return unique_users

def save_merged(**context):
    """Сохранение результата в файл"""
    ti = context['task_instance']
    users = ti.xcom_pull(task_ids='merge_users')
    
    # Сохранить в JSON
    output_path = "/opt/airflow/data/extracted/user_catalog.json"
    
    with open(output_path, 'w') as f:
        json.dump(users, f, indent=2)
    
    # Сохранить в CSV
    df = pd.DataFrame(users)
    df.to_csv("/opt/airflow/data/extracted/user_catalog.csv", index=False)
    
    return {
        "total_users": len(users),
        "output_file": output_path
    }
```

</details>

4. Граф задач:

    ```python
   start >> [pg_users_task, http_users_task, csv_users_task] >> merge_task >> save_task >> end
    ```

**Тестовые данные**

1. Postgres (users table)

    ```sql
    INSERT INTO users (id, email, full_name) VALUES
    (1, 'alice@example.com', 'Alice Smith'),
    (2, 'bob@example.com', 'Bob Johnson'),
    (3, 'charlie@example.com', 'Charlie Brown');
    ```

2. CSV [data/csv/users.csv](data/csv/users.csv)

    ```csv
    user_id,email,full_name
    101,david@example.com,David Lee
    102,alice@example.com,Alice Williams
    103,emma@example.com,Emma Davis
    104,oliver@example.com,Oliver Brown
    105,robert@example.com,Robert Johnson
    ```

3. HTTP

    Используйте https://jsonplaceholder.typicode.com/users

### Bonus задание 3: Статистика

В задаче **"Каталог пользователей из разных источников" (Unified User Catalog)** добавить задачи:

- валидатор `validate_users`, осуществляющий проверку итогового файла CSV на уникальность и непустые поля email, непустые поля full_name
- подсчет статистики `create_statistics`, выводящей итоговую статистику извлечения данных

<details>

<summary>Пример кода</summary>

```python
def validate_users(**context):
    """Валидация пользователей"""
    ti = context['task_instance']
    result = ti.xcom_pull(task_ids='save_task')
        
    # Проверить файл
    df = pd.read_csv("/opt/airflow/data/extracted/user_catalog.csv")
    
    # Проверить required колонки
    required_cols = ['user_id', 'email', 'full_name']
    assert all(col in df.columns for col in required_cols)
    
    # Проверить уникальность email
    assert df['email'].is_unique, "Duplicate emails found"
    
    logger.info("  Validation passed")
    return {"status": "valid"}


def create_statistics(**context):
    """Статистика по источникам"""
    ti = context['task_instance']
    users = ti.xcom_pull(task_ids='save_task')
    
    df = pd.DataFrame(users)
    
    stats = {
        'total_users': len(df),
        'by_source': df.groupby('source').size().to_dict(),
        'email_domains': df['email'].str.split('@').str[1].value_counts().to_dict()
    }
    
    print(json.dumps(stats, indent=2))
    return stats
```

</details>

### Bonus задание 2: Обработка ошибок

Добавьте обработку ошибок во все extract функции:

```python
def extract_with_fallback(extractor, extract_func, source_name):
    """Wrapper с обработкой ошибок"""
    try:
        return extract_func()
    except Exception as e:
        logger.error(f"Failed to extract from {source_name}: {e}")
        # Вернуть пустой список вместо падения
        return []
```

### Что сдавать

1. **Код** - все реализованные файлы DAG и Extractors.
2. **Скриншоты**
    - Скриншоты Graph View
    - логи последних задач, работающих с данными
    - сформированные файлы CSV или JSON

3. **Документация** (homework.md) - краткое текстовое описание (1 страница):
    - какие использованы источники
    - какая структура данных
    - как создать тестовые данные
    - как настроить Connections
    - как запустить DAG с примерами и скриншотами
    - результаты выполнения с примерами и скриншотами

---

## Best practices для Extract

**1. Инкрементальная загрузка**

```python
# ХОРОШО: Только новые данные
df = extractor.extract_incremental(
    table_name="orders",
    date_column="created_at",
    start_date=context['ds'],
    end_date=context['ds']
)

# ПЛОХО: Всё каждый раз
df = extractor.extract("SELECT * FROM orders")
```

**2. Фильтрация на стороне источника**

```python
# ХОРОШО: Фильтр в SQL
sql = "SELECT * FROM orders WHERE status = 'completed'"

# ПЛОХО: Фильтр в Python
df = extractor.extract("SELECT * FROM orders")
df = df[df['status'] == 'completed']
```

**3. Обработка больших данных**

```python
# ХОРОШО: Чанками
for chunk in extractor.extract_in_chunks("SELECT * FROM large_table", chunk_size=10000):
    process(chunk)

# ПЛОХО: Всё в память
df = extractor.extract("SELECT * FROM large_table")  # OOM!
```

**4. Логирование - всегда логируйте количество полученных строк, список колонок, размер данных.**

```python
# ХОРОШО: Детальное логирование
logger.info(f"Extracting from {source}")
df = extractor.extract(...)
logger.info(f"Extracted {len(df)} records")
logger.info(f"Columns: {list(df.columns)}")

# ПЛОХО: Без логов
df = extractor.extract(...)
```

**5. Обработка ошибок - следите за таймаутами и повторами для внешних систем (особенно HTTP/FTP)**

```python
# ХОРОШО: С retry и fallback
from airflow.exceptions import AirflowException

def extract_with_retry(**context):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            return extractor.extract(...)
        except Exception as e:
            if attempt == max_retries - 1:
                raise
            logger.warning(f"Retry {attempt + 1}/{max_retries}")
            time.sleep(5 * (attempt + 1))
```

**6. Выносите SQL/запросы в отдельные модули или файлы, а не пишите в самом коде DAG.**

---

## Чек‑лист по теме

**Подготовка:**

- `[ ]` Запущен Docker Compose
- `[ ]` Созданы все Connections
- `[ ]` Проверены тестовые данные

**Примеры:**

- `[ ]` Запустил `dag_01_postgres`
- `[ ]` Запустил `dag_02_incremental`
- `[ ]` Запустил `dag_03_http`
- `[ ]` Запустил `dag_04_multi_source`
- `[ ]` Изучил логи всех задач
- `[ ]` Проверил extracted файлы

**Упражнения:**

- `[ ]` Выполнил упражнение 1
- `[ ]` Выполнил упражнение 2
- `[ ]` Выполнил упражнение 3
- `[ ]` Выполнил упражнение 4

**Домашнее задание:**

- `[ ]` Создал JSONExtractor
- `[ ]` Создал User Extractors
- `[ ]` Реализовал merge_users
- `[ ]` Добавил валидацию

---

## Что дальше?

После освоения Темы 3 переходите к:

- **Тема 4:** Transform - очистка, валидация, обогащение данных
- **Тема 5:** Load - загрузка в хранилище и создание витрин

---
