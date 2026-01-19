# Тема 2 – Расширенные возможности работы с Apache Airflow: Hooks, Operators, Connections, Variables

## Цель занятия

После этого занятия вы сможете:

1. **Работать с Connections** - безопасно хранить параметры подключений к внешним системам
2. **Использовать Variables** - управлять конфигурацией DAG без изменения кода
3. **Создавать Hooks** - разрабатывать переиспользуемую логику работы с внешними системами
4. **Разрабатывать Operators** - создавать кастомные операторы для решения специфичных задач

## Необходимые знания

- Темы 1 (основы Airflow, DAG, операторы)
- Базовое знание Python и ООП
- Понимание REST API
- Опыт работы с Docker

---

## Теоретическая часть

### Connections (Подключения)

**Что это:**
Безопасное хранилище параметров подключения к внешним системам.

**Зачем нужны:**

- Централизованное управление credentials
- Безопасность: не храним пароли в коде
- Легко меняем окружение (dev/prod)
- Поддержка разных типов: HTTP, Postgres, SSH, S3, и др.

**Пример:**

```python
# ПЛОХО: Хардкод в коде
url = "https://api.example.com"
api_key = "secret_key_12345"  # ОПАСНО!

# ХОРОШО: Использование Connection
from airflow.hooks.base import BaseHook
conn = BaseHook.get_connection('my_api_conn')
url = f"{conn.schema}://{conn.host}"
api_key = conn.extra_dejson.get('api_key')
```

### Variables (Переменные)

**Что это:**
Глобальное хранилище key-value для конфигурации DAG.

**Зачем нужны:**

- Параметры без перезапуска Airflow
- Feature flags (включение/выключение функций)
- Константы для всех DAG
- Управление через UI или CLI

**Пример:**

```python
from airflow.models import Variable

# Получить значение
batch_size = Variable.get("batch_size", default_var=100)
env = Variable.get("environment")  # dev, staging, prod

# В DAG можно использовать для условной логики
if Variable.get("feature_enabled") == "true":
    # Включаем новую функциональность
    pass
```

### Hooks (Хуки)

**Что это:**
Обёртка вокруг внешней системы для переиспользуемой логики.

**Зачем нужны:**

- Абстракция работы с внешними системами
- Переиспользование кода между DAG
- Автоматическое получение credentials из Connections
- Стандартизированный интерфейс

**Архитектура:**

```text
Connection (credentials) → Hook (логика) → Operator (задача в DAG)
```

**Пример:**

```python
class SimpleHTTPHook(BaseHook):
    def __init__(self, http_conn_id):
        self.http_conn_id = http_conn_id
    
    def get(self, endpoint):
        conn = self.get_connection(self.http_conn_id)
        url = f"{conn.schema}://{conn.host}{endpoint}"
        # ... выполнить запрос
        return response
```

### Operators (Операторы)

**Что это:**
Класс, описывающий ЧТО делает задача (Task) в DAG.

**Зачем создавать кастомные:**

- Инкапсуляция сложной логики
- Переиспользование между DAG
- Поддержка Jinja-шаблонов
- Стандартизация задач

**Пример:**

```python
class HttpToFileOperator(BaseOperator):
    template_fields = ['endpoint', 'output_path']  # Поддержка {{ ds }}
    
    def __init__(self, http_conn_id, endpoint, output_path, **kwargs):
        super().__init__(**kwargs)
        self.http_conn_id = http_conn_id
        self.endpoint = endpoint
        self.output_path = output_path
    
    def execute(self, context):
        hook = SimpleHTTPHook(self.http_conn_id)
        data = hook.get(self.endpoint)
        # Сохранить в файл
        return data
```

---

## Практическая часть

### 1. Подготовка окружения

#### 1.1 Запуск инфраструктуры

```bash
# Запуск Docker Compose
docker compose up -d

# Проверка статуса
docker compose ps

# Ожидание запуска (~30-60 секунд)
docker compose logs -f airflow

# Когда увидите "Webserver started" - откройте браузер
# http://localhost:8080
# admin / admin
```

#### 1.2 Создание Connection

**Через Web UI:**

1. Откройте **Admin → Connections**
2. Нажмите **+ (Add a new record)**
3. Заполните:
   - **Connection Id**: `simple_http_default`
   - **Connection Type**: `HTTP`
   - **Host**: `jsonplaceholder.typicode.com` (бесплатный тестовый API)
   - **Schema**: `https`
   - **Extra** (JSON):

     ```json
     {
       "timeout": 30
     }
     ```

4. Нажмите **Save**

**Альтернатива - через CLI:**

```bash
docker compose exec -it airflow /bin/bash

airflow connections add 'simple_http_default' \
    --conn-type 'HTTP' \
    --conn-host 'jsonplaceholder.typicode.com' \
    --conn-schema 'https' \
    --conn-extra '{"timeout": 30}'
```

**Альтернатива - через переменную окружения:**

```bash
# В docker-compose.yml добавить:
AIRFLOW_CONN_SIMPLE_HTTP_DEFAULT: "http://jsonplaceholder.typicode.com"
```

#### 1.3 Создание Variables

**Через Web UI:**

1. Откройте **Admin → Variables**
2. Нажмите **+ (Add a new record)**
3. Создайте переменные:

| Key | Value | Description |
|-----|-------|-------------|
| `env_name` | `dev` | Окружение |
| `feature_http_etl_enabled` | `true` | Feature flag |
| `http_items_limit` | `10` | Лимит записей |

**Альтернатива - через CLI:**

```bash
docker compose exec -i airflow /bin/bash

airflow variables set env_name "dev"
airflow variables set feature_http_etl_enabled "true"
airflow variables set http_items_limit "10"
```

---

### 2. Структура проекта

```text
advanced_airflow/
├── dags/
│   └── dag_01_hooks_operators.py        # Основной DAG
├── plugins/
│   ├── custom_hooks/
│   │   └── simple_http_hook.py          # Кастомный Hook
│   └── custom_operators/
│       └── http_to_file_operator.py     # Кастомный Operator
├── init/
│   └── simple_init.sql                  # Инициализация БД
├── data/
│   └── http/                            # Куда сохраняются данные
├── docker-compose.yml
└── README.md
```

---

### 3. Пошаговое изучение

#### Шаг 1: Работа с Connections

**Задача:** Научиться получать параметры подключения программно.

1. Откройте DAG в UI: `dag_01_hooks_operators`
2. Запустите DAG (Trigger DAG)
3. Откройте логи задачи `print_connection_info`

**Что наблюдать:**

```yaml
Connection simple_http_default:
  host=jsonplaceholder.typicode.com
  schema=https
  port=None
  extra_keys=['timeout']
```

**Упражнение 1:**

- Измените `host` в Connection на другой API
- Перезапустите DAG
- Убедитесь, что используется новый host

#### Шаг 2: Работа с Variables

**Задача:** Управлять конфигурацией через Variables.

1. Откройте логи задачи `print_variables_demo`

**Что наблюдать:**

```text
Current env_name=dev
Feature flag: feature_http_etl_enabled=true
HTTP items limit: 10
```

**Упражнение 2:**

- Измените `http_items_limit` на `20` через UI
- Перезапустите DAG
- Убедитесь, что новое значение используется

#### Шаг 3: Кастомный Hook

**Задача:** Понять как Hook использует Connection.

1. Откройте файл [plugins/custom_hooks/simple_http_hook.py](plugins/custom_hooks/simple_http_hook.py)
2. Изучите методы:
   - `get_conn_config()` - получает Connection
   - `build_url()` - строит URL
   - `get()` - выполняет GET-запрос

**Ключевые моменты:**

```python
# 1. Получение Connection
conn = self.get_connection(self.http_conn_id)

# 2. Безопасная работа с extra
extra = conn.extra_dejson or {}
timeout = extra.get('timeout', 30)

# 3. Построение URL
url = f"{conn.schema}://{conn.host}{endpoint}"

# 4. Логирование (без секретов!)
self.log.info(f"Requesting URL={url}")
```

**Упражнение 3:**

- Добавьте в Hook метод `post()` для POST-запросов
- Используйте его в новом операторе

#### Шаг 4: Кастомный Operator

**Задача:** Создать переиспользуемую задачу.

1. Откройте файл `plugins/custom_operators/http_to_file_operator.py`
2. Изучите структуру:

```python
class HttpToFileOperator(BaseOperator):
    # Поля для Jinja-шаблонов
    template_fields = ['endpoint', 'output_path', 'params']
    
    def __init__(self, http_conn_id, endpoint, output_path, **kwargs):
        super().__init__(**kwargs)
        # Сохраняем параметры
        
    def execute(self, context):
        # 1. Создаём Hook
        # 2. Получаем данные
        # 3. Сохраняем в файл
        # 4. Возвращаем результат
```

**Использование в DAG:**

```python
download_task = HttpToFileOperator(
    task_id='download_posts',
    http_conn_id='simple_http_default',
    endpoint='/posts',
    output_path='/opt/airflow/data/http/posts_{{ ds }}.json',
    params={'_limit': 10},
)
```

**Упражнение 4:**

- Запустите задачу `download_http_data`
- Проверьте файл в `/opt/airflow/data/http/`
- Изучите содержимое JSON

#### Шаг 5: Расширенный пример

**Задача:** Создать полный ETL pipeline.

В DAG добавлены дополнительные задачи:

```
print_connection_info ─┐
                       ├─> download_posts → process_posts → save_summary
print_variables_demo ──┘
```

1. `download_posts` - загружает данные через HTTP API
2. `process_posts` - обрабатывает JSON (фильтрация, трансформация)
3. `save_summary` - сохраняет статистику

**Упражнение 5:**

- Изучите логи всех задач
- Проверьте созданные файлы
- Модифицируйте обработку данных

---

## Практические упражнения

### Упражнение 1: Новый Hook для другого API

Создайте `GitHubHook`:

```python
class GitHubHook(BaseHook):
    def __init__(self, github_conn_id='github_default'):
        self.github_conn_id = github_conn_id
    
    def get_repo_info(self, owner, repo):
        """Получить информацию о репозитории"""
        # TODO: реализовать
        pass
    
    def list_issues(self, owner, repo):
        """Получить список issues"""
        # TODO: реализовать
        pass
```

**Задание:**

1. Создайте Connection для GitHub API
2. Реализуйте методы Hook
3. Напишите Operator `GitHubToFileOperator`
4. Создайте DAG для загрузки issues

### Упражнение 2: Operator с валидацией

Создайте `HttpToPostgresOperator`:

```python
class HttpToPostgresOperator(BaseOperator):
    def __init__(self, 
                 http_conn_id,
                 postgres_conn_id,
                 endpoint,
                 table_name,
                 **kwargs):
        # TODO: реализовать
    
    def execute(self, context):
        # 1. Получить данные через HttpHook
        # 2. Валидировать данные
        # 3. Загрузить в Postgres через PostgresHook
        pass
```

### Упражнение 3: Feature Flag

Модифицируйте DAG чтобы использовать feature flag:

```python
if Variable.get("enable_data_quality_check") == "true":
    validate_task = DataQualityOperator(...)
    download_task >> validate_task >> process_task
else:
    download_task >> process_task
```

---

## Домашнее задание

### Задание: Weather ETL Pipeline

Создайте ETL pipeline для получения данных о погоде.

#### Требования

**1. Connection: `weather_api_conn`**

- Type: HTTP
- Host: `www.weatherbit.io`
- Schema: `https`
- Extra: `{"key": "YOUR_API_KEY"}`

Получить бесплатный API key: https://www.weatherbit.io/

Endpoint:

- текущая погода -  `v2.0/current`
- предсказание погоды - `v2.0/forecast/daily`

**2. Variables:**

- `city`: название города (например, "Moscow")
- `units`: единицы измерения (`M` - [DEFAULT] Metric (Celsius, m/s, mm), `S` - Scientific (Kelvin, m/s, mm), `I` - Fahrenheit (F, mph, in))
- `lang`: язык ответа (`en` - [DEFAULT], `ru`)

**3. WeatherHook** (`plugins/custom_hooks/weather_hook.py`):

```python
class WeatherHook(BaseHook):
    def __init__(self, weather_conn_id='weather_api_conn'):
        """Инициализация Hook"""
        pass
    
    def get_current_weather(self, city, units='M', lang='en'):
        """
        Получить текущую погоду для города.
        
        Endpoint: v2.0/current
        
        Returns:
            dict: Данные о погоде
        """
        pass
    
    def get_forecast(self, city, units='M', lang='en'):
        """
        Получить прогноз погоды на 5 дней.
        
        Endpoint: v2.0/forecast/daily
        
        Returns:
            dict: Прогноз погоды
        """
        pass
```

**4. WeatherToFileOperator** (`plugins/custom_operators/weather_operator.py`):

```python
class WeatherToFileOperator(BaseOperator):
    template_fields = ['city', 'output_path']
    
    def __init__(self,
                 weather_conn_id,
                 city,
                 output_path,
                 forecast=False,
                 **kwargs):
        """
        Args:
            weather_conn_id: ID подключения
            city: Название города (может быть шаблон: {{ var.value.weather_city }})
            output_path: Путь для сохранения (может быть шаблон: /data/weather_{{ ds }}.json)
            forecast: Получить прогноз вместо текущей погоды
        """
        pass
    
    def execute(self, context):
        """
        1. Создать WeatherHook
        2. Получить данные о погоде
        3. Извлечь нужные поля (температура, влажность, описание)
        4. Сохранить в JSON файл
        5. Вернуть summary через XCom
        """
        pass
```

**5. DAG** (`dags/dag_weather_etl.py`):

```python
with DAG(
    dag_id='weather_etl_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@hourly',  # Каждый час
    catchup=False,
    tags=['homework', 'weather', 'etl'],
) as dag:
    
    # Задача 1: Получить текущую погоду
    current_weather = WeatherToFileOperator(
        task_id='get_current_weather',
        weather_conn_id='weather_api_conn',
        city='{{ var.value.weather_city }}',
        output_path='/opt/airflow/data/weather/current_{{ ds }}_{{ ts_nodash }}.json',
        forecast=False,
    )
    
    # Задача 2: Получить прогноз
    forecast_weather = WeatherToFileOperator(
        task_id='get_forecast',
        weather_conn_id='weather_api_conn',
        city='{{ var.value.weather_city }}',
        output_path='/opt/airflow/data/weather/forecast_{{ ds }}.json',
        forecast=True,
    )
    
    # Задача 3: Создать summary (PythonOperator)
    def create_weather_summary(**context):
        """
        Прочитать оба файла и создать summary:
        - Текущая температура
        - Мин/макс температура из прогноза
        - Средняя влажность
        - Описание погоды
        """
        pass
    
    summary = PythonOperator(
        task_id='create_summary',
        python_callable=create_weather_summary,
    )
    
    # Зависимости
    [current_weather, forecast_weather] >> summary
```

#### Порядок сдачи

1. **Код:**
   - `plugins/custom_hooks/weather_hook.py`
   - `plugins/custom_operators/weather_operator.py`
   - `dags/dag_weather_etl.py`

2. **Скриншоты:**
   - Connection `weather_api_conn` в UI
   - Variables в UI
   - Graph View выполненного DAG
   - Лог задачи `create_summary`
   - Содержимое одного из JSON файлов

3. **Документация:**
   - `HOMEWORK.md` с описанием:
     - Как получить API key
     - Как настроить Connection
     - Какие Variables создать
     - Как запустить DAG
     - Примеры выходных данных

### Bonus задания

**Bonus 1: Уведомления**
Добавьте задачу, которая отправляет уведомление если:

- Температура < 0°C
- Влажность > 80%
- Скорость ветра > 10 m/s

**Bonus 2: История погоды**
Сохраняйте данные в Postgres:

```sql
CREATE TABLE weather_history (
    id SERIAL PRIMARY KEY,
    city VARCHAR(100),
    temperature FLOAT,
    humidity INT,
    description TEXT,
    recorded_at TIMESTAMP
);
```

**Bonus 3: Сравнение городов**
Получайте погоду для 3 городов параллельно:

```python
cities = ['Moscow', 'London', 'New York']

for city in cities:
    task = WeatherToFileOperator(
        task_id=f'weather_{city.lower()}',
        city=city,
        ...
    )
```

---

## Best Practices

### 1. Безопасность

```python
# ПЛОХО: Пароли в коде
password = "my_secret_password"

# ХОРОШО: Использование Connection
conn = BaseHook.get_connection('my_conn')
password = conn.password

# ПЛОХО: Логирование секретов
self.log.info(f"Password: {password}")

# ХОРОШО: Логирование безопасных данных
self.log.info(f"Host: {conn.host}")
```

### 2. Переиспользование

```python
# ПЛОХО: Дублирование кода в DAG
task1 = PythonOperator(
    task_id='task1',
    python_callable=lambda: requests.get('http://...'),
)

# ХОРОШО: Использование Hook/Operator
task1 = HttpToFileOperator(
    task_id='task1',
    http_conn_id='my_api',
    ...
)
```

### 3. Идемпотентность

```python
def execute(self, context):
    # Проверяем существование файла
    if os.path.exists(self.output_path):
        self.log.info("File already exists, skipping")
        return
    
    # Загружаем данные
    data = self.hook.get(self.endpoint)
    
    # Атомарная запись
    with open(self.output_path + '.tmp', 'w') as f:
        json.dump(data, f)
    
    os.rename(self.output_path + '.tmp', self.output_path)
```

---

## Дополнительные ресурсы

- [Connections Documentation](https://airflow.apache.org/docs/apache-airflow/stable/howto/connection.html)
- [Variables Documentation](https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/variables.html)
- [Custom Hooks](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html)
- [Custom Operators](https://airflow.apache.org/docs/apache-airflow/stable/howto/custom-operator.html)

---

## Чек-лист

- `[ ]` Установил и запустил Airflow
- `[ ]` Создал Connection `simple_http_default`
- `[ ]` Создал Variables (env_name, feature_http_etl_enabled, http_items_limit)
- `[ ]` Запустил DAG `dag_01_hooks_operators`
- `[ ]` Изучил логи всех задач
- `[ ]` Просмотрел созданные файлы в `/data/http/`
- `[ ]` Изучил код Hook в `plugins/custom_hooks/`
- `[ ]` Изучил код Operator в `plugins/custom_operators/`
- `[ ]` Выполнил упражнения 1-5
- `[ ]` Начал выполнение домашнего задания
- `[ ]` Получил API key для сервиса погоды
- `[ ]` Создал WeatherHook
- `[ ]` Создал WeatherToFileOperator
- `[ ]` Создал DAG для погоды
- `[ ]` Протестировал весь pipeline

---
