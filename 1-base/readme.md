# Тема 1 – Основы работы с Airflow: DAG, BashOperator, PythonOperator

## Цель

1. Научиться описывать DAG в Airflow (id, расписание, зависимости).
2. Показать последовательное и параллельное выполнение задач.
3. Использовать BashOperator и PythonOperator.
4. Передавать данные между задачами через XCom

## Необходимые знания

- Базовое знание Python
- Понимание командной строки Linux/Bash
- Базовое понимание Docker и утилиты docker compose (для запуска окружения)

## Последовательность действий

### 1. Развёртывание инфраструктуры

#### 1.1 Структура проекта

Содание папок проекта для airflow:

```bash
# Корневая папка проекта
mkdir -p airflow_project
cd airflow_project
# структура проекта
mkdir -p init dags src data plugins logs
```

**Назначение директорий:**

- `init/` - SQL-скрипты для инициализации БД
- `dags/` - файлы с описанием DAG (наши конвейеры)
- `src/` - вспомогательные Python-модули
- `data/` - данные для обработки
- `plugins/` - кастомные плагины Airflow
- `logs/` - логи выполнения задач

#### 1.2 Инициализация БД

**Файл:** `init/simple_init.sql` ([init/simple_init.sql](init/simple_init.sql))

Этот скрипт создаёт:

- БД `airflow_db` для метаданных Airflow
- Пользователя `airflow` с необходимыми правами

#### 1.3 Файл docker-compose.yml

Файл [docker_compose.yml](docker_compose.yml) описывает контейнеры:

1. __Postgres__ - СУБД для хранения метаданных Airflow
2. __PgAdmin4__ - web-интерфейс для работы с БД _(опционально)_
3. __Airflow__ - ETL-система

#### 1.4 Запуск Airflow

**Пошаговая инструкция:**

```bash
# 1. Проверка, что Docker запущен
docker --version
docker compose version

# 2. Запуск всей инфраструктуры
docker compose up -d

# 3. Проверка статуса контейнеров
docker compose ps
# Должно появиться 3 контейнера: postgres, pgadmin, airflow

# 4. Просмотр логов Airflow
docker compose logs -f airflow

# 5. Когда появится надпись "Webserver started" - можно открывать браузер
# URL: http://localhost:8080
# Логин: admin
# Пароль: admin
```

**Полезные команды:**

```bash
# Остановка всех контейнеров
docker compose down

# Полная очистка данных (БД + volumes)
docker compose down -v --remove-orphans
docker volume prune -f

# Перезапуск только Airflow
docker compose restart airflow

# Просмотр логов конкретного сервиса
docker compose logs -f postgres
docker compose logs -f airflow

# Вход в контейнер Airflow
docker compose exec -i airflow /bin/bash
```

### 2. Web-интерфейс Airflow

#### 2.1 Основные разделы

После входа в web-интерфейс (`http://localhost:8080`) появятся разделы:

1. **DAGs (главная страница)**

    - Список всех DAG
    - Статус выполнения
    - График запусков
    - Возможность включить/отключить DAG

2. **Graph (вкладка в DAG)**

    - Визуализация графа задач
    - Статус каждой задачи (зелёный = success, красный = failed)
    - Возможность перезапустить задачу

3. **Gantt (вкладка в DAG)**

    - Временная диаграмма выполнения
    - Показывает параллельное vs последовательное выполнение
    - Полезно для оптимизации времени выполнения

4. **Task Duration**

    - Анализ времени выполнения задач
    - Сравнение между запусками

5. **Admin → XCom**

    - Просмотр данных, переданных между задачами
    - Отладка передачи данных

6. **Browse → Task Instances**

    - История выполнения всех задач
    - Логи и метаданные

#### 2.2 Работа с фильтрами

**Теги**

Используйте теги для группировки DAG:

```python
tags=['tutorial', 'etl', 'parallel']
```

**Поиск**

- По имени DAG
- По описанию
- По владельцу

**Состояние**

- `Success` (зелёный)
- `Failed` (красный)
- `Running` (зелёный с анимацией)
- `Queued` (серый)


#### 2.3 Практическая работа с интерфейсом

**Задание 1: Запуск DAG**

1. Откройте web-интерфейс Airflow
2. Найдите DAG `dag_01_bash`
3. Включите его (`toggle` справа)
4. Нажмите ▶️ (`Play`) → `"Trigger DAG"`
5. Дождитесь выполнения (~10 секунд)

**Задание 2: Анализ графа**

1. Откройте DAG `dag_01_bash`
2. Перейдите во вкладку **Graph**
3. Проанализируйте:
   - Сколько задач в DAG?
   - Какие задачи выполняются последовательно?
   - Есть ли параллельные ветки?

**Задание 3: Просмотр логов**

1. В графе кликните на задачу `print_date`
2. Выберите **Log**
3. Найдите:
   - Дату и время запуска
   - Вывод команды `date`
   - Статус выполнения

**Задание 4: Параллельное выполнение**

1. Запустите DAG `dag_03_parallel`
2. Откройте вкладку **Gantt**
3. Убедитесь, что `process_chunk_a` и `process_chunk_b` выполняются **одновременно**
4. Сравните время выполнения с последовательным вариантом

**Задание 5: Просмотр XCom**

1. Запустите DAG `dag_04_xcom`
2. Перейдите в **Admin → XCom**
3. Найдите записи для вашего DAG run
4. Посмотрите, какие данные передаются между задачами

---

## Описание демонстрационных DAG

### DAG 01: Основы BashOperator

**Файл:** [dags/dag_01_bash.py](dags/dag_01_bash.py)

**Что демонстрирует:**

- Базовая структура DAG
- Использование BashOperator
- Последовательное выполнение задач
- Использование Jinja-шаблонов в Bash-командах

**Граф:**

```
start → print_hello → print_date → print_working_directory → 
print_system_info → create_temp_file → print_bye → end
```

**Ключевые моменты:**

```python
# 1. Определение DAG
dag = DAG(
    'dag_01_bash',                    # ID должен быть уникальным
    schedule_interval='@daily',       # Расписание запуска
    catchup=False,                    # Не запускать пропущенные интервалы
)

# 2. Создание задачи
print_date = BashOperator(
    task_id='print_date',             # ID задачи (уникальный в рамках DAG)
    bash_command='date',              # Команда для выполнения
    dag=dag,
)

# 3. Зависимости (порядок выполнения)
task1 >> task2 >> task3               # task2 после task1, task3 после task2
```

### DAG 02: Основы PythonOperator

**Файл:** [dags/dag_02_python.py](dags/dag_02_python.py)

**Что демонстрирует:**

- PythonOperator
- Передача параметров через `op_args`
- Работа с контекстом `**context`
- Логирование

**Граф:**

```
start → greet_user → calculate_sum → get_execution_info → analyze_data → end
```

**Ключевые моменты:**

```python
# 1. Функция для задачи
def greet_user(**context):
    logger = logging.getLogger(__name__)
    logger.info(f"DAG ID: {context['dag'].dag_id}")
    return "Success"

# 2. Создание задачи с функцией
greet_task = PythonOperator(
    task_id='greet_user',
    python_callable=greet_user,       # Ссылка на функцию
    provide_context=True,             # Передать context в функцию
    dag=dag,
)

# 3. Передача аргументов
calculate_task = PythonOperator(
    task_id='calculate_sum',
    python_callable=calculate_sum,
    op_args=[10, 25],                 # Позиционные аргументы
    provide_context=True,
    dag=dag,
)
```

### DAG 03: Параллельное выполнение

**Файл:** [dags/dag_03_parallel.py](dags/dag_03_parallel.py)

**Что демонстрирует:**

- Параллельное и последовательное выполнение
- Комбинация BashOperator и PythonOperator
- Передача параметров через `op_kwargs`

**Граф:**

```
start → greet_task → show_date → print_context → 
                                  ├─ process_chunk_a (параллельно)
                                  └─ process_chunk_b (параллельно)
                                               ↓
                                          bye_task → end
```

**Ключевые моменты:**

```python
# Параллельное ветвление
print_context >> [process_chunk_a, process_chunk_b] >> bye_task

# Это означает:
# 1. Сначала выполнится print_context
# 2. Затем ОДНОВРЕМЕННО process_chunk_a И process_chunk_b
# 3. После их завершения - bye_task
```

### DAG 04: Передача данных через XCom

**Файл:** [dags/dag_04_xcom.py](dags/dag_04_xcom.py)

**Что демонстрирует:**

- Передача данных между задачами
- `xcom_push()` и `xcom_pull()`
- Автоматическая передача через `return`
- Best practices для XCom

**Граф:**

```
start → generate_data → ├─ process_data (параллельно)
                         ├─ calculate_stats (параллельно)
                         └─ generate_additional_data (параллельно)
                                      ↓
                              combine_results → save_report → 
                              xcom_best_practices → end
```

**Ключевые моменты:**

```python
# 1. Передача данных через return (автоматически)
def generate_data(**context):
    data = {'numbers': [1, 2, 3]}
    return data  # Автоматически сохраняется в XCom

# 2. Явная передача через xcom_push
def generate_additional_data(**context):
    ti = context['task_instance']
    ti.xcom_push(key='my_data', value={'a': 1})

# 3. Получение данных
def process_data(**context):
    ti = context['task_instance']
    data = ti.xcom_pull(task_ids='generate_data')  # Получаем return value
    additional = ti.xcom_pull(
        task_ids='generate_additional_data',
        key='my_data'  # Получаем конкретный ключ
    )
```

## Домашнее задание: «Сервис утренних проверок»

### Задача

Создайте DAG `morning_checks`, который имитирует ежедневную проверку состояния системы.

### Требования

**1. Параметры DAG**

```python
dag = DAG(
    'morning_checks',
    schedule_interval='@daily',       # Запуск каждый день
    start_date=datetime(2025, 12, 1),
    catchup=False,
    default_args={
        'owner': 'student',
        'retries': 2,                 # Две попытки при ошибке
        'retry_delay': timedelta(minutes=3),
    },
    tags=['homework', 'monitoring'],
)
```

**2. Задачи DAG**

#### Задача 1: `print_start_info` (PythonOperator)

Функция должна вывести в лог:

- `ds` (дата выполнения в формате YYYY-MM-DD)
- `run_id` (ID запуска)
- `data_interval_start` (начало интервала данных)
- `data_interval_end` (конец интервала данных)

```python
def print_start_info(**context):
    logger = logging.getLogger(__name__)
    logger.info("=" * 50)
    logger.info("ИНФОРМАЦИЯ О ЗАПУСКЕ")
    logger.info("=" * 50)
    logger.info(f"Дата выполнения (ds): {context['ds']}")
    logger.info(f"Run ID: {context['run_id']}")
    # TODO: добавьте data_interval_start и data_interval_end
    logger.info("=" * 50)
```

#### Задача 2: `check_disk` (BashOperator)

Команда должна вывести информацию о дисковом пространстве:

```python
check_disk = BashOperator(
    task_id='check_disk',
    bash_command='df -h | head -n 5 | sleep 3',  # Первые 5 строк информации о дисках
    dag=dag,
)
```

#### Задача 3: `check_db` (PythonOperator)

Функция должна имитировать проверку подключения к БД:

```python
def check_db(**context):
    logger = logging.getLogger(__name__)
    logger.info("Проверка подключения к БД...")
    
    # Симуляция проверки
    import random
    is_connected = random.choice([True, True, True, False])  # 75% успех
    
    # Ожидаем подключение к БД
    sleep(5)

    if is_connected:
        logger.info("  БД доступна")
        return {'status': 'ok', 'response_time_ms': random.randint(10, 100)}
    else:
        logger.error("  БД недоступна")
        raise Exception("Database connection failed")
```

#### Задача 4: `send_summary` (PythonOperator)

Функция должна собрать результаты предыдущих задач через XCom:

```python
def send_summary(**context):
    ti = context['task_instance']
    
    # Получаем результаты
    db_result = ti.xcom_pull(task_ids='check_db')
    
    logger.info("=" * 50)
    logger.info("ИТОГОВЫЙ ОТЧЁТ")
    logger.info("=" * 50)
    logger.info(f"БД статус: {db_result.get('status')}")
    logger.info(f"Время ответа: {db_result.get('response_time_ms')} мс")
    # TODO: добавьте информацию из других задач
    logger.info("=" * 50)
```

**3. Граф зависимостей**

```python
print_start_info >> [check_disk, check_db] >> send_summary
```

Это означает:

1. Сначала выполняется `print_start_info`
2. Затем **параллельно** `check_disk` и `check_db`
3. После завершения обеих - `send_summary`

### Дополнительное задание (*)

Параметризуйте поведение `check_db` через Airflow Variable:

```python
from airflow.models import Variable

def check_db(**context):
    # Получаем режим из переменной Airflow
    mode = Variable.get('db_check_mode', default_var='ok')
    
    if mode == 'ok':
        # Всегда успех
        return {'status': 'ok'}
    elif mode == 'fail':
        # Всегда ошибка
        raise Exception("Database check failed")
    else:
        # Случайный результат
        ...
```

Установить переменную можно через:
- Веб-интерфейс: Admin → Variables → Add a new record
- CLI: `airflow variables set db_check_mode ok`

### Порядок сдачи

1. **Файл DAG**: `morning_checks.py`
2. **Скриншот 1**: Graph View с выполненным DAG
3. **Скриншот 2**: Логи задачи `send_summary`
4. **Краткое описание** (2-3 предложения) о том, что делает ваш DAG

---

## Best Practices

### 1. Именование

```python
# Хорошо: понятные, описательные имена
dag_id = 'etl_users_daily'
task_id = 'extract_from_postgres'

# Плохо: непонятные сокращения
dag_id = 'dag1'
task_id = 'task1'
```

### 2. Параметры по умолчанию

```python
# Хорошо: настройте retry и уведомления
default_args = {
    'owner': 'data_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['team@example.com'],
}

# Плохо: без retry задача упадёт при временной ошибке
default_args = {
    'owner': 'me',
}
```

### 3. Зависимости

```python
# Хорошо: используйте >> для читаемости
# Предпочтительный способ
task1 >> task2 >> task3

# Хорошо: параллельное выполнение
task1 >> [task2, task3] >> task4

# Избегайте: длинные цепочки set_downstream
task1.set_downstream(task2)
task2.set_downstream(task3)
# ...
```

### 4. Работа с XCom

```python
# Не передавайте большие данные (>1MB)
# Используйте для координации, а не для данных

# Хорошо: передавайте только метаданные
return {'status': 'success', 'count': 1000}

# Плохо: не передавайте большие данные
return huge_dataframe  # Используйте S3/файл вместо этого

# Хорошо: проверяйте наличие данных
data = ti.xcom_pull(task_ids='previous_task')
if data is None:
    raise ValueError("No data received")

# Плохо: без проверки
result = data['key']  # Упадёт если data=None
```

### 5. Логирование

```python
# Хорошо: используйте logger
import logging
logger = logging.getLogger(__name__)
logger.info("Processing started")
logger.error("Failed to connect")

# Плохо: print не попадёт в логи Airflow корректно
print("Processing started")
```

---

## Частые ошибки

### Ошибка 1: DAG не появляется в списке

**Симптомы:**

- Скопировали файл в `dags/`, но DAG не отображается

**Решения:**

```bash
# 1. Проверьте синтаксис Python
python dags/your_dag.py

# 3. Проверьте путь
docker compose exec -i airflow bash
ls /opt/airflow/dags/

# 3. Перезапустите airflow
docker compose restart airflow
```

### Ошибка 2: Задачи зависают в queued

**Симптомы:**

- Задачи не запускаются, висят в очереди

**Решения:**

```bash
# 1. Проверьте, что scheduler запущен
docker compose ps

# 2. Проверьте executor
docker compose exec -i airflow /bin/bash
airflow config get-value core executor
# Должно быть: LocalExecutor

# 3. Проверьте логи scheduler
docker compose logs airflow
```

### Ошибка 3: ImportError при запуске задачи

**Симптомы:**
- Задача завершается с ошибкой `ModuleNotFoundError`

**Решения:**

```bash
# 1. Установите пакет в контейнер
docker compose exec -i airflow /bin/bash
pip install <package_name>

# 2. Или добавьте в docker-compose.yml
command:
  - -c
  - |
    pip install pandas numpy <your_package>
    airflow webserver &
    airflow scheduler
```

### Ошибка 4: Duplicate task_id

**Симптомы:**

- Ошибка `Duplicate task_id 'task_name'`

**Решение:**

```python
# Плохо: одинаковые task_id
hello_task = BashOperator(task_id='print_hello', ...)
bye_task = BashOperator(task_id='print_hello', ...)  # ← ОШИБКА!

# Хорошо: уникальные task_id
hello_task = BashOperator(task_id='print_hello', ...)
bye_task = BashOperator(task_id='print_bye', ...)
```

---

## Полезные ресурсы

- [Официальная документация Airflow](https://airflow.apache.org/docs/)
- [Best Practices](https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html)
- [Примеры DAG](https://github.com/apache/airflow/tree/main/airflow/example_dags)
- [Airflow Concepts](https://airflow.apache.org/docs/apache-airflow/stable/concepts/index.html)

---

## Чек-лист для практики

Отметьте выполненные шаги:

**Установка и запуск:**

- `[ ]` Создал структуру проекта
- `[ ]` Запустил Docker Compose
- `[ ]` Открыл веб-интерфейс (http://localhost:8080)
- `[ ]` Вошёл под admin/admin

**Работа с DAG:**

- `[ ]` Запустил `dag_01_bash`
- `[ ]` Изучил Graph View
- `[ ]` Просмотрел логи задачи
- `[ ]` Запустил `dag_02_python`
- `[ ]` Проанализировал контекст в логах

**Параллельное выполнение:**

- `[ ]` Запустил `dag_03_parallel`
- `[ ]` Изучил Gantt Chart
- `[ ]` Убедился в параллельном выполнении

**Передача данных:**

- `[ ]` Запустил `dag_04_xcom`
- `[ ]` Посмотрел XCom в Admin
- `[ ]` Изучил передачу данных

**Домашнее задание:**

- `[ ]` Создал `morning_checks.py`
- `[ ]` Протестировал DAG
- `[ ]` Сделал скриншоты
- `[ ]` Подготовил к сдаче

---

## Что дальше?

После освоения Темы 1 переходите к:

- **Тема 2:** Дополнительные инструменты и возможности Apache Airflow (Operators, Connectors, Hooks)
- **Тема 3:** Извлечение данных из различных источников (Postgres, MongoDB, API, CSV)
- **Тема 4:** Трансформация и валидация данных
- **Тема 5:** Загрузка данных и создание витрин

---
