# Тема 1 – Основы работы с Airflow: DAG, BashOperator, PythonOperator

## Цель

1. Научиться описывать DAG в Airflow (id, расписание, зависимости).
2. Показать последовательное и параллельное выполнение задач.
3. Использовать BashOperator и PythonOperator.

## Последовательность действий

### 1. Развёртывание инфраструктуры

#### 1.1 Структура проекта

Содание папок проекта для airflow:

```bash
mkdir -p init dags src data plugins
```

#### 1.2 Инициализация БД

Создание и редактирование файла [init/simple_init.sql](init/simple_init.sql), создающий БД airflow_db для хранения служебной информации и метаданных Airflow.

#### 1.3 Файл docker-compose.yml

Файл [docker_compose.yml](docker_compose.yml), описывающий контейнеры:

1. __Postgres__ - СУБД для хранения метаданных Airflow
2. __PgAdmin4__ - web-интерфейс для работы с СУБД Postgres
3. __Airflow__ - ETL-система

#### 1.4 Запуск Airflow

```bash
# Запуск всей инфраструктуры
docker compose up -d

# Проверка статуса контейнеров
docker compose ps

# Просмотр логов
docker compose logs -f airflow

# Остановка
docker compose down

# Полная очистка данных
docker compose down -v --remove-orphans
docker volume prune -f

# Подождать инициализации (~30 секунд)
# Открыть браузер: http://localhost:8080
# Логин: admin / Пароль: admin
```

### 2. Web-интерфейс Airflow

#### Основные разделы

- __DAGs__: список всех DAG
- __Graph__: визуализация графа задач
- __Gantt__: временная диаграмма выполнения
- __Task Duration__: анализ времени выполнения
- __Admin → XCom__: просмотр переданных данных
- __Browse → Task Instances__: история выполнения задач

#### Полезные фильтры

- __Теги__: используйте теги для группировки DAG (tutorial, etl, parallel)
- __Поиск__: найти DAG по имени или описанию
- __Состояние__: фильтр по success/failed/running

#### Порядок действий

1. В web-интерфейсе Airflow найти список всех DAG-ов.
2. Запустить DAG-и вручную по-очереди.
3. Открыть вкладку __Graph__ и проанализировать ход выполнения задач для каждого DAG-а
4. В конвейере `dag_03_parallel` во вкладке __Graph__ проанализировать параллельное выполнение задач: после `print_context` две задачи (`process_chunk_a`, `process_chunk_b`) должны выполняться параллельно.
5. В конвейерах `dag_01_bash` и `dag_03_parallel` на задачах открыть вкладку __Logs__ и проанализировать вывод bash.
6. В конвейерах `dag_01_python` и `dag_03_parallel` на задачах открыть вкладку __Logs__ и проанализировать содержимое контекста (`context`) и аргументов Python-функций.
7. В разделе __XCOM__ посмотрите на результат выполнения задач из конвейера `dag_04_xcom`.

## Домашнее задание: «Сервис утренних проверок»

### Задача

Сделать свой DAG `morning_checks`, который имитирует ежедневную проверку состояния системы.

### Требования

__Параметры DAG__

```json
{
  schedule_interval="@daily",
  catchup=False
}
```

Глобальные default_args с retries и retry_delay.

### Задачи DAG

1. `print_start_info` (PythonOperator):

    Вывести `ds`, `run_id`, `data_interval_start` и `data_interval_end`.

2. `check_disk` (BashOperator):

    В Linux-контейнере: вывести процент занятого диска (`df -h | head -n 5`).

3. `check_db` (PythonOperator, можно заглушку):

    _«Проверка подключения к БД»_ — просто логирование сообщения и условная «симуляция» успеха/ошибки через параметр.

4. `send_summary` (PythonOperator):

    Собрать результаты предыдущих задач (через XCom или просто логами) и вывести краткий текстовый отчёт.

### Граф DAG

```python
print_start_info >> [check_disk, check_db] >> send_summary
```

__Дополнительно(*):__

Параметризовать поведение `check_db` через `Variable` (например, `db_check_mode = "ok" | "fail"`), чтобы изменить результат без изменения кода.

### Порядок сдачи

1. Файл с описанием DAG `morning_checks.py`.
2. Скриншот графа DAG в UI и пример логов `send_summary`.

## Best Practices

### 1. Именование

```python
# Хорошо
dag_id = 'etl_users_daily'
task_id = 'extract_from_postgres'

# Плохо
dag_id = 'dag1'
task_id = 'task1'
```

### 2. Параметры по умолчанию

```python
default_args = {
    'owner': 'data_team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
}
```

### 3. Зависимости

```python
# Предпочтительный способ
task1 >> task2 >> task3

# Параллельное выполнение
task1 >> [task2, task3] >> task4
```

### 4. XCom

```python
# Не передавайте большие данные (>1MB)
# Используйте для координации, а не для данных

# Хорошо
return {'status': 'success', 'count': 1000}

# Плохо
return huge_dataframe  # Используйте S3/файл вместо этого
```

## Частые ошибки

### DAG не появляется

```bash
# Проверьте синтаксис
python ~/airflow/dags/your_dag.py

# Проверьте путь
echo $AIRFLOW_HOME
ls -la $AIRFLOW_HOME/dags/

# Перезапустите airflow
docker compose down
docker compose up -d
```

## Ресурсы

- Официальная документация – https://airflow.apache.org/docs/
- Best Practices – https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html


## Чек-лист для практики

  `[ ]` Установил Airflow  
  `[ ]` Создал пользователя  
  `[ ]` Запустил webserver и scheduler  
  `[ ]` Скопировал DAG файлы в ~/airflow/dags/  
  `[ ]` Запустил dag_01_simple_bash  
  `[ ]` Изучил Graph View  
  `[ ]` Просмотрел логи задачи  
  `[ ]` Запустил dag_03_parallel  
  `[ ]` Изучил Gantt Chart (параллельное выполнение)  
  `[ ]` Посмотрел XCom в веб-интерфейсе (dag_04)  
  `[ ]` Протестировал задачу через CLI  
  `[ ]` Создал свой первый DAG  
  `[ ]` Сделал домашнее задание  

---
