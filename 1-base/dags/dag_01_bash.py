"""
DAG: Основы работы с BashOperator

Цель:
-----
Познакомиться с базовой структурой DAG и научиться использовать BashOperator
для выполнения простых bash-команд.

Что демонстрирует:
------------------
- Создание простого DAG
- Использование BashOperator для выполнения bash-команд
- Определение последовательных зависимостей между задачами
- Использование Jinja-шаблонов в командах ({{ dag.dag_id }}, {{ ds }})

Структура:
----------
start → hello → date → pwd → system_info → temp_file → bye → end
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


# =============================================================================
# ПАРАМЕТРЫ ПО УМОЛЧАНИЮ
# =============================================================================
# Эти параметры применяются ко всем задачам в DAG, если не переопределены

default_args = {
    'owner': 'data_engineer',        # Владелец DAG (для организации)
    'depends_on_past': False,        # Не зависит от успешности предыдущих запусков
    'start_date': datetime(2025, 12, 1),  # С какой даты начинать выполнение
    'email': ['student@example.com'],     # Email для уведомлений
    'email_on_failure': False,       # Отправлять email при ошибке?
    'email_on_retry': False,         # Отправлять email при повторе?
    'retries': 1,                    # Сколько раз повторять при ошибке
    'retry_delay': timedelta(minutes=5),  # Пауза между повторами
}


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id='dag_01_bash',            # Уникальный ID DAG (должен быть уникальным!)
    default_args=default_args,       # Применяем параметры выше
    description='Простой DAG с BashOperator',  # Описание для веб-интерфейса
    schedule_interval='@daily',      # Расписание: каждый день в 00:00
                                     # Другие варианты: '@hourly', '@weekly', '0 12 * * *'
    catchup=False,                   # НЕ запускать пропущенные интервалы
                                     # (полезно при разработке)
    tags=['tutorial', 'bash', 'basics'],  # Теги для фильтрации в UI
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ (TASKS)
# =============================================================================

# Задача 0: Dummy task для обозначения начала DAG
# DummyOperator ничего не делает, нужен для логической структуры
start_task = DummyOperator(
    task_id='start',                 # ID задачи (уникальный в рамках DAG)
    dag=dag,                         # К какому DAG относится
)


# Задача 1: Приветствие
# BashOperator выполняет bash-команду и возвращает её результат
hello_task = BashOperator(
    task_id='print_hello',           # ID должен быть описательным
    bash_command='echo "Hello, Airflow! Добро пожаловать в ETL-процессы!"',
    dag=dag,
)


# Задача 2: Вывод текущей даты
# Полезно для проверки времени выполнения
print_date = BashOperator(
    task_id='print_date',
    bash_command='date',             # Простая команда без аргументов
    dag=dag,
)


# Задача 3: Вывод текущей рабочей директории
# Показывает, где выполняются команды (обычно /opt/airflow)
print_working_directory = BashOperator(
    task_id='print_working_directory',
    bash_command='pwd',
    dag=dag,
)


# Задача 4: Информация о системе
# Демонстрирует выполнение нескольких команд через &&
print_system_info = BashOperator(
    task_id='print_system_info',
    # && означает "выполнить следующую команду только если предыдущая успешна"
    bash_command='''
        echo "Информация о системе:" && \
        echo "Hostname: $(hostname)" && \
        echo "User: $(whoami)" && \
        echo "Python: $(python3 --version)"
    ''',
    dag=dag,
)


# Задача 5: Создание временного файла с использованием Jinja-шаблонов
# Демонстрирует использование переменных Airflow в bash-командах
create_temp_file = BashOperator(
    task_id='create_temp_file',
    bash_command='''
        echo "Создание временного файла..." && \
        echo "Этот файл создан DAG: {{ dag.dag_id }}" > /tmp/airflow_test.txt && \
        echo "Дата выполнения: {{ ds }}" >> /tmp/airflow_test.txt && \
        echo "Execution date: {{ execution_date }}" >> /tmp/airflow_test.txt && \
        echo "" && \
        echo "Содержимое файла:" && \
        cat /tmp/airflow_test.txt
    ''',
    # Jinja-шаблоны:
    # {{ dag.dag_id }} - ID текущего DAG
    # {{ ds }} - дата выполнения в формате YYYY-MM-DD
    # {{ execution_date }} - полная дата и время выполнения
    dag=dag,
)


# Задача 6: Прощание
bye_task = BashOperator(
    task_id='print_bye',
    bash_command='echo "Good Bye! DAG завершён успешно."',
    dag=dag,
)


# Задача 7: Dummy task для обозначения конца DAG
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ (DEPENDENCIES)
# =============================================================================
# Определяет, в каком порядке выполняются задачи

# Способ 1: Использование оператора >> (рекомендуется)
# Читается как: "start выполняется перед hello, hello перед date, и т.д."
start_task >> hello_task >> print_date >> print_working_directory >> \
    print_system_info >> create_temp_file >> bye_task >> end_task

# Альтернативные способы (НЕ рекомендуются, но полезно знать):
#
# Способ 2: set_downstream (более многословно)
# start_task.set_downstream(hello_task)
# hello_task.set_downstream(print_date)
# ...
#
# Способ 3: Оператор << (обратное направление)
# end_task << bye_task << create_temp_file << ...
#
# Способ 4: Параллельное выполнение (пример на будущее)
# task1 >> [task2, task3] >> task4
# Означает: task2 и task3 выполнятся параллельно после task1,
# затем task4 выполнится после обоих


# =============================================================================
# ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ
# =============================================================================

"""
Как запустить этот DAG:
-----------------------

1. Через веб-интерфейс:
   - Откройте http://localhost:8080
   - Найдите 'dag_01_bash' в списке
   - Включите toggle справа (если DAG отключён)
   - Нажмите ▶️ (Play) → "Trigger DAG"

2. Через CLI (из контейнера):
   docker exec -it airflow_main bash
   airflow dags trigger dag_01_bash

3. Тестирование отдельной задачи:
   docker exec -it airflow_main bash
   airflow tasks test dag_01_bash print_date 2025-12-01


Как посмотреть результаты:
--------------------------

1. Graph View:
   - Откройте DAG → вкладка Graph
   - Увидите визуализацию всех задач и их связей
   - Зелёный цвет = успех, красный = ошибка

2. Логи задачи:
   - Кликните на задачу в Graph View
   - Выберите "Log"
   - Увидите вывод bash-команды

3. Gantt Chart:
   - Откройте DAG → вкладка Gantt
   - Увидите временную диаграмму выполнения


Частые проблемы:
----------------

Проблема: DAG не появляется в списке
Решение: 
  - Проверьте синтаксис: python dags/dag_01_bash.py
  - Проверьте логи: docker compose logs airflow | grep ERROR
  - Подождите 30-60 секунд (Airflow обновляет список DAG периодически)

Проблема: Задача зависла в queued
Решение:
  - Проверьте, что scheduler запущен: docker compose ps
  - Посмотрите логи scheduler: docker compose logs airflow


Упражнения:
-----------

1. Добавьте задачу, которая создаёт директорию:
   bash_command='mkdir -p /tmp/my_airflow_folder'

2. Добавьте задачу, которая выводит список файлов в /tmp:
   bash_command='ls -lh /tmp'

3. Создайте параллельное выполнение:
   task1 >> [task2_a, task2_b] >> task3

4. Используйте другие Jinja-переменные:
   {{ run_id }}, {{ task.task_id }}, {{ ts }}

5. Измените schedule_interval на '@hourly' и посмотрите разницу
"""
