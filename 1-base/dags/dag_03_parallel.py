"""
DAG: Параллельное и последовательное выполнение задач

Цель:
-----
Научиться создавать DAG с комбинацией последовательного и параллельного
выполнения задач для оптимизации времени обработки.

Что демонстрирует:
------------------
- Комбинацию BashOperator и PythonOperator
- Последовательное выполнение задач
- Параллельное выполнение независимых задач
- Передачу параметров через op_kwargs
- Имитацию реальной обработки данных

Структура:
----------
                                    ┌─ process_chunk_a (10 сек)
start → greet → show_date → print →│
                                    └─ process_chunk_b (15 сек)
                                               ↓
                                            finish → end

Время выполнения:
- Последовательно: ~25 секунд
- Параллельно: ~15 секунд (экономия 40%!)

Автор: Курс "ETL - Автоматизация подготовки данных с использованием Apache Airflow"
"""

from datetime import datetime, timedelta
from time import sleep
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging


# =============================================================================
# ПАРАМЕТРЫ ПО УМОЛЧАНИЮ
# =============================================================================

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# =============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# =============================================================================

def print_context(**context):
    """
    Выводит информацию о контексте выполнения.
    
    Эта функция полезна для отладки и понимания того,
    какие данные доступны в контексте Airflow.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("КОНТЕКСТ ВЫПОЛНЕНИЯ")
    logger.info("-" * 50)
    logger.info(f"  Дата выполнения (ds): {context['ds']}")
    logger.info(f"  Run ID: {context['run_id']}")
    logger.info(f"  DAG ID: {context['dag'].dag_id}")
    logger.info(f"  Task ID: {context['task'].task_id}")
    logger.info("-" * 50)


def process_data_chunk(chunk_name, timeout=1, **context):
    """
    Имитирует обработку части данных.
    В реальном приложении это могла бы быть обработка:
    - Части большого файла
    - Данных за определённый период
    - Данных из конкретного источника
    Аргументы:
    ----------
    chunk_name : str
        Название обрабатываемой части (например, "chunk_a", "chunk_b")
    timeout : int
        Время обработки в секундах (имитация)
    **context : dict
        Контекст Airflow
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info(f"ОБРАБОТКА ДАННЫХ: {chunk_name.upper()}")
    logger.info("-" * 50)
    logger.info(f"  Начало обработки: {chunk_name}")
    logger.info(f"  Ожидаемое время: {timeout} секунд")
    
    # Имитация долгой обработки
    # В реальности здесь была бы настоящая обработка данных
    for i in range(timeout):
        logger.info(f"{chunk_name}: обработано {i+1}/{timeout} секунд")
        sleep(1)
    
    logger.info(f"  Обработка {chunk_name} завершена!")
    logger.info("-" * 50)
    
    # Возвращаем результат (будет сохранён в XCom)
    return f"processed_{chunk_name}"


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id='dag_03_parallel',
    default_args=default_args,
    description='DAG с параллельным и последовательным выполнением',
    schedule_interval='@daily',
    catchup=False,
    tags=['tutorial', 'parallel', 'basics'],
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# =============================================================================

# Начало DAG
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)


# Задача 1: Приветствие (BashOperator)
greet_task = BashOperator(
    task_id='greet_task',
    # $(date) - вставит текущую дату/время в bash
    bash_command='echo "DAG запущен в: $(date)"',
    dag=dag,
)


# Задача 2: Показать дату выполнения (BashOperator с Jinja)
show_date = BashOperator(
    task_id='show_date',
    # {{ ds }} - дата выполнения из Airflow (Jinja-шаблон)
    bash_command='echo "Дата выполнения Airflow: {{ ds }}"',
    dag=dag,
)


# Задача 3: Вывести контекст (PythonOperator)
print_context_task = PythonOperator(
    task_id='print_context',
    python_callable=print_context,
    provide_context=True,
    dag=dag,
)


# ============================================
# ПАРАЛЛЕЛЬНЫЕ ЗАДАЧИ
# ============================================
# Эти задачи будут выполняться ОДНОВРЕМЕННО
# Это экономит время когда задачи независимы

# Параллельная задача A: Обработка первой части данных
process_chunk_a = PythonOperator(
    task_id='process_chunk_a',
    python_callable=process_data_chunk,
    op_kwargs={
        'chunk_name': 'chunk_a',
        'timeout': 10,                # Эта задача займёт 10 секунд
    },
    dag=dag,
)


# Параллельная задача B: Обработка второй части данных
process_chunk_b = PythonOperator(
    task_id='process_chunk_b',
    python_callable=process_data_chunk,
    op_kwargs={
        'chunk_name': 'chunk_b',
        'timeout': 15,                # Эта задача займёт 15 секунд
    },
    dag=dag,
)


# Задача 4: Завершение (BashOperator)
finish_task = BashOperator(
    task_id='finish',
    bash_command='echo "DAG завершён в: $(date)"',
    dag=dag,
)


# Конец DAG
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# =============================================================================

# Последовательное выполнение
start_task >> greet_task >> show_date >> print_context_task

# ПАРАЛЛЕЛЬНОЕ ВЕТВЛЕНИЕ
# После print_context_task запускаются ДВЕ задачи ОДНОВРЕМЕННО
print_context_task >> [process_chunk_a, process_chunk_b]

# После завершения ОБЕИХ параллельных задач запускается finish_task
[process_chunk_a, process_chunk_b] >> finish_task >> end_task

# Альтернативная запись (то же самое):
# print_context_task >> process_chunk_a >> finish_task
# print_context_task >> process_chunk_b >> finish_task


# =============================================================================
# ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ
# =============================================================================

"""
Параллельное выполнение - ключевые моменты:
-------------------------------------------

1. Синтаксис параллельного ветвления:
   task1 >> [task2, task3] >> task4
   
   Означает:
   - task1 выполнится первым
   - task2 и task3 выполнятся ОДНОВРЕМЕННО после task1
   - task4 выполнится только после ОБОИХ (task2 И task3)

2. Когда использовать ПАРАЛЛЕЛЬНОЕ выполнение:
   - Задачи независимы (не зависят друг от друга)
   - Обработка разных источников данных
   - Параллельная обработка частей большого датасета
   - Независимые проверки/валидации
   
   Когда НЕ использовать:
   - Задачи зависят друг от друга (task2 нужны данные из task1)
   - Ограничены ресурсы (CPU, память)
   - Внешний API имеет rate limit

3. Визуализация в Airflow:
   - Graph View: покажет ветвление графа
   - Gantt Chart: покажет что задачи выполняются одновременно
   
4. Экономия времени:
   Последовательно: 10 + 15 = 25 секунд
   Параллельно: max(10, 15) = 15 секунд
   Экономия: 10 секунд (40%)


Как запустить и наблюдать:
--------------------------

1. Запустите DAG:
   http://localhost:8080 → dag_03_parallel → Trigger DAG

2. Откройте Graph View:
   - Увидите визуализацию параллельного ветвления
   - process_chunk_a и process_chunk_b на одном уровне

3. Откройте Gantt Chart:
   - Увидите что chunk_a и chunk_b выполняются одновременно
   - Их временные полосы будут overlap (пересекаться)

4. Посмотрите логи:
   - В логах chunk_a увидите обработку 10 секунд
   - В логах chunk_b увидите обработку 15 секунд
   - Но общее время выполнения ~15 секунд, не 25!


Упражнения:
-----------

1. Добавьте третью параллельную задачу:
   process_chunk_c = PythonOperator(
       task_id='process_chunk_c',
       python_callable=process_data_chunk,
       op_kwargs={'chunk_name': 'chunk_c', 'timeout': 8},
   )
   
   И обновите зависимости:
   print_context_task >> [process_chunk_a, process_chunk_b, process_chunk_c]

2. Создайте две параллельные ветки с разной логикой:
   
   ветка A: validate_data_a → process_data_a → save_data_a
   ветка B: validate_data_b → process_data_b → save_data_b
   
   Обе ветки запускаются параллельно!

3. Измените timeout задач и сравните время выполнения:
   - Сделайте chunk_a: 20 секунд, chunk_b: 5 секунд
   - Посмотрите в Gantt, что общее время ~20 секунд

4. Создайте DAG с "песочными часами":
   
   start → prepare → [branch_a, branch_b, branch_c] → merge → end
   
   Сначала одна задача, потом три параллельно, потом одна


Реальные примеры использования:
--------------------------------

1. ETL из нескольких источников:
   extract_postgres >> process_postgres
   extract_mongo >> process_mongo        } параллельно
   extract_api >> process_api
   
   [process_postgres, process_mongo, process_api] >> merge_data

2. Обработка файлов:
   split_file >> [process_part1, process_part2, process_part3] >> merge_results

3. Множественные проверки:
   prepare >> [check_data, check_schema, check_duplicates] >> final_validation


Частые ошибки:
--------------

ОШИБКА: Создание циклических зависимостей
task1 >> task2 >> task3 >> task1  # ЦИКЛ! Не работает!

ОШИБКА: Зависимость между параллельными задачами
[task_a, task_b]  # Это не создаёт зависимости между ними!
task_a >> task_b  # Нужно явно указать если task_b зависит от task_a

ПРАВИЛЬНО: Параллельные задачи независимы
print_context_task >> [process_chunk_a, process_chunk_b]
# chunk_a и chunk_b выполнятся параллельно, независимо друг от друга
"""
