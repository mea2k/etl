"""
DAG: Основы работы с PythonOperator

Цель:
-----
Научиться использовать PythonOperator для выполнения Python-функций как задач,
работать с контекстом выполнения и передавать параметры.

Что демонстрирует:
------------------
- PythonOperator для выполнения Python-функций
- Работа с контекстом (**context)
- Передача параметров через op_args и op_kwargs
- Логирование в Airflow
- Возврат значений из функций

Структура:
----------
start → greet → calculate → get_info → analyze → end
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import random


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
# Каждая функция станет отдельной задачей в DAG

def greet_user(**context):
    """
    Функция приветствия.
    Аргументы:
    ----------
    **context : dict
        Контекст выполнения Airflow, содержит информацию о DAG, задаче,
        времени выполнения и многое другое.
    Примеры ключей в context:
    - dag: объект DAG
    - task: объект Task
    - execution_date: дата/время выполнения
    - ds: дата выполнения в формате YYYY-MM-DD
    - run_id: ID текущего запуска
    """
    # Получаем logger для вывода в логи Airflow
    # ВАЖНО: используйте logger, а не print()!
    logger = logging.getLogger(__name__)
    
    # Извлекаем полезную информацию из контекста
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    
    # Выводим информацию в лог
    logger.info("-" * 50)
    logger.info("ПРИВЕТСТВИЕ ОТ APACHE AIRFLOW!")
    logger.info("-" * 50)
    logger.info(f"DAG ID: {dag_id}")
    logger.info(f"Task ID: {task_id}")
    logger.info(f"Дата выполнения: {execution_date}")
    logger.info("-" * 50)
    
    # Функция может возвращать значение
    # Оно автоматически сохранится в XCom (об этом в dag_04)
    return "Приветствие выполнено успешно"


def calculate_sum(a, b, **context):
    """
    Функция для выполнения вычислений.
    Демонстрирует передачу параметров в функцию через op_args.
    Аргументы:
    ----------
    a : int
        Первое число
    b : int
        Второе число
    **context : dict
        Контекст Airflow (необязательно, но полезно)
    Возвращает:
    -----------
    int
        Сумма a и b
    """
    logger = logging.getLogger(__name__)
    
    # Выполняем вычисление
    result = a + b
    
    # Логируем результат
    logger.info("-" * 50)
    logger.info("ВЫЧИСЛЕНИЕ")
    logger.info("-" * 50)
    logger.info(f"  Вычисляем: {a} + {b} = {result}")
    logger.info("-" * 50)
    
    # Возвращаем результат (он сохранится в XCom)
    return result


def get_execution_info(**context):
    """
    Функция для получения информации о выполнении DAG.
    Демонстрирует доступ к различным параметрам контекста.
    Полезные ключи контекста:
    -------------------------
    ds : str
        Дата выполнения в формате YYYY-MM-DD
    ds_nodash : str
        Дата выполнения в формате YYYYMMDD
    ts : str
        Timestamp в формате ISO
    run_id : str
        ID текущего запуска DAG
    dag_run : DagRun
        Объект текущего запуска
    """
    logger = logging.getLogger(__name__)
    
    # Извлекаем различные параметры
    ds = context['ds']                    # Дата: 2025-12-01
    ds_nodash = context['ds_nodash']      # Дата: 20251201
    ts = context['ts']                    # Timestamp: 2025-12-01T00:00:00+00:00
    run_id = context['run_id']            # ID запуска
    dag_run = context['dag_run']          # Объект DagRun
    
    # Выводим информацию
    logger.info("-" * 50)
    logger.info("ИНФОРМАЦИЯ О ВЫПОЛНЕНИИ")
    logger.info("-" * 50)
    logger.info(f"  Дата выполнения (ds): {ds}")
    logger.info(f"  Дата без дефисов (ds_nodash): {ds_nodash}")
    logger.info(f"  Timestamp (ts): {ts}")
    logger.info(f"  Run ID: {run_id}")
    logger.info(f"  Тип запуска: {dag_run.run_type}")  # manual, scheduled, etc.
    logger.info("-" * 50)
    
    # Возвращаем словарь с информацией
    return {
        'ds': ds,
        'ds_nodash': ds_nodash,
        'ts': ts,
        'run_id': run_id,
        'run_type': dag_run.run_type
    }


def analyze_data(**context):
    """
    Функция для имитации анализа данных.
    Демонстрирует:
    - Генерацию тестовых данных
    - Базовые вычисления
    - Вывод статистики
    """
    logger = logging.getLogger(__name__)
    
    # Генерируем случайные данные для демонстрации
    # В реальном проекте здесь были бы настоящие данные
    data = [random.randint(1, 100) for _ in range(20)]
    
    # Вычисляем простые статистики
    total = sum(data)
    average = total / len(data)
    minimum = min(data)
    maximum = max(data)
    
    # Выводим результаты
    logger.info("-" * 50)
    logger.info("АНАЛИЗ ДАННЫХ")
    logger.info("-" * 50)
    logger.info(f"  Количество элементов: {len(data)}")
    logger.info(f"  Данные: {data}")
    logger.info(f"  Сумма: {total}")
    logger.info(f"  Среднее: {average:.2f}")
    logger.info(f"  Минимум: {minimum}")
    logger.info(f"  Максимум: {maximum}")
    logger.info("-" * 50)
    
    # Возвращаем результаты
    return {
        'count': len(data),
        'total': total,
        'average': average,
        'min': minimum,
        'max': maximum
    }


def demonstrate_op_kwargs(person_name, age, city="Unknown", **context):
    """
    Демонстрирует передачу именованных параметров через op_kwargs.
    
    Аргументы:
    ----------
    person_name : str
        Имя человека
    age : int
        Возраст
    city : str, optional
        Город (по умолчанию "Unknown")
    **context : dict
        Контекст Airflow
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("ДЕМОНСТРАЦИЯ OP_KWARGS")
    logger.info("-" * 50)
    logger.info(f"  Имя: {person_name}")
    logger.info(f"  Возраст: {age}")
    logger.info(f"  Город: {city}")
    logger.info("-" * 50)
    
    return f"Обработан пользователь: {person_name}"


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id='dag_02_python',
    default_args=default_args,
    description='Простой DAG с PythonOperator',
    schedule_interval='@daily',
    catchup=False,
    tags=['tutorial', 'python', 'basics'],
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# =============================================================================

# Начало DAG
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Задача 1: Приветствие (функция с контекстом)
greet_task = PythonOperator(
    task_id='greet_user',
    python_callable=greet_user,       # Ссылка на функцию (БЕЗ скобок!)
    provide_context=True,             # Передать **context в функцию
    dag=dag,
)

# Задача 2: Вычисление (функция с параметрами через op_args)
calculate_task = PythonOperator(
    task_id='calculate_sum',
    python_callable=calculate_sum,
    op_args=[10, 25],                 # Позиционные аргументы: a=10, b=25
    provide_context=True,
    dag=dag,
)

# Задача 3: Информация о выполнении
execution_info_task = PythonOperator(
    task_id='get_execution_info',
    python_callable=get_execution_info,
    provide_context=True,
    dag=dag,
)

# Задача 4: Анализ данных
analyze_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    provide_context=True,
    dag=dag,
)

# Задача 5: Демонстрация op_kwargs (именованные параметры)
kwargs_task = PythonOperator(
    task_id='demonstrate_kwargs',
    python_callable=demonstrate_op_kwargs,
    op_kwargs={                       # Именованные аргументы
        'person_name': 'Иван Иванов',
        'age': 30,
        'city': 'Москва'
    },
    provide_context=True,
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

# Последовательное выполнение всех задач
start_task >> greet_task >> calculate_task >> execution_info_task >> \
    analyze_task >> kwargs_task >> end_task


# =============================================================================
# ДОПОЛНИТЕЛЬНАЯ ИНФОРМАЦИЯ
# =============================================================================

"""
Ключевые концепции:
-------------------

1. PythonOperator vs BashOperator:
   - BashOperator: выполняет bash-команды
   - PythonOperator: выполняет Python-функции

2. Передача параметров:
   - op_args: список позиционных аргументов [1, 2, 3]
   - op_kwargs: словарь именованных аргументов {'a': 1, 'b': 2}
   - provide_context: передаёт **context в функцию

3. Контекст (**context):
   Содержит полезную информацию:
   - dag: объект DAG
   - task: объект Task
   - ds: дата в формате YYYY-MM-DD
   - execution_date: дата/время выполнения
   - run_id: ID запуска
   - И многое другое!

4. Возвращаемые значения:
   - Функция может возвращать значение
   - Оно автоматически сохраняется в XCom
   - Другие задачи могут получить это значение (см. dag_04)


Как запустить:
--------------

1. Через веб-интерфейс:
   http://localhost:8080 → dag_02_python → Trigger DAG

2. Через CLI:
   docker exec -it airflow_main bash
   airflow dags trigger dag_02_python

3. Тестирование конкретной задачи:
   airflow tasks test dag_02_python greet_user 2025-12-01


Как посмотреть результаты:
--------------------------

1. Откройте DAG → Graph View
2. Кликните на задачу → Log
3. Изучите вывод в логах


Упражнения:
-----------

1. Создайте функцию, которая:
   - Принимает список чисел через op_args
   - Возвращает квадраты этих чисел
   - Выводит результат в лог

   Подсказка:
   def square_numbers(numbers, **context):
       result = [n**2 for n in numbers]
       logger.info(f"Квадраты: {result}")
       return result

2. Добавьте функцию, которая:
   - Читает переменную окружения
   - Выводит её значение в лог
   
   Подсказка:
   import os
   value = os.getenv('MY_VAR', 'default')

3. Модифицируйте analyze_data():
   - Используйте ds из контекста как seed для random
   - Это сделает данные детерминированными
   
   Подсказка:
   seed = int(context['ds_nodash'])
   random.seed(seed)

4. Создайте функцию с обработкой ошибок:
   def process_with_error_handling(**context):
       try:
           # Ваш код
           pass
       except Exception as e:
           logger.error(f"Ошибка: {str(e)}")
           raise  # Пробросить ошибку дальше


Частые ошибки:
--------------

python_callable=greet_user()  # ОШИБКА: со скобками!
python_callable=greet_user    # ПРАВИЛЬНО: без скобок

logger = logging.getLogger('my_logger')  # Работает, но не рекомендуется
logger = logging.getLogger(__name__)     # Правильный подход

print("Hello")                # Работает, но не попадёт в логи Airflow
logger.info("Hello")          # Правильно: попадёт в логи
"""
