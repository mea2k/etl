"""
DAG 2: Простой пример использования PythonOperator

Описание:
---------
Этот DAG демонстрирует использование PythonOperator:
- Выполнение Python-функций как задач
- Логирование внутри задач
- Работа с контекстом выполнения
- Использование параметров функций

Структура:
----------
start → greet_user → calculate_sum → get_execution_info → analyze_data → end

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import random


# Параметры по умолчанию
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# ============================================================================
# ФУНКЦИИ ДЛЯ ЗАДАЧ
# ============================================================================

def greet_user(**context):
    """
    Простая функция приветствия.
    
    Параметр **context позволяет получить доступ к контексту выполнения Airflow,
    включая информацию о DAG, задаче, времени выполнения и т.д.
    """
    # Получаем logger для вывода информации в логи Airflow
    logger = logging.getLogger(__name__)
    
    # Извлекаем информацию из контекста
    dag_id = context['dag'].dag_id
    task_id = context['task'].task_id
    execution_date = context['execution_date']
    
    # Логируем информацию
    logger.info("=" * 50)
    logger.info("Приветствие от Apache Airflow!")
    logger.info(f"DAG ID: {dag_id}")
    logger.info(f"Task ID: {task_id}")
    logger.info(f"Дата выполнения: {execution_date}")
    logger.info("=" * 50)
    
    return "Приветствие выполнено успешно"


def calculate_sum(a: int, b: int, **context):
    """
    Функция для выполнения вычислений.
    
    Демонстрирует:
    - Передачу параметров в функцию через op_args
    - Выполнение вычислений
    - Возврат результата
    """
    logger = logging.getLogger(__name__)
    
    result = a + b
    
    logger.info(f"Вычисление: {a} + {b} = {result}")
    
    # Результат можно вернуть для дальнейшего использования (XCom)
    return result


def get_execution_info(**context):
    """
    Функция для получения информации о выполнении.
    
    Демонстрирует доступ к различным параметрам контекста:
    - ds: execution date в формате YYYY-MM-DD
    - ds_nodash: execution date в формате YYYYMMDD
    - ts: timestamp в формате ISO
    - dag_run: объект DagRun
    """
    logger = logging.getLogger(__name__)
    
    # Извлекаем различные параметры контекста
    ds = context['ds']                          # Дата выполнения (YYYY-MM-DD)
    ds_nodash = context['ds_nodash']            # Дата выполнения (YYYYMMDD)
    ts = context['ts']                          # Timestamp (ISO формат)
    dag_run = context['dag_run']                # Объект текущего запуска DAG
    
    logger.info("=" * 50)
    logger.info("ИНФОРМАЦИЯ О ВЫПОЛНЕНИИ:")
    logger.info(f"Дата выполнения (ds): {ds}")
    logger.info(f"Дата выполнения без дефисов (ds_nodash): {ds_nodash}")
    logger.info(f"Timestamp (ts): {ts}")
    logger.info(f"Run ID: {dag_run.run_id}")
    logger.info(f"Тип запуска: {dag_run.run_type}")
    logger.info("=" * 50)
    
    return {
        'ds': ds,
        'ds_nodash': ds_nodash,
        'ts': ts,
        'run_id': dag_run.run_id
    }


def analyze_data(**context):
    """
    Функция для имитации анализа данных.
    
    Демонстрирует:
    - Генерацию данных
    - Базовую обработку
    - Вывод статистики
    """
    logger = logging.getLogger(__name__)
    
    # Генерируем случайные данные для анализа
    data = [random.randint(1, 100) for _ in range(20)]
    
    # Вычисляем статистики
    total = sum(data)
    average = total / len(data)
    minimum = min(data)
    maximum = max(data)
    
    logger.info("=" * 50)
    logger.info("АНАЛИЗ ДАННЫХ:")
    logger.info(f"Количество элементов: {len(data)}")
    logger.info(f"Данные: {data}")
    logger.info(f"Сумма: {total}")
    logger.info(f"Среднее: {average:.2f}")
    logger.info(f"Минимум: {minimum}")
    logger.info(f"Максимум: {maximum}")
    logger.info("=" * 50)
    
    return {
        'count': len(data),
        'total': total,
        'average': average,
        'min': minimum,
        'max': maximum
    }


# ============================================================================
# СОЗДАНИЕ DAG
# ============================================================================

dag = DAG(
    'dag_02_python',
    default_args=default_args,
    description='Простой DAG с PythonOperator',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['tutorial', 'python', 'basics'],
)


# ============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# ============================================================================

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# PythonOperator с доступом к контексту
greet_task = PythonOperator(
    task_id='greet_user',
    python_callable=greet_user,                  # Функция для выполнения
    provide_context=True,                        # Передать контекст в функцию
    dag=dag,
)

# PythonOperator с параметрами
calculate_task = PythonOperator(
    task_id='calculate_sum',
    python_callable=calculate_sum,
    op_args=[10, 25],                            # Позиционные аргументы для функции
    provide_context=True,
    dag=dag,
)

# PythonOperator для получения информации о выполнении
execution_info_task = PythonOperator(
    task_id='get_execution_info',
    python_callable=get_execution_info,
    provide_context=True,
    dag=dag,
)

# PythonOperator для анализа данных
analyze_task = PythonOperator(
    task_id='analyze_data',
    python_callable=analyze_data,
    provide_context=True,
    dag=dag,
)

# Завершающая задача
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


# ============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# ============================================================================

# Последовательное выполнение всех задач
start_task >> greet_task >> calculate_task >> execution_info_task >> analyze_task >> end_task


"""
Упражнения для самостоятельной работы:
---------------------------------------

1. Создайте новую функцию, которая принимает список чисел через op_args
   и возвращает их квадраты.

2. Добавьте функцию, которая читает переменную окружения и логирует её значение.

3. Модифицируйте analyze_data, чтобы она генерировала данные на основе
   параметра из контекста (например, используйте ds для seed генератора).

4. Создайте функцию с op_kwargs (именованными параметрами) вместо op_args.

5. Раскомментируйте raise в process_with_error_handling и посмотрите,
   как Airflow обрабатывает падение задачи.
"""
