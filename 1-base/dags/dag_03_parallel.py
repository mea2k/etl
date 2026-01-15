"""
DAG 3: Параллельный и последовательный запуск задач в DAG

Описание:
---------
Этот DAG демонстрирует использование PythonOperator и BashOperator для
создания задач, которые выполняются как последовательно, так и параллельно.

Структура:
----------
start → show_date → print_context → [process_chunk_a, process_chunk_b] → finish
"""

from datetime import datetime, timedelta
from time import sleep
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator


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

def _print_context(**context):
    print("Execution date:", context["ds"])
    print("Run ID:", context["run_id"])


def _process_data(task_param: str, timeout: int = 1,  **context):
    print(f"Processing chunk: {task_param}")
    sleep(timeout)
    return f"processed_{task_param}"

# ============================================================================
# СОЗДАНИЕ DAG
# ============================================================================

dag = DAG(
    'dag_03_parallel',
    default_args=default_args,
    description='Простой DAG с BashOperator, PythonOperator',
    schedule_interval="@daily",
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

greet_task = BashOperator(
    task_id="greet_task",
    bash_command="echo 'DAG started at $(date)'",
)

show_date = BashOperator(
    task_id="show_date",
    bash_command="echo 'Today is: {{ ds }}'",
)

print_context = PythonOperator(
    task_id="print_context",
    python_callable=_print_context,
    provide_context=True,
)

# Параллельная ветка обработки
process_chunk_a = PythonOperator(
    task_id="process_chunk_a",
    python_callable=_process_data,
    op_kwargs={"task_param": "chunk_a", "timeout": 10},
)

process_chunk_b = PythonOperator(
    task_id="process_chunk_b",
    python_callable=_process_data,
    op_kwargs={"task_param": "chunk_b", "timeout": 15},
)

bye_task = BashOperator(
    task_id="finish",
    bash_command="echo 'Lesson 1 DAG finished at $(date)'",
)

# Завершающая задача
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Последовательное выполнение
start_task >> greet_task >> show_date >> print_context

# Параллельное разветвление
print_context >> [process_chunk_a, process_chunk_b] >> bye_task >> end_task