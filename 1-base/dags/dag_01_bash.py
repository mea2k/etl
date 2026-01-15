"""
DAG 1: Простой пример использования BashOperator

Описание:
---------
Этот DAG демонстрирует базовые возможности Apache Airflow:
- Создание простого DAG
- Использование BashOperator для выполнения команд
- Определение зависимостей между задачами

Структура:
----------
start_task → hello_task → print_date → print_working_directory → bye_task → end_task

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator


# Параметры по умолчанию для всех задач в DAG
default_args = {
    'owner': 'data_engineer',                    # Владелец DAG
    'depends_on_past': False,                    # Не зависит от предыдущих запусков
    'start_date': datetime(2025, 12, 1),         # Дата начала выполнения
    'email': ['student@example.com'],            # Email для уведомлений
    'email_on_failure': False,                   # Не отправлять email при ошибке
    'email_on_retry': False,                     # Не отправлять email при повторе
    'retries': 1,                                # Количество попыток при ошибке
    'retry_delay': timedelta(minutes=5),         # Задержка перед повтором
}


# Создание объекта DAG
dag = DAG(
    'dag_01_bash',                        # Уникальный идентификатор DAG
    default_args=default_args,                   # Применяем параметры по умолчанию
    description='Простой DAG с BashOperator',    # Описание для веб-интерфейса
    schedule_interval=timedelta(days=1),         # Запуск каждый день
    catchup=False,                               # Не запускать пропущенные интервалы
    tags=['tutorial', 'bash', 'basics'],         # Теги для фильтрации в UI
)


# Задача 0: Начало DAG (используется для логической структуры)
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Задача 1: Вывод текста
# BashOperator выполняет bash-команду
hello_task = BashOperator(
    task_id='print_hello',
    bash_command='echo "Hello, World!"',         # Команда для выполнения
    dag=dag,
)

# Задача 2: Вывод текущей даты
# BashOperator выполняет bash-команду
print_date = BashOperator(
    task_id='print_date',
    bash_command='date',                         # Команда для выполнения
    dag=dag,
)

# Задача 3: Вывод текущей рабочей директории
print_working_directory = BashOperator(
    task_id='print_working_directory',
    bash_command='pwd',
    dag=dag,
)

# Задача 4: Вывод информации о системе
print_system_info = BashOperator(
    task_id='print_system_info',
    bash_command='echo "Hostname: $(hostname)" && echo "User: $(whoami)" && echo "Python: $(python3 --version)"',
    dag=dag,
)


# Задача 5: Создание временного файла и вывод его содержимого
create_temp_file = BashOperator(
    task_id='create_temp_file',
    bash_command='''
        echo "Создание временного файла..." && \
        echo "Этот файл создан DAG: {{ dag.dag_id }}" > /tmp/airflow_test.txt && \
        echo "Дата выполнения: {{ ds }}" >> /tmp/airflow_test.txt && \
        echo "Содержимое файла:" && \
        cat /tmp/airflow_test.txt
    ''',
    dag=dag,
)


# Задача 6: Вывод текста
# BashOperator выполняет bash-команду
bye_task = BashOperator(
    task_id='print_hello',
    bash_command='echo "Good Bye!"',
    dag=dag,
)


# Задача 7: Завершение DAG
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


# Определение порядка выполнения задач
# Способ 1: Использование оператора >> (рекомендуемый)
start_task >> hello_task >> print_date >> print_working_directory >> print_system_info >> create_temp_file >> bye_task >> end_task

# Альтернативные способы определения зависимостей:
#
# Способ 2: Использование set_downstream/set_upstream
# start_task.set_downstream(print_date)
# print_date.set_downstream(print_working_directory)
# и т.д.
#
# Способ 3: Использование оператора <<
# end_task << create_temp_file << print_system_info << print_working_directory << print_date << start_task

