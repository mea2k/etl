from __future__ import annotations

import sys
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable  # для работы с Variables[web:54]
from airflow.hooks.base import BaseHook

# Добавляем путь к плагинам
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../plugins'))

from custom_operators.http_to_file_operator import HttpToFileOperator


def print_connection_info(**context):
    """
    Демонстрация программного доступа к Connection.
    Печатаем host и extra по conn_id, ничего чувствительного не логируем.
    """
    conn_id = "simple_http_default"
    conn = BaseHook.get_connection(conn_id)
    # ВАЖНО: логировать только безопасные поля (host, schema, port).
    print(f"Connection {conn_id}: host={conn.host}, schema={conn.schema}, port={conn.port}")
    print(f"Connection extra keys: {list((conn.extra_dejson or {}).keys())}")


def print_variables_demo(**context):
    """
    Демонстрация Airflow Variables.
    """
    # Variable.get с default — безопасный способ, если переменная ещё не создана
    env_name = Variable.get("env_name", default_var="dev")
    feature_flag = Variable.get("feature_http_etl_enabled", default_var="true")

    print(f"Current env_name={env_name}")
    print(f"Feature flag: feature_http_etl_enabled={feature_flag}")


with DAG(
    dag_id="dag_01_advanced",
    description="Hooks, custom Operators, Connections, Variables",
    start_date=datetime(2025, 12, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["advanced", "hooks", "operators", "connections", "variables"],
) as dag:

    # 1. Задача 1 показывает, как получить Connection программно
    t_print_conn = PythonOperator(
        task_id="print_connection_info",
        python_callable=print_connection_info,
    )

    # 2. Задача 2 демонстрирует работу с Variables
    t_print_vars = PythonOperator(
        task_id="print_variables_demo",
        python_callable=print_variables_demo,
    )

    # 3. Задача 3: получение данных по HTTP и сохранение в файл через кастомный Operator
    t_http_to_file = HttpToFileOperator(
        task_id="download_http_data",
        http_conn_id="simple_http_default",    # Connection, заданный через UI/ENV
        endpoint="/posts",                     # будет шаблонизироваться, если нужно
        output_path="/opt/airflow/data/http/items_{{ ds }}.json",  # шаблон в пути
        params={"limit": 10},                       # дефолтные параметры запроса
    )

    # Пример простых зависимостей: 
    # сначала проверяем настройки, затем выполняем запрос
    [t_print_conn, t_print_vars] >> t_http_to_file
