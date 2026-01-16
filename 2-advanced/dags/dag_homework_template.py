"""
ШАБЛОН ДЛЯ ДОМАШНЕГО ЗАДАНИЯ: Weather ETL Pipeline

Этот файл содержит заготовки для выполнения домашнего задания.
Заполните TODO секции своим кодом.

Задание: Создать ETL pipeline для получения данных о погоде.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator

# TODO: Импортируйте ваши кастомные классы
# from custom_hooks.weather_hook import WeatherHook
# from custom_operators.weather_operator import WeatherToFileOperator


# =============================================================================
# ПАРАМЕТРЫ DAG
# =============================================================================

default_args = {
    'owner': 'student',  # TODO: Укажите своё имя
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================

def create_weather_summary(**context):
    """
    Создание summary на основе загруженных данных о погоде.
    
    TODO: Реализуйте эту функцию:
    1. Получите результаты из XCom (task_ids='get_current_weather' и 'get_forecast')
    2. Прочитайте JSON файлы
    3. Извлеките нужные данные:
       - Текущая температура
       - Мин/макс температура из прогноза
       - Средняя влажность
       - Описание погоды
    4. Сохраните summary в JSON файл
    5. Верните результат
    
    Args:
        **context: Контекст Airflow (содержит task_instance, ds, и т.д.)
    
    Returns:
        dict: Summary с ключевыми метриками
    """
    import logging
    import json
    
    logger = logging.getLogger(__name__)
    ti = context['task_instance']
    
    logger.info("-" * 50)
    logger.info("СОЗДАНИЕ WEATHER SUMMARY")
    logger.info("-" * 50)
    
    # TODO: Получите результаты из XCom
    # current_result = ti.xcom_pull(task_ids='get_current_weather')
    # forecast_result = ti.xcom_pull(task_ids='get_forecast')
    
    # TODO: Прочитайте файлы
    # with open(current_result['file_path'], 'r') as f:
    #     current_data = json.load(f)
    
    # TODO: Извлеките данные
    # current_temp = current_data['main']['temp']
    # humidity = current_data['main']['humidity']
    # description = current_data['weather'][0]['description']
    
    # TODO: Обработайте прогноз
    # ...
    
    # TODO: Создайте summary
    summary = {
        'execution_date': context['ds'],
        'city': 'TODO',
        'current_temperature': 0,  # TODO
        'min_temperature': 0,       # TODO
        'max_temperature': 0,       # TODO
        'avg_humidity': 0,          # TODO
        'description': 'TODO',      # TODO
    }
    
    # TODO: Сохраните summary
    # summary_path = '/opt/airflow/data/weather/summary_{{ ds }}.json'
    # with open(summary_path, 'w') as f:
    #     json.dump(summary, f, indent=2)
    
    logger.info(f"Summary: {summary}")
    logger.info("-" * 50)
    
    return summary


# =============================================================================
# СОЗДАНИЕ DAG
# =============================================================================

dag = DAG(
    dag_id='weather_etl_pipeline',  # TODO: Можете изменить название
    default_args=default_args,
    description='ETL pipeline для получения данных о погоде',
    schedule_interval='@hourly',  # Каждый час
    catchup=False,
    tags=['homework', 'weather', 'etl'],
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# =============================================================================

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)


# TODO: Задача 1 - Получение текущей погоды
# current_weather_task = WeatherToFileOperator(
#     task_id='get_current_weather',
#     weather_conn_id='weather_api_conn',
#     city='{{ var.value.weather_city }}',  # Из Variable
#     output_path='/opt/airflow/data/weather/current_{{ ds }}_{{ ts_nodash }}.json',
#     forecast=False,
#     dag=dag,
# )


# TODO: Задача 2 - Получение прогноза погоды
# forecast_task = WeatherToFileOperator(
#     task_id='get_forecast',
#     weather_conn_id='weather_api_conn',
#     city='{{ var.value.weather_city }}',
#     output_path='/opt/airflow/data/weather/forecast_{{ ds }}.json',
#     forecast=True,
#     dag=dag,
# )


# Задача 3 - Создание summary
summary_task = PythonOperator(
    task_id='create_summary',
    python_callable=create_weather_summary,
    provide_context=True,
    dag=dag,
)


end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


# =============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# =============================================================================

# TODO: Определите зависимости
# start_task >> [current_weather_task, forecast_task] >> summary_task >> end_task


# =============================================================================
# ПОДСКАЗКИ ДЛЯ ВЫПОЛНЕНИЯ
# =============================================================================

"""
Шаги выполнения:
----------------

1. Получите API key:
   - Зарегистрируйтесь на https://openweathermap.org/
   - Получите бесплатный API key
   - Сохраните его

2. Создайте Connection:
   - Admin → Connections → +
   - Connection Id: weather_api_conn
   - Connection Type: HTTP
   - Host: api.openweathermap.org
   - Schema: https
   - Extra: {"appid": "YOUR_API_KEY"}

3. Создайте Variables:
   - weather_city: Moscow (или любой город)
   - weather_units: metric
   - weather_lang: ru

4. Реализуйте WeatherHook:
   - Файл: plugins/custom_hooks/weather_hook.py
   - Методы: get_current_weather(), get_forecast()
   - Используйте SimpleHTTPHook как пример

5. Реализуйте WeatherToFileOperator:
   - Файл: plugins/custom_operators/weather_operator.py
   - Используйте WeatherHook
   - Сохраняйте данные в JSON
   - Поддержка template_fields

6. Завершите этот DAG:
   - Раскомментируйте TODO секции
   - Импортируйте ваши классы
   - Определите задачи и зависимости

7. Тестирование:
   - Запустите DAG
   - Проверьте логи
   - Проверьте созданные файлы
   - Проверьте summary


Полезные ссылки:
----------------

OpenWeatherMap API:
- Документация: https://openweathermap.org/api
- Current Weather: https://openweathermap.org/current
- 5 Day Forecast: https://openweathermap.org/forecast5

Примеры запросов:
- Current: /data/2.5/weather?q=Moscow&appid=YOUR_KEY&units=metric
- Forecast: /data/2.5/forecast?q=Moscow&appid=YOUR_KEY&units=metric


Пример ответа Current Weather:
{
  "main": {
    "temp": -5.2,
    "feels_like": -10.3,
    "temp_min": -6.0,
    "temp_max": -4.0,
    "pressure": 1015,
    "humidity": 75
  },
  "weather": [
    {
      "description": "light snow"
    }
  ],
  "wind": {
    "speed": 4.5
  }
}


Критерии оценки:
----------------
- WeatherHook работает: 3 балла
- WeatherToFileOperator работает: 3 балла
- DAG запускается: 2 балла
- Summary создаётся: 2 балла
- Код прокомментирован: 1 балл
- Variables используются: 1 балл
- Bonus: обработка ошибок: +2 балла
- Bonus: валидация данных: +2 балла

ИТОГО: 12 баллов (+4 bonus)
"""
