"""
DAG: Передача данных между задачами через XCom

Описание:
---------
Этот DAG демонстрирует механизм XCom (Cross-Communication):
- Передача данных между задачами
- Использование ti.xcom_push() и ti.xcom_pull()
- Работа с разными типами данных
- Best practices для XCom

Структура:
----------
start → generate_data → [process_data, calculate_stats] → combine_results → save_report → end
                                ↓                                   ↑
                                └─────── (XCom передача данных) ───┘

"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import logging
import json
import random


# Параметры по умолчанию
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}


# ============================================================================
# ФУНКЦИИ С ИСПОЛЬЗОВАНИЕМ XCOM
# ============================================================================

def generate_data(**context):
    """
    Генерация данных и передача их следующим задачам через XCom.
    
    XCom (Cross-Communication) - механизм для обмена данными между задачами.
    Данные автоматически сохраняются в метаданных Airflow.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("ГЕНЕРАЦИЯ ДАННЫХ")
    logger.info("-" * 50)
    
    # Генерируем тестовый датасет
    dataset_size = 100
    data = {
        'numbers': [random.randint(1, 100) for _ in range(dataset_size)],
        'categories': [random.choice(['A', 'B', 'C', 'D']) for _ in range(dataset_size)],
        'timestamps': [f'2024-01-{random.randint(1, 28):02d}' for _ in range(dataset_size)]
    }
    
    logger.info(f"Сгенерировано записей: {dataset_size}")
    logger.info(f"Пример данных: {data['numbers'][:5]}")
    
    # Дополнительные метаданные
    metadata = {
        'generated_at': datetime.now().isoformat(),
        'dataset_size': dataset_size,
        'execution_date': context['ds']
    }
    
    logger.info("Метаданные:")
    for key, value in metadata.items():
        logger.info(f"  {key}: {value}")
    
    # Способ 1: Автоматическая передача через return
    # Если функция что-то возвращает, это автоматически сохраняется в XCom
    logger.info("\n→ Данные будут переданы через XCom (return)")
    logger.info("-" * 50)
    
    return {
        'data': data,
        'metadata': metadata
    }


def generate_additional_data(**context):
    """
    Генерация дополнительных данных с явным использованием xcom_push.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("ГЕНЕРАЦИЯ ДОПОЛНИТЕЛЬНЫХ ДАННЫХ")
    logger.info("-" * 50)
    
    # Получаем TaskInstance из контекста
    ti = context['task_instance']
    
    # Генерируем дополнительные данные
    additional_data = {
        'weights': [random.uniform(0, 1) for _ in range(50)],
        'labels': [random.choice(['positive', 'negative', 'neutral']) for _ in range(50)]
    }
    
    logger.info(f"Сгенерировано дополнительных данных: {len(additional_data['weights'])} записей")
    
    # Способ 2: Явная передача с использованием xcom_push
    # Позволяет передавать несколько значений с разными ключами
    ti.xcom_push(key='additional_data', value=additional_data)
    ti.xcom_push(key='data_type', value='additional')
    ti.xcom_push(key='version', value='1.0')
    
    logger.info("Данные сохранены в XCom с ключами:")
    logger.info("  - additional_data")
    logger.info("  - data_type")
    logger.info("  - version")
    logger.info("-" * 50)
    
    return "Additional data generated"


def process_data(**context):
    """
    Обработка данных, полученных из предыдущей задачи через XCom.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("ОБРАБОТКА ДАННЫХ")
    logger.info("-" * 50)
    
    # Получаем TaskInstance из контекста
    ti = context['task_instance']
    
    # Способ 1: Получение данных с помощью xcom_pull
    # По умолчанию берет return value предыдущей задачи с ключом 'return_value'
    raw_data = ti.xcom_pull(task_ids='generate_data')
    
    logger.info("Получены данные из задачи 'generate_data'")
    logger.info(f"Тип данных: {type(raw_data)}")
    
    if raw_data is None:
        logger.error("Данные не найдены в XCom!")
        raise ValueError("No data received from generate_data task")
    
    # Извлекаем данные
    data = raw_data['data']
    metadata = raw_data['metadata']
    
    logger.info(f"Обработка {metadata['dataset_size']} записей...")
    
    # Обработка: фильтрация чисел > 50
    numbers = data['numbers']
    filtered_numbers = [n for n in numbers if n > 50]
    
    logger.info(f"Исходных чисел: {len(numbers)}")
    logger.info(f"После фильтрации (>50): {len(filtered_numbers)}")
    
    # Категоризация
    category_counts = {}
    for cat in data['categories']:
        category_counts[cat] = category_counts.get(cat, 0) + 1
    
    logger.info(f"Распределение по категориям: {category_counts}")
    
    # Результат обработки
    processed_result = {
        'filtered_numbers': filtered_numbers,
        'filter_threshold': 50,
        'filtered_count': len(filtered_numbers),
        'category_distribution': category_counts,
        'processed_at': datetime.now().isoformat()
    }
    
    logger.info("→ Результаты обработки будут переданы через XCom")
    logger.info("-" * 50)
    
    return processed_result


def calculate_statistics(**context):
    """
    Вычисление статистики на основе данных из XCom.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("ВЫЧИСЛЕНИЕ СТАТИСТИКИ")
    logger.info("-" * 50)
    
    ti = context['task_instance']
    
    # Получаем исходные данные
    raw_data = ti.xcom_pull(task_ids='generate_data')
    
    if raw_data is None:
        logger.error("Данные не найдены!")
        raise ValueError("No data received")
    
    numbers = raw_data['data']['numbers']
    
    logger.info(f"Вычисление статистики для {len(numbers)} чисел...")
    
    # Вычисляем статистики
    statistics = {
        'count': len(numbers),
        'sum': sum(numbers),
        'mean': sum(numbers) / len(numbers),
        'min': min(numbers),
        'max': max(numbers),
        'median': sorted(numbers)[len(numbers)//2],
        'range': max(numbers) - min(numbers)
    }
    
    # Вычисляем стандартное отклонение
    mean = statistics['mean']
    variance = sum((x - mean) ** 2 for x in numbers) / len(numbers)
    statistics['std_dev'] = variance ** 0.5
    
    logger.info("СТАТИСТИКА:")
    for key, value in statistics.items():
        if isinstance(value, float):
            logger.info(f"  {key}: {value:.2f}")
        else:
            logger.info(f"  {key}: {value}")
    
    logger.info("-" * 50)
    
    return statistics


def combine_results(**context):
    """
    Объединение результатов из нескольких задач.
    Демонстрирует получение данных из нескольких источников.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("ОБЪЕДИНЕНИЕ РЕЗУЛЬТАТОВ")
    logger.info("-" * 50)
    
    ti = context['task_instance']
    
    # Получаем данные из разных задач
    logger.info("  Получение данных из XCom...")
    
    # Данные из generate_data
    raw_data = ti.xcom_pull(task_ids='generate_data')
    logger.info("  Получены исходные данные")
    
    # Результаты обработки
    processed_result = ti.xcom_pull(task_ids='process_data')
    logger.info("  Получены результаты обработки")
    
    # Статистика
    statistics = ti.xcom_pull(task_ids='calculate_stats')
    logger.info("  Получена статистика")
    
    # Дополнительные данные (с конкретным ключом)
    additional_data = ti.xcom_pull(task_ids='generate_additional_data', key='additional_data')
    data_type = ti.xcom_pull(task_ids='generate_additional_data', key='data_type')
    
    if additional_data:
        logger.info("  Получены дополнительные данные")
        logger.info(f"  Тип: {data_type}")
    
    # Объединяем все результаты
    combined = {
        'execution_info': {
            'dag_id': context['dag'].dag_id,
            'execution_date': context['ds'],
            'combined_at': datetime.now().isoformat()
        },
        'original_metadata': raw_data['metadata'],
        'processing_results': {
            'filtered_count': processed_result['filtered_count'],
            'category_distribution': processed_result['category_distribution']
        },
        'statistics': statistics,
        'has_additional_data': additional_data is not None
    }
    
    logger.info("\nОБЪЕДИНЕННЫЕ РЕЗУЛЬТАТЫ:")
    logger.info(f"  Исходный размер датасета: {raw_data['metadata']['dataset_size']}")
    logger.info(f"  Отфильтровано записей: {processed_result['filtered_count']}")
    logger.info(f"  Среднее значение: {statistics['mean']:.2f}")
    logger.info(f"  Стандартное отклонение: {statistics['std_dev']:.2f}")
    
    logger.info("-" * 50)
    
    return combined


def save_final_report(**context):
    """
    Сохранение финального отчета на основе всех данных из XCom.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("СОХРАНЕНИЕ ФИНАЛЬНОГО ОТЧЕТА")
    logger.info("-" * 50)
    
    ti = context['task_instance']
    
    # Получаем объединенные результаты
    combined_results = ti.xcom_pull(task_ids='combine_results')
    
    if combined_results is None:
        logger.error("Объединенные результаты не найдены!")
        raise ValueError("No combined results found")
    
    # Формируем финальный отчет
    final_report = {
        'report_title': 'Airflow XCom Data Pipeline Report',
        'generated_at': datetime.now().isoformat(),
        **combined_results
    }
    
    # Сохраняем отчет в файл
    report_file = '/tmp/xcom_final_report.json'
    with open(report_file, 'w') as f:
        json.dump(final_report, f, indent=2)
    
    logger.info(f"Отчет сохранен: {report_file}")
    
    # Также можем посмотреть все XCom значения для текущего DAG run
    logger.info("\nВСЕ XCOM ЗНАЧЕНИЯ (для справки):")
    logger.info("Используйте веб-интерфейс: Admin → XCom")
    logger.info("Там вы увидите все переданные данные между задачами")
    
    logger.info("-" * 50)
    
    return {
        'status': 'report_saved',
        'report_file': report_file,
        'report_size_bytes': len(json.dumps(final_report))
    }


def demonstrate_xcom_cleanup(**context):
    """
    Демонстрация работы с XCom: очистка и best practices.
    """
    logger = logging.getLogger(__name__)
    
    logger.info("-" * 50)
    logger.info("XCOM BEST PRACTICES")
    logger.info("-" * 50)
    
    ti = context['task_instance']
    
    logger.info("ВАЖНЫЕ МОМЕНТЫ ПРИ РАБОТЕ С XCOM:")
    logger.info("1. РАЗМЕР ДАННЫХ:")
    logger.info("   - XCom хранит данные в метаданных Airflow (БД)")
    logger.info("   - Не передавайте большие объемы данных (>1MB)")
    logger.info("   - Для больших данных используйте внешнее хранилище (S3, HDFS)")
    
    logger.info("2. СЕРИАЛИЗАЦИЯ:")
    logger.info("   - XCom автоматически сериализует данные в JSON")
    logger.info("   - Передавайте простые типы: dict, list, str, int, float")
    logger.info("   - Избегайте передачи объектов классов или функций")
    
    logger.info("3. ВРЕМЯ ЖИЗНИ:")
    logger.info("   - XCom данные привязаны к конкретному DAG run")
    logger.info("   - Автоматически очищаются при удалении DAG run")
    logger.info("   - Можно настроить автоочистку старых данных")
    
    logger.info("4. АЛЬТЕРНАТИВЫ XCOM:")
    logger.info("   - Для больших данных: S3, HDFS, NFS")
    logger.info("   - Для настроек: Airflow Variables")
    logger.info("   - Для credentials: Airflow Connections")
    
    logger.info("5. ОТЛАДКА:")
    logger.info("   - Просмотр XCom: Admin → XCom в веб-интерфейсе")
    logger.info("   - CLI: airflow tasks xcom-list <dag_id> <task_id> <execution_date>")
    
    # Пример получения ВСЕХ XCom значений
    logger.info("Попытка получить все XCom из текущего run:")
    all_task_ids = ['generate_data', 'generate_additional_data', 'process_data', 'calculate_stats']
    for task_id in all_task_ids:
        try:
            value = ti.xcom_pull(task_ids=task_id)
            if value:
                logger.info(f"  {task_id}: данные получены")
        except Exception as e:
            logger.info(f"  {task_id}: {str(e)}")
    
    logger.info("-" * 50)
    
    return "XCom demonstration complete"


# ============================================================================
# СОЗДАНИЕ DAG
# ============================================================================

dag = DAG(
    'dag_04_xcom',
    default_args=default_args,
    description='Передача данных между задачами через XCom',
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=['tutorial', 'xcom', 'data-passing'],
)


# ============================================================================
# ОПРЕДЕЛЕНИЕ ЗАДАЧ
# ============================================================================

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

# Генерация основных данных (автоматическая передача через return)
generate_task = PythonOperator(
    task_id='generate_data',
    python_callable=generate_data,
    provide_context=True,
    dag=dag,
)

# Генерация дополнительных данных (явная передача через xcom_push)
generate_additional_task = PythonOperator(
    task_id='generate_additional_data',
    python_callable=generate_additional_data,
    provide_context=True,
    dag=dag,
)

# Параллельная обработка и анализ
process_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

stats_task = PythonOperator(
    task_id='calculate_stats',
    python_callable=calculate_statistics,
    provide_context=True,
    dag=dag,
)

# Объединение результатов
combine_task = PythonOperator(
    task_id='combine_results',
    python_callable=combine_results,
    provide_context=True,
    dag=dag,
)

# Сохранение отчета
save_task = PythonOperator(
    task_id='save_report',
    python_callable=save_final_report,
    provide_context=True,
    dag=dag,
)

# Демонстрация best practices
demo_task = PythonOperator(
    task_id='xcom_best_practices',
    python_callable=demonstrate_xcom_cleanup,
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


# ============================================================================
# ОПРЕДЕЛЕНИЕ ЗАВИСИМОСТЕЙ
# ============================================================================

# Основной поток данных
start_task >> generate_task >> [process_task, stats_task, generate_additional_task]
[process_task, stats_task, generate_additional_task] >> combine_task
combine_task >> save_task >> demo_task >> end_task


"""
Что изучить в веб-интерфейсе:
------------------------------
1. Graph View: посмотрите связи между задачами
2. XCom (Admin): изучите все переданные значения
3. Logs: посмотрите как данные передаются и принимаются
4. Task Instance Details: информация о конкретном выполнении


Упражнения для самостоятельной работы:
---------------------------------------

1. Создайте задачу, которая получает данные из нескольких предыдущих задач
   и вычисляет комплексную метрику

2. Реализуйте pipeline с передачей данных через промежуточное хранилище (файл)
   вместо XCom и сравните подходы

3. Добавьте обработку ошибок: что если xcom_pull возвращает None?

4. Создайте задачу, которая передает не только данные, но и метаданные
   о времени выполнения, использованной памяти и т.д.

5. Реализуйте incremental processing: используйте XCom для хранения
   информации о последней обработанной записи
"""
