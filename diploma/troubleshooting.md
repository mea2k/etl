# Troubleshooting

## Проблема: Airflow не видит DAG

**Причины:**

1. Синтаксическая ошибка в Python коде
2. DAG файл не в директории dags/
3. Scheduler не запущен

**Решение:**

```bash
# Проверить синтаксис
python dags/main_etl_dag.py

# Проверить логи scheduler
docker compose logs airflow

# Перезапустить scheduler
docker compose restart airflow

# Проверить список DAG
docker compose exec -i airflow airflow dags list
```

## Проблема: Ошибка подключения к БД

**Причины:**

1. Неправильные данные в .env
2. Connection не настроен в Airflow
3. БД не запущена

**Решение:**

```bash
# Проверить статус контейнеров
docker compose ps

# Проверить переменные окружения
docker compose exec -i airflow env | grep POSTGRES

# Проверить Connections
docker compose exec -i airflow airflow connections list

# Протестировать подключение напрямую
docker compose exec -i postgres-source psql -U source_user -d source_db

# Пересоздать Connection
docker compose exec -i airflow python /opt/airflow/scripts/setup_connections.py
```

## Проблема: SCD Type 2 не работает

**Симптомы:**

- Создается несколько записей с is_current=TRUE
- Старые версии не закрываются
- Факты привязываются к неправильным версиям

**Решение:**

```sql
-- Проверить состояние dim_customers
SELECT 
    customer_id,
    city,
    effective_date,
    expiration_date,
    is_current
FROM dim_customers
WHERE customer_id = <ваш_customer_id>
ORDER BY effective_date;

-- Должно быть ТОЛЬКО ОДНО is_current=TRUE для каждого customer_id

-- Если есть ошибки - очистить и перезагрузить
TRUNCATE TABLE fact_orders;
TRUNCATE TABLE dim_customers RESTART IDENTITY CASCADE;

-- Перезапустить DAG
```

## Типичные ошибки в коде SCD Type 2

```python
# НЕПРАВИЛЬНО - не проверяем is_current
query = "SELECT * FROM dim_customers WHERE customer_id = %s"

# ПРАВИЛЬНО - обязательно проверяем is_current
query = """
    SELECT * FROM dim_customers 
    WHERE customer_id = %s AND is_current = TRUE
"""

# НЕПРАВИЛЬНО - просто UPDATE is_current
UPDATE dim_customers SET is_current = FALSE WHERE customer_id = 123

# ПРАВИЛЬНО - UPDATE с условием на surrogate key
UPDATE dim_customers 
SET is_current = FALSE, expiration_date = %s
WHERE customer_key = 1001  -- конкретный surrogate key!
```

## Проблема: Медленная загрузка данных

**Решение:**

```python
# Используйте batch insert вместо построчной вставки

# МЕДЛЕННО
for row in df.iterrows():
    cursor.execute("INSERT INTO table VALUES (%s, %s)", row)

# БЫСТРО
data = [tuple(row) for row in df.values]
cursor.executemany("INSERT INTO table VALUES (%s, %s)", data)
```

```sql
-- Добавьте индексы
CREATE INDEX idx_customers_current ON dim_customers(customer_id, is_current);
CREATE INDEX idx_orders_date ON fact_orders(date_key);

-- Используйте EXPLAIN для анализа
EXPLAIN ANALYZE
SELECT * FROM fact_orders f
JOIN dim_customers c ON f.customer_key = c.customer_key
WHERE c.customer_id = 123;
```

## Проблема: Out of Memory

**Решение:**

```python
# Не загружайте все данные в память сразу

# ПЛОХО
df = pd.read_sql("SELECT * FROM huge_table", conn)

# ХОРОШО - используйте chunksize
for chunk in pd.read_sql("SELECT * FROM huge_table", conn, chunksize=10000):
    process_chunk(chunk)
```

```yaml
# Увеличьте память для Docker
# В docker-compose.yml
services:
  airflow-scheduler:
    mem_limit: 4g
```

## Частые вопросы (FAQ)

**Q: Как проверить что SCD Type 2 работает правильно?**

A: Выполните SQL запрос:

```sql
SELECT 
    customer_id,
    COUNT(*) as versions,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_versions
FROM dim_customers
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

Для каждого customer_id должна быть ТОЛЬКО ОДНА версия с is_current=TRUE.

**Q: Как сгенерировать тестовые данные?**

A: Используйте DAG generate_test_data или скрипт:

```bash
docker compose exec -i airflow python /opt/airflow/scripts/generate_sample_data.py
```

**Q: Можно ли использовать другую предметную область?**

A: Да, можно! Главное чтобы:

- Было минимум 3 источника данных
- Была реализована SCD Type 2
- Была соблюдена безопасность

---