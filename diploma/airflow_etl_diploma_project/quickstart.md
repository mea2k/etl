# Быстрый старт

## 1. Подготовка окружения

```bash
# Клонирование репозитория
git clone <repo-url>
cd airflow_etl_diploma_project

# Создание .env файла
cp .env.example .env

# Генерация Fernet Key
python3 -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
# Скопируйте полученный ключ в .env в AIRFLOW__CORE__FERNET_KEY
```

## 2. Редактирование .env

Отредактируйте .env файл и укажите:

- Пароли для баз данных
- API токены
- FTP credentials

## 3. Запуск Docker

```bash
# Запуск всех сервисов
docker compose  up -d

# Проверка статуса
docker compose ps
```

## 4. Инициализация баз данных

```bash
# PostgreSQL Source
docker compose exec -i postgres_source \
    psql -U sourceuser -d source_db -f /opt/airflow/sql/create_source_tables.sql

# PostgreSQL Analytics
docker compose exec -i postgres_analytics \
    psql -U analyticsuser -d analytics_db -f /opt/airflow/sql/create_analytics_tables.sql

# PostgreSQL DWH
docker compose exec -i postgres_analytics \
    psql -U analyticsuser -d analytics_db -f /opt/airflow/sql/create_dwh_schema.sql
```

## 5. Доступ к интерфейсам

- Airflow UI: http://localhost:8080 (admin/admin)
- Grafana: http://localhost:3000 (admin/admin123)

## 6. Настройка Connections в Airflow

1. Откройте Airflow UI
2. Admin → Connections
3. Создайте подключения из файла config/connections.json

## 7. Запуск DAG

1. Включите DAG `business_analytics_etl`
2. Нажмите "Trigger DAG"
3. Мониторьте выполнение в Graph View

## Остановка

```bash
docker compose down
```

## Полная очистка (включая volumes)

```bash
docker compose down -v --remove-orphans
docker volume prune -f

```
