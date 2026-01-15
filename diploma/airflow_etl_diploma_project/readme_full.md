# Дипломная работа: ETL-Pipeline с Apache Airflow

## Тема: "Дашборд аналитика бизнес-процессов"

### Цель проекта

Разработать полноценный ETL-pipeline на базе Apache Airflow 2.11 для автоматизированного сбора, обработки и визуализации данных бизнес-процессов. Система должна ежедневно (в 9:00) собирать данные из различных источников, трансформировать их и загружать в аналитическую БД и Data Warehouse для последующей визуализации.

---

## Ключевые требования

### 1. Data Warehouse с SCD Type 2

Хранилище данных должно использовать стратегию **Slowly Changing Dimensions (SCD) Type 2** для отслеживания исторических изменений.

### 2. Безопасность подключений

- Все подключения через **Airflow Connections**
- Учетные данные через **переменные окружения (.env)**
- Жесткое кодирование паролей **ЗАПРЕЩЕНО**

### 3. Минимум 3 источника данных

- PostgreSQL, MongoDB, CSV/FTP, REST API

---

## Структура проекта

```text
airflow_etl_diploma_project/
├── dags/                      # DAG-файлы
├── plugins/                   # Extractors, Transformers, Loaders
├── init/                      # SQL скрипты инициализации БД
├── scripts/                   # Вспомогательные скрипты
├── data/                      # Данные
├── config/                    # Конфигурация
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Быстрый старт

### 1. Настройка .env

```bash
cp .env.example .env
# Отредактируйте .env
```

### 2. Запуск

```bash
docker compose up -d
```

### 3. Доступ

- Airflow: http://localhost:8080
- Grafana: http://localhost:3000

---

## ETL Pipeline

### Extract

- PostgreSQL (orders, customers)
- MongoDB (feedback)
- CSV (products)
- FTP (delivery logs)

### Transform

- Очистка данных
- Валидация
- Нормализация
- Обогащение

### Load

- Аналитическая БД
- Data Warehouse (SCD Type 2)

---
