-- СОЗДАНИЕ БАЗ ДАННЫХ
CREATE DATABASE airflow_db;

-- СОЗДАНИЕ ПОЛЬЗОВАТЕЛЯ AIRFLOW
CREATE USER airflow WITH PASSWORD 'airflow';
GRANT CONNECT ON DATABASE airflow_db TO airflow;

-- Предоставление доступа пользователю airflow 
-- в БД airflow_db полного доступа
\c airflow_db;
ALTER DATABASE airflow_db OWNER TO airflow;
GRANT USAGE ON SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES  ON SEQUENCES TO airflow;
