#!/bin/bash

set -e
set -u

# Заполняем БД TARGET_POSTGRES_DATABASE
if [ -n "$TARGET_POSTGRES_DATABASE" ] && [ -n "$TARGET_POSTGRES_TABLE" ]; then
    echo "Creating table ${TARGET_POSTGRES_TABLE} in ${TARGET_POSTGRES_DATABASE}"
    psql -v ON_ERROR_STOP=1 --username "$TARGET_POSTGRES_USER" --dbname "$TARGET_POSTGRES_DATABASE" >/dev/null <<-EOSQL
      \c $TARGET_POSTGRES_DATABASE;
      DROP TABLE IF EXISTS $TARGET_POSTGRES_TABLE;

      -- Таблица клиентов для "чистых" данных
     
      CREATE TABLE IF NOT EXISTS $TARGET_POSTGRES_TABLE (
          customer_id VARCHAR(255) PRIMARY KEY,
          first_name VARCHAR(255),
          last_name VARCHAR(255),
          email VARCHAR(255) UNIQUE,
          phone VARCHAR(50),
          registration_date TIMESTAMP,
          last_purchase_date TIMESTAMP,
          total_purchases INTEGER,
          total_amount_spent DECIMAL(10, 2),
          age INTEGER,
          city VARCHAR(255),
          country VARCHAR(255),
          recency INTEGER,
          frequency INTEGER,
          monetary DECIMAL(10, 2),
          r_score INTEGER,
          f_score INTEGER,
          m_score INTEGER,
          rfm_score VARCHAR(10),
          rfm_total INTEGER,
          segment VARCHAR(50),
          updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
      );
EOSQL

fi