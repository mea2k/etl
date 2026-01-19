-- ============================================
-- ИНИЦИАЛИЗАЦИЯ АНАЛИТИЧЕСКОЙ БД
-- ============================================

-- Таблица ежедневной аналитики
CREATE TABLE IF NOT EXISTS daily_business_analytics (
    id SERIAL PRIMARY KEY,
    analytics_date DATE NOT NULL UNIQUE,
    
    -- Метрики заказов
    total_orders INTEGER DEFAULT 0,
    total_revenue DECIMAL(12, 2) DEFAULT 0,
    avg_order_value DECIMAL(10, 2) DEFAULT 0,
    
    -- Статусы заказов
    orders_pending INTEGER DEFAULT 0,
    orders_processing INTEGER DEFAULT 0,
    orders_shipped INTEGER DEFAULT 0,
    orders_delivered INTEGER DEFAULT 0,
    orders_cancelled INTEGER DEFAULT 0,
    
    -- Клиенты
    unique_customers INTEGER DEFAULT 0,
    new_customers INTEGER DEFAULT 0,
    avg_customer_rating DECIMAL(3, 2) DEFAULT 0,
    
    -- Продукты
    total_products_sold INTEGER DEFAULT 0,
    top_category VARCHAR(100),
    top_product VARCHAR(255),
    
    -- Веб-метрики
    total_page_views INTEGER DEFAULT 0,
    unique_visitors INTEGER DEFAULT 0,
    conversion_rate DECIMAL(5, 4) DEFAULT 0,
    
    -- Доставка
    total_deliveries INTEGER DEFAULT 0,
    avg_delivery_time_minutes INTEGER DEFAULT 0,
    successful_delivery_rate DECIMAL(5, 4) DEFAULT 0,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Индекс по дате
CREATE INDEX IF NOT EXISTS idx_daily_analytics_date ON daily_business_analytics(analytics_date DESC);

-- Таблица для хранения сырых данных из различных источников
CREATE TABLE IF NOT EXISTS raw_data_staging (
    id SERIAL PRIMARY KEY,
    source_name VARCHAR(50) NOT NULL,
    source_type VARCHAR(50) NOT NULL,
    extraction_date TIMESTAMP NOT NULL,
    data JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица для хранения метрик качества данных
CREATE TABLE IF NOT EXISTS data_quality_metrics (
    id SERIAL PRIMARY KEY,
    run_date TIMESTAMP NOT NULL,
    dag_id VARCHAR(255) NOT NULL,
    task_id VARCHAR(255) NOT NULL,
    source_name VARCHAR(100) NOT NULL,
    
    -- Метрики качества
    total_records INTEGER,
    valid_records INTEGER,
    invalid_records INTEGER,
    duplicate_records INTEGER,
    null_values_count INTEGER,
    
    -- Процентные показатели
    completeness_rate DECIMAL(5, 4),
    validity_rate DECIMAL(5, 4),
    uniqueness_rate DECIMAL(5, 4),
    
    -- Детали
    error_details JSONB,
    
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dq_metrics_run_date ON data_quality_metrics(run_date);
CREATE INDEX IF NOT EXISTS idx_dq_metrics_source ON data_quality_metrics(source_name);


-- Триггер для daily_business_analytics
CREATE TRIGGER update_analytics_updated_at BEFORE UPDATE ON daily_business_analytics
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- Представление для быстрого доступа к последним метрикам
CREATE OR REPLACE VIEW latest_analytics AS
SELECT * FROM daily_business_analytics
ORDER BY analytics_date DESC
LIMIT 30;

-- Представление для агрегации логов ETL
CREATE OR REPLACE VIEW etl_summary AS
SELECT 
    dag_id,
    task_id,
    DATE(execution_date) as execution_day,
    COUNT(*) as total_runs,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_runs,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs,
    AVG(duration_seconds) as avg_duration_seconds,
    SUM(records_processed) as total_records_processed
FROM etl_process_logs
GROUP BY dag_id, task_id, DATE(execution_date)
ORDER BY execution_day DESC;



COMMENT ON TABLE daily_business_analytics IS 'Ежедневная бизнес-аналитика';
COMMENT ON TABLE data_quality_metrics IS 'Метрики качества данных';
