-- ============================================================================
-- ИНИЦИАЛИЗАЦИЯ POSTGRES ДЛЯ ТЕМЫ 3
-- ============================================================================

-- Создание БД для Airflow метаданных
CREATE DATABASE airflow_db;

-- Создание БД для источника данных (source)
CREATE DATABASE source_db;

-- Создание пользователя Airflow
CREATE USER airflow WITH PASSWORD 'airflow';

-- Права для airflow_db
GRANT ALL PRIVILEGES ON DATABASE airflow_db TO airflow;
GRANT ALL PRIVILEGES ON DATABASE source_db TO airflow;

-- ============================================================================
-- НАСТРОЙКА AIRFLOW_DB
-- ============================================================================

\c airflow_db;

ALTER DATABASE airflow_db OWNER TO airflow;
GRANT USAGE ON SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON SEQUENCES TO airflow;


-- ============================================================================
-- СОЗДАНИЕ ТЕСТОВЫХ ДАННЫХ В SOURCE_DB
-- ============================================================================

\c source_db;

-- Таблица пользователей
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    full_name VARCHAR(255) NOT NULL,
    active BOOLEAN DEFAULT true,
    registration_date DATE DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица заказов
CREATE TABLE orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES users(id),
    order_date DATE NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Таблица продуктов
CREATE TABLE products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    price DECIMAL(10, 2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- ВСТАВКА ТЕСТОВЫХ ДАННЫХ
-- ============================================================================

-- Пользователи (20 записей)
INSERT INTO users (email, full_name, active, registration_date) VALUES
('alice@example.com', 'Alice Smith', true, '2024-01-15'),
('bob@example.com', 'Bob Johnson', true, '2024-02-20'),
('charlie@example.com', 'Charlie Brown', true, '2024-03-10'),
('david@example.com', 'David Lee', true, '2024-04-05'),
('emma@example.com', 'Emma Davis', true, '2024-05-12'),
('frank@example.com', 'Frank Miller', false, '2024-06-18'),
('grace@example.com', 'Grace Wilson', true, '2024-07-22'),
('henry@example.com', 'Henry Taylor', true, '2024-08-30'),
('iris@example.com', 'Iris Anderson', true, '2024-09-14'),
('jack@example.com', 'Jack Thomas', true, '2024-10-08'),
('kate@example.com', 'Kate Jackson', true, '2024-11-25'),
('leo@example.com', 'Leo White', false, '2024-12-01'),
('maria@example.com', 'Maria Garcia', true, '2025-01-05'),
('nathan@example.com', 'Nathan Martinez', true, '2025-01-10'),
('olivia@example.com', 'Olivia Rodriguez', true, '2025-01-11'),
('paul@example.com', 'Paul Martinez', true, '2025-01-12'),
('quinn@example.com', 'Quinn Hernandez', false, '2025-01-12'),
('rachel@example.com', 'Rachel Lopez', true, '2025-01-13'),
('sam@example.com', 'Sam Gonzalez', true, '2025-01-14'),
('tina@example.com', 'Tina Wilson', true, '2025-01-15');

-- Заказы (50 записей)
INSERT INTO orders (user_id, order_date, amount, status) VALUES
(1, '2025-01-10', 150.00, 'completed'),
(1, '2025-01-11', 75.50, 'completed'),
(2, '2025-01-10', 200.00, 'completed'),
(3, '2025-01-11', 89.99, 'pending'),
(4, '2025-01-11', 125.00, 'completed'),
(5, '2025-01-12', 310.50, 'completed'),
(6, '2025-01-12', 45.00, 'cancelled'),
(7, '2025-01-12', 99.99, 'completed'),
(8, '2025-01-13', 175.25, 'pending'),
(9, '2025-01-13', 50.00, 'completed'),
(10, '2025-01-13', 220.00, 'completed'),
(1, '2025-01-14', 95.75, 'completed'),
(2, '2025-01-14', 140.00, 'pending'),
(3, '2025-01-14', 67.50, 'completed'),
(4, '2025-01-15', 185.00, 'completed'),
(5, '2025-01-15', 290.00, 'completed'),
(11, '2025-01-10', 120.00, 'completed'),
(12, '2025-01-11', 80.00, 'cancelled'),
(13, '2025-01-12', 155.50, 'completed'),
(14, '2025-01-13', 99.00, 'pending'),
(15, '2025-01-14', 210.00, 'completed'),
(16, '2025-01-15', 145.75, 'completed'),
(1, '2025-01-15', 88.00, 'pending'),
(2, '2025-01-15', 195.50, 'completed'),
(3, '2025-01-16', 125.25, 'pending');

-- Продукты (30 записей)
INSERT INTO products (name, category, price, stock_quantity) VALUES
('Laptop Pro 15', 'Electronics', 1299.99, 15),
('Wireless Mouse', 'Electronics', 29.99, 50),
('USB-C Cable', 'Electronics', 12.99, 100),
('Mechanical Keyboard', 'Electronics', 89.99, 30),
('4K Monitor', 'Electronics', 399.99, 20),
('Webcam HD', 'Electronics', 59.99, 25),
('Desk Lamp LED', 'Office', 34.99, 40),
('Office Chair', 'Office', 199.99, 12),
('Standing Desk', 'Office', 449.99, 8),
('Notebook A4', 'Office', 4.99, 200),
('Pen Set', 'Office', 9.99, 150),
('Python Crash Course', 'Books', 39.99, 50),
('Clean Code', 'Books', 34.99, 45),
('Design Patterns', 'Books', 44.99, 30),
('SQL Cookbook', 'Books', 29.99, 35),
('Data Engineering', 'Books', 49.99, 25),
('Coffee Maker', 'Home', 79.99, 20),
('Blender Pro', 'Home', 99.99, 15),
('Vacuum Cleaner', 'Home', 149.99, 10),
('Air Purifier', 'Home', 129.99, 18),
('Headphones Wireless', 'Electronics', 149.99, 35),
('Smartphone Case', 'Electronics', 19.99, 80),
('Power Bank 20000mAh', 'Electronics', 39.99, 45),
('HDMI Cable 2m', 'Electronics', 14.99, 70),
('External SSD 1TB', 'Electronics', 119.99, 25),
('Router WiFi 6', 'Electronics', 89.99, 30),
('Smart Watch', 'Electronics', 299.99, 15),
('Fitness Tracker', 'Electronics', 79.99, 40),
('Gaming Mouse', 'Electronics', 49.99, 35),
('Laptop Bag', 'Electronics', 39.99, 50);

-- Предоставление прав пользователю airflow
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO airflow;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO airflow;
ALTER DEFAULT PRIVILEGES IN SCHEMA public GRANT ALL PRIVILEGES ON TABLES TO airflow;

-- ============================================================================
-- ИНФОРМАЦИЯ
-- ============================================================================

-- Проверка данных
SELECT 'Users count:' as info, COUNT(*) as count FROM users;
SELECT 'Orders count:' as info, COUNT(*) as count FROM orders;
SELECT 'Products count:' as info, COUNT(*) as count FROM products;

-- Примеры запросов для тестирования

-- 1. Активные пользователи
-- SELECT * FROM users WHERE active = true;

-- 2. Заказы за сегодня
-- SELECT * FROM orders WHERE order_date = CURRENT_DATE;

-- 3. Инкрементальная выборка
-- SELECT * FROM orders WHERE created_at BETWEEN '2025-01-10' AND '2025-01-15';

-- 4. Join пользователей и заказов
-- SELECT u.email, u.full_name, COUNT(o.id) as order_count, SUM(o.amount) as total_amount
-- FROM users u
-- LEFT JOIN orders o ON u.id = o.user_id
-- GROUP BY u.id, u.email, u.full_name
-- ORDER BY total_amount DESC;
