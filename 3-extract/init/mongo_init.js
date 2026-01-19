// =============================================================================
// MongoDB Initialization Script
// 
// Создаёт базу данных analytics_db и заполняет её тестовыми данными
//
// База данных: analytics_db
// Коллекции:
//   - events (события пользователей)
//   - feedback (отзывы пользователей)
//   - products_viewed (просмотры продуктов)
// =============================================================================
print("Mongo init start...");

// =============================================================================
// СОЗДАНИЕ ПОЛЬЗОВАТЕЛЯ ДЛЯ AIRFLOW
// =============================================================================

print('Creating Airflow user');

// Переключаемся на admin базу для создания пользователя
db = db.getSiblingDB('admin');

// Создаём пользователя airflow с правами на analytics_db
try {
    db.createUser({
        user: 'airflow',
        pwd: 'airflow',
        roles: [
            { role: 'readWrite', db: 'analytics_db' },
            { role: 'dbAdmin', db: 'analytics_db' }
        ],
    });

    print('  User "airflow" created successfully');

} catch (e) {
    if (e.code === 51003) {
        print('   User "airflow" already exists - skipping');
    } else {
        print(`  Error creating user: ${e}`);
    }
}
print('');

// =============================================================================
// =============================================================================


db = db.getSiblingDB('analytics_db');

print('Database: analytics_db');

// =============================================================================
// КОЛЛЕКЦИЯ 1: events - События пользователей
// =============================================================================

print('  Creating collection: events');

// Удаляем коллекцию если существует (для чистоты)
db.events.drop();

// Создаём коллекцию events
db.createCollection('events');

// Вставляем тестовые события
// Генерируем события за последние 30 дней
const eventTypes = ['page_view', 'button_click', 'form_submit', 'purchase', 'search', 'add_to_cart'];
const pages = ['/home', '/products', '/cart', '/checkout', '/profile', '/search'];
const userIds = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

let events = [];
const today = new Date();

// Генерируем события за последние 30 дней
for (let dayOffset = 0; dayOffset < 30; dayOffset++) {
    const eventDate = new Date(today);
    eventDate.setDate(today.getDate() - dayOffset);
    const dateStr = eventDate.toISOString().split('T')[0]; // YYYY-MM-DD
    
    // 5-15 событий в день
    const eventsPerDay = Math.floor(Math.random() * 11) + 5;
    
    for (let i = 0; i < eventsPerDay; i++) {
        const eventType = eventTypes[Math.floor(Math.random() * eventTypes.length)];
        const userId = userIds[Math.floor(Math.random() * userIds.length)];
        const page = pages[Math.floor(Math.random() * pages.length)];
        
        // Время события в течение дня
        const hour = Math.floor(Math.random() * 24);
        const minute = Math.floor(Math.random() * 60);
        const timestamp = new Date(eventDate);
        timestamp.setHours(hour, minute, 0, 0);
        
        events.push({
            event_id: `evt_${dayOffset}_${i}`,
            user_id: userId,
            event_type: eventType,
            event_date: dateStr,  // Строка для фильтрации в DAG
            event_timestamp: timestamp,  // ISODate для сортировки
            page: page,
            session_id: `session_${userId}_${dayOffset}`,
            metadata: {
                browser: ['Chrome', 'Firefox', 'Safari', 'Edge'][Math.floor(Math.random() * 4)],
                device: ['desktop', 'mobile', 'tablet'][Math.floor(Math.random() * 3)],
                country: ['US', 'UK', 'DE', 'FR', 'JP'][Math.floor(Math.random() * 5)]
            },
            created_at: timestamp
        });
    }
}

// Вставляем события в коллекцию
db.events.insertMany(events);

print(`  Inserted ${events.length} events`);
print(`  Date range: last 30 days`);

// Создаём индексы для быстрого поиска
db.events.createIndex({ event_date: 1 });
db.events.createIndex({ user_id: 1 });
db.events.createIndex({ event_type: 1 });
db.events.createIndex({ event_timestamp: -1 });

print('  Indexes created on events collection');
print(`  Total events: ${db.events.countDocuments()}`);
print('');

// =============================================================================
// КОЛЛЕКЦИЯ 2: feedback - Отзывы пользователей
// =============================================================================

print('Creating collection: feedback');

db.feedback.drop();
db.createCollection('feedback');

// Генерируем отзывы за последние 30 дней
let feedbackItems = [];
const feedbackTypes = ['product_review', 'service_feedback', 'bug_report', 'feature_request'];
const ratings = [1, 2, 3, 4, 5];

for (let dayOffset = 0; dayOffset < 30; dayOffset++) {
    const feedbackDate = new Date(today);
    feedbackDate.setDate(today.getDate() - dayOffset);
    const dateStr = feedbackDate.toISOString().split('T')[0];
    
    // 1-3 отзыва в день
    const feedbackPerDay = Math.floor(Math.random() * 3) + 1;
    
    for (let i = 0; i < feedbackPerDay; i++) {
        const userId = userIds[Math.floor(Math.random() * userIds.length)];
        const rating = ratings[Math.floor(Math.random() * ratings.length)];
        const feedbackType = feedbackTypes[Math.floor(Math.random() * feedbackTypes.length)];
        
        feedbackItems.push({
            feedback_id: `fb_${dayOffset}_${i}`,
            user_id: userId,
            type: feedbackType,
            rating: rating,
            title: `Feedback ${i + 1} for day ${dayOffset}`,
            comment: `This is a sample feedback comment. Rating: ${rating} stars.`,
            feedback_date: dateStr,
            created_at: feedbackDate,
            status: ['new', 'in_progress', 'resolved'][Math.floor(Math.random() * 3)],
            tags: rating >= 4 ? ['positive', 'helpful'] : rating <= 2 ? ['negative', 'needs_improvement'] : ['neutral']
        });
    }
}

db.feedback.insertMany(feedbackItems);

print(`  Inserted ${feedbackItems.length} feedback items`);

// Индексы
db.feedback.createIndex({ feedback_date: 1 });
db.feedback.createIndex({ user_id: 1 });
db.feedback.createIndex({ rating: 1 });
db.feedback.createIndex({ type: 1 });

print('  Indexes created on feedback collection');

// Статистика
print(`  Total feedback: ${db.feedback.countDocuments()}`);
print('');

// =============================================================================
// КОЛЛЕКЦИЯ 3: products_viewed - Просмотры продуктов
// =============================================================================

print('Creating collection: products_viewed');

db.products_viewed.drop();
db.createCollection('products_viewed');

// Генерируем просмотры продуктов
let productsViewed = [];
const productIds = [101, 102, 103, 104, 105, 106, 107, 108, 109, 110];
const categories = ['Electronics', 'Books', 'Clothing', 'Home', 'Sports'];

for (let dayOffset = 0; dayOffset < 30; dayOffset++) {
    const viewDate = new Date(today);
    viewDate.setDate(today.getDate() - dayOffset);
    const dateStr = viewDate.toISOString().split('T')[0];
    
    // 10-20 просмотров в день
    const viewsPerDay = Math.floor(Math.random() * 11) + 10;
    
    for (let i = 0; i < viewsPerDay; i++) {
        const userId = userIds[Math.floor(Math.random() * userIds.length)];
        const productId = productIds[Math.floor(Math.random() * productIds.length)];
        const category = categories[Math.floor(Math.random() * categories.length)];
        
        productsViewed.push({
            view_id: `view_${dayOffset}_${i}`,
            user_id: userId,
            product_id: productId,
            product_name: `Product ${productId}`,
            category: category,
            view_date: dateStr,
            view_duration_seconds: Math.floor(Math.random() * 300) + 10, // 10-310 секунд
            viewed_at: viewDate,
            added_to_cart: Math.random() > 0.7, // 30% добавили в корзину
            purchased: Math.random() > 0.9 // 10% купили
        });
    }
}

db.products_viewed.insertMany(productsViewed);

print(`  Inserted ${productsViewed.length} product views`);

// Индексы
db.products_viewed.createIndex({ view_date: 1 });
db.products_viewed.createIndex({ user_id: 1 });
db.products_viewed.createIndex({ product_id: 1 });
db.products_viewed.createIndex({ category: 1 });

print('  Indexes created on products_viewed collection');

// Статистика
print(`  Total views: ${db.products_viewed.countDocuments()}`);
print('');


// =============================================================================
// СОЗДАНИЕ ПОЛЬЗОВАТЕЛЯ ДЛЯ AIRFLOW
// =============================================================================

print('Creating Airflow user');

// Переключаемся на admin базу для создания пользователя
db = db.getSiblingDB('admin');

// Создаём пользователя airflow с правами на analytics_db
try {
    db.createUser({
        user: 'airflow',
        pwd: 'airflow',
        roles: [
            { role: 'readWrite', db: 'analytics_db' },
            { role: 'dbAdmin', db: 'analytics_db' }
        ]
    });
    print('  User "airflow" created successfully');
} catch (e) {
    if (e.code === 51003) {
        print('  User "airflow" already exists - skipping');
    } else {
        print(`  Error creating user: ${e}`);
    }
}
print('');

// =============================================================================

print("Mongo init completed successfully!");