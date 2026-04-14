# reviews-analytics
###Система сбора и анализа отзывов ВВГУ с агрегаторов: 2GIS, vl.ru, Яндекс Карты.
Возможности
- Парсинг отзывов с трёх источников (Selenium + BeautifulSoup)
- Хранение данных в PostgreSQL с дедупликацией
- Анализ тональности через YandexGPT API
- Автоматизация через Apache Airflow (еженедельный запуск)
- Визуализация в Yandex DataLens
###Технологический стек
Язык - Python 3.11
Парсинг - Selenium, BeautifulSoup
БД - PostgreSQL
Оркестрация - Apache Airflow
LLM - YandexGPT API
Визуализация - Yandex DataLens
Контейнеризация - Docker
###Настройка переменных окружения (.env)
Проект использует два файла .env для разделения конфигурации:
Корень проекта (project/.env) и папка Airflow (project/airflow/.env)
```
# Настройки базы данных
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database_name
DB_USER=your_db_user
DB_PASSWORD=your_secure_password
DB_SCHEMA=public
DB_SSLMODE=disable

# YandexGPT настройки
YC_FOLDER_ID=your_yandex_folder_id
YC_API_KEY=your_yandex_api_key
```
