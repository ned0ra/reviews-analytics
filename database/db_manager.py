"""Управление подключением к PostgreSQL и загрузка данных.

Модуль предоставляет класс DatabaseManager для управления соединением
с базой данных PostgreSQL, создания схемы таблиц и выполнения операций
загрузки, выборки и обновления данных об отзывах.
"""

import logging
import os
from typing import List, Dict

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логирования: вывод в консоль с временной меткой
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger(__name__)


class DatabaseManager:
    """Менеджер подключения к PostgreSQL и операций с данными отзывов.

    Класс инкапсулирует логику подключения к базе данных, создания
    необходимой схемы таблиц и выполнения основных операций CRUD
    для работы с отзывами из различных источников.

    Атрибуты:
        conn: Активное соединение с базой данных psycopg2.
        cursor: Курсор для выполнения SQL-запросов.
    """

    def __init__(self):
        """Инициализация менеджера: подключение к БД и создание таблиц."""
        self.conn = None
        self.cursor = None
        self._connect()
        self._create_tables()

    def _connect(self):
        """Установка соединения с базой данных PostgreSQL.

        Параметры подключения читаются из переменных окружения с возможностью
        переопределения через .env файл. После подключения устанавливается
        search_path для работы с указанной схемой.

        Raises:
            psycopg2.OperationalError: При ошибке подключения к серверу БД.
        """
        self.conn = psycopg2.connect(
            host=os.getenv('DB_HOST', 'localhost'),
            port=os.getenv('DB_PORT', '5432'),
            database=os.getenv('DB_NAME', 'msod_database'),
            user=os.getenv('DB_USER', 'u_reviews_parser'),
            password=os.getenv('DB_PASSWORD', 'H4d00p_Spark$'),
            sslmode=os.getenv('DB_SSLMODE', 'prefer')
        )
        self.cursor = self.conn.cursor()

        # Установка search_path для работы с целевой схемой
        schema = os.getenv('DB_SCHEMA', 'public')
        self.cursor.execute("SET search_path TO %s, public;", (schema,))

        logger.info("Подключение к БД установлено")

    def _create_tables(self):
        """Создание таблицы reviews и индексов, если они не существуют.

        Таблица включает поля для хранения метаданных отзыва, исходного текста,
        результатов анализа тональности и служебных флагов. Индексы оптимизируют
        выборку по источнику, дате, статусу обработки и тональности.
        """
        schema = os.getenv('DB_SCHEMA', 'public')

        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {schema}.reviews (
            review_id VARCHAR(64) PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            author VARCHAR(255),
            review_date DATE,
            rating NUMERIC(2,1) CHECK (rating >= 0 AND rating <= 5),
            comment TEXT,
            pros TEXT,
            cons TEXT,
            likes INTEGER DEFAULT 0,
            badges TEXT,
            org_response TEXT,
            processed BOOLEAN DEFAULT FALSE,
            parsed_at TIMESTAMP,
            sentiment VARCHAR(20),
            positive_aspects TEXT[],
            negative_aspects TEXT[],
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_reviews_source ON {schema}.reviews(source);
        CREATE INDEX IF NOT EXISTS idx_reviews_date ON {schema}.reviews(review_date);
        CREATE INDEX IF NOT EXISTS idx_reviews_processed ON {schema}.reviews(processed) WHERE processed = FALSE;
        CREATE INDEX IF NOT EXISTS idx_reviews_sentiment ON {schema}.reviews(sentiment);
        """
        self.cursor.execute(create_table_query)
        self.conn.commit()
        logger.info("Таблицы созданы/обновлены")

    def load_reviews(self, reviews: List[Dict]) -> bool:
        """Загрузка списка отзывов в базу данных с обработкой дубликатов.

        Метод реализует дедупликацию по полю review_id перед вставкой,
        а также использует UPSERT-логику (ON CONFLICT DO UPDATE) для
        обновления существующих записей без создания дублей.

        Args:
            reviews: Список словарей, где каждый словарь представляет отзыв
                с полями: review_id, source, author, review_date, rating,
                comment, pros, cons, likes, badges, org_response, processed,
                parsed_at, sentiment, positive_aspects, negative_aspects.

        Returns:
            True при успешной загрузке данных, False если список пуст.

        Raises:
            Exception: При ошибке выполнения SQL-запроса (исключение
                пробрасывается после отката транзакции).
        """
        if not reviews:
            logger.warning("Нет данных для загрузки")
            return False

        # Дедупликация по review_id перед массовой вставкой
        seen_ids = set()
        unique_reviews = []
        for r in reviews:
            if r['review_id'] not in seen_ids:
                seen_ids.add(r['review_id'])
                unique_reviews.append(r)

        if len(unique_reviews) < len(reviews):
            removed = len(reviews) - len(unique_reviews)
            logger.info(f"Удалено {removed} дубликатов перед загрузкой в БД")

        schema = os.getenv('DB_SCHEMA', 'public')
        query = f"""
        INSERT INTO {schema}.reviews (
            review_id, source, author, review_date, rating, comment,
            pros, cons, likes, badges, org_response, processed, parsed_at,
            sentiment, positive_aspects, negative_aspects
        ) VALUES %s
        ON CONFLICT (review_id) DO UPDATE SET
            rating = EXCLUDED.rating,
            comment = EXCLUDED.comment,
            pros = EXCLUDED.pros,
            cons = EXCLUDED.cons,
            likes = EXCLUDED.likes,
            parsed_at = EXCLUDED.parsed_at,
            sentiment = EXCLUDED.sentiment,
            positive_aspects = EXCLUDED.positive_aspects,
            negative_aspects = EXCLUDED.negative_aspects,
            processed = EXCLUDED.processed,
            updated_at = CURRENT_TIMESTAMP
        """

        values = [
            (
                r['review_id'], r['source'], r['author'], r['review_date'],
                r['rating'], r['comment'],
                r.get('pros'), r.get('cons'),
                r.get('likes', 0), r.get('badges'), r.get('org_response'),
                r.get('processed', False), r['parsed_at'],
                r.get('sentiment'),
                r.get('positive_aspects', []),
                r.get('negative_aspects', [])
            )
            for r in unique_reviews
        ]

        try:
            execute_values(self.cursor, query, values)
            self.conn.commit()
            logger.info(f"Загружено {len(unique_reviews)} отзывов в БД")
            return True
        except Exception as e:
            self.conn.rollback()
            logger.error(f"Ошибка загрузки: {e}")
            raise

    def get_unprocessed_reviews(self, limit: int = 100) -> List[Dict]:
        """Получение списка непроанализированных отзывов для обработки.

        Метод возвращает отзывы с флагом processed = FALSE, ограничивая
        выборку указанным количеством записей. Используется в Airflow DAG
        для пакетного анализа через YandexGPT.

        Args:
            limit: Максимальное количество возвращаемых записей.

        Returns:
            Список словарей, где каждый словарь содержит поля отзыва:
            review_id, source, author, review_date, rating, comment, pros, cons.
        """
        schema = os.getenv('DB_SCHEMA', 'public')
        query = f"""
        SELECT review_id, source, author, review_date, rating, comment, pros, cons
        FROM {schema}.reviews
        WHERE processed = FALSE
        LIMIT %s
        """
        self.cursor.execute(query, (limit,))
        columns = [desc[0] for desc in self.cursor.description]
        return [dict(zip(columns, row)) for row in self.cursor.fetchall()]

    def update_analysis_result(
        self,
        review_id: str,
        sentiment: str,
        positive_aspects: List[str],
        negative_aspects: List[str]
    ):
        """Обновление результатов анализа тональности для конкретного отзыва.

        Метод устанавливает поля sentiment, positive_aspects, negative_aspects
        и обновляет флаг processed на TRUE, фиксируя завершение обработки.

        Args:
            review_id: Уникальный идентификатор отзыва для обновления.
            sentiment: Результат анализа тональности: 'positive', 'negative' или 'neutral'.
            positive_aspects: Список выявленных позитивных аспектов отзыва.
            negative_aspects: Список выявленных негативных аспектов отзыва.
        """
        schema = os.getenv('DB_SCHEMA', 'public')
        query = f"""
        UPDATE {schema}.reviews
        SET
            sentiment = %s,
            positive_aspects = %s,
            negative_aspects = %s,
            processed = TRUE,
            updated_at = CURRENT_TIMESTAMP
        WHERE review_id = %s
        """
        self.cursor.execute(
            query,
            (sentiment, positive_aspects, negative_aspects, review_id)
        )
        self.conn.commit()

    def close(self):
        """Закрытие курсора и соединения с базой данных."""
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
        logger.info("Подключение к БД закрыто")