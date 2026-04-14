"""DAG для сбора, загрузки и анализа отзывов с агрегаторов.

Модуль определяет Apache Airflow DAG для автоматизированного выполнения
сквозного пайплайна обработки отзывов: сбор данных с внешних источников,
загрузка в реляционную базу данных и семантический анализ через YandexGPT API.

Пайплайн выполняется еженедельно и обрабатывает последние 10 отзывов
с каждого из трёх источников: 2GIS, vl.ru и Яндекс Карты.
"""

import logging
import os
import sys
import time
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

# Добавление корня проекта в путь поиска модулей для корректных импортов
# Путь соответствует конфигурации volumes в docker-compose.yml
PROJECT_ROOT = '/opt/project'
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# Загрузка переменных окружения из .env файла
load_dotenv()

# Локальные импорты после настройки sys.path
from database.db_manager import DatabaseManager
from parsers.gis.gis_parser import TwoGISParser
from parsers.vlru.vlru_parser import VLParser
from parsers.yandex.yandex_parser import YandexParser
from yandexgpt_analyzer import YandexGPTAnalyzer

# Настройка логирования для задач DAG
logger = logging.getLogger(__name__)


def parse_all_sources(**context) -> int:
    """Сбор последних отзывов с каждого источника.

    Функция последовательно инициализирует парсеры для трёх платформ,
    выполняет загрузку ограниченного количества отзывов с каждой,
    агрегирует результаты и передаёт их в XCom для следующей задачи.

    Args:
        **context: Словарь контекста выполнения задачи Airflow,
            содержащий объект TaskInstance для работы с XCom.

    Returns:
        Общее количество успешно собранных отзывов.

    Raises:
        Exception: При критической ошибке парсинга (исключение
            пробрасывается после очистки ресурсов).
    """
    all_reviews = []

    # Конфигурация парсеров: имя, экземпляр, URL, лимит
    parsers_config = [
        {
            'name': '2gis',
            'parser': TwoGISParser(headless=True),
            'url': "https://2gis.ru/vladivostok/firm/3518965489880232/tab/reviews",
            'limit': 10
        },
        {
            'name': 'vlru',
            'parser': VLParser(headless=True),
            'url': "https://www.vl.ru/vgues-vladivostoxkij-gosudarstvennyj-universitet",
            'limit': 10
        },
        {
            'name': 'yandex',
            'parser': YandexParser(headless=True),
            'url': "https://yandex.ru/maps/org/vladivostokskiy_gosudarstvenny_universitet/1033268555/reviews/",
            'limit': 10
        }
    ]

    try:
        for config in parsers_config:
            logger.info(f"Парсинг {config['name']}...")

            # Загрузка элементов отзывов со страницы
            elements = config['parser'].load_all_reviews(
                url=config['url'],
                limit=config['limit']
            )

            # Парсинг каждого элемента в структурированные данные
            reviews = [config['parser'].parse_review(el) for el in elements]
            all_reviews.extend(reviews)

            logger.info(f"С {config['name']} собрано {len(reviews)} отзывов")

            # Освобождение ресурсов WebDriver после использования
            config['parser'].close()

        # Передача собранных данных в XCom для следующей задачи
        ti = context['ti']
        ti.xcom_push(key='parsed_reviews', value=all_reviews)

        logger.info(f"Всего собрано {len(all_reviews)} отзывов")
        return len(all_reviews)

    except Exception as e:
        logger.error(f"Ошибка парсинга: {e}")
        # Гарантированная очистка ресурсов в случае ошибки
        for config in parsers_config:
            try:
                config['parser'].close()
            except Exception:
                pass
        raise


def load_to_database(**context) -> int:
    """Загрузка собранных отзывов в базу данных PostgreSQL.

    Функция извлекает данные из XCom, нормализует структуру ключей
    (приводит 'date' к 'review_date') и выполняет массовую загрузку
    через DatabaseManager с обработкой дубликатов.

    Args:
        **context: Словарь контекста выполнения задачи Airflow.

    Returns:
        Количество успешно загруженных отзывов или 0 при ошибке.
    """
    ti = context['ti']
    reviews = ti.xcom_pull(task_ids='parse_all_sources', key='parsed_reviews')

    if not reviews:
        logger.warning("Нет отзывов для загрузки")
        return 0

    # Нормализация ключей: парсеры используют 'date', БД ожидает 'review_date'
    for r in reviews:
        if 'review_date' not in r and 'date' in r:
            r['review_date'] = r['date']

    db = DatabaseManager()
    try:
        success = db.load_reviews(reviews)
        return len(reviews) if success else 0
    finally:
        db.close()


def analyze_unprocessed_reviews(**context) -> int:
    """Анализ непроанализированных отзывов через YandexGPT API.

    Функция выгружает из БД отзывы с флагом processed=FALSE,
    последовательно отправляет их в YandexGPT для определения
    тональности и выделения аспектов, затем обновляет результаты
    в базе данных.

    Args:
        **context: Словарь контекста выполнения задачи Airflow.

    Returns:
        Количество успешно проанализированных отзывов.
    """
    # Инициализация компонентов
    db = DatabaseManager()
    analyzer = YandexGPTAnalyzer(
        folder_id=os.getenv('YC_FOLDER_ID'),
        api_key=os.getenv('YC_API_KEY')
    )

    try:
        # Получение списка непроанализированных отзывов
        reviews = db.get_unprocessed_reviews(limit=None)
        logger.info(f"Найдено {len(reviews)} отзывов для анализа")

        if not reviews:
            logger.info("Нет непроанализированных отзывов")
            return 0

        analyzed_count = 0

        for i, review in enumerate(reviews, 1):
            try:
                logger.info(f"[{i}/{len(reviews)}] Анализ отзыва {review['review_id']}")

                # Формирование полного текста для анализа
                # Учитываем раздельные поля pros/cons для vl.ru
                comment = review.get('comment') or ''
                pros = review.get('pros') or ''
                cons = review.get('cons') or ''
                full_text = f"{pros} {cons} {comment}".strip() or comment

                # Вызов YandexGPT API для анализа тональности
                result = analyzer.analyze_review(
                    comment=full_text,
                    author=review.get('author'),
                    rating=review.get('rating')
                )

                # Обновление результатов анализа в БД
                db.update_analysis_result(
                    review_id=review['review_id'],
                    sentiment=result['sentiment'],
                    positive_aspects=result['positive_aspects'],
                    negative_aspects=result['negative_aspects']
                )
                analyzed_count += 1

                # Rate limiting: пауза между запросами к API
                if i < len(reviews):
                    time.sleep(2)

            except Exception as e:
                logger.error(
                    f"Ошибка анализа отзыва {review.get('review_id')}: {e}"
                )
                # Продолжаем обработку остальных отзывов при ошибке
                continue

        logger.info(f"Проанализировано {analyzed_count} отзывов")
        return analyzed_count

    finally:
        db.close()


# === Конфигурация DAG ===

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

with DAG(
    dag_id='reviews_collection_and_analysis',
    default_args=default_args,
    description='Сбор и анализ отзывов с агрегаторов (2GIS, vl.ru, Yandex)',
    schedule_interval='0 9 * * 1',  # Каждый понедельник в 09:00
    catchup=False,
    tags=['reviews', 'vl.ru', '2gis', 'yandex', 'llm'],
) as dag:

    task_parse = PythonOperator(
        task_id='parse_all_sources',
        python_callable=parse_all_sources,
        provide_context=True,
        execution_timeout=timedelta(minutes=30),
    )

    task_load = PythonOperator(
        task_id='load_to_db',
        python_callable=load_to_database,
        provide_context=True,
    )

    task_analyze = PythonOperator(
        task_id='analyze_unprocessed',
        python_callable=analyze_unprocessed_reviews,
        provide_context=True,
        execution_timeout=timedelta(minutes=45),
    )

    # Определение последовательности выполнения задач
    task_parse >> task_load >> task_analyze