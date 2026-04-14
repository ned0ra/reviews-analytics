"""Сводный анализ отзывов со всех источников (vl.ru, 2gis, yandex).

Модуль обеспечивает приведение данных к единой структуре базы данных
и семантический анализ через YandexGPT API. Поддерживает загрузку отзывов
из различных источников с разной структурой данных и сохранение результатов
в унифицированном формате.
"""

import csv
import json
import logging
import os
import re
import time
from collections import Counter
from datetime import datetime
from typing import List, Dict, Optional

import requests
from dotenv import load_dotenv

# Загрузка переменных окружения из .env файла
load_dotenv()

# Настройка логирования: вывод в файл и консоль
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('llm_all_sources.log', encoding='utf-8', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


class YandexGPTAnalyzer:
    """Класс для анализа отзывов через YandexGPT API.

    Атрибуты:
        folder_id: Идентификатор каталога Yandex Cloud.
        api_key: API-ключ для аутентификации.
        api_url: URL эндпоинта YandexGPT API.
    """

    def __init__(self, folder_id: str, api_key: str):
        """Инициализация анализатора с параметрами подключения к API.

        Args:
            folder_id: Идентификатор каталога Yandex Cloud.
            api_key: API-ключ для аутентификации запросов.
        """
        self.folder_id = folder_id
        self.api_key = api_key
        self.api_url = "https://llm.api.cloud.yandex.net/foundationModels/v1/completion"

    def analyze_review(
        self,
        comment: str,
        author: Optional[str] = None,
        rating: Optional[str] = None
    ) -> Dict:
        """Анализирует отзыв и возвращает тональность с аспектами.

        Метод формирует промпт для YandexGPT, отправляет запрос к API,
        обрабатывает ответ и извлекает структурированные данные о тональности
        и ключевых аспектах отзыва.

        Args:
            comment: Текст отзыва для анализа.
            author: Имя автора отзыва (опционально).
            rating: Числовая оценка отзыва (опционально).

        Returns:
            Словарь с полями:
                - sentiment: "positive", "negative" или "neutral"
                - positive_aspects: список позитивных аспектов
                - negative_aspects: список негативных аспектов
                - confidence: оценка уверенности модели (0.0–1.0)
                - analysis_error: описание ошибки или None
        """
        if not comment or len(comment.strip()) < 10:
            return {
                'sentiment': 'neutral',
                'positive_aspects': [],
                'negative_aspects': [],
                'confidence': 0.0,
                'analysis_error': 'too_short'
            }

        prompt = f"""Ты — аналитик отзывов об образовательных учреждениях.
Проанализируй отзыв и верни ТОЛЬКО валидный JSON без пояснений.

Отзыв:
Автор: {author or 'Аноним'}
Оценка: {rating or 'N/A'}/5
Текст: {comment[:1000]}

Верни строго JSON:
{{
  "sentiment": "positive" или "negative" или "neutral",
  "positive_aspects": ["конкретный плюс 1"],
  "negative_aspects": ["конкретный минус 1"],
  "confidence": 0.9
}}

Правила:
1. sentiment: "positive" если оценка 4-5 или текст хвалит, "negative" если оценка 1-2 или текст критикует,
"neutral" если смешанный
2. positive_aspects: конкретные плюсы из текста
3. negative_aspects: конкретные минусы из текста
4. Если аспектов нет — верни пустой массив []"""

        body = {
            "modelUri": f"gpt://{self.folder_id}/yandexgpt/latest",
            "completionOptions": {
                "stream": False,
                "temperature": 0.1,
                "maxTokens": 1000
            },
            "messages": [
                {"role": "system", "text": "Отвечай только валидным JSON, без markdown и пояснений."},
                {"role": "user", "text": prompt}
            ]
        }

        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Api-Key {self.api_key}',
            'x-folder-id': self.folder_id
        }

        try:
            response = requests.post(self.api_url, headers=headers, json=body, timeout=30)
            response.raise_for_status()
            data = response.json()

            result_text = data.get('result', {}).get('alternatives', [{}])[0].get('message', {}).get('text', '')

            # Очистка от markdown-разметки
            result_text = result_text.strip()
            result_text = re.sub(r'^```(?:json)?\s*', '', result_text)
            result_text = re.sub(r'\s*```$', '', result_text)
            result_text = result_text.strip()

            parsed = json.loads(result_text)

            # Установка значений по умолчанию для отсутствующих полей
            parsed.setdefault('sentiment', 'neutral')
            parsed.setdefault('positive_aspects', [])
            parsed.setdefault('negative_aspects', [])
            parsed.setdefault('confidence', 0.5)

            # Валидация значения sentiment
            if parsed['sentiment'].lower() not in ['positive', 'negative', 'neutral']:
                parsed['sentiment'] = 'neutral'

            parsed['analysis_error'] = None
            return parsed

        except Exception as e:
            logger.error(f"Ошибка анализа: {e}")
            return {
                'sentiment': 'neutral',
                'positive_aspects': [],
                'negative_aspects': [],
                'confidence': 0.0,
                'analysis_error': str(e)
            }


def load_vl_reviews(filepath: str, limit: int = 5) -> List[Dict]:
    """Загружает отзывы с vl.ru с учётом специфической структуры данных.

    Для vl.ru отзывы содержат раздельные поля pros/cons/comment,
    которые объединяются для анализа, но сохраняются раздельно.

    Args:
        filepath: Путь к CSV-файлу с отзывами.
        limit: Максимальное количество отзывов для загрузки.

    Returns:
        Список словарей с отзывами в унифицированном формате.
    """
    reviews = []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if len(reviews) >= limit:
                break

            pros = row.get('pros', '') or ''
            cons = row.get('cons', '') or ''
            comment = row.get('comment', '') or ''

            # Объединение полей для отправки в LLM
            full_text = f"{pros}\n{cons}\n{comment}".strip()

            reviews.append({
                'review_id': row.get('review_id', ''),
                'source': 'vl.ru',
                'author': row.get('author', ''),
                'review_date': row.get('date', ''),
                'rating': row.get('rating', ''),
                'comment': comment,
                'pros': pros,
                'cons': cons,
                'likes': row.get('likes', 0),
                'badges': row.get('badges', ''),
                'org_response': row.get('org_response', ''),
                'full_text_for_analysis': full_text
            })

    logger.info(f"Загружено {len(reviews)} отзывов из vl.ru")
    return reviews


def load_generic_reviews(filepath: str, source_name: str, limit: int = 5) -> List[Dict]:
    """Загружает отзывы с 2gis/yandex со стандартной структурой данных.

    Для 2gis и Яндекс Карт отзывы содержат единое поле comment,
    которое используется как для хранения, так и для анализа.

    Args:
        filepath: Путь к CSV-файлу с отзывами.
        source_name: Название источника данных.
        limit: Максимальное количество отзывов для загрузки.

    Returns:
        Список словарей с отзывами в унифицированном формате.
    """
    reviews = []
    with open(filepath, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if len(reviews) >= limit:
                break

            comment = row.get('comment', '') or ''
            if comment is None or str(comment).lower() == 'nan':
                comment = ''

            reviews.append({
                'review_id': row.get('review_id', ''),
                'source': source_name,
                'author': row.get('author', ''),
                'review_date': row.get('date', ''),
                'rating': row.get('rating', ''),
                'comment': str(comment).strip(),
                'pros': '',
                'cons': '',
                'likes': row.get('likes', 0),
                'badges': row.get('badges', ''),
                'org_response': row.get('org_response', ''),
                'full_text_for_analysis': comment
            })

    logger.info(f"Загружено {len(reviews)} отзывов из {source_name}")
    return reviews


def save_to_unified_csv(reviews: List[Dict], output_path: str):
    """Сохраняет обработанные отзывы в CSV-файл с унифицированной структурой.

    Формат выходного файла соответствует схеме таблицы reviews в БД,
    включая поля для результатов анализа тональности.

    Args:
        reviews: Список обработанных отзывов.
        output_path: Путь для сохранения выходного файла.
    """
    if not reviews:
        logger.warning("Нет данных для сохранения")
        return

    fieldnames = [
        'review_id', 'source', 'author', 'review_date', 'rating',
        'comment', 'pros', 'cons', 'likes', 'badges', 'org_response',
        'processed', 'parsed_at',
        'sentiment', 'positive_aspects', 'negative_aspects',
        'analysis_error', 'analyzed_at'
    ]

    with open(output_path, 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(reviews)

    logger.info(f"Сохранено {len(reviews)} отзывов в {output_path}")


def main():
    """Основная функция: загрузка отзывов, анализ через LLM, сохранение результатов."""

    # Конфигурация источников данных
    CONFIG = {
        'vl_ru': {
            'path': 'parsers/data/vl_reviews.csv',
            'source': 'vl.ru',
            'limit': 5,
            'type': 'vl'
        },
        '2gis': {
            'path': 'parsers/data/2gis_reviews.csv',
            'source': '2gis.ru',
            'limit': 5,
            'type': 'generic'
        },
        'yandex': {
            'path': 'parsers/data/yandex_reviews.csv',
            'source': 'yandex.ru/maps',
            'limit': 5,
            'type': 'generic'
        }
    }

    OUTPUT_FILE = 'parsers/data/all_reviews_analyzed.csv'
    DELAY_BETWEEN_REQUESTS = 2  # Задержка между запросами к API (секунды)

    folder_id = os.getenv('YC_FOLDER_ID')
    api_key = os.getenv('YC_API_KEY')

    if not folder_id or not api_key:
        logger.error("Не найдены YC_FOLDER_ID или YC_API_KEY в .env")
        return

    analyzer = YandexGPTAnalyzer(folder_id, api_key)
    logger.info("YandexGPTAnalyzer инициализирован")

    # Загрузка отзывов из всех источников
    all_reviews = []
    for key, config in CONFIG.items():
        try:
            if not os.path.exists(config['path']):
                logger.warning(f"Файл не найден: {config['path']}")
                continue

            if config['type'] == 'vl':
                reviews = load_vl_reviews(config['path'], limit=config['limit'])
            else:
                reviews = load_generic_reviews(
                    config['path'],
                    config['source'],
                    limit=config['limit']
                )
            all_reviews.extend(reviews)

        except Exception as e:
            logger.error(f"Ошибка загрузки {config['path']}: {e}")

    if not all_reviews:
        logger.error("Нет отзывов для анализа")
        return

    logger.info(f"Всего загружено {len(all_reviews)} отзывов из {len(CONFIG)} источников")

    # Анализ отзывов через YandexGPT
    logger.info(f"Начинаю анализ {len(all_reviews)} отзывов...")
    start_time = time.time()

    for i, review in enumerate(all_reviews, 1):
        source = review.get('source', 'unknown')
        author = review.get('author', 'N/A')
        logger.info(f"[{i}/{len(all_reviews)}] Анализ: {source} - {author}")

        result = analyzer.analyze_review(
            comment=review.get('full_text_for_analysis', ''),
            author=author,
            rating=review.get('rating')
        )

        # Добавление результатов анализа к отзыву
        review.update(result)
        review['analyzed_at'] = datetime.now().isoformat()
        review['processed'] = True

        sentiment = review.get('sentiment', 'N/A')
        pos_count = len(review.get('positive_aspects', []))
        neg_count = len(review.get('negative_aspects', []))
        logger.info(f"  -> {sentiment} (+{pos_count}/-{neg_count})")

        # Rate limiting для API
        if i < len(all_reviews):
            time.sleep(DELAY_BETWEEN_REQUESTS)

    elapsed = time.time() - start_time
    logger.info(f"Анализ завершён за {elapsed:.1f}с ({elapsed/len(all_reviews):.2f}с/отзыв)")

    # Статистика по тональности
    sentiments = [r.get('sentiment') for r in all_reviews if r.get('sentiment')]
    if sentiments:
        stats = Counter(sentiments)
        logger.info(f"Распределение тональности: {dict(stats)}")

    # Статистика по источникам
    by_source = {}
    for r in all_reviews:
        src = r.get('source', 'unknown')
        if src not in by_source:
            by_source[src] = []
        by_source[src].append(r.get('sentiment'))

    logger.info("По источникам:")
    for src, sents in by_source.items():
        stats = Counter(sents)
        logger.info(f"  {src}: {dict(stats)}")

    # Сохранение результатов
    save_to_unified_csv(all_reviews, OUTPUT_FILE)

    logger.info(f"Готово! Результаты в: {OUTPUT_FILE}")
    logger.info(f"ИТОГО: {len(all_reviews)} отзывов проанализировано")
    if sentiments:
        logger.info(f"Тональность: {dict(Counter(sentiments))}")


if __name__ == "__main__":
    main()