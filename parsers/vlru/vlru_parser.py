"""Парсер отзывов с сайта vl.ru.

Модуль предоставляет функционал для автоматизированного сбора отзывов
с платформы vl.ru, включая обработку динамической подгрузки контента,
разделение текста на структурные поля и сохранение результатов в форматах
CSV и PostgreSQL.
"""

import argparse
import csv
import hashlib
import logging
import os
import platform
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional

from bs4 import BeautifulSoup
from dotenv import load_dotenv
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By

# Добавление корня проекта в путь поиска модулей для корректных импортов
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

load_dotenv()

# Настройка логирования: вывод в файл и консоль
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    handlers=[
        logging.FileHandler('vl_parser.log', encoding='utf-8', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Путь к директории для сохранения данных
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def parse_russian_date(date_str: str) -> Optional[str]:
    """Преобразует дату из русского формата в формат ISO (YYYY-MM-DD).

    Args:
        date_str: Строка с датой на русском языке (например, "15 марта 2024").

    Returns:
        Дата в формате 'YYYY-MM-DD' или None, если парсинг не удался.
    """
    if not date_str:
        return None

    months = {
        'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
        'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
        'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12',
    }

    try:
        # Удаление суффиксов и пометок об редактировании
        date_str = re.sub(r'\s*г\.?\s*$', '', date_str.strip())
        date_str = re.sub(r'\s*,\s*отредактирован[оа]?\s*$', '', date_str, flags=re.IGNORECASE)

        # Замена названия месяца на числовой эквивалент
        month_num = None
        for ru_month, num_month in months.items():
            if ru_month.lower() in date_str.lower():
                month_num = num_month
                date_str = re.sub(rf'{re.escape(ru_month)}', num_month, date_str, flags=re.IGNORECASE)
                break

        if not month_num:
            return None

        # Извлечение дня и года из строки
        numbers = re.findall(r'\d+', date_str)
        day = year = None
        for num in numbers:
            if len(num) == 4:
                year = num
            elif len(num) <= 2 and day is None:
                day = num.zfill(2)

        if not year:
            year = str(datetime.now().year)

        if day and month_num:
            return f"{year}-{month_num}-{day}"
    except Exception:
        pass

    return None


class VLParser:
    """Парсер отзывов с сайта vl.ru.

    Атрибуты:
        driver: Экземпляр Selenium WebDriver для автоматизации браузера.
        db_manager: Опциональный экземпляр DatabaseManager для загрузки данных.
    """

    def __init__(self, headless: bool = True, db_manager=None):
        """Инициализация WebDriver с авто-определением окружения.

        Args:
            headless: Запуск браузера в фоновом режиме.
            db_manager: Экземпляр DatabaseManager для загрузки в БД (опционально).
        """
        opts = Options()
        if headless:
            # Выбор режима headless в зависимости от ОС
            opts.add_argument("--headless=new" if platform.system() != "Windows" else "--headless")
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument("--window-size=1920,1080")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_argument("--disable-blink-features=AutomationControlled")

        # Выбор драйвера: системный для Docker/Linux, авто-установка для локального запуска
        if platform.system() == "Linux" and os.path.exists("/usr/bin/chromedriver"):
            service = Service(executable_path="/usr/bin/chromedriver")
            logger.info("Используется системный chromedriver (Docker/Linux)")
        else:
            from webdriver_manager.chrome import ChromeDriverManager
            service = Service(ChromeDriverManager().install())
            logger.info("ChromeDriver установлен через webdriver-manager")

        self.driver = webdriver.Chrome(service=service, options=opts)
        # Скрытие признака автоматизации для обхода базовой защиты
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        })
        self.db_manager = db_manager
        logger.info("WebDriver инициализирован")

    def _generate_review_id(self, author: str, date: str, comment: str) -> str:
        """Генерация уникального идентификатора отзыва.

        Создает хеш на основе комбинации автора, даты и текста комментария
        для детекции дубликатов при повторном парсинге.

        Args:
            author: Имя автора отзыва.
            date: Дата публикации отзыва.
            comment: Текст отзыва.

        Returns:
            Уникальный идентификатор (первые 16 символов MD5-хеша).
        """
        content = f"{author or ''}{date or ''}{comment or ''}"
        return hashlib.md5(content.encode('utf-8')).hexdigest()[:16]

    def count_reviews(self) -> int:
        """Подсчёт количества основных отзывов на странице.

        Исключает ответы организации и другие вторичные элементы.

        Returns:
            Количество найденных основных отзывов.
        """
        elements = self.driver.find_elements(By.CSS_SELECTOR, 'li[data-type="review"]')
        count = 0
        for el in elements:
            cls = el.get_attribute('class') or ''
            dtype = el.get_attribute('data-type')
            if 'answer' not in cls.lower() and dtype == 'review':
                count += 1
        return count

    def load_all_reviews(self, url: str, limit: Optional[int] = 10, max_clicks: int = 50) -> List:
        """Загрузка отзывов с указанной страницы с поддержкой динамической подгрузки.

        Алгоритм:
        1. Открытие страницы и начальная задержка для рендеринга.
        2. Циклический клик по кнопке "Показать ещё" до достижения лимита.
        3. Остановка при отсутствии новых отзывов или достижении лимита итераций.

        Args:
            url: URL страницы с отзывами.
            limit: Максимальное количество отзывов для сбора (None = все).
            max_clicks: Максимальное количество кликов по кнопке подгрузки.

        Returns:
            Список WebElement объектов с загруженными отзывами.
        """
        logger.info(f"Открытие страницы: {url}, лимит: {limit if limit else 'все'}")
        self.driver.get(url)
        time.sleep(3)

        prev_count = self.count_reviews()
        if limit and prev_count >= limit:
            return self._get_review_elements(limit)

        for click_num in range(1, max_clicks + 1):
            try:
                btn = self.driver.find_element(
                    By.CSS_SELECTOR,
                    'p.loadMoreComments[action="loadMoreComments"]'
                )
            except Exception:
                logger.info("Кнопка подгрузки не найдена — все отзывы загружены")
                break

            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
            time.sleep(1.5)
            logger.info(f"Клик #{click_num}")
            self.driver.execute_script("arguments[0].click();", btn)
            time.sleep(3)

            curr_count = self.count_reviews()
            if curr_count <= prev_count:
                logger.info("Новые отзывы не появились")
                break

            if limit and curr_count >= limit:
                logger.info(f"Достигнут лимит: {curr_count}")
                break

            prev_count = curr_count

        return self._get_review_elements(limit)

    def _get_review_elements(self, limit: Optional[int] = None) -> List:
        """Извлечение элементов основных отзывов из DOM.

        Фильтрует элементы, исключая ответы организации и другие типы.

        Args:
            limit: Ограничение количества возвращаемых элементов.

        Returns:
            Отфильтрованный список WebElement объектов.
        """
        elements = self.driver.find_elements(By.CSS_SELECTOR, 'li[data-type="review"]')
        main_reviews = [
            el for el in elements
            if 'answer' not in (el.get_attribute('class') or '').lower()
            and el.get_attribute('data-type') == 'review'
        ]

        if limit:
            main_reviews = main_reviews[:limit]
        return main_reviews

    def _extract_text_by_label(self, html: str, label: str) -> Optional[str]:
        """Извлечение текста по метке из HTML-блока отзыва.

        Ищет паттерн: <b>Метка:</b> текст

        Args:
            html: HTML-строка для парсинга.
            label: Искомая метка (например, "Достоинства").

        Returns:
            Извлечённый текст или None, если метка не найдена.
        """
        pattern = rf'<b>{label}:</b>\s*(.+?)(?=<br\s*/?>|<b>|$)'
        match = re.search(pattern, html, re.DOTALL | re.IGNORECASE)
        if match:
            # Удаление HTML-тегов и нормализация пробелов
            text = re.sub(r'<[^>]+>', ' ', match.group(1))
            return re.sub(r'\s+', ' ', text).strip()
        return None

    def parse_review(self, element) -> Dict:
        """Парсинг данных из одного WebElement контейнера отзыва.

        Алгоритм:
        1. Извлечение HTML и парсинг через BeautifulSoup.
        2. Сбор всех полей: автор, дата, рейтинг, текст, лайки, бейджи.
        3. Разделение текста отзыва на Достоинства/Недостатки/Комментарий.
        4. Генерация уникального ID для дедупликации.

        Args:
            element: WebElement контейнера отзыва.

        Returns:
            Словарь с данными отзыва, включая:
            - review_id: Уникальный хеш-идентификатор
            - source: Источник данных
            - author, date, rating: Основные поля отзыва
            - pros, cons, comment: Структурированный текст отзыва
            - likes, badges, org_response: Дополнительные метаданные
            - processed, parsed_at: Служебные поля
        """
        html = element.get_attribute('outerHTML')
        soup = BeautifulSoup(html, 'lxml')
        li = soup.find('li') or soup

        def safe_text(selector: str) -> Optional[str]:
            el = soup.select_one(selector)
            return el.get_text(strip=True) if el else None

        def safe_attr(selector: str, attr: str) -> Optional[str]:
            el = soup.select_one(selector)
            return el.get(attr) if el else None

        # Извлечение рейтинга (конвертация из 5-балльной шкалы сайта)
        rating_val = safe_attr("div.star-rating .active", "data-value")
        rating = round(float(rating_val) * 5, 1) if rating_val else None

        # Парсинг текста отзыва: разделение на Достоинства/Недостатки/Комментарий
        blockquote = soup.select_one("blockquote")
        pros = cons = comment = None
        if blockquote:
            block_html = str(blockquote)
            pros = self._extract_text_by_label(block_html, "Достоинства")
            cons = self._extract_text_by_label(block_html, "Недостатки")
            comment = self._extract_text_by_label(block_html, "Комментарий")

        author = safe_text("span.user-name[data-content='name']")
        date_raw = safe_text("span.time")

        likes_text = safe_text("span.likes__counter")
        likes = int(likes_text) if likes_text and likes_text.isdigit() else 0

        return {
            "review_id": self._generate_review_id(author, date_raw, comment),
            "source": "vl.ru",
            "author": author,
            "date": parse_russian_date(date_raw),
            "rating": rating,
            "pros": pros,
            "cons": cons,
            "comment": comment,
            "likes": likes,
            "badges": None,
            "org_response": None,
            "processed": False,
            "parsed_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

    def save_to_csv(self, reviews: List[Dict], filename: str) -> bool:
        """Сохранение списка отзывов в CSV-файл.

        Args:
            reviews: Список словарей с данными отзывов.
            filename: Имя файла для сохранения (без пути).

        Returns:
            True при успешном сохранении, False если список пуст.
        """
        if not reviews:
            logger.warning("Нет данных для сохранения")
            return False

        filepath = os.path.join(DATA_DIR, filename)
        fieldnames = [
            "review_id", "source", "author", "date", "rating",
            "pros", "cons", "comment", "likes",
            "badges", "org_response", "processed", "parsed_at"
        ]

        with open(filepath, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            writer.writeheader()
            writer.writerows(reviews)

        logger.info(f"Сохранено {len(reviews)} отзывов в {filepath}")
        return True

    def load_to_database(self, reviews: List[Dict]) -> bool:
        """Загрузка отзывов в БД через DatabaseManager.

        Если передан db_manager — использует его (режим Airflow DAG).
        Иначе создаёт временный DatabaseManager (режим standalone).

        Args:
            reviews: Список словарей с данными отзывов.

        Returns:
            True при успешной загрузке, False если список пуст.
        """
        if not reviews:
            logger.warning("Нет данных для загрузки в БД")
            return False

        # Нормализация ключей: парсеры отдают 'date', БД ожидает 'review_date'
        for r in reviews:
            if 'review_date' not in r and 'date' in r:
                r['review_date'] = r['date']

        if self.db_manager:
            return self.db_manager.load_reviews(reviews)

        from database.db_manager import DatabaseManager
        db = DatabaseManager()
        try:
            return db.load_reviews(reviews)
        finally:
            db.close()

    def close(self):
        """Корректное завершение работы WebDriver и освобождение ресурсов."""
        if self.driver:
            self.driver.quit()
            logger.info("WebDriver закрыт")


def main():
    """Точка входа: запуск парсинга с параметрами командной строки."""
    arg_parser = argparse.ArgumentParser(description='Парсер отзывов с vl.ru')
    arg_parser.add_argument('--limit', type=int, default=10, help='Количество отзывов (None = все)')
    arg_parser.add_argument('--no-db', action='store_true', help='Не загружать в БД, только сохранить CSV')
    arg_parser.add_argument('--url', type=str,
                           default="https://www.vl.ru/vgues-vladivostoxkij-gosudarstvennyj-universitet",
                           help='URL страницы с отзывами')
    args = arg_parser.parse_args()

    parser = None
    try:
        parser = VLParser(headless=True)
        elements = parser.load_all_reviews(args.url, limit=args.limit)

        if not elements:
            logger.error("Отзывы не найдены")
            return

        reviews = [parser.parse_review(el) for el in elements]
        logger.info(f"Спаршено отзывов: {len(reviews)}")

        if reviews:
            filename = f"vl_reviews_{args.limit if args.limit else 'all'}.csv"
            parser.save_to_csv(reviews, filename)

            if not args.no_db:
                parser.load_to_database(reviews)
                logger.info("Данные загружены в БД")

    except Exception as e:
        logger.error(f"Критическая ошибка: {e}", exc_info=True)
        raise
    finally:
        if parser:
            parser.close()


if __name__ == "__main__":
    main()