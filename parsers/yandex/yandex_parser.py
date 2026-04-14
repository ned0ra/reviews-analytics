"""Парсер отзывов с сайта Яндекс Карт.

Модуль предоставляет функционал для автоматизированного сбора отзывов
с платформы Яндекс Карт, включая обработку динамической подгрузки контента,
раскрытие скрытого текста и ответов организаций, нормализацию данных
и сохранение результатов в форматах CSV и PostgreSQL.
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
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

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
        logging.FileHandler('yandex_parser.log', encoding='utf-8', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Путь к директории для сохранения данных
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def parse_russian_date(date_text: str) -> Optional[str]:
    """Конвертирует дату из русскоязычного формата в формат ISO (YYYY-MM-DD).

    Поддерживает обработку:
    - Относительных дат ("сегодня", "вчера", "неделю назад")
    - Русских названий месяцев ("15 марта 2024")
    - Уже отформатированных дат в формате ISO

    Args:
        date_text: Строка с датой на русском языке.

    Returns:
        Дата в формате 'YYYY-MM-DD' или None, если парсинг не удался.
    """
    if not date_text:
        return None

    try:
        # Если дата уже в нужном формате — возвращаем как есть
        if re.match(r'\d{4}-\d{2}-\d{2}', date_text):
            return date_text[:10]

        # Удаление пометки об редактировании
        date_text = re.sub(
            r',\s*отредактирован[оа]?\s*$',
            '',
            date_text.strip(),
            flags=re.IGNORECASE
        )

        # Обработка относительных дат
        relative_patterns = [
            r'сегодня', r'вчера', r'неделю', r'месяц', r'год',
            r'день назад', r'дня назад', r'дней назад'
        ]
        for pattern in relative_patterns:
            if pattern in date_text.lower():
                return datetime.now().strftime("%Y-%m-%d")

        # Словарь для замены русских месяцев на числовые значения
        months = {
            'января': '01', 'февраля': '02', 'марта': '03', 'апреля': '04',
            'мая': '05', 'июня': '06', 'июля': '07', 'августа': '08',
            'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
        }

        # Замена русского названия месяца на число
        for ru, num in months.items():
            if ru in date_text:
                date_text = re.sub(rf'\b{ru}\b', num, date_text)
                break

        # Парсинг формата "день месяц год"
        parts = date_text.split()
        if len(parts) >= 2:
            year = datetime.now().year
            return f"{year}-{parts[1]}-{parts[0].zfill(2)}"
    except Exception:
        pass

    return None


class YandexParser:
    """Парсер отзывов с сайта Яндекс Карт.

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

    def _close_cookie_banner(self):
        """Закрытие баннера с согласием на использование cookie, если он отображается."""
        try:
            btn = self.driver.find_element(
                By.XPATH,
                "//button[contains(., 'Принять') or contains(., 'OK') or @aria-label='Закрыть']"
            )
            self.driver.execute_script("arguments[0].click();", btn)
            time.sleep(1)
        except Exception:
            pass

    def _set_sorting_by_newest(self) -> bool:
        """Установка сортировки отзывов по новизне.

        Returns:
            True, если сортировка успешно установлена или уже была "По новизне",
            False в случае ошибки.
        """
        try:
            time.sleep(3)
            sort_btn = WebDriverWait(self.driver, 10).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, "div.rating-ranking-view[role='button']"))
            )
            current_sort = sort_btn.text.strip()

            if "По новизне" in current_sort:
                return True

            self.driver.execute_script("arguments[0].click();", sort_btn)
            time.sleep(1)
            newest_option = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//div[contains(@class, 'rating-ranking-view__popup-line') and contains(text(), 'По новизне')]"
                ))
            )
            self.driver.execute_script("arguments[0].click();", newest_option)
            time.sleep(3)
            return True
        except Exception as e:
            logger.warning(f"Не удалось изменить сортировку: {e}")
            return False

    def _get_review_containers(self) -> List:
        """Получение списка DOM-элементов, содержащих отзывы.

        Returns:
            Список WebElement объектов с отзывами.
        """
        return self.driver.find_elements(
            By.CSS_SELECTOR,
            'div.business-reviews-card-view__review[role="listitem"]'
        )

    def _expand_review_if_needed(self, container):
        """Раскрытие полного текста отзыва, если он скрыт под кнопкой "Ещё".

        Args:
            container: WebElement контейнера отзыва.
        """
        try:
            btn = container.find_element(
                By.CSS_SELECTOR,
                'span.business-review-view__expand, button.business-review-view__expand'
            )
            if btn.is_displayed() and "Ещё" in btn.text:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
                time.sleep(0.5)
                self.driver.execute_script("arguments[0].click();", btn)
                time.sleep(0.5)
        except Exception:
            pass

    def _expand_org_response_if_needed(self, container):
        """Раскрытие ответа организации на отзыв, если он доступен.

        Args:
            container: WebElement контейнера отзыва.
        """
        try:
            btn = container.find_element(
                By.XPATH,
                ".//div[contains(@class, 'business-review-view__comment-expand') and contains(text(), 'Посмотреть ответ')]"
            )
            if btn.is_displayed():
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", btn)
                time.sleep(0.3)
                self.driver.execute_script("arguments[0].click();", btn)
                time.sleep(1)
        except Exception:
            pass

    def _extract_rating(self, soup: BeautifulSoup) -> Optional[float]:
        """Извлечение числового рейтинга отзыва из HTML-разметки.

        Сначала пытается найти значение в meta-теге, затем парсит aria-label
        со звёздами.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Рейтинг с точностью до одного знака после запятой или None.
        """
        # Попытка извлечь из meta-тега
        rating_meta = soup.select_one('meta[itemprop="ratingValue"]')
        if rating_meta and rating_meta.get('content'):
            try:
                return round(float(rating_meta['content']), 1)
            except Exception:
                pass

        # Попытка извлечь из aria-label контейнера со звёздами
        stars_container = soup.select_one('div[aria-label*="Оценка"]')
        if stars_container:
            label = stars_container.get('aria-label', '')
            match = re.search(r'Оценка\s*(\d+\.?\d*)\s*Из', label)
            if match:
                return round(float(match.group(1)), 1)
        return None

    def _extract_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение и нормализация даты публикации отзыва.

        Поддерживает несколько форматов хранения даты в DOM:
        meta-теги, time-теги, текстовые элементы.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Дата в формате 'YYYY-MM-DD' или None.
        """
        # Извлечение из meta-тега с ISO-датой
        date_meta = soup.select_one('meta[itemprop="datePublished"]')
        if date_meta and date_meta.get('content'):
            content = date_meta['content']
            if 'T' in content:
                return content.split('T')[0]
            return content[:10]

        # Извлечение из time-тега
        time_el = soup.select_one('time[itemprop="datePublished"]')
        if time_el and time_el.get('datetime'):
            return time_el['datetime'][:10]

        # Парсинг текстового представления даты
        date_el = soup.select_one('span.business-review-view__date, time')
        if date_el:
            date_text = date_el.get_text(strip=True)
            return parse_russian_date(date_text)
        return None

    def _extract_comment(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение текста отзыва из HTML-разметки.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Текст отзыва или None, если не найден.
        """
        text_el = soup.select_one(
            'span.spoiler-view__text, div.spoiler-view__text, div[itemprop="reviewBody"]'
        )
        return text_el.get_text(strip=True) if text_el else None

    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение имени автора отзыва.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Имя автора или None.
        """
        author_el = soup.select_one('span[itemprop="name"], .business-review-view__author-name')
        return author_el.get_text(strip=True) if author_el else None

    def _extract_badges(self, soup: BeautifulSoup) -> List[str]:
        """Извлечение бейджей/статусов автора отзыва.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Список текстовых описаний бейджей.
        """
        badges = []
        caption_el = soup.select_one('div.business-review-view__author-caption')
        if caption_el:
            text = caption_el.get_text(strip=True)
            if text:
                badges.append(text)
        return badges

    def _extract_org_response(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение текста ответа организации на отзыв.

        Метод проверяет несколько возможных селекторов для ответа,
        фильтрует пустые и слишком короткие значения.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Текст ответа организации или None.
        """
        # Первый вариант селектора
        resp_bubble = soup.select_one('div.business-review-comment-content__bubble')
        if resp_bubble:
            text = resp_bubble.get_text(strip=True)
            if text and len(text) > 10:
                return text

        # Второй вариант селектора
        resp_container = soup.select_one('div.business-review-comment__comment')
        if resp_container:
            text = resp_container.get_text(strip=True)
            if text and len(text) > 20:
                # Берём последнюю непустую строку (сам ответ)
                lines = [l.strip() for l in text.split('\n') if len(l.strip()) > 10]
                if lines:
                    return lines[-1]
        return None

    def load_all_reviews(self, url: str, limit: Optional[int] = 10, max_scrolls: int = 100) -> List:
        """Загрузка отзывов с указанной страницы с автоматической прокруткой.

        Метод реализует умную логику завершения загрузки:
        - Остановка при достижении лимита отзывов
        - Детекция конца списка по индикаторам "Больше нет"
        - Выход при отсутствии новых элементов после нескольких попыток

        Args:
            url: URL страницы с отзывами.
            limit: Максимальное количество отзывов для загрузки (None = все).
            max_scrolls: Максимальное количество итераций прокрутки.

        Returns:
            Список WebElement объектов с загруженными отзывами.
        """
        logger.info(f"Открытие: {url}, лимит: {limit if limit else 'все'}")
        self.driver.get(url)

        # Первоначальная загрузка страницы и закрытие баннеров
        time.sleep(6)
        self._close_cookie_banner()
        time.sleep(2)

        # Установка сортировки по новизне
        self._set_sorting_by_newest()
        time.sleep(3)

        # Проверка: если уже есть достаточно отзывов — возвращаем сразу
        prev_count = len(self._get_review_containers())
        if limit and prev_count >= limit:
            return self._get_review_containers()[:limit]

        # Счётчики для детекции завершения загрузки
        no_new_count = 0
        max_no_new = 8
        prev_height = 0
        same_height_count = 0

        for iteration in range(1, max_scrolls + 1):
            # Пошаговая прокрутка страницы
            current_scroll = 0
            max_scroll = self.driver.execute_script("return document.body.scrollHeight")

            while current_scroll < max_scroll:
                scroll_step = min(current_scroll + 300, max_scroll)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_step});")
                time.sleep(0.5)
                current_scroll = scroll_step

                # Проверка появления новых отзывов во время прокрутки
                if len(self._get_review_containers()) > prev_count:
                    break

            # Пауза для подгрузки динамического контента
            time.sleep(4)

            # Дополнительная проверка с задержками
            for _ in range(4):
                time.sleep(2)
                if len(self._get_review_containers()) > prev_count:
                    break

            # Анализ результатов итерации
            curr_count = len(self._get_review_containers())
            curr_height = self.driver.execute_script("return document.body.scrollHeight")
            new_added = curr_count - prev_count

            if new_added > 0:
                # Новые отзывы добавлены — сбрасываем счётчики
                no_new_count = 0
                same_height_count = 0
                prev_count = curr_count

                if limit and curr_count >= limit:
                    break
            else:
                # Новые отзывы не добавлены
                no_new_count += 1

                # Проверка стабильности высоты страницы
                if curr_height == prev_height:
                    same_height_count += 1
                    if same_height_count >= 3 and no_new_count >= 3:
                        break
                else:
                    same_height_count = 0

                # Максимальное количество пустых итераций
                if no_new_count >= max_no_new:
                    break

            # Проверка индикаторов конца списка
            try:
                end_indicators = [
                    "//text()[contains(., 'Больше нет')]",
                    "//text()[contains(., 'Все отзывы показаны')]"
                ]
                for xpath in end_indicators:
                    if self.driver.find_elements(By.XPATH, xpath):
                        no_new_count = max_no_new
                        break
            except Exception:
                pass

            prev_height = curr_height

        # Формирование итогового списка с учётом лимита
        elements = self._get_review_containers()
        if limit and len(elements) > limit:
            elements = elements[:limit]

        return elements

    def parse_review(self, element) -> Dict:
        """Парсинг данных из WebElement контейнера отзыва.

        Метод последовательно раскрывает скрытый контент, извлекает все поля
        и формирует словарь с нормализованными данными.

        Args:
            element: WebElement контейнера отзыва.

        Returns:
            Словарь с данными отзыва, включая:
            - review_id: Уникальный хеш-идентификатор
            - source: Источник данных
            - author, date, rating, comment: Основные поля отзыва
            - likes, badges, org_response: Дополнительные метаданные
            - processed, parsed_at: Служебные поля
        """
        # Раскрытие полного контента отзыва и ответа организации
        self._expand_review_if_needed(element)
        time.sleep(0.3)
        self._expand_org_response_if_needed(element)
        time.sleep(0.8)

        # Парсинг HTML через BeautifulSoup
        html = element.get_attribute('outerHTML')
        soup = BeautifulSoup(html, 'lxml')

        return {
            "review_id": self._generate_review_id(
                self._extract_author(soup),
                self._extract_date(soup),
                self._extract_comment(soup)
            ),
            "source": "yandex.ru/maps",
            "author": self._extract_author(soup),
            "date": self._extract_date(soup),
            "rating": self._extract_rating(soup),
            "comment": self._extract_comment(soup),
            "likes": 0,
            "badges": "; ".join(self._extract_badges(soup)) or None,
            "org_response": self._extract_org_response(soup),
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
            "comment", "likes", "badges", "org_response",
            "processed", "parsed_at"
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
    arg_parser = argparse.ArgumentParser(description='Парсер отзывов с Яндекс Карт')
    arg_parser.add_argument('--limit', type=int, default=10, help='Количество отзывов (None = все)')
    arg_parser.add_argument('--no-db', action='store_true', help='Не загружать в БД, только сохранить CSV')
    arg_parser.add_argument('--url', type=str,
                           default="https://yandex.ru/maps/org/vladivostokskiy_gosudarstvenny_universitet/1033268555/reviews/",
                           help='URL страницы с отзывами')
    args = arg_parser.parse_args()

    parser = None
    try:
        parser = YandexParser(headless=True)
        elements = parser.load_all_reviews(args.url, limit=args.limit)

        if not elements:
            logger.error("Отзывы не найдены")
            return

        reviews = [parser.parse_review(el) for el in elements]
        logger.info(f"Спаршено отзывов: {len(reviews)}")

        if reviews:
            filename = f"yandex_reviews_{args.limit if args.limit else 'all'}.csv"
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