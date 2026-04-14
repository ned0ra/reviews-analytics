"""Парсер отзывов с сайта 2GIS.

Модуль предоставляет функционал для автоматизированного сбора отзывов
с платформы 2GIS, включая обработку динамической подгрузки контента,
нормализацию данных и сохранение результатов в форматах CSV и PostgreSQL.
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
        logging.FileHandler('2gis_parser.log', encoding='utf-8', mode='w'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Путь к директории для сохранения данных
DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'data')
os.makedirs(DATA_DIR, exist_ok=True)


def parse_russian_date(date_str: str) -> Optional[str]:
    """Преобразует русскоязычную дату в формат ISO (YYYY-MM-DD).

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
        'сентября': '09', 'октября': '10', 'ноября': '11', 'декабря': '12'
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


class TwoGISParser:
    """Парсер отзывов с сайта 2GIS.

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

    def _close_modal_if_present(self):
        """Закрытие модального окна с согласием на cookie, если оно присутствует."""
        try:
            btn = self.driver.find_element(By.XPATH, "//button[contains(.//span, 'Хорошо')]")
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
                EC.element_to_be_clickable((
                    By.CSS_SELECTOR,
                    "div._jyy5a0, div[title='По доверию'], div[title='По новизне']"
                ))
            )
            current_sort = sort_btn.text.strip() or sort_btn.get_attribute('title')

            if "По новизне" in current_sort:
                return True

            self.driver.execute_script("arguments[0].click();", sort_btn)
            time.sleep(1)
            newest_option = WebDriverWait(self.driver, 5).until(
                EC.element_to_be_clickable((
                    By.XPATH,
                    "//div[contains(@title, 'По новизне') or contains(text(), 'По новизне')]"
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
        return self.driver.find_elements(By.CSS_SELECTOR, 'div[class*="_1k5soqfl"]')

    def _expand_review_if_needed(self, container):
        """Раскрытие полного текста отзыва, если он скрыт под кнопкой "Читать целиком".

        Args:
            container: WebElement контейнера отзыва.
        """
        try:
            btn = container.find_element(By.XPATH, ".//span[contains(text(), 'Читать целиком')]")
            self.driver.execute_script("arguments[0].click();", btn)
            time.sleep(0.5)
        except Exception:
            pass

    def _extract_rating(self, soup: BeautifulSoup) -> Optional[float]:
        """Извлечение числового рейтинга из HTML-разметки.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Рейтинг от 0 до 5 или None, если не удалось извлечь.
        """
        stars = soup.select('svg[fill="#ffb81c"], svg[fill="#FFB81C"]')
        return min(float(len(stars)), 5.0) if stars else None

    def _extract_date(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение и нормализация даты публикации отзыва.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Дата в формате 'YYYY-MM-DD' или None.
        """
        date_el = soup.select_one('div[class*="_a5f6uz"]')
        if not date_el:
            return None
        date_text = date_el.get_text(strip=True)
        return parse_russian_date(date_text) if date_text else None

    def _extract_comment(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение текста отзыва из HTML-разметки.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Текст отзыва или None, если не найден.
        """
        text_link = soup.select_one('a[class*="_1msln3t"], a[class*="_1wlx08h"]')
        return text_link.get_text(strip=True) if text_link else None

    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение имени автора отзыва.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Имя автора или None.
        """
        author_el = soup.select_one('span[class*="_16s5yj36"]')
        if author_el:
            return author_el.get('title') or author_el.get_text(strip=True)
        return None

    def _extract_likes(self, soup: BeautifulSoup) -> int:
        """Извлечение количества лайков отзыва.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Количество лайков (0, если не найдено или не является числом).
        """
        likes_el = soup.select_one('span[class*="_11fxohc"]')
        if likes_el:
            text = likes_el.get_text(strip=True)
            return int(text) if text.isdigit() else 0
        return 0

    def _extract_badges(self, soup: BeautifulSoup) -> List[str]:
        """Извлечение бейджей/статусов автора отзыва.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Список текстовых описаний бейджей.
        """
        badges = []
        for cls in ['_1biptd8', '_1jx4hur']:
            els = soup.select(f'div[class*="{cls}"]')
            for el in els:
                text = el.get_text(strip=True)
                if text:
                    badges.append(text)
        return badges

    def _extract_org_response(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение текста ответа организации на отзыв.

        Args:
            soup: BeautifulSoup объект с разметкой отзыва.

        Returns:
            Текст ответа организации или None.
        """
        resp_el = soup.select_one('div[class*="_1wk3bjs"]')
        return resp_el.get_text(strip=True) if resp_el else None

    def load_all_reviews(self, url: str, limit: Optional[int] = 10, max_scrolls: int = 100) -> List:
        """Загрузка отзывов с указанной страницы с поддержкой динамической подгрузки.

        Алгоритм:
        1. Открытие страницы и начальная задержка для рендеринга.
        2. Закрытие модальных окон и установка сортировки по новизне.
        3. Циклическая прокрутка страницы до появления новых отзывов.
        4. Клик по кнопке "Загрузить ещё" при необходимости.
        5. Остановка при достижении лимита или отсутствии новых данных.

        Args:
            url: URL страницы с отзывами.
            limit: Максимальное количество отзывов для сбора (None = все).
            max_scrolls: Максимальное количество итераций прокрутки.

        Returns:
            Список WebElement объектов с загруженными отзывами.
        """
        logger.info(f"Открытие: {url}, лимит: {limit if limit else 'все'}")
        self.driver.get(url)
        time.sleep(5)
        self._close_modal_if_present()
        time.sleep(2)
        self._set_sorting_by_newest()
        time.sleep(3)

        prev_count = len(self._get_review_containers())
        if limit and prev_count >= limit:
            return self._get_review_containers()[:limit]

        no_new_count = 0
        max_no_new = 8

        for iteration in range(1, max_scrolls + 1):
            # Пошаговая прокрутка для триггера ленивой загрузки
            current_scroll = 0
            max_scroll = self.driver.execute_script("return document.body.scrollHeight")

            while current_scroll < max_scroll:
                scroll_step = min(current_scroll + 300, max_scroll)
                self.driver.execute_script(f"window.scrollTo(0, {scroll_step});")
                time.sleep(0.5)
                current_scroll = scroll_step
                if len(self._get_review_containers()) > prev_count:
                    break

            # Ожидание завершения динамической подгрузки
            time.sleep(4)
            for _ in range(4):
                time.sleep(2)
                if len(self._get_review_containers()) > prev_count:
                    break

            curr_count = len(self._get_review_containers())
            new_added = curr_count - prev_count
            logger.info(f"Итерация #{iteration}: {prev_count} -> {curr_count} (+{new_added})")

            if new_added > 0:
                no_new_count = 0
                prev_count = curr_count
                if limit and curr_count >= limit:
                    break
            else:
                # Попытка кликнуть "Загрузить ещё"
                try:
                    load_more_btn = self.driver.find_element(
                        By.XPATH,
                        "//button[contains(text(), 'Загрузить ещё') or contains(text(), 'Загрузить еще')]"
                    )
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", load_more_btn)
                    time.sleep(1)
                    self.driver.execute_script("arguments[0].click();", load_more_btn)
                    logger.info(f"Клик по кнопке 'Загрузить ещё' (итерация {iteration})")
                    time.sleep(5)

                    after_click_count = len(self._get_review_containers())
                    if after_click_count > curr_count:
                        prev_count = after_click_count
                        no_new_count = 0
                        continue
                except Exception:
                    no_new_count += 1

                if no_new_count >= max_no_new:
                    break

            # Дополнительная проверка: если высота страницы не меняется — конец
            try:
                current_height = self.driver.execute_script("return document.body.scrollHeight")
                time.sleep(2)
                self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
                time.sleep(2)
                new_height = self.driver.execute_script("return document.body.scrollHeight")
                if current_height == new_height and no_new_count > 0:
                    break
            except Exception:
                pass

        elements = self._get_review_containers()
        if limit and len(elements) > limit:
            elements = elements[:limit]
        return elements

    def parse_review(self, element) -> Dict:
        """Парсинг данных из одного WebElement контейнера отзыва.

        Алгоритм:
        1. Раскрытие полного текста при необходимости.
        2. Извлечение HTML и парсинг через BeautifulSoup.
        3. Сбор всех полей: автор, дата, рейтинг, текст, лайки, бейджи, ответ.
        4. Генерация уникального ID для дедупликации.

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
        self._expand_review_if_needed(element)
        time.sleep(0.3)

        html = element.get_attribute('outerHTML')
        soup = BeautifulSoup(html, 'lxml')

        author = self._extract_author(soup)
        date_raw = soup.select_one('div[class*="_a5f6uz"]')
        date_text = date_raw.get_text(strip=True) if date_raw else None

        return {
            "review_id": self._generate_review_id(author, date_text, self._extract_comment(soup)),
            "source": "2gis.ru",
            "author": author,
            "date": self._extract_date(soup),
            "rating": self._extract_rating(soup),
            "comment": self._extract_comment(soup),
            "likes": self._extract_likes(soup),
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
    arg_parser = argparse.ArgumentParser(description='Парсер отзывов с 2GIS')
    arg_parser.add_argument('--limit', type=int, default=10, help='Количество отзывов (None = все)')
    arg_parser.add_argument('--no-db', action='store_true', help='Не загружать в БД, только сохранить CSV')
    arg_parser.add_argument('--url', type=str,
                           default="https://2gis.ru/vladivostok/firm/3518965489880232/tab/reviews",
                           help='URL страницы с отзывами')
    args = arg_parser.parse_args()

    parser = None
    try:
        parser = TwoGISParser(headless=True)
        elements = parser.load_all_reviews(args.url, limit=args.limit)

        if not elements:
            logger.error("Отзывы не найдены")
            return

        reviews = [parser.parse_review(el) for el in elements]
        logger.info(f"Спаршено отзывов: {len(reviews)}")

        if reviews:
            filename = f"2gis_reviews_{args.limit if args.limit else 'all'}.csv"
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