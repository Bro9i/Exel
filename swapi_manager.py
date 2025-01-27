import requests
import pandas as pd
import argparse
import json
import logging

# Налаштування логера
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Клас SWAPIClient
class SWAPIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_json(self, endpoint: str) -> list:
        """
        Завантажує всі сторінки JSON для вказаного endpoint з API.
        """
        all_data = []
        url = self.base_url + endpoint
        while url:
            logger.info(f"Отримання даних з: {url}")
            response = requests.get(url)
            response.raise_for_status()  # Генерувати помилку для невдалих запитів
            data = response.json()
            all_data.extend(data['results'])
            url = data.get('next')  # Перехід до наступної сторінки
        return all_data

# Базовий процесор для сутностей
class EntityProcessor:
    def process(self, json_data: list) -> pd.DataFrame:
        """Метод для обробки даних. Реалізується в дочірніх класах."""
        pass

# Процесор для людей
class PeopleProcessor(EntityProcessor):
    def process(self, json_data: list) -> pd.DataFrame:
        df = pd.DataFrame(json_data)
        df['full_name'] = df['name']  # Додавання нового поля для повного імені
        return df

# Процесор для планет
class PlanetsProcessor(EntityProcessor):
    def process(self, json_data: list) -> pd.DataFrame:
        df = pd.DataFrame(json_data)
        df['population'] = pd.to_numeric(df['population'], errors='coerce')  # Перетворення популяції в числове значення
        return df

# Процесор для фільмів
class FilmsProcessor(EntityProcessor):
    def process(self, json_data: list) -> pd.DataFrame:
        df = pd.DataFrame(json_data)
        df['film_titles'] = df['title']  # Створюємо нову колонку з назвами фільмів
        return df

# Клас SWAPIDataManager для роботи з даними
class SWAPIDataManager:
    def __init__(self, client: SWAPIClient):
        self.client = client
        self.data = {}
        self.processors = {}

    def register_processor(self, endpoint: str, processor: EntityProcessor):
        """Реєстрація процесора для конкретної сутності."""
        self.processors[endpoint] = processor

    def fetch_entity(self, endpoint: str):
        """Завантажує і обробляє дані для сутності."""
        json_data = self.client.fetch_json(endpoint)
        processor = self.processors.get(endpoint)
        if processor:
            self.data[endpoint] = processor.process(json_data)

    def apply_filter(self, endpoint: str, columns_to_drop: list):
        """Фільтрація даних для конкретної сутності."""
        if endpoint in self.data:
            self.data[endpoint].drop(columns=columns_to_drop, inplace=True)

    def save_to_excel(self, filename: str):
        """Зберігає всі дані у файл Excel."""
        with pd.ExcelWriter(filename) as writer:
            for endpoint, df in self.data.items():
                df.to_excel(writer, sheet_name=endpoint.capitalize(), index=False)

# CLI для запуску програми
def main():
    parser = argparse.ArgumentParser(description="Завантаження та обробка даних SWAPI.")
    parser.add_argument("--endpoint", type=str, required=True, help="Список сутностей через кому (наприклад, people,planets,films).")
    parser.add_argument("--output", type=str, required=True, help="Ім'я вихідного Excel-файлу.")
    parser.add_argument("--filters", type=str, help="JSON-рядок з фільтрами для сутностей.")

    args = parser.parse_args()

    # Ініціалізація клієнта і менеджера
    client = SWAPIClient(base_url="https://swapi.dev/api/")
    manager = SWAPIDataManager(client)

    # Реєстрація процесорів
    manager.register_processor("people", PeopleProcessor())
    manager.register_processor("planets", PlanetsProcessor())
    manager.register_processor("films", FilmsProcessor())

    # Парсинг фільтрів
    filters = json.loads(args.filters) if args.filters else {}

    # Завантаження та обробка сутностей
    for endpoint in args.endpoint.split(","):
        manager.fetch_entity(endpoint)
        if endpoint in filters:
            manager.apply_filter(endpoint, filters[endpoint])

    # Збереження у файл
    manager.save_to_excel(args.output)
    print(f"Дані збережено у файл {args.output}")

if __name__ == "__main__":
    main()
