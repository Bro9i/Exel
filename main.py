import pandas as pd
import requests
import logging
from openpyxl import Workbook

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

class PeopleProcessor:
    def process(self, json_data: list) -> pd.DataFrame:
        df = pd.DataFrame(json_data)
        df['full_name'] = df['name']
        return df

class PlanetsProcessor:
    def process(self, json_data: list) -> pd.DataFrame:
        df = pd.DataFrame(json_data)
        df['population'] = pd.to_numeric(df['population'], errors='coerce')
        return df

class FilmsProcessor:
    def process(self, json_data: list) -> pd.DataFrame:
        df = pd.DataFrame(json_data)
        df['film_title'] = df['title']
        return df

class SWAPIClient:
    def __init__(self, base_url: str):
        self.base_url = base_url

    def fetch_json(self, endpoint: str) -> list:
        url = f"{self.base_url}/{endpoint}/"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()['results']

class SWAPIDataManager:
    def __init__(self, client: SWAPIClient):
        self.client = client
        self.data = {}
        self.processors = {}

    def register_processor(self, endpoint: str, processor):
        self.processors[endpoint] = processor

    def fetch_entity(self, endpoint: str):
        if endpoint not in self.processors:
            raise ValueError(f"Не зареєстровано процесор для {endpoint}")

        logger.info(f"Отримання даних про {endpoint}...")
        json_data = self.client.fetch_json(endpoint)
        processor = self.processors[endpoint]
        self.data[endpoint] = processor.process(json_data)

    def save_to_excel(self, filename: str):
        logger.info(f"Запис даних у Excel файл: {filename}")
        with pd.ExcelWriter(filename) as writer:
            for endpoint, df in self.data.items():
                df.to_excel(writer, sheet_name=endpoint, index=False)
        logger.info("Дані успішно записано у Excel.")

if __name__ == "__main__":
    client = SWAPIClient(base_url="https://swapi.dev/api/")
    manager = SWAPIDataManager(client)

    manager.register_processor("people", PeopleProcessor())
    manager.register_processor("planets", PlanetsProcessor())
    manager.register_processor("films", FilmsProcessor())

    manager.fetch_entity("people")
    manager.fetch_entity("planets")
    manager.fetch_entity("films")

    manager.save_to_excel("C:/Users/VPU29Lviv/Desktop/Swapi.xlsx")