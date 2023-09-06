import requests
import json
from time import sleep
class HeadHunterAPI():
    """
        Класс для работы с API сайта HeadHunter.
    """

    def __init__(self, user_agent, employer_ids):
        """
        Конструктор класса.

        Параметры:
            user_agent (str): Заголовок User-Agent для запросов к API
        """
        self.employer_ids = employer_ids
        self.all_vacancies = []
        self.user_agent = user_agent

    def get_vacancies_by_areas(self, areas_data):
        per_page = 100
        for employer_id in self.employer_ids:
            for area_id in areas_data:
                page = 0

                while True:
                    sleep(0.5)
                    params = {
                        "employer_id": employer_id,
                        "page": page,
                        "per_page": per_page,
                        "only_with_salary": True,
                        "area": area_id,
                    }

                    headers = {
                        'User-Agent': self.user_agent,
                    }
                    url = f'https://api.hh.ru/vacancies'
                    response = requests.get(url, headers=headers, params=params)

                    if response.status_code == 200:
                        response_data = response.json()
                        vacancies = response_data.get('items', [])
                        total = response_data.get('found', 0)
                        print(
                            f"на hh.ru найдено {total} вакансий. "
                            f"Добавлено {len(vacancies)} вакансий для работодателя {employer_id} и региона {area_id}. "
                            f"Страница {page + 1}")
                        self.all_vacancies.extend(vacancies)

                        if total <= (page + 1) * per_page:
                            break
                        else:
                            page += 1
                    else:
                        print(f"Request failed with status code: {response.status_code}")
                        break

    def save_unique_vacancies_to_json(self, filename):
        """
        Сохраняет уникальные вакансии в JSON файл.

        Параметры:
            filename (str): Имя файла для сохранения вакансий
        """
        unique_vacancies = {}
        for vacancy in self.all_vacancies:
            vacancy_id = vacancy.get("id")
            if vacancy_id:
                unique_vacancies[vacancy_id] = vacancy
        with open(filename, 'w', encoding='utf-8') as json_file:
            json.dump(list(unique_vacancies.values()), json_file, ensure_ascii=False, indent=4)

        print(f"Сохранено {len(unique_vacancies)} уникальных вакансий в файл {filename}")
