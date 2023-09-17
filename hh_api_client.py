from time import sleep
import requests
from typing import List, Dict, Union, Tuple

class HeadHunterAPI:
    """
    Класс для работы с API HeadHunter.
    """

    def __init__(self, user_agent: str):
        """
        Конструктор класса.

        Аргументы:
            user_agent (str): Заголовок User-Agent для запросов к API.
        """
        self.processed_companies = {}
        self.user_agent = user_agent
        self.headers = {'User-Agent': self.user_agent}
        self.all_vacancies = []

    def get_vacancies_by_areas(self, areas_data: List[int], employers_ids: List[int]):
        """
        Получение вакансий по регионам и ID работодателей.

        Аргументы:
            areas_data (List[int]): Список ID регионов.
            employers_ids (List[int]): Список ID работодателей.
        """
        per_page: int = 100
        for employer_id in employers_ids:
            params = {
                "locale": "RU",
                "employer_id": employer_id,
                "only_with_salary": True
            }
            url: str = f'https://api.hh.ru/vacancies'
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                response_data = response.json()
                total_ru = response_data.get('found', 0)
            for num, area_ids in enumerate(areas_data, start=1):
                page: int = 0

                while True:
                    sleep(0.5)
                    params = {
                        "employer_id": employer_id,
                        "page": page,
                        "per_page": per_page,
                        "only_with_salary": True,
                        "area": area_ids,
                    }
                    url = f'https://api.hh.ru/vacancies'
                    response = requests.get(url, headers=self.headers, params=params)

                    if response.status_code == 200:
                        response_data = response.json()
                        vacancies = response_data.get('items', [])
                        total_region = response_data.get('found', 0)
                        self.all_vacancies.extend(vacancies)
                        print(f"Добавлено {len(self.all_vacancies)} вакансий из {total_ru}: группа регионов {num}, "
                              f"страница {page + 1} из {total_region // 100 + 1} ")
                        if total_region <= (page + 1) * per_page:
                            break
                        else:
                            page += 1
                    else:
                        print(f"Запрос завершился с ошибкой: {response.status_code}")
                        break

    def get_companies_info(self, company_names: List[str]):
        """
        Получение информации о компаниях по названию.

        Аргументы:
            company_names (List[str]): Список названий компаний.
        """
        total_processed = 0
        for company_name in company_names:
            params = {
                "locale": "RU",
                "page": 0,
                "per_page": 100,
                "text": company_name,
                "employer_type": "company"
            }
            url = f'https://api.hh.ru/employers'
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                response_data = response.json()
                employers = response_data.get('items', [])
                total = response_data.get('found', 0)
                print(
                    f"По запросу {company_name} на hh.ru найдено {total} работодателей.")
                for employer in employers:
                    total_processed += 1
                    employer_url = employer['url']
                    response = requests.get(employer_url, headers=self.headers)
                    if response.status_code == 200:
                        response_data = response.json()
                        company_id = response_data['id']
                        name = response_data['name']
                        city = response_data['area']['name']
                        industry_info = response_data.get('industries', {})
                        self.processed_companies[total_processed] = (int(company_id), name)
                        if not industry_info:
                            industry_info = "не указана"
                        else:
                            industry_info = ', '.join(industry['name'] for industry in industry_info)
                        print(f'{total_processed}. {name}, {city}. Отрасль - {industry_info}')
                    else:
                        print(f"Запрос завершился с ошибкой: {response.status_code}")
            else:
                print(f"Запрос завершился с ошибкой: {response.status_code}")

    def fetch_company_info(self, company_ids: List[int]) -> List[Dict]:
        """
        Получение информации о компаниях по их ID.

        Аргументы:
            company_ids (List[int]): Список ID компаний.

        Возвращает:
            List[Dict]: Список словарей, содержащих информацию о компаниях.
        """
        companies_info = []
        for company_id in company_ids:
            params = {
                "locale": "RU",
                "employer_id": company_id
            }
            url = f'https://api.hh.ru/employers/{company_id}'
            response = requests.get(url, headers=self.headers, params=params)
            if response.status_code == 200:
                response_data = response.json()
                companies_info.append(response_data)
        return companies_info



