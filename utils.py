import json
from typing import List, Dict, Optional
import requests
from bs4 import BeautifulSoup

def get_regions_by_group() -> List[List[int]]:
    """
    Получает список с группами регионов, сгруппированных по населению.
    Данная группировка позволяет обойти ограничение на глубину выдачи вакансий по запросу к hh.ru,
    так как существует ограничение в 2000 вакансий на один запрос. Путем разбиения на группы
    на основе населения, можно получить все вакансии по России.
    Возвращает:
        List[List[int]]: Список списков идентификаторов регионов, сгруппированных по населению.
    """
    url = "https://ru.wikipedia.org/wiki/%D0%9D%D0%B0%D1%81%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D0%B5_%D1%81%D1%83%D0%B1%D1%8A%D0%B5%D0%BA%D1%82%D0%BE%D0%B2_%D0%A0%D0%BE%D1%81%D1%81%D0%B8%D0%B9%D1%81%D0%BA%D0%BE%D0%B9_%D0%A4%D0%B5%D0%B4%D0%B5%D1%80%D0%B0%D1%86%D0%B8%D0%B8"

    response = requests.get(url)
    html_content = response.text
    soup = BeautifulSoup(html_content, "html.parser")
    table = soup.find("table", class_="standard sortable")
    population_data = {}

    for row in table.find_all("tr")[1:-1]:
        columns = row.find_all("td")
        if len(columns) >= 3:
            subject = columns[1].find("a").get("title")
            population = int(columns[2]["data-sort-value"])
            population_data[subject]=population

    sorted_population_data = dict(sorted(population_data.items(), key=lambda x: x[1], reverse=True))
    grouped_data = {}
    current_group = 1
    current_population = 0

    for subject, population in sorted_population_data.items():
        if current_group not in grouped_data:
            grouped_data[current_group] = {}

        grouped_data[current_group][subject] = population
        current_population += population

        if current_population >= 7000000:
            current_group += 1
            current_population = 0

    with open('areas.json', 'r', encoding='utf-8') as areas_file:
        areas_data = json.load(areas_file)
    regions_by_group = {}
    def find_region_id(data: List[Dict[str, str]], region_name: str) -> Optional[int]:
        """
        Находит идентификатор региона по его имени.

        Аргументы:
            data (List[Dict[str, str]]): Список данных о регионах.
            region_name (str): Имя региона.

        Возвращает:
            Optional[int]: Идентификатор региона или None, если регион не найден.
        """
        for item in data:
            if region_name.lower() in item['name'].lower():
                return item['id']
        return None

    for group, subjects in grouped_data.items():
        region_ids = []
        for subject in subjects.keys():
            region_id = find_region_id(areas_data['areas'], subject)
            if region_id:
                region_ids.append(int(region_id))
        regions_by_group[group] = region_ids

    return regions_by_group.values()


def fetch_currency_data(user_agent: str) -> Dict[str, float]:
    """
    Получает данные о валютах.

    Аргументы:
        user_agent (str): Заголовок User-Agent для запросов.

    Возвращает:
        Dict[str, float]: Словарь с кодами валют в качестве ключей и их курсами обмена в качестве значений.
    """
    params = {
        "locale": "RU"
    }
    headers = {'User-Agent': user_agent}
    url = f'https://api.hh.ru/dictionaries'
    response = requests.get(url, headers=headers, params=params)
    currencies = {}
    if response.status_code == 200:
        response_data = response.json()
        for currency in response_data.get('currency', []):
            currencies[currency['code']] = currency['rate']
        return currencies
