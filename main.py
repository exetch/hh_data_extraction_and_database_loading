import os
from dotenv import load_dotenv
from database_manager import DatabaseManager
from userinterface import UserInterface
from hh_api_client import HeadHunterAPI
from utils import get_regions_by_group, fetch_currency_data

# Загрузка переменных окружения
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
AREAS = 'areas.json'
INDUSTRIES = 'industries.json'
MAX_POPULATION = 8_000_000  #Для группировки регионов России по населению

def main():
    # Создаем экземпляр класса DatabaseManager, передавая параметры для подключения к базе данных
    db_manager = DatabaseManager(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)

    # Создаем базу данных, если она еще не существует
    db_manager.create_database()

    # Создаем таблицы в базе данных, если они еще не созданы
    db_manager.create_tables()

    # Заполняем таблицу "regions" данными из JSON файла
    db_manager.fill_regions_from_json(AREAS)

    # Заполняем таблицу "cities" данными из JSON файла
    db_manager.fill_cities_from_json(AREAS)

    # Заполняем таблицу "industries" данными из JSON файла
    db_manager.fill_industries_from_json(INDUSTRIES)

    # Получаем сгруппированные по населению регионы России
    areas_data = get_regions_by_group(MAX_POPULATION)

    # Получаем данные о курсах валют
    currencies = fetch_currency_data(USER_AGENT)

    while True:
        # Создаем экземпляр класса HeadHunterAPI, передавая User-Agent
        hh_api = HeadHunterAPI(USER_AGENT)

        # Получаем от пользователя запросы для поиска компаний по названию
        company_names = UserInterface.get_company_names()

        # Получаем информацию о компаниях с использованием HeadHunter API
        hh_api.get_companies_info(company_names)

        # Если не удалось найти информацию о компаниях, предлагаем пользователю повторить запрос
        if not hh_api.processed_companies:
            print(f"К сожалению, по этому запросу ничего не удалось найти, попробуем еще раз?")
            continue

        # Получаем ID компаний, которые пользователь хочет добавить в базу данных
        company_ids = UserInterface.get_companies_ids(hh_api.processed_companies)

        # Если пользователь не выбрал ни одной компании, переходим к следующей итерации цикла
        if not company_ids:
            continue

        # Фильтруем ID компаний, оставляя только те, которых еще нет в базе данных
        new_ids = [company_id for company_id in company_ids if db_manager.check_employer_exists_by_id(company_id)]

        # Если есть новые компании для добавления
        if new_ids:
            # Получаем информацию о компаниях с использованием HeadHunter API
            companies_info = hh_api.fetch_company_info(new_ids)

            # Заполняем таблицу "employers" данными о работодателях
            db_manager.fill_employers_from_info(companies_info)

            # Получаем вакансии компаний с использованием HeadHunter API по группам регионов
            hh_api.get_vacancies_by_areas(areas_data, new_ids)

            # Если есть вакансии для добавления
            if hh_api.all_vacancies:
                # Заполняем таблицу "vacancies" данными о вакансиях
                db_manager.fill_vacancies(hh_api.all_vacancies, currencies)

        # Запрашиваем у пользователя, хочет ли он добавить еще компаний в базу данных
        user_input = input("Хотите добавить еще компаний в базу данных? (да/нет): ").strip().lower()
        if user_input != 'да':
            break

    # Отображаем пользовательский интерфейс для выполнения различных действий
    UserInterface.display_menu(db_manager)

if __name__ == "__main__":
    main()
