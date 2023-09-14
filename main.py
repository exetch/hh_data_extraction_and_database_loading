import os
from dotenv import load_dotenv
from database_manager import DatabaseManager
from userinterface import UserInterface
from hh_api_client import HeadHunterAPI
from utils import get_regions_by_group, feth_currency_data

# Загрузка переменных окружения
load_dotenv()

DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
AREAS = 'areas.json'
INDUSTRIES = 'industries.json'
def main():
    db_manager = DatabaseManager(DB_HOST, DB_NAME, DB_USER, DB_PASSWORD)

    db_manager.create_database()

    db_manager.create_tables()

    db_manager.fill_regions_from_json(AREAS)
    db_manager.fill_cities_from_json(AREAS)
    db_manager.fill_industries_from_json(INDUSTRIES)
    areas_data = get_regions_by_group()
    currencies = feth_currency_data(USER_AGENT)
    while True:
        hh_api = HeadHunterAPI(USER_AGENT)
        company_names = UserInterface.get_company_names()
        hh_api.get_companies_info(company_names)
        if not hh_api.processed_companies:
            print(f"К сожалению, по этому запросу ничего не удалось найти, попробеум еще раз?")
            continue
        company_ids = UserInterface.get_companies_ids(hh_api.processed_companies)
        new_ids = [company_id for company_id in company_ids if db_manager.check_employer_exists_by_id(company_id)]
        if new_ids:
            companies_info = hh_api.fetch_company_info(new_ids)
            db_manager.fill_employers_from_info(companies_info)
            hh_api.get_vacancies_by_areas(areas_data, new_ids)
            # print(hh_api.all_vacancies)
            db_manager.fill_vacancies(hh_api.all_vacancies, currencies)
        user_input = input("Хотите добавить еще компаний в базу данных? (да/нет): ").strip().lower()
        if user_input != 'да':
            break
    company_vac = db_manager.get_companies_and_vacancies_count()
    print(company_vac)


if __name__ == "__main__":
    main()




