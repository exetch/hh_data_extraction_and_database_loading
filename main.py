import os
import json
from dotenv import load_dotenv
import requests
import psycopg2
from hh_api_client import HeadHunterAPI
from population import get_regions_by_group

USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
UNIQUE_VACANCIES = 'unique_vacancies.json'
def check_existing_employer(employer_id):
    cursor = conn.cursor()
    select_query = "SELECT employer_id FROM employers WHERE employer_id = %s"
    cursor.execute(select_query, (employer_id,))
    result = cursor.fetchone()
    cursor.close()
    return result is not None


if __name__ == "__main__":
    load_dotenv()

    DB_HOST = os.getenv('DB_HOST')
    DB_NAME = os.getenv('DB_NAME')
    DB_USER = os.getenv('DB_USER')
    DB_PASSWORD = os.getenv('DB_PASSWORD')
    conn = psycopg2.connect(database=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST)

    with open('best_employers_2022.json', 'r', encoding="utf-8") as json_file:
        employer_ids = json.load(json_file)

    with open('areas.json', 'r', encoding='utf-8') as areas_file:
        areas_data = json.load(areas_file)

    headers = {'User-Agent': USER_AGENT}

    for employer_id in employer_ids:
        if check_existing_employer(employer_id):
            print(f"Employer with employer_id={employer_id} already exists, skipping...")
            continue

        api_url = f'https://api.hh.ru/employers/{employer_id}'
        response = requests.get(api_url, headers=headers)

        if response.status_code == 200:
            company_data = response.json()
            name = company_data.get('name', 'N/A')
            site_url = company_data.get('site_url', 'N/A')
            city = company_data.get('area', {}).get('name', 'N/A')
            industries = company_data.get('industries', [])
            industry = industries[0]['name'] if industries else 'N/A'

            cursor = conn.cursor()
            insert_query = "INSERT INTO employers (employer_id, name, site_url, city, industry) VALUES (%s, %s, %s, %s, %s)"
            data_tuple = (employer_id, name, site_url, city, industry)
            cursor.execute(insert_query, data_tuple)
            conn.commit()
            cursor.close()
    hh_api = HeadHunterAPI(USER_AGENT, employer_ids)
    regions_list = get_regions_by_group()


    hh_api.get_vacancies_by_areas(regions_list)
    hh_api.save_unique_vacancies_to_json(UNIQUE_VACANCIES)


    conn.close()





