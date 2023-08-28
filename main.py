import os
import json
from dotenv import load_dotenv
import requests
import psycopg2


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
        data = json.load(json_file)

    headers = {'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",}

    for company_type, employers_ids in data.items():
        for employer_id in employers_ids:
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

    conn.close()





