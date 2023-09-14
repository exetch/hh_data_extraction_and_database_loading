import psycopg2
from psycopg2 import sql
import json
from psycopg2.extras import DictCursor

class DatabaseManager:
    def __init__(self, db_host, db_name, db_user, db_password):
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

    def _get_connection(self):
        return psycopg2.connect(
            database=self.db_name, user=self.db_user, password=self.db_password, host=self.db_host
        )
    def _execute_query(self, query, data=None):
        try:
            with self._get_connection() as conn, conn.cursor(cursor_factory=DictCursor) as cursor:
                if data:
                    cursor.executemany(query, data)
                else:
                    cursor.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Ошибка при выполнении запроса:", e)

    def create_database(self):
        try:
            conn = psycopg2.connect(host=self.db_host, user=self.db_user, password=self.db_password)
            conn.autocommit = True  # Отключаем автокоммит для выполнения операций создания базы данных
            cursor = conn.cursor()

            # Создаем базу данных
            cursor.execute(f"CREATE DATABASE {self.db_name}")

            print(f"База данных {self.db_name} успешно создана.")
        except psycopg2.Error as e:
            print("Ошибка при создании базы данных:", e)
        finally:
            if conn:
                conn.close()

    def create_tables(self):
        create_table_query = """
            CREATE TABLE IF NOT EXISTS regions (
                region_id SERIAL PRIMARY KEY,
                region_name VARCHAR(255) UNIQUE
            );

            CREATE TABLE IF NOT EXISTS industries (
                id_industry DECIMAL(6, 3) PRIMARY KEY,
                name_industry VARCHAR(255) UNIQUE
            );

            CREATE TABLE IF NOT EXISTS cities (
                city_id SERIAL PRIMARY KEY,
                city_name VARCHAR(255),
                region_id INT,
                FOREIGN KEY (region_id) REFERENCES regions(region_id)
            );

            CREATE TABLE IF NOT EXISTS employers (
                employer_id SERIAL PRIMARY KEY,
                company_name VARCHAR(255),
                accredited_it_employer BOOLEAN,
                employer_url VARCHAR(255),
                city_id INT,
                FOREIGN KEY (city_id) REFERENCES cities(city_id)
            );

            CREATE TABLE IF NOT EXISTS employer_industry (
                employer_id INT,
                industry_id DECIMAL(6, 3),
                FOREIGN KEY (employer_id) REFERENCES employers(employer_id),
                FOREIGN KEY (industry_id) REFERENCES industries(id_industry),
                PRIMARY KEY (employer_id, industry_id)
            );
            CREATE TABLE IF NOT EXISTS vacancies (
                vacancy_id SERIAL PRIMARY KEY,
                vacancy_title VARCHAR(255),
                city_id INT,
                salary INT,
                published_at DATE,
                archived BOOLEAN,
                address VARCHAR(255),
                employer_id INT,
                FOREIGN KEY (city_id) REFERENCES cities(city_id),
                FOREIGN KEY (employer_id) REFERENCES employers(employer_id)
            );
            """
        self._execute_query(create_table_query)
        print("Таблицы успешно созданы.")

    def check_table_has_data(self, table_name):
        query = sql.SQL(f"SELECT COUNT(*) FROM {table_name}")
        try:
            with self._get_connection() as conn, conn.cursor() as cursor:
                cursor.execute(query)
                count = cursor.fetchone()[0]
            return count > 0
        except psycopg2.Error as e:
            print(f"Ошибка при проверке наличия записей в таблице {table_name}:", e)
            return False



    def fill_regions_from_json(self, json_file_path):
        if self.check_table_has_data('regions'):
            print("Таблица regions уже заполнена.")
            return
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)

        regions_data = [(int(region['id']), region['name']) for region in data['areas']]
        # Добавляем регион "Россия" с id = 113
        regions_data.append((113, 'Россия'))
        query = sql.SQL("INSERT INTO regions (region_id, region_name) VALUES (%s, %s)")
        self._execute_query(query, regions_data)
        print("Данные о регионах успешно добавлены в таблицу regions.")

    def fill_cities_from_json(self, json_file_path):
        if self.check_table_has_data('cities'):
            print("Таблица cities уже заполнена.")
            return
        with open(json_file_path, 'r', encoding='utf-8') as json_file:
            data = json.load(json_file)

        def add_cities(region_id, city_data):
            cities_data = [(int(city['id']), city['name'], region_id) for city in city_data]
            query = sql.SQL("INSERT INTO cities (city_id, city_name, region_id) VALUES (%s, %s, %s)")
            self._execute_query(query, cities_data)

        cities_to_insert = []

        for region in data['areas']:
            if int(region['id']) not in (1, 2):
                add_cities(int(region['id']), region['areas'])

        # Добавляем Москву и Санкт-Петербург отдельно
        for region in data['areas']:
            if int(region['id']) in (1, 2):
                city_name = region['name']
                region_id = int(region['id'])
                cities_to_insert.append((region_id, city_name, region_id))

        query = sql.SQL("INSERT INTO cities (city_id, city_name, region_id) VALUES (%s, %s, %s)")
        self._execute_query(query, cities_to_insert)

        russia_query = sql.SQL("INSERT INTO cities (city_id, city_name, region_id) VALUES (%s, %s, %s)")
        russia_data = (113, 'Россия', 113)
        self._execute_query(russia_query, [russia_data])

        print("Данные о городах успешно добавлены в таблицу cities.")

    def fill_industries_from_json(self, json_file_path):
        if self.check_table_has_data('industries'):
            print("Таблица industries уже заполнена.")
            return
        try:
            with open(json_file_path, 'r', encoding='utf-8') as json_file:
                data = json.load(json_file)

            industries_data = [(float(industry['id']), industry['name']) for industry_dict in data for industry in
                               industry_dict.get('industries', [])]

            query = sql.SQL("INSERT INTO industries (id_industry, name_industry) VALUES (%s, %s)")
            self._execute_query(query, industries_data)

            print("Данные об отраслях успешно добавлены в таблицу industries.")
        except Exception as e:
            print("Ошибка при добавлении данных об отраслях из JSON-файла:", e)

    def fill_employers_from_info(self, companies_info):
        try:

            employers_data = [(company_info['id'],
                               company_info['name'],
                               company_info['accredited_it_employer'],
                               company_info['alternate_url'],
                               company_info['area']['id']) for company_info in companies_info]
            employer_industry_data = [
                (company_info['id'], float(industry['id']))
                for company_info in companies_info
                for industry in company_info.get('industries', None)
            ]
            if employers_data:
                employers_query = sql.SQL(
                    "INSERT INTO employers (employer_id, company_name, accredited_it_employer, employer_url, city_id) VALUES (%s, %s, %s, %s, %s)")
                self._execute_query(employers_query, employers_data)
            if employer_industry_data:

                employer_industry_query = sql.SQL(
                    "INSERT INTO employer_industry (employer_id, industry_id) VALUES (%s, %s)")
                self._execute_query(employer_industry_query, employer_industry_data)
            companies = [company_info['name'] for company_info in companies_info]
            for company in companies:
                print(f"Данные о работодатее {company} успешно добавлены в таблицы employers и employers_industry.")

        except Exception as e:
            print("Ошибка при добавлении данных в таблицы", e)

    def check_employer_exists_by_id(self, employer_id):
        try:
            with self._get_connection() as conn, conn.cursor(cursor_factory=DictCursor) as cursor:
                query = sql.SQL("SELECT 1 FROM employers WHERE employer_id = %s LIMIT 1")
                cursor.execute(query, (employer_id,))
                result = cursor.fetchone()
                if result:
                    print(f'Работодатель с ID {employer_id} уже существует в базе')
                    return False
                else:
                    return True
        except psycopg2.Error as e:
            print("Ошибка при проверке наличия работодателя в базе:", e)
            return True

    def fill_vacancies(self, vacancies_data, currencies):
        try:
            vacancies_to_insert = []
            unique_vacancy_ids = set()
            for vacancy in vacancies_data:
                vacancy_id = int(vacancy['id'])
                if vacancy_id not in unique_vacancy_ids:
                    unique_vacancy_ids.add(vacancy_id)
                    vacancy_title = vacancy['name']
                    city_id = int(vacancy['area']['id'])
                    published_at = vacancy['published_at']
                    archived = vacancy['archived']
                    address = vacancy['address']['raw'] if vacancy['address'] and 'raw' in vacancy['address'] else None
                    employer_id = vacancy['employer']['id']

                    salary = None
                    if vacancy['salary']:
                        if vacancy['salary']['to']:
                            salary = vacancy['salary']['to']
                        if vacancy['salary']['from']:
                            salary = vacancy['salary']['from']

                    if salary is not None:
                        salary /= currencies[vacancy['salary']['currency']]

                        if vacancy['salary']['gross']:
                            salary -= salary * 0.13

                    vacancies_to_insert.append((vacancy_id, vacancy_title, city_id, salary, published_at, archived, address, employer_id))

            query = sql.SQL(
                "INSERT INTO vacancies (vacancy_id, vacancy_title, city_id, salary, published_at, archived, address, employer_id) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)")
            self._execute_query(query, vacancies_to_insert)

            print("Данные о вакансиях успешно добавлены в таблицу vacancies.")
        except Exception as e:
            print("Ошибка при добавлении данных о вакансиях:", e)

    def get_companies_and_vacancies_count(self):
        try:
            with self._get_connection() as connection, connection.cursor() as cursor:
                query = """
                    SELECT e.company_name, COUNT(v.vacancy_id) as vacancy_count
                    FROM employers e
                    LEFT JOIN vacancies v ON e.employer_id = v.employer_id
                    GROUP BY e.company_name
                    ORDER BY vacancy_count DESC;
                """

                cursor.execute(query)
                company_vacancies = cursor.fetchall()

                return company_vacancies

        except Exception as e:
            print("Ошибка при получении списка компаний и количества вакансий:", e)

