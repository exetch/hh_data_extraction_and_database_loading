from typing import Dict, Any, List, Tuple
import json
import psycopg2
from psycopg2 import sql
from psycopg2.extras import DictCursor


class DatabaseManager:
    def __init__(self, db_host: str, db_name: str, db_user: str, db_password: str):
        """
        Конструктор класса.

        Args:
            db_host (str): Хост базы данных.
            db_name (str): Имя базы данных.
            db_user (str): Имя пользователя базы данных.
            db_password (str): Пароль пользователя базы данных.
        """
        self.db_host = db_host
        self.db_name = db_name
        self.db_user = db_user
        self.db_password = db_password

    def _get_connection(self) -> psycopg2.extensions.connection:
        """
        Получает соединение с базой данных.

        Returns:
            psycopg2.extensions.connection: Объект соединения с базой данных.
        """
        return psycopg2.connect(
            database=self.db_name, user=self.db_user, password=self.db_password, host=self.db_host
        )

    def _execute_query(self, query: str, data=None) -> None:
        """
        Выполняет SQL-запрос к базе данных.

        Args:
            query (str): SQL-запрос.
            data (list, optional): Данные для выполнения множественных операций (INSERT). По умолчанию None.

        Returns:
            None
        """
        try:
            with self._get_connection() as conn, conn.cursor(cursor_factory=DictCursor) as cursor:
                if data:
                    cursor.executemany(query, data)
                else:
                    cursor.execute(query)
            conn.commit()
        except psycopg2.Error as e:
            print("Ошибка при выполнении запроса:", e)

    def create_database(self) -> None:
        """
        Создает базу данных.

        Returns:
            None
        """
        try:
            conn = psycopg2.connect(host=self.db_host, user=self.db_user, password=self.db_password)
            conn.autocommit = True
            cursor = conn.cursor()
            cursor.execute(f"CREATE DATABASE {self.db_name}")
            print(f"База данных {self.db_name} успешно создана.")
        except psycopg2.Error as e:
            print("Ошибка при создании базы данных:", e)
        finally:
            if conn:
                conn.close()

    def create_tables(self) -> None:
        """
        Создает таблицы в базе данных, если они не существуют.

        Returns:
            None
        """
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
                vacancy_url VARCHAR(255),
                FOREIGN KEY (city_id) REFERENCES cities(city_id),
                FOREIGN KEY (employer_id) REFERENCES employers(employer_id)
            );
            """
        self._execute_query(create_table_query)
        print("Таблицы успешно созданы.")

    def check_table_has_data(self, table_name: str) -> bool:
        """
        Проверяет наличие записей в указанной таблице.

        Args:
            table_name (str): Название таблицы для проверки.

        Returns:
            bool: True, если таблица содержит записи, иначе False.
        """
        query = sql.SQL(f"SELECT COUNT(*) FROM {table_name}")
        try:
            with self._get_connection() as conn, conn.cursor() as cursor:
                cursor.execute(query)
                count = cursor.fetchone()[0]
            return count > 0
        except psycopg2.Error as e:
            print(f"Ошибка при проверке наличия записей в таблице {table_name}:", e)
            return False

    def fill_regions_from_json(self, json_file_path: str) -> None:
        """
        Заполняет таблицу 'regions' данными из JSON-файла.

        Args:
            json_file_path (str): Путь к JSON-файлу с данными о регионах.

        Returns:
            None
        """
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

    def fill_cities_from_json(self, json_file_path: str) -> None:
        """
        Заполняет таблицу 'cities' данными из JSON-файла.

        Args:
            json_file_path (str): Путь к JSON-файлу с данными о городах.

        Returns:
            None
        """
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

    def fill_industries_from_json(self, json_file_path: str) -> None:
        """
        Заполняет таблицу 'industries' данными из JSON-файла.

        Args:
            json_file_path (str): Путь к JSON-файлу с данными об отраслях.

        Returns:
            None
        """
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

    def fill_employers_from_info(self, companies_info: List[Dict[str, Any]]) -> None:
        """
        Заполняет таблицы 'employers' и 'employer_industry' данными о работодателях и их отраслях.

        Args:
            companies_info (List[Dict[str, Any]]): Список словарей с информацией о работодателях.

        Returns:
            None
        """
        try:

            employers_data = [(company_info['id'],
                               company_info['name'],
                               company_info['accredited_it_employer'],
                               company_info['alternate_url'],
                               company_info['area']['id']
                               ) for company_info in companies_info]
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

    def check_employer_exists_by_id(self, employer_id: int) -> bool:
        """
        Проверяет наличие работодателя в базе данных по его ID.

        Args:
            employer_id (int): ID работодателя для проверки.

        Returns:
            bool: True, если работодатель с указанным ID существует в базе, иначе False.
        """
        try:
            with self._get_connection() as conn, conn.cursor(cursor_factory=DictCursor) as cursor:
                query = sql.SQL("SELECT 1 FROM employers WHERE employer_id = %s LIMIT 1")
                cursor.execute(query, (employer_id,))
                result = cursor.fetchone()
                if result:
                    print(f'Работодатель с ID {employer_id} в базе уже есть')
                    return False
                else:
                    return True
        except psycopg2.Error as e:
            print("Ошибка при проверке наличия работодателя в базе:", e)
            return True

    def fill_vacancies(self, vacancies_data: List[Dict[str, Any]], currencies: Dict[str, float]) -> None:
        """
        Заполняет таблицу 'vacancies' данными о вакансиях.

        Args:
            vacancies_data (List[Dict[str, Any]]): Список словарей с информацией о вакансиях.
            currencies (Dict[str, float]): Словарь с данными о курсах валют.

        Returns:
            None
        """
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
                    vacancy_url = vacancy['alternate_url']

                    salary = None
                    if vacancy['salary']:
                        if vacancy['salary']['to']:
                            salary = vacancy['salary']['to']
                        if vacancy['salary']['from']:
                            salary = vacancy['salary']['from']

                    # конвертируем все зарплаты в одну валюту
                    if salary is not None:
                        salary /= currencies[vacancy['salary']['currency']]

                        # переводим все зарплаты в вариант после уплаты налога:
                        if vacancy['salary']['gross']:
                            salary -= salary * 0.13

                    vacancies_to_insert.append((vacancy_id, vacancy_title, city_id, salary, published_at, archived,
                                                address, employer_id, vacancy_url))

            query = sql.SQL(
                "INSERT INTO vacancies (vacancy_id, vacancy_title, city_id, salary, published_at, archived, address, employer_id, vacancy_url) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)")
            self._execute_query(query, vacancies_to_insert)

            print("Данные о вакансиях успешно добавлены в таблицу vacancies.")
        except Exception as e:
            print("Ошибка при добавлении данных о вакансиях:", e)

    def get_companies_and_vacancies_count(self) -> List[Tuple[str, int]]:
        """
        Получает список компаний и количества их вакансий, отсортированный по убыванию количества вакансий.

        Returns:
            List[Tuple[str, int]]: Список кортежей, где первый элемент - название компании,
            второй элемент - количество вакансий у компании.
        """
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

    def get_all_vacancies(self) -> List[Tuple[str, str, int, str]]:
        """
        Получает список всех вакансий, включая информацию о компаниях, зарплате и ссылках на вакансии.

        Returns:
            List[Tuple[str, str, int, str]]: Список кортежей, где первый элемент - название компании,
            второй элемент - название вакансии, третий элемент - зарплата, четвертый элемент - ссылка на вакансию.
        """
        try:
            with self._get_connection() as connection, connection.cursor() as cursor:
                query = """
                    SELECT e.company_name, v.vacancy_title, v.salary, v.vacancy_url
                    FROM vacancies v
                    LEFT JOIN employers e ON v.employer_id = e.employer_id;
                """

                cursor.execute(query)
                all_vacancies = cursor.fetchall()

                return all_vacancies

        except Exception as e:
            print("Ошибка при получении списка всех вакансий:", e)

    def get_avg_salary(self) -> int:
        """
        Получает среднюю зарплату среди всех вакансий.

        Returns:
            int: Средняя зарплата среди всех вакансий, округленная до целого числа.
        """
        try:
            with self._get_connection() as connection, connection.cursor() as cursor:
                query = "SELECT AVG(salary) FROM vacancies;"
                cursor.execute(query)
                avg_salary = cursor.fetchone()[0]
                return int(avg_salary)

        except Exception as e:
            print("Ошибка при получении средней зарплаты:", e)

    def get_vacancies_with_higher_salary(self) -> List[Tuple[str, int, str]]:
        """
        Получает список вакансий с зарплатой выше средней, включая название компании.

        Returns:
            List[Tuple[str, int, str]]: Список кортежей, где первый элемент - название вакансии,
            второй элемент - зарплата, третий элемент - название компании.
        """
        try:
            with self._get_connection() as connection, connection.cursor() as cursor:
                query = """
                    SELECT v.vacancy_title, v.salary, e.company_name
                    FROM vacancies v
                    JOIN employers e ON v.employer_id = e.employer_id
                    WHERE v.salary > (
                        SELECT AVG(salary) FROM vacancies
                    );
                """

                cursor.execute(query)
                high_salary_vacancies = cursor.fetchall()

                return high_salary_vacancies

        except Exception as e:
            print("Ошибка при получении списка вакансий с высокой зарплатой:", e)
    def get_vacancies_with_keyword(self, keyword: str) -> List[Tuple[str, str, int, str]]:
        """
        Получает список вакансий с ключевым словом в названии.

        Args:
            keyword (str): Ключевое слово для поиска.

        Returns:
            List[Tuple[str, str, int, str]]: Список кортежей, где первый элемент - название вакансии,
            второй элемент - название компании, третий элемент - зарплата, четвертый элемент - ссылка на вакансию.
        """
        try:
            with self._get_connection() as connection, connection.cursor() as cursor:
                query = """
                    SELECT v.vacancy_title, e.company_name, v.salary, v.vacancy_url
                    FROM vacancies v
                    JOIN employers e ON v.employer_id = e.employer_id
                    WHERE LOWER(v.vacancy_title) LIKE %s OR LOWER(v.vacancy_title) LIKE %s OR LOWER(v.vacancy_title) LIKE %s;
                """

                cursor.execute(query, ('%' + keyword, keyword + '%', '%' + keyword + '%'))
                keyword_vacancies = cursor.fetchall()

                return keyword_vacancies

        except Exception as e:
            print("Ошибка при получении списка вакансий с ключевым словом:", e)
