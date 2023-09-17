
class UserInterface:
    @staticmethod
    def get_company_names() -> list[str]:
        """
        Получает от пользователя запрос для поиска компаний по названию.

        Returns:
            list[str]: Список названий компаний, введенных пользователем (разделенных запятыми).
        """
        while True:
            input_text = input("Введите запрос для поиска компаний по названию (можно несколько, разделяйте запятыми): ")
            if not input_text:
                continue
            company_names = [name.strip() for name in input_text.split(',')]
            return company_names
            break

    @staticmethod
    def get_companies_ids(processed_companies: dict) -> list[int]:
        """
        Получает id компаний.

        Args:
            processed_companies (dict): Словарь работодателей и их id, найденных по запросу пользователя.

        Returns:
            list[int]: Список идентификаторов компаний, выбранных пользователем.
        """
        while True:
            print('Введите номера работодателей, информацию о которых следует внести в базу данных. Если не нашли нужную вам компанию, то 0')
            input_text = input("Введите номер или номера через запятую: ")
            if input_text.strip() == "0":
                break
            companies_info = [processed_companies.get(int(num.strip()), None) for num in input_text.split(',') if
                           num.strip().isdigit()]
            company_ids = [company_info[0] for company_info in companies_info if
                           companies_info is not None]
            if company_ids:
                return company_ids
                break
            else:
                print("Некорректные номера работодателей.")

    @staticmethod
    def display_menu(db_manager) -> None:
        """
        Отображает главное меню для взаимодействия с базой данных.

        Args:
            db_manager: Менеджер базы данных.

        Returns:
            None
        """
        while True:
            print("\nЧто будем делать?")
            print("1. Показать все компании с количеством доступных вакансий")
            print("2. Показать все вакансии")
            print("3. Показать среднюю зарплату")
            print("4. Показать вакансии с более высокой зарплатой")
            print("5. Поиск вакансий по ключевому слову")
            print("6. Отдыхать...")

            choice = input("Введите свой выбор: ")

            if choice == '1':
                UserInterface.show_companies_and_vacancies_count(db_manager)
            elif choice == '2':
                UserInterface.show_all_vacancies(db_manager)
            elif choice == '3':
                UserInterface.show_avg_salary(db_manager)
            elif choice == '4':
                UserInterface.show_higher_salary_vacancies(db_manager)
            elif choice == '5':
                keyword = input("Введите ключевое слово для поиска(поиск ведется в названии вакансий): ").lower().strip()
                UserInterface.show_vacancies_with_keyword(db_manager, keyword)
            elif choice == '6':
                print("Выход из программы.")
                break
            else:
                print("Некорректный выбор. Попробуйте снова.")

    @staticmethod
    def show_companies_and_vacancies_count(db_manager) -> None:
        """
        Отображает список всех компаний с количеством доступных вакансий.

        Args:
            db_manager: Менеджер базы данных.

        Returns:
            None
        """
        company_vacancies = db_manager.get_companies_and_vacancies_count()
        if company_vacancies:
            print("Список всех компаний в базе данных:")
            for company in company_vacancies:
                print(f'Компания "{company[0]}", {company[1]} вакансий')
        else:
            print("Список компаний пуст.")
    @staticmethod
    def show_all_vacancies(db_manager) -> None:
        """
        Отображает список всех вакансий.

        Args:
            db_manager: Менеджер базы данных.

        Returns:
            None
        """
        vacancies = db_manager.get_all_vacancies()
        if vacancies:
            print("Список всех вакансий:")
            for vacancy in vacancies:
                vacancy_info = f"{vacancy[0]}, {vacancy[1]}, {vacancy[2]} руб., {vacancy[3]}2"
                print(vacancy_info)
        else:
            print("Нет доступных вакансий.")

    @staticmethod
    def show_avg_salary(db_manager) -> None:
        """
        Отображает среднюю зарплату.

        Args:
            db_manager: Менеджер базы данных.

        Returns:
            None
        """
        avg_salary = db_manager.get_avg_salary()
        if avg_salary is not None:
            print(f"Средняя зарплата: {avg_salary}")
        else:
            print("Нет данных о зарплате.")

    @staticmethod
    def show_higher_salary_vacancies(db_manager) -> None:
        """
        Отображает вакансии с более высокой зарплатой.

        Args:
            db_manager: Менеджер базы данных.

        Returns:
            None
        """
        higher_salary_vacancies = db_manager.get_vacancies_with_higher_salary()
        if higher_salary_vacancies:
            print("Вакансии с более высокой зарплатой:")
            for vacancy in higher_salary_vacancies:
                vacancy_info = f"{vacancy[0]}, {vacancy[1]} руб., {vacancy[2]}"
                print(vacancy_info)
        else:
            print("Нет вакансий с более высокой зарплатой.")

    @staticmethod
    def show_vacancies_with_keyword(db_manager, keyword: str) -> None:
        """
        Отображает вакансии, содержащие ключевое слово в названии.

        Args:
            db_manager: Менеджер базы данных.
            keyword (str): Ключевое слово для поиска.

        Returns:
            None
        """
        vacancies = db_manager.get_vacancies_with_keyword(keyword)
        if vacancies:
            print(f"Вакансии с ключевым словом '{keyword}':")
            for vacancy in vacancies:
                print(vacancy)
        else:
            print(f"Нет вакансий с ключевым словом '{keyword}'.")
