import requests


class UserInterface:

    @staticmethod
    def get_company_names():
        while True:
            input_text = input("Введите название компании (или компаний через запятую): ")
            if not input_text:
                continue
            company_names = [name.strip() for name in input_text.split(',')]
            return company_names
            break

    @staticmethod
    def get_companies_ids(processed_companies):
        while True:
            input_text = input("Введите номера работодателей, информацию о которых следует внести в базу данных (через запятую): ")
            companies_info = [processed_companies.get(int(num.strip()), None) for num in input_text.split(',') if
                           num.strip().isdigit()]
            company_ids = [company_info[0] for company_info in companies_info if
                           companies_info is not None]
            if company_ids:
                return company_ids
                break
            else:
                print("Некорректные номера работодателей.")
