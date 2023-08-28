import requests
import json

if __name__ == "__main__":

    params = {
        "locale": "RU"
    }

    headers = {
        'User-Agent': "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36",
    }
    url = f'https://api.hh.ru/dictionaries'
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        response_data = response.json()
        with open('dictionaries.json', 'w', encoding='utf-8') as json_file:
            json.dump(response_data, json_file, ensure_ascii=False, indent=4)
