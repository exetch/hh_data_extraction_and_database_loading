import re
import json
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

if __name__ == "__main__":
    driver_path = 'C:/chromedriver/chromedriver.exe'
    driver = webdriver.Chrome(executable_path=driver_path)


    tab_name_mapping = {
        "Крупнейшие компании": "largest_companies",
        "Крупные компании": "big_companies",
        "Средние компании": "medium_companies",
        "Небольшие компании": "small_companies"
    }

    try:
        driver.get("https://rating.hh.ru/history/rating2022/summary?tab=giant")
        wait = WebDriverWait(driver, 3)

        tab_names = list(tab_name_mapping.keys())
        tab_data = {}

        for tab_name in tab_names:
            button = wait.until(EC.element_to_be_clickable((By.XPATH, f"//span[text()='{tab_name}']")))
            button.click()

            elements = wait.until(EC.presence_of_all_elements_located((By.CLASS_NAME, "_5f0_Xwlgwy")))

            tab_data[tab_name_mapping.get(tab_name)] = [
                re.search(r'/employer/(\d+)\?', a_tag.get_attribute("href")).group(1)
                for element in elements
                for a_tag in element.find_elements(By.TAG_NAME, "a")
                if re.search(r'/employer/\d+\?', a_tag.get_attribute("href"))
            ]

        with open('best_employers_2022.json', 'w', encoding='utf-8') as json_file:
            json.dump(tab_data, json_file, ensure_ascii=False, indent=4)

    finally:
        driver.quit()
