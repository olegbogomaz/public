
import time
import logging
from datetime import datetime, timedelta
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager

def login(browser_instance, url):
    try:
        browser_instance.get(url)
        time.sleep(3)
        user_bar = browser_instance.find_element(By.ID, "okta-signin-username")
        user_bar.send_keys("hossein@iis.com")
        user_bar.send_keys(Keys.RETURN)
        time.sleep(3)
        password_bar = browser_instance.find_element(By.ID, "okta-signin-password")
        password_bar.send_keys("4rfv$RFV")
        password_bar.send_keys(Keys.RETURN)
    except Exception as e:
        logging.error(f"An error occurred while logging in: {e}")
    time.sleep(3)

def test_data_exports(browser_instance):

    browser_instance.get('https://nbininfinitedp.com/DataExports')

    firm_id = 'IAVM'
    firm_text = 'IA PRIVATE WEALTH INC. [IAVM]'
    report_id = 'ACS'
    report_text = 'Account Summary - Status Export'

    WebDriverWait(browser_instance, 10).until(
        EC.presence_of_element_located((By.ID, 'data-export-combo-data-requested'))
    ).send_keys(report_text)

    WebDriverWait(browser_instance, 10).until(
        EC.presence_of_element_located((By.ID, 'data-export-combo-select-firm'))
    ).send_keys(firm_text)

    current_time = datetime.now() - timedelta(minutes=1)

    browser_instance.find_element(By.ID, 'data-export-button-submit').click()

    while True:
        table = WebDriverWait(browser_instance, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "k-table-tbody"))
        )
        rows = table.find_elements(By.TAG_NAME, 'tr')
        find_waiting = False

        for row in rows:
            td_list = row.find_elements(By.TAG_NAME, 'td')

            last_update_text = td_list[7].text
            last_update_on = datetime.strptime(last_update_text, '%Y-%m-%d %H:%M')

            if last_update_on < current_time:
                find_waiting = False
                break
            if td_list[2].text == firm_id and td_list[1].text == report_id and td_list[4].text == "Waiting":
                find_waiting = True
                break

        if not find_waiting:
            break
        time.sleep(3)

#browser_instance = webdriver.Chrome('C:/ChromeDriver/chromedriver.exe')
#test_data_exports(browser_instance)
#browser_instance.quit()

options = Options()
#options.add_argument('--headless')
#options.add_argument('--no-sandbox')
options.add_argument('--disable-dev-shm-usage')
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

login(driver, 'https://nbininfinitedp.com/DataExports')
time.sleep(5)
test_data_exports(driver)

driver.close()
