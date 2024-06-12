from time import sleep
import contextlib
import re
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from seleniumwire import webdriver
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
import random
import time
import pandas as pd
import requests
from concurrent import futures
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from tqdm import tqdm

class WebDriverProfileScraper:
    def __init__(self, lk_credentials):
        self.audience_id = lk_credentials['audience_id']
        self.lk_credentials = lk_credentials
        self.baserow_api_token = "wMWVhs8wuDQBLauICWYxXeN1LCE6eUwI"
        self.baserow_table_id = "292983"
        baserow_url = f"https://api.baserow.io/api/database/rows/table/{self.baserow_table_id}/"
        self.filter_profiles = "?user_field_names=true&filters=%7B%22filter_type%22%3A%22AND%22%2C%22filters%22%3A%5B%5D%2C%22groups%22%3A%5B%7B%22filter_type%22%3A%22OR%22%2C%22filters%22%3A%5B%7B%22type%22%3A%22link_row_has%22%2C%22field%22%3A%22Audience%22%2C%22value%22%3A%22200%22%7D%2C%7B%22type%22%3A%22link_row_has%22%2C%22field%22%3A%22Audience%22%2C%22value%22%3A%22201%22%7D%5D%2C%22groups%22%3A%5B%5D%7D%2C%7B%22filter_type%22%3A%22OR%22%2C%22filters%22%3A%5B%7B%22type%22%3A%22equal%22%2C%22field%22%3A%22Email%22%2C%22value%22%3A%22No%20result%22%7D%2C%7B%22type%22%3A%22empty%22%2C%22field%22%3A%22Email%22%2C%22value%22%3A%22%22%7D%5D%2C%22groups%22%3A%5B%5D%7D%5D%7D"
        self.filter_companies="?user_field_names=true&filters=%7B%22filter_type%22%3A%22AND%22%2C%22filters%22%3A%5B%5D%2C%22groups%22%3A%5B%7B%22filter_type%22%3A%22OR%22%2C%22filters%22%3A%5B%7B%22type%22%3A%22link_row_has%22%2C%22field%22%3A%22Audience%22%2C%22value%22%3A%22200%22%7D%2C%7B%22type%22%3A%22link_row_has%22%2C%22field%22%3A%22Audience%22%2C%22value%22%3A%22201%22%7D%5D%2C%22groups%22%3A%5B%5D%7D%2C%7B%22filter_type%22%3A%22OR%22%2C%22filters%22%3A%5B%7B%22type%22%3A%22empty%22%2C%22field%22%3A%22Company%20Description%22%2C%22value%22%3A%22%22%7D%2C%7B%22type%22%3A%22equal%22%2C%22field%22%3A%22Company%20Description%22%2C%22value%22%3A%22No%20Result%22%7D%5D%2C%22groups%22%3A%5B%5D%7D%5D%7D"
        self.rows_profiles_to_process = self.fetch_filtered_baserow_table_data_concurrently(baserow_url, self.headers, self.baserow_table_id, self.filter_profiles)
        self.rows_companies_to_process = self.fetch_filtered_baserow_table_data_concurrently(baserow_url, self.headers, self.baserow_table_id, self.filter_companies)
        self.headers = {'Authorization': f'Token {self.baserow_api_token}', 'Content-Type': 'application/json'}
        self.idListProfiles = [row['id'] for row in self.rows_profiles_to_process]
        self.idListCompanies = [row['id'] for row in self.rows_companies_to_process]
        self.linkProfiles = [row['Prospect Linkedin URL'] for row in self.rows_profiles_to_process]
        self.linkCompanies = [row.get('Company Linkedin ID URL') for row in self.rows_companies_to_process 
                if row.get('Company Linkedin ID URL')]
        self.driver = self.setup_chrome_driver()
        self.proxyList = ["http://Tib2VnMkxa4CTYp:M3ejKPdY0Z6NevS@62.164.237.237:47617",
                          "http://bF4b1viYZHyzPhl:jGLyie044dxPdsE@62.164.236.142:44669",
                          "http://1RcRjOSJQgbr9nT:lWmpkkupQI4cSXC@212.32.98.184:48669",
                          "http://uAKTn0ltRv5Q3T1:o3qIHSrHc9K0tix@212.32.103.164:49819",
                          "http://zxM5j0D3MZJMwJ1:O1X8vlBix9xLdIF@207.228.47.128:44219",
                          "http://K4ln3EXwpsr6m3u:z7S29SONRKP119p@207.228.40.215:48155",
                          "http://vvkNK4m2NnaegFf:QmUWRHQbHyevnht@207.228.22.131:45556",
                          "http://JqgBSueZPRHdLPH:yYkgQoj8GGaAQzF@207.228.20.235:43478"]
        self.proxy_id = lk_credentials['proxy_id']
        self.SCRAPEOPS_API_KEY = 'b7c14682-a15e-48d9-a133-6b91cc022d6d'

    def get_user_agent_list(self):
        response = requests.get('http://headers.scrapeops.io/v1/user-agents?api_key=' + self.SCRAPEOPS_API_KEY)
        json_response = response.json()
        return json_response.get('result', [])

    def get_random_user_agent(self, user_agent_list):
        random_index = random.randint(0, len(user_agent_list) - 1)
        return user_agent_list[random_index]

    def fetch_page_baserow_table_data(self, url, headers, table_id, page, filter_query, page_size=100, attempt=1):
        """Fetch a single page of data from the Baserow table with retry on 429."""
        request_url = f"{url}{filter_query}&page={page}&size={page_size}"
        response = requests.get(request_url, headers=headers)
        if response.status_code == 200:
            return response.json().get('results', [])
        elif response.status_code == 429:
            if attempt <= 5:  # Max retry attempts
                sleep_time = attempt * 2  # Exponential backoff
                print(f"Rate limit hit, retrying page {page} after {sleep_time} seconds...")
                time.sleep(sleep_time)
                return self.fetch_page_baserow_table_data(url=url, headers=headers, table_id=table_id, page=page, page_size=page_size, attempt=attempt + 1)
            else:
                print(f"Failed to fetch page {page} after {attempt} attempts.")
                return []
        else:
            print(f"Error fetching page {page}: {response.status_code}")
            return []
    # Function to fetch rows based on Prospect Headline

    def fetch_filtered_baserow_table_data_concurrently(self, url, headers, table_id, filter_query, max_workers=10):
        """Fetch filtered rows from the Baserow table using concurrent requests with rate limiting."""
        all_data = []
        response = requests.get(url+filter_query, headers=headers)
        if response.status_code != 200:
            print(f"Error fetching initial data: {response.status_code}")
            return []
        total_pages = response.json().get('count', 0) // 100 + 1
        with futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.fetch_page_baserow_table_data, url, headers, table_id, page, filter_query)
                    for page in range(1, total_pages + 1)]
            for future in futures.as_completed(futures):
                data = future.result()
                all_data.extend(data)
        return all_data



    def setup_chrome_driver(self):
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_experimental_option("excludeSwitches", ['enable-automation'])
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.accept_insecure_certs = True
        chrome_options.add_argument(r'--load-extension=/opt/bin/Kaspr,/opt/bin/CapSolver')
        chrome_options.add_argument('--headless')
        chrome_options.binary_location = "/opt/bin/chromium"
        chrome_options.add_argument('--user-agent=' + self.get_random_user_agent(self.user_agent_list))
        options = {
            'proxy': {
                'http': self.proxyList[self.proxy_id],
                'https': self.proxyList[self.proxy_id],
                'no_proxy': 'localhost,127.0.0.1'
            }
        }
        driver = webdriver.Chrome(options=chrome_options, seleniumwire_options=options)
        driver.maximize_window()
        driver.get('chrome://settings/')
        time.sleep(2)
        driver.execute_script('chrome.settingsPrivate.setDefaultZoom(0.25);')
        time.sleep(1)
        driver.get("https://www.linkedin.com/login/")
        if self.lk_credentials['cookies'] != '':
            driver.add_cookie({"name": "li_at", "value": self.lk_credentials['cookies']})
        else:
            self.enter_ids_on_lk_signin(driver, self.lk_credentials['email'], self.lk_credentials['password'])
        driver.get("https://app.kaspr.io/signin?utm_source=Widget&utm_medium=Connect")
        time.sleep(4)
        kUser = driver.find_element(By.XPATH, "//input[@type='text']")
        time.sleep(0.3)
        kUser.send_keys(self.lk_credentials["kEmail"])
        time.sleep(0.2)
        kBtn = driver.find_element(By.TAG_NAME, "button")
        time.sleep(0.4)
        kBtn.click()
        time.sleep(2)
        kPword = driver.find_element(By.XPATH, "//input[@type='password']")
        kPword.send_keys(self.lk_credentials["kPassword"])
        kBtn = driver.find_element(By.TAG_NAME, "button")
        time.sleep(0.3)
        kBtn.click()
        time.sleep(3)
        return driver

    def add_trailing_slash(self, url):
        if not url.endswith('/'):
            return url + '/'
        return url

    def export_to_baserow(self, row_data, row_id):
        baserow_url = f'https://api.baserow.io/api/database/rows/table/292983/{row_id}/?user_field_names=true/'
        response = requests.patch(baserow_url, headers=self.headers, json=row_data)
        if response.status_code == 200:
            print('Successfully updated row')
        else:
            print(f'Failed to update row: {response.text}')

    def give_emoji_free_text(self, text):
        import demoji
        return demoji.replace(text, '')

    def relogin(self, driver):
        driver.delete_all_cookies()
        driver.refresh()
        driver.implicitly_wait(10)
        driver.get("https://www.linkedin.com/login")
        time.sleep(5)
        driver.implicitly_wait(10)
        self.enter_ids_on_lk_signin(driver, self.lk_credentials['email'], self.lk_credentials['password'])
        time.sleep(5)
        if "checkpoint/challenge" in driver.current_url:
            print("Capsolver solving Captcha: waiting for 2 minutes")
            time.sleep(120)
        if driver.find_element(By.XPATH, "//*[contains(text(), 'restricted')]"):
            print("Account is restricted. Please contact support.")
            driver.quit()
            exit()

    def enter_ids_on_lk_signin(self, driver, email, password):
        time.sleep(4)
        username_input_element = driver.find_element(By.ID, "username")
        if username_input_element.get_attribute('value') == "":
            username_input_element.send_keys(email)
            password_input_element = driver.find_element(By.ID, "password")
            password_input_element.send_keys(password)
        submit_element = driver.find_element(
            By.CSS_SELECTOR,
            "#organic-div > form > div.login__form_action_container > button",
        )
        time.sleep(1)
        submit_element.click()
        time.sleep(5)

    def relogin_if_needed(self, company_url):
        self.driver.get(company_url + '/about')
        sleep(4)
        try:
            name = self.driver.find_element(By.XPATH, '//div/h1')
        except:
            self.relogin(self.driver)
            print(f"Relogin for {company_url}")
            sleep(5)
            self.driver.get(company_url + '/about')
            sleep(4)
            self.driver.implicitly_wait(10)
            with contextlib.suppress(Exception):
                self.driver.execute_script("window.scrollBy(0, 300)")

    def get_page_content(self, url):
        response = requests.get(url, cookies=self.driver.get_cookies())
        if response.status_code == 200:
            return response.content
        else:
            print(f"Failed to retrieve content for {url}: {response.text}")
            return None

    def choose_random_action(self, driver):
        action = random.choice(["scroll_up", "scroll_down", "click_link", "wait"])
        if action == "scroll_up":
            start_num = self.gInt6_11() * 10
            end_num = self.gInt3_6() * 100
            pixels = round(random.uniform(start_num, end_num))
            driver.execute_script(f"window.scrollBy(0, -{pixels})")
        elif action == "scroll_down":
            start_num = self.gInt6_11() * 10
            end_num = self.gInt3_6() * 100
            pixels = round(random.uniform(start_num, end_num))
            driver.execute_script(f"window.scrollBy(0, {pixels})")
        elif action == "click_link":
            try:
                lis_links = driver.find_elements(By.TAG_NAME, "a")
                link = lis_links[random.randint(0, len(lis_links) - 1)]
                driver.get(link.get_attribute("href"))
            except:
                pass
            time_to_wait = self.gInt3_6()
            time.sleep(time_to_wait)
        else:
            time_to_wait = self.gInt1_3()
            time.sleep(time_to_wait)

    def gInt01_05(self):
        return random.uniform(0.3, 0.8)

    def gInt05_1(self):
        return random.uniform(0.7, 1.2)

    def gInt1_3(self):
        return random.uniform(1, 3)

    def gInt3_6(self):
        return random.uniform(3, 6)

    def gInt6_11(self):
        return random.uniform(6, 11)

    def get_company_info(self, cList, counter, idList):
        fields = ['Company Description', 'Prospect Industry', 'Company Specialities', 'Company Website',
                  'Company Employee Range']
        for companies in cList[counter * 25:(counter + 1) * 25]:
            overview, industry, specialties, website, employees, row_id = "No Result", "No Result", "No Result", "No Result", "No Result", "No Result"

            if companies:
                row_id = idList[cList.index(companies)]
                self.driver.get(companies + '/about')
                sleep(4)
                # driver.implicitly_wait(10)

                try:
                    name = self.driver.find_element(By.XPATH, '//div/h1')
                except:

                    self.relogin(self.driver)
                    print(f"Relogin for {companies}")
                    sleep(5)
                    self.driver.get(companies + '/about')
                    sleep(4)
                    self.driver.implicitly_wait(10)
                    with contextlib.suppress(Exception):
                        self.driver.execute_script("window.scrollBy(0, 300)")
                with contextlib.suppress(Exception):
                    overview = self.driver.find_element(By.XPATH,
                                                   "//h2[normalize-space()='Overview']/following-sibling::p[1]").text

                with contextlib.suppress(Exception):
                    industry = self.driver.find_element(By.XPATH,
                                                   "//dt[normalize-space()='Industry']/following-sibling::dd[1]").text

                with contextlib.suppress(Exception):
                    website = self.driver.find_element(By.XPATH,
                                                  "//dt[normalize-space()='Website']/following-sibling::dd[1]").text
                    website = self.add_trailing_slash(website)

                with contextlib.suppress(Exception):
                    employees = self.driver.find_element(By.XPATH,
                                                    "//dt[normalize-space()='Company size']/following-sibling::dd[1]").text

                with contextlib.suppress(Exception):
                    specialties = self.driver.find_element(By.XPATH,
                                                      "//dt[normalize-space()='Specialties']/following-sibling::dd[1]").text

                self.choose_random_action(self.driver)

            newDict = {
                fields[0]: overview,
                fields[1]: industry,
                fields[2]: specialties,
                fields[3]: website,
                fields[4]: employees
            }
            self.export_to_baserow(newDict, row_id)
        return
    def get_profile_info(self, pList, counter, idList):
        fields = ['Prospect Headline', 'Prospect Summary', 'Prospect Position Description',
                  'Prospect Current Positions', 'Company Location', 'Prospect Connections', 'Email']
        job_part_list = []
        job_description_list = []
        positions_list = []
        locations_list = []
        wait_after_page_loaded = 5

        if True:
            for profiles in pList[counter*25:(counter+1)*25]:
                if profiles is not None:
                    row_id = idList[pList.index(profiles)]
                    self.driver.get(profiles)
                    sleep(3)
                    if True:
                        try:
                            self.driver.find_element(By.XPATH, "//div[@class= 'error-code']")
                        except:
                            try:
                                self.driver.find_element(By.XPATH, "//div/h1/following-sibling::p[1]")
                            except:
                                WebDriverWait(self.driver, 10).until(EC.presence_of_element_located((By.XPATH, "//a/h1")))
                            else:
                                print('Had to relogin')
                                self.relogin(self.driver)
                                sleep(wait_after_page_loaded)
                                self.driver.get(profiles)
                                sleep(wait_after_page_loaded)
                        else:
                            error = self.driver.find_element(By.XPATH, "//div[@class= 'error-code']").text
                            print('Error: ' + error + ' - ' + profiles)
                            self.relogin(self.driver)
                            sleep(wait_after_page_loaded)
                            self.driver.get(profiles)
                            sleep(wait_after_page_loaded)
                    try:
                        headline = self.driver.find_element(By.XPATH, '//*[starts-with(@class, "text-body-medium break-words")]').text
                    except:
                        headline = "No Result"
                    with contextlib.suppress(Exception):
                        self.driver.execute_script("window.scrollBy(0, 500)")
                    try:
                        self.driver.find_element(By.XPATH, "//div[contains(@class,'display-flex ph5 pv3')]")
                    except:
                        summary = 'No Result'
                    else:
                        summary = self.driver.find_element(By.XPATH, '//div[@class = "display-flex ph5 pv3"]//span[@aria-hidden = "true"]')
                        summary = summary.text

                    try:
                        connect = self.driver.find_element(By.XPATH, "//span[contains(.,'connections')]")
                        connect = connect.text
                    except:
                        connect = "No Result"

                    try:
                        kaspr_btn = self.driver.find_element(By.XPATH, "//div[@id='KasprPluginBtn']/button")
                        kaspr_btn.click()
                        sleep(0.2)
                    except:
                        pass
                    try:
                        kaspr_cnt_btn = self.driver.find_element(By.XPATH, "//button[normalize-space()='Reveal contact details']")
                        sleep(0.2)
                        kaspr_cnt_btn.click()
                    except:
                        pass
                    try:
                        sleep(1.5)
                        email_w = self.driver.find_element(By.XPATH, "//span[@class='star']//span[@class='to-clipboard']")
                        email = email_w.text
                    except:
                        email = "No result"
                    print("Email: " + email)
                    job_description = None
                    job_title = None
                    job_location = None
                    job_part_list = self.driver.find_elements(By.XPATH, "//span[@aria-hidden='true'][normalize-space()='Experience']/ancestor::div[4]/following-sibling::div/ul/li[1]//div[contains(@class,'display-flex full-width')]/ancestor::li[3]")
                    if job_part_list == []:
                        job_description = "No Result"
                        job_title = "No Result"
                        job_location = "No Result"
                        positions_list.append(job_title)
                        locations_list.append(job_location)
                        job_description_list.append(job_description)
                    else:
                        i = 0
                        this_p_list = []
                        this_d_list = []
                        this_l_list = []
                        try:
                            while i < len(job_part_list):
                                job = job_part_list[i]
                                try:
                                    job_description = job.find_element(By.XPATH, "./div/div/div/ul/li//span[@aria-hidden='true']").text
                                except:
                                    job_description = "No Result"
                                try:
                                    job_title = job.find_element(By.XPATH, "./div/div/div/ul/li/div/ul/li/div/div/div/div/span[@aria-hidden='true']").text
                                except:
                                    job_title = "No Result"
                                try:
                                    job_location = job.find_element(By.XPATH, "./div/div/div/div/span[3]/span[@aria-hidden='true']").text
                                except:
                                    job_location = "No Result"
                                if job_description and job_title and job_location == "No Result":
                                    break
                                this_p_list.append(job_title)
                                this_l_list.append(job_location)
                                this_d_list.append(job_description)
                                i = i + 1
                        except AttributeError:
                            pass
                        positions_list.append(this_p_list)
                        locations_list.append(this_l_list)
                        job_description_list.append(this_d_list)
                    self.choose_random_action(self.driver)
                else:
                    row_id = "No Result"
                    headline = "No Result"
                    summary = "No Result"
                    connect = "No Result"
                    email = "No Result"
                    job_description_list = "No Result"
                    positions_list = "No Result"
                    locations_list = "No Result"
                new_dict = {
                    fields[0]: headline,
                    fields[1]: summary,
                    fields[2]: str(job_description_list),
                    fields[3]: str(positions_list),
                    fields[4]: str(locations_list),
                    fields[5]: connect,
                    fields[6]: email
                }
                self.export_to_baserow(new_dict, row_id)
        return
    
    def scrape(self, counter=0):
        self.get_company_info(self.linkCompanies, counter, self.idListCompanies)
        self.get_profile_info(self.linkProfiles, counter, self.idListProfiles)
        cookie_jar = self.driver.get_cookies()
        self.driver.quit()
        return cookie_jar




class WebDriverSalesNavScraper:


    def __init__(self, lk_credentials, baserow_api_token='292983',start_page=1, end_page=1, wait_time_between_pages=5,
                 wait_after_page_loaded=3, wait_after_scroll_down=3, save_format="csv"):
        self.audience_id = lk_credentials['audience_id']  # audience_id
        self.SCROLL_TO_BOTTOM_COMMAND = "document.getElementById('search-results-container').scrollTop+=100000;"
        self.search_url = self.get_sales_nav_search_url(self.audience_id, '293210')
        self.baserow_api_token = baserow_api_token
        self.headers = {'Authorization': f'Token {self.baserow_api_token}', 'Content-Type': 'application/json'}
        self.lk_credentials = lk_credentials
        self.start_page = start_page
        self.end_page = end_page
        self.wait_time_between_pages = wait_time_between_pages
        self.wait_after_page_loaded = wait_after_page_loaded
        self.wait_after_scroll_down = wait_after_scroll_down
        self.save_format = save_format
        self.SCRAPEOPS_API_KEY = 'b7c14682-a15e-48d9-a133-6b91cc022d6d'
        self.search_url_base = self.remove_url_parameter(self.search_url, "page")
        self.driver = self.setup_chrome_driver()
        self.total_info = []
        self.proxyList = ["http://Tib2VnMkxa4CTYp:M3ejKPdY0Z6NevS@62.164.237.237:47617",
                          "http://bF4b1viYZHyzPhl:jGLyie044dxPdsE@62.164.236.142:44669",
                          "http://1RcRjOSJQgbr9nT:lWmpkkupQI4cSXC@212.32.98.184:48669",
                          "http://uAKTn0ltRv5Q3T1:o3qIHSrHc9K0tix@212.32.103.164:49819",
                          "http://zxM5j0D3MZJMwJ1:O1X8vlBix9xLdIF@207.228.47.128:44219",
                          "http://K4ln3EXwpsr6m3u:z7S29SONRKP119p@207.228.40.215:48155",
                          "http://vvkNK4m2NnaegFf:QmUWRHQbHyevnht@207.228.22.131:45556",
                          "http://JqgBSueZPRHdLPH:yYkgQoj8GGaAQzF@207.228.20.235:43478"]
        self.proxy_id = lk_credentials["proxyID"]
        self.user_agent_list = self.get_user_agent_list()

    def get_user_agent_list(self):
        response = requests.get('http://headers.scrapeops.io/v1/user-agents?api_key=' + self.SCRAPEOPS_API_KEY)
        json_response = response.json()
        return json_response.get('result', [])

    def get_random_user_agent(self, user_agent_list):
        random_index = random.randint(0, len(user_agent_list) - 1)
        return user_agent_list[random_index]

    

    def get_sales_nav_search_url(self, audience_id, sub_audiences_table_id):
        """
        Retrieves the LinkedIn Sales Navigator search URL from the sub-audiences table using the provided audience_id.

        :param sub_audiences_table_id: The ID of the sub-audiences table in the Baserow workspace.
        :param audience_id: The ID of the sub-audience row in the Baserow table.
        :return: The LinkedIn Sales Navigator search URL if found, else None.
        """
        baserow_url = f'https://api.baserow.io/api/database/rows/table/{sub_audiences_table_id}/{audience_id}/?user_field_names=true'
        response = requests.get(baserow_url, headers=self.headers)
        if response.status_code == 200:
            row_data = response.json()
            sales_nav_search_url = row_data.get('LI Sales Navigator URL')  # Replace with the actual field name
            return sales_nav_search_url
        else:
            print(f"Failed to retrieve data: {response.text}")
            return None

    def add_trailing_slash(self, url):
        if url is not None and url[-1] != "/":
            url += "/"
        return url

    def remove_url_parameter(self, url, param):
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)

        if param in query_params:
            del query_params[param]

        new_query = urlencode(query_params, doseq=True)
        new_url = urlunparse(
            (
                parsed_url.scheme,
                parsed_url.netloc,
                parsed_url.path,
                parsed_url.params,
                new_query,
                parsed_url.fragment,
            )
        )

        return new_url
    
    def get_lk_url_from_sales_lk_url(self, url):
        parsed = re.search("/lead/(.*?),", url, re.IGNORECASE)
        if parsed:
            return self.add_trailing_slash(f"https://www.linkedin.com/in/{parsed.group(1)}")
        return None

    def get_lk_company_url_from_sales_lk_url(self, url):
        cParsed = re.search("/company/(.*?)\?", url, re.IGNORECASE)
        if cParsed:
            return self.add_trailing_slash(f"https://www.linkedin.com/company/{cParsed.group(1)}")
        return None
    def setup_chrome_driver(self):
        logging.getLogger("selenium").setLevel(logging.CRITICAL)
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--excludeSwitches=enable-automation, disable-popup-blocking, enable-logging")
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        chrome_options.accept_insecure_certs = True
        chrome_options.add_argument(r'--load-extension=/opt/bin/Kaspr,/opt/bin/CapSolver')
        chrome_options.binary_location = "/opt/bin/chromium"
        chrome_options.add_argument('--headless')
        #chrome_options.add_argument('--disable-gpu')
        chrome_options.add_argument('--user-agent=' + self.get_random_user_agent(self.user_agent_list))
        options = {
            'proxy': {
                'http': self.proxyList[self.proxy_id],
                'https': self.proxyList[self.proxy_id],
                'no_proxy': 'localhost,127.0.0.1'
            }
        }
        driver = webdriver.Chrome(options=chrome_options, seleniumwire_options=options)
        driver.maximize_window()
        driver.get('chrome://settings/')
        time.sleep(2)
        driver.execute_script('chrome.settingsPrivate.setDefaultZoom(0.25);')
        time.sleep(1)
        driver.get("https://www.linkedin.com/login/")
        if self.lk_credentials['cookies'] != '':
            driver.add_cookie({"name": "li_at", "value": self.lk_credentials['cookies']})
        else:
            self.enter_ids_on_lk_signin(driver, self.lk_credentials['email'], self.lk_credentials['password'])
        driver.get("https://app.kaspr.io/signin?utm_source=Widget&utm_medium=Connect")
        time.sleep(4)
        kUser = driver.find_element(By.XPATH, "//input[@type='text']")
        time.sleep(0.3)
        kUser.send_keys(self.lk_credentials["kEmail"])
        time.sleep(0.2)
        kBtn = driver.find_element(By.TAG_NAME, "button")
        time.sleep(0.4)
        kBtn.click()
        time.sleep(2)
        kPword = driver.find_element(By.XPATH, "//input[@type='password']")
        kPword.send_keys(self.lk_credentials["kPassword"])
        kBtn = driver.find_element(By.TAG_NAME, "button")
        time.sleep(0.3)
        kBtn.click()
        return driver

    def get_profile_link_from_result_el(self, result_el):
        selector = "div > div > div.flex.justify-space-between.full-width > div.flex.flex-column > div.mb3 > div > div.artdeco-entity-lockup__content.ember-view > div.flex.flex-wrap.align-items-center > div.artdeco-entity-lockup__title.ember-view > a"
        els = result_el.select(selector)
        link_to_profile = ""
        if len(els) > 0:
            link_to_profile = els[0]["href"]
        return {"link_to_profile": link_to_profile}

    def get_name_from_result_el(self, result_el):
        selector = "div > div > div.flex.justify-space-between.full-width > div.flex.flex-column > div.mb3 > div > div.artdeco-entity-lockup__content.ember-view > div.flex.flex-wrap.align-items-center > div.artdeco-entity-lockup__title.ember-view > a > span"
        els = result_el.select(selector)
        name = ""
        if len(els) > 0:
            el_contents = els[0].contents
            if len(el_contents) > 0:
                name = el_contents[0].strip()
        return {"full_name": name}

    def get_role_info_from_result_el(self, result_el):
        selector = "div > div > div.flex.justify-space-between.full-width > div.flex.flex-column > div.mb3 > div > div.artdeco-entity-lockup__content.ember-view > div.artdeco-entity-lockup__subtitle.ember-view.t-14 > span"
        els = result_el.select(selector)
        role_name = ""
        if len(els) > 0:
            el_contents = els[0].contents
            if len(el_contents) > 0:
                role_name = el_contents[0].strip()
        return {"role_name": role_name}

    def get_location_from_result_el(self, result_el):
        selector = "div > div > div.flex.justify-space-between.full-width > div.flex.flex-column > div.mb3 > div > div.artdeco-entity-lockup__content > div.artdeco-entity-lockup__caption > span"
        els = result_el.select(selector)
        location = ""
        if len(els) > 0:
            el_contents = els[0].contents
            if len(el_contents) > 0:
                location = el_contents[0].strip()
        return {"location": location}

    def get_company_info_from_result_el(self, result_el):
        selector = "div > div > div.flex.justify-space-between.full-width > div.flex.flex-column > div.mb3 > div > div.artdeco-entity-lockup__content.ember-view > div.artdeco-entity-lockup__subtitle.ember-view.t-14 > a"
        els = result_el.select(selector)
        link_to_company = ""
        company_name = ""
        if len(els) > 0:
            link_to_company = els[0]["href"]
            el_contents = els[0].contents
            if len(el_contents) > 0:
                company_name = el_contents[0].strip()
        return {"link_to_company": link_to_company, "company_name": company_name}

    def get_info_from_result_el(self, result_el):
        r = []
        r.append(self.get_name_from_result_el(result_el))
        r.append(self.get_profile_link_from_result_el(result_el))
        r.append(self.get_location_from_result_el(result_el))
        r.append(self.get_role_info_from_result_el(result_el))
        r.append(self.get_company_info_from_result_el(result_el))
        info = {}
        for obj in r:
            for k in obj.keys():
                info[k] = obj[k]
        return info

    def get_result_els(self, page_source):
        soup = BeautifulSoup(page_source, "html.parser")
        full_results_selector = "#search-results-container > div > ol > li"
        all_results_el = soup.select(full_results_selector)
        return all_results_el

    def get_all_info_from_page_source(self, page_source, pScrapedProfiles):
        print("Getting all result elements...")
        result_els = self.get_result_els(page_source)
        n = len(result_els)
        print(f"Found {n} elements.")
        print("Getting the info for all elements...")
        infos = []
        for i in tqdm(range(n)):
            new_info = self.get_info_from_result_el(result_els[i])
            pScrapedProfiles += 1
            self.driver.implicitly_wait(10)
            infos.append(new_info)
        return infos

    def get_all_info_from_search_url(self, driver, url, pScrapedProfiles, wait_after_page_loaded=3,
                                     wait_after_scroll_down=2):
        driver.get(url)
        print(f"Waiting for {wait_after_page_loaded}s...")
        time.sleep(wait_after_page_loaded)
        driver.implicitly_wait(100)
        try:
            driver.execute_script(self.SCROLL_TO_BOTTOM_COMMAND)
        except:
            print("There was an error scrolling down")
        print(f"Waiting for {wait_after_scroll_down}s...")
        time.sleep(wait_after_scroll_down)
        driver.implicitly_wait(100)
        page_source = driver.page_source
        page_parsed_info = self.get_all_info_from_page_source(page_source, pScrapedProfiles)
        return page_parsed_info

    def scrap_lksn_pages(self):
        page_list = range(self.start_page, self.end_page + 1)
        counter_for_wait = 0
        for p in page_list:
            if True:
                print(f"Waiting for {self.wait_time_between_pages}s...")
                time.sleep(self.wait_time_between_pages)
                self.driver.implicitly_wait(100)
                print(f"Getting new page: {p}.")
                info = self.get_all_info_from_search_url(self.driver, self.get_search_url(p), self.pScrapedProfiles,
                                                         wait_after_page_loaded=self.wait_after_page_loaded,
                                                         wait_after_scroll_down=self.wait_after_scroll_down)
                self.total_info += info
                counter_for_wait += 1
            """else:
                print("Waiting for 60s...")
                time.sleep(60)
                counter_for_wait = 0"""
        return self.total_info

    def get_search_url(self, page=1):
        url = self.search_url_base + f"&page={page}"
        return url

    def run(self):
        self.driver.get(self.search_url)
        time.sleep(5)
        self.driver.implicitly_wait(10)
        print("Starting the scraping...")
        lksnSearchInfos = self.scrap_lksn_pages()
        df = pd.DataFrame(lksnSearchInfos)
        df["linkedin_url"] = df.link_to_profile.apply(self.get_lk_url_from_sales_lk_url)
        df["company_link"] = df.link_to_company.apply(self.get_lk_company_url_from_sales_lk_url)
        total = self.end_page - self.start_page + 1
        df['audience_id'] = pd.Series([[self.audience_id] for x in range(len(df.index))])
        linkProfiles = df["linkedin_url"].tolist()
        linkCompanies = df["company_link"].tolist()
        p_link_file_name = r"C:\\Users\gabee\anaconda3\envs\linkedinSalesNavScraper\links.json"
        baserow_api_token = 'wMWVhs8wuDQBLauICWYxXeN1LCE6eUwI'
        baserow_table_id = '292983'
        baserow_url = r'https://api.baserow.io/api/database/rows/table/292983/'
        headers = {'Authorization': f'Token {baserow_api_token}', 'Content-Type': 'application/json'}
        idList = []
        for index, row in df.iterrows():
            data = {
                'field_2097240': row['full_name'],
                'field_2097241': row['first_name'],
                'field_2097242': row['last_name'],
                'field_2097246': row['company_name'],
                'field_2097249': row['location'],
                'field_2097250': row['linkedin_url'],
                'field_2097261': row['company_link'],
                'field_2097268': row['link_to_profile'],
                'field_2097271': row['has_linkedin_premium'],
                'field_2109937': row['audience_id']
            }
            response = requests.post(baserow_url, headers=headers, json=data)
            if response.status_code == 200:
                row_id = response.json().get('id', None)
                if row_id:
                    print(f'Successfully inserted row {row_id}')
                    idList.append(row_id)
            else:
                print(f'Failed to insert row {index}: {response.text}')
        print(idList)
        print(f"Scrapped {total} pages.")
        # self.driver.delete_all_cookies()
        """self.driver.get("https://www.linkedin.com/login/")
        time.sleep(3)
        self.driver.implicitly_wait(10)
        self.driver.add_cookie({"name": "li_at", "value": "YOUR_LINKEDIN_COOKIE_VALUE"})
        self.driver.refresh()
        for c in range(total):
            # Assuming get_profile_info and get_company_info are defined elsewhere
            get_profile_info(self.driver, linkProfiles, c, idList)
            get_company_info(self.driver, linkCompanies, c, idList)
            time.sleep(600)
        if self.save_format == "csv":
            file_name = f"{str(int(time.time() * 1000))}_lk_salesnav_export.csv"
            df.to_csv(f"./lksn_data/{file_name}", index=False)
            print(f"Saved to ./lksn_data/{file_name}")
        else:
            file_name = f"{str(int(time.time() * 1000))}_lk_salesnav_export.xlsx"
            df.to_excel(f"./lksn_data/{file_name}", index=False)
            print(f"Saved to ./lksn_data/{file_name}")"""
        cookie_jar = self.driver.get_cookies()
        self.driver.close()
        return cookie_jar



