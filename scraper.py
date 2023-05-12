from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup
from threading import Lock
import requests
import concurrent.futures
import os
import zipfile
import pandas as pd
import time
import inquirer
import datetime

chrome_driver_path = os.getenv('CHROME_DRIVER_PATH')
script_dir = os.path.dirname(os.path.abspath(__file__))

options = webdriver.ChromeOptions()
options.add_argument("--headless")
service = Service(chrome_driver_path)
driver = webdriver.Chrome(options=options)

lock = Lock()
temp_csv_dir = os.path.join(script_dir, f'temp_{int(time.time())}')

csv_columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy_volume', 'ignore']
list_of_time_frames = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d']
list_of_market_types = ['spot', 'futures']

def extract_links_from_page(coin, time_frame, num_of_days, data_type):
    if data_type == 'spot':
        page_url = f'https://data.binance.vision/?prefix=data/spot/daily/klines/{coin}/{time_frame}/'
    elif data_type == 'futures':
        page_url = f'https://data.binance.vision/?prefix=data/futures/um/daily/klines/{coin}/{time_frame}/'

    driver.get(page_url)
    wait = WebDriverWait(driver, 16)
    element_id = "listing"
    expected_children = 10
    wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, f"#{element_id} > *:nth-child({expected_children})")))    

    soup = BeautifulSoup(driver.page_source, 'html.parser')
    zip_links = []

    tbody = soup.find('tbody')
    rows = tbody.find_all('tr')

    for row in rows:
        cells = row.find_all('td')
        for cell in cells:
            a_tag = cell.find('a')
            if a_tag and a_tag['href'].endswith('.zip'):
                zip_links.append(a_tag['href'])

    return zip_links if num_of_days == 'all' else zip_links[:num_of_days]

def download_and_extract_zip(args):
    i, link = args
    response = requests.get(link)
    filename = link.split('/')[-1]
    with open(os.path.join(temp_csv_dir, filename), 'wb') as f:
        f.write(response.content)

    zip_ref = zipfile.ZipFile(os.path.join(temp_csv_dir, filename), 'r')
    extracted_files = set()
    for file in zip_ref.namelist():
        extracted_files.add(file.split('/')[-1])
        zip_ref.extract(file, temp_csv_dir)

    zip_ref.close()

    csv_files = [f for f in os.listdir(temp_csv_dir) if f.endswith('.csv')]
    temp_data_list = []

    for csv_file in csv_files:
        if csv_file in processed_files:
            continue
        processed_files.add(csv_file)
        temp_data_list.append(process_csv_file(os.path.join(temp_csv_dir, csv_file)))

    return temp_data_list

def process_csv_file(csv_file):
    if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
        data = pd.read_csv(csv_file)
        data.columns = csv_columns
        return data
    else:
        time.sleep(3)
        if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
            return process_csv_file(csv_file)
        else:
            print(f"{csv_file} does not exist or is empty AGAIN")

def validate_date(answers, current):
    try:
        date = datetime.datetime.strptime(current, "%Y-%m-%d")

        today = datetime.datetime.now().date()
        if date.date() > today:
            return "Please enter a date before today"

        start_date = datetime.datetime.strptime(answers.get("start_date", ""), "%Y-%m-%d")
        if current and start_date and start_date >= date:
            return "Start date should be earlier than the end date"

        return True
    except ValueError:
        return "Please enter a valid date in the format YYYY-MM-DD"

def get_date_difference(start_date, end_date):
    return (end_date - start_date).days + 1

def get_user_input():
    data_type_prompt = [
        inquirer.List('data_type', message='Enter the data type you want to scrape', choices=list_of_market_types)
    ]

    coin_prompt = [
        inquirer.Text('coin', message='Enter the coin you want to scrape (e.g., BTCUSDT)')
    ]

    time_frame_prompt = [
        inquirer.List('time_frame', message='Enter the time frame you want to scrape', choices=list_of_time_frames)
    ]

    select_date_type_prompt = [
        inquirer.List('date_type', message='How much data you want to scrape', choices=['all', 'date range', 'number of days'])
    ]

    data_type = inquirer.prompt(data_type_prompt)['data_type'].lower()
    coin = inquirer.prompt(coin_prompt)['coin'].upper()
    time_frame = inquirer.prompt(time_frame_prompt)['time_frame'].lower()

    date_type = inquirer.prompt(select_date_type_prompt)['date_type'].lower()

    if date_type == 'date range':
        questions = [
            inquirer.Text("start_date", message="Enter the start date (YYYY-MM-DD)", validate=validate_date),
            inquirer.Text("end_date", message="Enter the end date (YYYY-MM-DD)", validate=validate_date),
        ]

        answers = inquirer.prompt(questions)
        start_date = datetime.datetime.strptime(answers["start_date"], "%Y-%m-%d")
        end_date = datetime.datetime.strptime(answers["end_date"], "%Y-%m-%d")
        num_of_days = get_date_difference(start_date, end_date)
    elif date_type == 'number of days':
        num_of_days_prompt = [
            inquirer.Text('num_of_days', message='Enter the number of previous days you want to scrape')
        ]

        num_of_days = int(inquirer.prompt(num_of_days_prompt)['num_of_days'])
    elif date_type == 'all':
        num_of_days = 'all'
    else:
        raise Exception('Invalid date type')

    return coin, time_frame, num_of_days, data_type

if __name__ == '__main__':
    coin, time_frame, num_of_days, data_type = get_user_input()
    zip_links = extract_links_from_page(coin, time_frame, num_of_days, data_type)
    
    if not os.path.exists(temp_csv_dir):
        os.makedirs(temp_csv_dir)

    data_list = []
    processed_files = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=40) as executor:
        futures = [executor.submit(download_and_extract_zip, (i, link)) for i, link in enumerate(zip_links)]
        for future in concurrent.futures.as_completed(futures):
            temp_data_list = future.result()
            data_list.extend(temp_data_list)

    if data_list:
        df = pd.concat(data_list)

        duplicates = df.duplicated()
        is_sorted = df.open_time.is_monotonic_increasing
        is_unique = df.open_time.is_unique

        if (not is_sorted):
            df.sort_values('open_time', inplace=True)

        print(f'Number of duplicates: {duplicates.sum()}')
        print(f'Number of rows      : {len(df)}')

        if (is_unique and is_sorted):
            print(f'Data has been successfully downloaded and merged to single csv file')

        df.to_csv(os.path.join(script_dir, f'{coin.lower()}_{time_frame}_{num_of_days}.csv'), index=False)

        for file in os.listdir(temp_csv_dir):
            os.remove(os.path.join(temp_csv_dir, file))

        os.rmdir(temp_csv_dir)

    driver.quit()
