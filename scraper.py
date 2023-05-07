from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from bs4 import BeautifulSoup
import requests
import concurrent.futures
import os
import zipfile
import pandas as pd
import time

chrome_driver_path = os.getenv('CHROME_DRIVER_PATH')
script_dir = os.path.dirname(os.path.abspath(__file__))

options = webdriver.ChromeOptions()
options.add_argument("--headless")
service = Service(chrome_driver_path)
driver = webdriver.Chrome(options=options)

def extract_links_from_page(coin, time_frame):
    page_url = f'https://data.binance.vision/?prefix=data/futures/um/daily/klines/{coin}/{time_frame}/'

    driver.get(page_url)
    time.sleep(7)

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

    for link in zip_links[:10]:
        print(link)

    return zip_links[:10]

def download_and_extract_zip(args):
    i, link = args
    response = requests.get(link)
    filename = link.split('/')[-1]
    with open(os.path.join(script_dir, filename), 'wb') as f:
        f.write(response.content)

    zip_ref = zipfile.ZipFile(os.path.join(script_dir, filename), 'r')
    extracted_files = set()
    for file in zip_ref.namelist():
        extracted_files.add(file.split('/')[-1])
        zip_ref.extract(file, temp_dir)

    zip_ref.close()

    csv_files = [f for f in os.listdir(temp_dir) if f.endswith('.csv')]
    temp_data_list = []

    for csv_file in csv_files:
        if csv_file in processed_files:
            continue
        processed_files.add(csv_file)
        temp_data_list.append(process_csv_file(os.path.join(temp_dir, csv_file)))

    return temp_data_list

def process_csv_file(csv_file):
    if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
        return pd.read_csv(csv_file)
    else:
        print(f"{csv_file} does not exist or is empty")
        time.sleep(2)
        if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
            return pd.read_csv(csv_file)
        else:
            print(f"{csv_file} does not exist or is empty AGAIN")

def extract_zip_file(zip_file):
    print(f'Extracting {zip_file} to {temp_dir}')
    try:
        zip_ref = zipfile.ZipFile(os.path.join(script_dir, zip_file), 'r')
        zip_ref.extractall(temp_dir)
        zip_ref.close()
    except Exception as e:
        print(f'Error extracting {zip_file}: {e}')

def get_user_input():
    coin = input('Enter the coin you want to scrape (e.g. BTCUSDT): ')
    list_of_time_frames = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d']
    time_frame = input(f'Enter the time frame you want to scrape\nTime frames available: {list_of_time_frames}: ')

    coin = coin.upper()
    time_frame = time_frame.lower()
    return coin, time_frame

if __name__ == '__main__':
    coin, time_frame = get_user_input()
    zip_links = extract_links_from_page(coin, time_frame)
    temp_dir = os.path.join(script_dir, 'temp')
    
    if not os.path.exists(temp_dir):
        os.makedirs(temp_dir)

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

        print(f'Number of duplicates: {duplicates.sum()}')
        print(f'data_list length    : {len(data_list)}')
        print(f'zip_links length    : {len(zip_links)}')
        print(f'Number of rows      : {len(df)}')
        print(f'open_time is unique : {df.open_time.is_unique}')
        print(f'open_time is sorted : {is_sorted}')

        if (not is_sorted):
            df.sort_values('open_time', inplace=True)
            print(f'open_time is sorted : {df.open_time.is_monotonic_increasing}')

        df.to_csv(os.path.join(script_dir, 'merged_data.csv'), index=False)

        for file in os.listdir(temp_dir):
            os.remove(os.path.join(temp_dir, file))
        os.rmdir(temp_dir)
        for link in zip_links:
            os.remove(os.path.join(script_dir, link.split('/')[-1]))

    driver.quit()
