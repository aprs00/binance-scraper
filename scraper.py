from threading import Lock
import requests
import concurrent.futures
import os
import zipfile
import pandas as pd
import time
import inquirer
import datetime
import uuid

script_dir = os.path.dirname(os.path.abspath(__file__))

lock = Lock()
temp_csv_klines_dir = os.path.join(script_dir, f'binance_scraper_klines_dir_{uuid.uuid4().hex}')
temp_csv_premium_index_dir = os.path.join(script_dir, f'binance_scraper_premium_index_dir_{uuid.uuid4().hex}')

csv_klines_columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy__quote_volume', 'ignore']
csv_metrics_columns = ['create_time', 'symbol', 'sum_open_interest', 'sum_open_interest_value', 'count_top_trader_long_short_ratio', 'sum_toptrader_long_short_ratio', 'count_long_short_ratio', 'sum_taker_long_short_vol_ratio']
premium_index_klines_columns = ['open_time', 'premium_index_open', 'premium_index_high', 'premium_index_low', 'premium_index_close', 'volume', 'close_time', 'quote_volume', 'count', 'taker_buy_volume', 'taker_buy__quote_volume', 'ignore']
list_of_time_frames = ['1m', '3m', '5m', '15m', '30m', '1h', '2h', '4h', '6h', '8h', '12h', '1d']
list_of_market_types = ['spot', 'futures']

def get_klines_zip_links(coin, time_frame, num_of_days, data_type):
    if data_type == 'spot':
        base_url = "https://data.binance.vision/data/spot/daily/klines/"
    elif data_type == 'futures':
        base_url = "https://data.binance.vision/data/futures/um/daily/klines/"
    else: 
        print("Invalid data type")
        return

    zip_links = []
    current_date = datetime.datetime.now().date() - datetime.timedelta(days=1)

    for _ in range(num_of_days):
        date_str = current_date.strftime("%Y-%m-%d")
        zip_url = f"{base_url}{coin}/{time_frame}/{coin}-{time_frame}-{date_str}.zip"
        zip_links.append(zip_url)
        current_date -= datetime.timedelta(days=1)

    return zip_links

def get_metrics_zip_links(coin, time_frame, num_of_days, data_type):
    time_frame = int(''.join([i for i in time_frame if i.isdigit()]))
    if data_type != 'futures' or time_frame < 5: return None

    base_url = "https://data.binance.vision/data/futures/um/daily/metrics/"

    zip_links = []
    current_date = datetime.datetime.now().date() - datetime.timedelta(days=1)

    for _ in range(num_of_days):
        date_str = current_date.strftime("%Y-%m-%d")
        zip_url = f"{base_url}{coin}/{coin}-metrics-{date_str}.zip"
        zip_links.append(zip_url)
        current_date -= datetime.timedelta(days=1)

    return zip_links

def get_premium_index_klines_zip_links(coin, time_frame, num_of_days, data_type):
    if data_type != 'futures': return None

    base_url = "https://data.binance.vision/data/futures/um/daily/premiumIndexKlines/"

    zip_links = []
    current_date = datetime.datetime.now().date() - datetime.timedelta(days=1)

    for _ in range(num_of_days):
        date_str = current_date.strftime("%Y-%m-%d")
        zip_url = f"{base_url}{coin}/{time_frame}/{coin}-{time_frame}-{date_str}.zip"
        zip_links.append(zip_url)
        current_date -= datetime.timedelta(days=1)

    return zip_links

def download_and_extract_zip(args):
    i, link, csv_column_names, temp_csv_dir = args
    filename = download_zip(link, temp_csv_dir)
    if filename is None:
        return None
    extracted_files = extract_zip(filename, temp_csv_dir)
    temp_data_list = process_extracted_files(extracted_files, csv_column_names, temp_csv_dir)
    
    return temp_data_list

def download_zip(link, temp_csv_dir):
    response = requests.get(link)
    if response.status_code != 200:
        print(f"Error downloading {link}: {response.status_code} {response.reason}")
        return None
    unique_id = uuid.uuid4().hex
    filename = f"{unique_id}-{link.split('/')[-1]}"
    with open(os.path.join(temp_csv_dir, filename), 'wb') as f:
        f.write(response.content)
    return filename

def extract_zip(filename, temp_csv_dir):
    zip_ref = zipfile.ZipFile(os.path.join(temp_csv_dir, filename), 'r')
    extracted_files = set()
    for file in zip_ref.namelist():
        unique_id = uuid.uuid4().hex
        concatted_filename = f"{unique_id}-{file}"
        extracted_files.add(concatted_filename)
        with open(os.path.join(temp_csv_dir, concatted_filename), 'wb') as f:
            f.write(zip_ref.read(file))
    zip_ref.close()
    return extracted_files

def process_extracted_files(extracted_files, csv_column_names, temp_csv_dir):
    temp_data_list = []
    for csv_file in extracted_files:
        if csv_file in processed_files:
            continue
        processed_files.add(csv_file)
        data = process_csv_file(os.path.join(temp_csv_dir, csv_file), csv_column_names)
        temp_data_list.append(data)
    return temp_data_list

def process_csv_file(csv_file, csv_column_names):
    if os.path.exists(csv_file) and os.path.getsize(csv_file) > 0:
        try:
            data = pd.read_csv(csv_file)
            data.columns = csv_column_names
            return data
        except Exception as e:
            print(f"Error reading {csv_file}: {e}")
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
        inquirer.List('data_type', message='Enter the data type', choices=list_of_market_types)
    ]
    coin_prompt = [
        inquirer.Text('coin', message='Enter coin (e.g. BTCUSDT)')
    ]
    time_frame_prompt = [
        inquirer.List('time_frame', message='Enter the time frame', choices=list_of_time_frames)
    ]
    select_date_type_prompt = [
        inquirer.List('date_type', message='How much data you want to scrape', choices=['date range', 'number of days'])
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

def delete_dir(dir):
    for file in os.listdir(dir):
        os.remove(os.path.join(dir, file))

    os.rmdir(dir)

if __name__ == '__main__':
    # coin, time_frame, num_of_days, data_type = get_user_input()
    coin, time_frame, num_of_days, data_type = 'SOLUSDT', '3m', 620, 'futures'
    klines_zip_links = get_klines_zip_links(coin, time_frame, num_of_days, data_type)
    metrics_zip_links = get_metrics_zip_links(coin, time_frame, num_of_days, data_type)
    premium_index_klines_zip_links = get_premium_index_klines_zip_links(coin, time_frame, num_of_days, data_type)

    if not os.path.exists(temp_csv_klines_dir):
        os.makedirs(temp_csv_klines_dir)
    
    if not os.path.exists(temp_csv_premium_index_dir):
        os.makedirs(temp_csv_premium_index_dir)

    data_list_klines = []
    data_list_metrics = []
    data_list_premium_index_klines = []
    processed_files = set()

    with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
        klines_futures = [
            executor.submit(download_and_extract_zip, (i, link, csv_klines_columns, temp_csv_klines_dir)) 
            for i, link in enumerate(klines_zip_links)
        ]
        premium_index_klines_futures = [
            executor.submit(download_and_extract_zip, (i, link, premium_index_klines_columns, temp_csv_klines_dir)) 
            for i, link in enumerate(premium_index_klines_zip_links)
        ]

        all_futures = klines_futures + premium_index_klines_futures

        for future in concurrent.futures.as_completed(all_futures):
            if future in klines_futures:
                temp_data_list_klines = future.result()
                if temp_data_list_klines is not None:
                    data_list_klines.extend(temp_data_list_klines)
            elif future in premium_index_klines_futures:
                temp_data_list_premium_index_klines = future.result()
                if temp_data_list_premium_index_klines is not None:
                    data_list_premium_index_klines.extend(temp_data_list_premium_index_klines)

    if data_list_klines and data_list_premium_index_klines:
        df_klines = pd.concat(data_list_klines).sort_values('open_time')
        df_premium_index = pd.concat(data_list_premium_index_klines).sort_values('open_time')

        df_merged = pd.merge(df_klines, df_premium_index[['open_time', 'premium_index_open', 'premium_index_high', 'premium_index_low', 'premium_index_close']], on='open_time', how='left')
        df_merged.to_csv(os.path.join(script_dir, f'{coin.lower()}_{time_frame}_{num_of_days}_klines_premium_index.csv'), index=False)

        is_merged_unique = df_merged.open_time.is_unique
        is_merged_sorted = df_merged.open_time.is_monotonic_increasing
        is_merged_duplicates = df_merged.duplicated(subset=['open_time']).any()

        if (is_merged_unique and is_merged_sorted and not is_merged_duplicates):
            print(f'Data has been successfully downloaded and merged to single csv file')
            print(f'Number of rows: {len(df_merged)}')
        else:
            raise Exception('Data is not unique or sorted')

        delete_dir(temp_csv_klines_dir)
        delete_dir(temp_csv_premium_index_dir)
