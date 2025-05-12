import requests
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
import pandas as pd
import time as t
import boto3
from datetime import datetime
import io
import pandas as pd
import botocore.exceptions
import wikipedia as wp



def get_table(title, match, use_cache=False):

    
    html = wp.page(title).html()
    df = pd.read_html(io.StringIO(html), header=0, match=match)[0]
        
        # df.to_csv(filename, header=True, index=False, encoding='utf-8')
            
    # df = pd.read_csv(filename)
    return df


def get_curr_sp500():
    pd.options.mode.chained_assignment = None  # default='warn'
    pd.set_option('display.max_rows', 600)
    title = 'List of S&P 500 companies'
    ans = []
    sp500 = get_table(title, match='Symbol')
    # print(sp500)
    for index, row in sp500.iterrows():
        ans.append(row['Symbol'])
    return ans




DATABASENAME = "freshDatabase"
TABLENAME = "fundamentalTable"

API_KEY = '1282oQR0Wchq4TZXfjuXyEJqF6fmNvXX'
def to_unix_1pm_est(date_str: str) -> int:
    """
    Converts a date string (YYYY-MM-DD) to a Unix timestamp
    for 1:00 PM on that day in US Eastern Time (EST/EDT).
    """
    # Parse date and create datetime at 1:00 PM
    dt_naive = datetime.strptime(date_str, "%Y-%m-%d")
    dt_est = datetime.combine(dt_naive.date(), time(hour=13), tzinfo=ZoneInfo("America/New_York"))

    # Return Unix timestamp in seconds
    return int(dt_est.timestamp())

def stampify(ts_str):
    # Parse the timestamp with nanosecond precision and convert to Unix epoch milliseconds
    dt = datetime.strptime(ts_str[:26], '%Y-%m-%d %H:%M:%S.%f')
    return str(int(dt.timestamp() * 1000))

def get_filing_dates(response):
    final = []
    if 'results' in response:
        for item in response['results']:
            final.append(item['filing_date'])
    return final


def get_financial_data_income(data, code):
    final = []
    if 'results' in data:
        for item in data['results']:
            try:
                final.append([to_unix_1pm_est(item['filing_date']), item['financials']['income_statement'][code]['value']])
            except KeyError:
                pass
    return final

def get_financial_data_balance(data, code):
    final = []
    if 'results' in data:
        for item in data['results']:
            if code in item['financials']['balance_sheet']:
                final.append([to_unix_1pm_est(item['filing_date']), item['financials']['balance_sheet'][code]['value']])
    return final

def get_financial_data_cash(data, code):
    final = []
    if 'results' in data:
        for item in data['results']:
            if code in item['financials']['cash_flow_statement']:
                final.append([to_unix_1pm_est(item['filing_date']), item['financials']['cash_flow_statement'][code]['value']])
    return final

def get_financial_data_comprehensive(data, code):
    final = []
    if 'results' in data:
        for item in data['results']:
            if code in item['financials']['comprehensive_income']:
                final.append([to_unix_1pm_est(item['filing_date']), item['financials']['comprehensive_income'][code]['value']])
    return final


def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    # Merge on 'timestamp', using an outer join to keep all timestamps
    merged = pd.merge(df1, df2, on='timestamp', how='outer')
    
    # Sort by timestamp
    merged = merged.sort_values('timestamp').reset_index(drop=True)
    
    # Fill missing values with 0
    merged = merged.fillna(0)
    
    return merged


# Ticker is all caps string: "NVDA"
#Sheet is one of four strings "balance", "income", "cash", "comprehensive"
#code is the financial data you want "revenue"
#period is either "Q" or "A"
def get_data(ticker, sheet, code, period):
    if period == 'Q':
        BASE_URL = 'https://api.polygon.io/vX/reference/financials?ticker=' + ticker + '&filing_date.gte=2009-01-01&timeframe=quarterly&order=asc&limit=100&sort=filing_date&apiKey=1282oQR0Wchq4TZXfjuXyEJqF6fmNvXX' 
    elif period == "A":
        BASE_URL = 'https://api.polygon.io/vX/reference/financials?ticker=' + ticker + '&filing_date.gte=2010-01-01&timeframe=annual&order=asc&limit=100&sort=filing_date&apiKey=1282oQR0Wchq4TZXfjuXyEJqF6fmNvXX' 
    params = {
    'ticker': ticker,
    'limit': 100,  # How many financial statements to return
    'apiKey': API_KEY
    }
    response = requests.get(BASE_URL, params=params)
    data = response.json()
    #print(data)
    if sheet == "income":
        return get_financial_data_income(data, code)
    elif sheet == 'balance':
        return get_financial_data_balance(data, code)
    elif sheet == 'cash':
        return get_financial_data_cash(data, code)
    elif sheet == 'comprehensive':
        return get_financial_data_comprehensive(data, code)
    else:
        return


#print(get_data('NVDA', 'income', 'research_and_development', "A"))
def impute_q4_from_annual_flow(df: pd.DataFrame) -> pd.DataFrame:
    """
    If quarterly value is 0, replace it with:
        annual_i - (quarterly_{i-1} + quarterly_{i-2} + quarterly_{i-3})
    Assumes df has columns ['timestamp', 'annual', 'quarterly'].
    """
    df = df.copy()  # avoid modifying original

    for i in range(len(df)):
        if df.loc[i, 'quarterly'] == 0 and i >= 3:
            q_sum = df.loc[i-1, 'quarterly'] + df.loc[i-2, 'quarterly'] + df.loc[i-3, 'quarterly']
            df.loc[i, 'quarterly'] = df.loc[i, 'annual'] - q_sum

    return df

def impute_q4_from_annual_stock(df: pd.DataFrame) -> pd.DataFrame:
    """
    If quarterly value is 0, replace it with:
        annual_i - (quarterly_{i-1} + quarterly_{i-2} + quarterly_{i-3})
    Assumes df has columns ['timestamp', 'annual', 'quarterly'].
    """
    df = df.copy()  # avoid modifying original

    for i in range(len(df)):
        if df.loc[i, 'quarterly'] == 0:
            df.loc[i, 'quarterly'] = df.loc[i, 'annual']

    return df

def get_full_quarterly(ticker, sheet, code, stock, seen_times):
    # print(ticker)
    Annual = pd.DataFrame(get_data(ticker, sheet, code, 'A'))
    Quarterly = pd.DataFrame(get_data(ticker, sheet, code, 'Q'))
    #print("here_full")
    # print(Annual)
    # print(Quarterly)
    # print(ticker)
    Annual.columns = ['timestamp', 'annual']
    try:
        Quarterly.columns = ['timestamp', 'quarterly']
    except IndexError:
        pass

# Merge on 'timestamp' using outer join to get the union of timestamps
    combined = pd.merge(Annual, Quarterly, on='timestamp', how='outer')

# Sort by timestamp
    combined = combined.sort_values(by='timestamp')

# Fill missing values with 0
    combined = combined.fillna(0)

# Reset index if desired
    combined.reset_index(drop=True, inplace=True)
    if stock:
        data = impute_q4_from_annual_stock(combined)
    else:
        data = impute_q4_from_annual_flow(combined)
    data = data.rename(columns={'quarterly': ticker})
    data = data.drop(columns=['annual'])
    threshold = 43200  # seconds, 12 hours
    # print(data)
    # print(list(seen_times)[:10])
    # print(len(data))
    df = data[~data['timestamp'].apply(
        lambda ts: any(abs(int(ts) - int(seen)) <= threshold for seen in seen_times)
    )]
    # print(len(df))
    # assert False
    return df

def fill_zeros_after_first_nonzero(df: pd.DataFrame) -> pd.DataFrame:
    df_copy = df.copy()
    
    for col in df.columns:
        first_nonzero_found = False
        for idx in df.index:
            value = df.at[idx, col]
            
            if not first_nonzero_found:
                if value != 0:
                    first_nonzero_found = True
                continue
            
            if first_nonzero_found and value == 0:
                # Replace 0 with the value above
                df_copy.at[idx, col] = df_copy.at[idx - 1, col]
    
    return df_copy

#print(get_full_quarterly('NVDA', 'income', 'net_income_loss', False))

def save_tickers(the_list):
    with open('tickers.txt', 'w') as f:
        for ticker in the_list:
            f.write(ticker + '\n')
            

def get_prev_sp500():
   
    ans = set()
    client = boto3.client('timestream-query')
    query_string = f"""SELECT Distinct Ticker FROM "{DATABASENAME}"."{TABLENAME}" """
    response = client.query(QueryString=query_string)
    for row in response['Rows']:
        # print(row)
        values = [datum.get('ScalarValue') for datum in row['Data']]
        for item in values:
            ans.add(item)
    # print("TIMES", ans)
    return list(ans)
    
def get_current_historical_sp500():
    # print("HEY")
    all_tickers = list(set(get_curr_sp500() + get_prev_sp500()))
    # save_tickers(all_tickers)
    # print(all_tickers)
    return all_tickers
    
STOCKS = get_current_historical_sp500()

# Here is the function we want. It takes in:
#  Stocks - a list of tickers
#  Sheet - where in the balance sheet it comes from. One of four strings 'income', 'balance, 'cash' or 'comprehensive'
#  Code - the dictionary key of the thing you want - 'net_income_loss' for instance
#  Stock - a boolean if the statistic is a stock i.e. a measure of how much of something the have at that moment i.e. "assets, liabilites"
#  Otherwise, the statistic is assumed to be a flow, a measure of how much of something occured over the previous period - "net_income_loss" for instance.

def get_all_quarterly(stocks, sheet, code, stock, seen_times):
    data = get_full_quarterly(stocks[0], sheet, code, stock, seen_times)
    i = 1
    #print(data)
    while i < len(stocks):
        # t.sleep(30)
        # print(stocks[i])
        try:
            new = get_full_quarterly(stocks[i], sheet, code, stock, seen_times)
            data = merge_dataframes(data, new)
        except ValueError:
            print(stocks[i], 'missing some data, not completed')
        i += 1
        
    return fill_zeros_after_first_nonzero(data)

def upload_the_lot_weekly(the_data, meas):
    # uploaded_times = get_uploaded_times(meas)
    client = boto3.client('timestream-write')
    current_time_seconds = t.time()
    timestamp_milliseconds = str(int(current_time_seconds * 1000))
    records = []
    for col in the_data.columns:
        # print("COL:", col)
        if col == 'timestamp':
            continue  # Skip the timestamp column
        for idx, value in enumerate(the_data[col]):
            timestamp = str(int(the_data.loc[idx, 'timestamp']) * 1000) 
        # print(f"Column: {col}, Timestamp: {timestamp}, Value: {value}")
            dimension = [{'Name': 'Ticker', 'Value': col}]
            record = {'Time': timestamp, 'Dimensions': dimension, 'MeasureName': meas, 'MeasureValue': str(float(value))}
            records.append(record)
            if len(records) >= 100:
              try:
                response = client.write_records(
                    DatabaseName=DATABASENAME,
                TableName=TABLENAME,
                    Records=records
                )
                # print("Write successful:", response)
                records = []
              except botocore.exceptions.ClientError as e:
                records = []
                print(e.response)
                if e.response['Error']['Code'] == 'RejectedRecordsException':
                    print("Some records were rejected:")
                    for rejected in e.response['RejectedRecords']:
                        print(f"Index: {rejected.get('RecordIndex')}, Reason: {rejected.get('Reason')}")
                        if 'ExistingVersion' in rejected:
                            print(f"ExistingVersion: {rejected['ExistingVersion']}")
                else:
                    raise  # Re-raise if it's a different error
              records = []
    if len(records) > 0:
        try:
                response = client.write_records(
                    DatabaseName=DATABASENAME,
                TableName=TABLENAME,
                    Records=records
                )
                print("Write successful:", response)

        except botocore.exceptions.ClientError as e:
                if e.response['Error']['Code'] == 'RejectedRecordsException':
                    print("Some records were rejected:")
                    for rejected in e.response['RejectedRecords']:
                        print(f"Index: {rejected.get('RecordIndex')}, Reason: {rejected.get('Reason')}")
                        if 'ExistingVersion' in rejected:
                            print(f"ExistingVersion: {rejected['ExistingVersion']}")
                else:
                    raise  # Re-raise if it's a different error
        return

def get_uploaded_times(measure):
    times_ans = set()
    client = boto3.client('timestream-query')
    query_string =  f"""SELECT Distinct time FROM "{DATABASENAME}"."{TABLENAME}" """
    response = client.query(QueryString=query_string)
    # print(response['Rows'])
    for row in response['Rows']:
        values = [datum.get('ScalarValue') for datum in row['Data']]
        for item in values:
            times_ans.add(int(stampify(item)) // 1000)
    # print("TIMES", times_ans)
    
    # print(times_ans)
  
    return times_ans
        

NO_NEW_STOCKS = True

def lambda_handler(event, context):
    if NO_NEW_STOCKS:
        times = get_uploaded_times('net_income_loss')
    else:
        times = []
    print("TIMES IS", times)
  
    get_curr_sp500()
    # print(get_curr_sp500)
    data = get_all_quarterly(STOCKS, 'income', 'net_income_loss', False, times)
    # print(data)
    upload_the_lot_weekly(data, 'net_income_loss')
    # print()

    
    return {
        'statusCode': 200,
        'body': 's'
    }
