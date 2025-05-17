import requests
from datetime import datetime, timedelta, time
from zoneinfo import ZoneInfo
import pandas as pd
import time as t
# import boto3
from datetime import datetime
import io
import pandas as pd
# import botocore.exceptions
# import wikipedia as wp


INCOME = ['common_stock_dividends', 'income_loss_before_equity_method_investments', 'net_income_loss_available_to_common_stockholders_basic', 'net_income_loss_attributable_to_parent', 'net_income_loss', 'participating_securities_distributed_and_undistributed_earnings_loss_basic', 'income_loss_from_equity_method_investments', 'selling_general_and_administrative_expenses', 'diluted_earnings_per_share', 'basic_earnings_per_share', 'net_income_loss_attributable_to_noncontrolling_interest', 'income_tax_expense_benefit', 'income_loss_from_continuing_operations_after_tax', 'preferred_stock_dividends_and_other_adjustments', 'income_loss_from_continuing_operations_before_tax', 'costs_and_expenses', 'interest_expense_operating']


COMPREHENSIVE = ['comprehensive_income_loss_attributable_to_noncontrolling_interest', 'other_comprehensive_income_loss', 'comprehensive_income_loss', 'comprehensive_income_loss_attributable_to_parent']

BALANCE = ['equity', 'assets', 'noncurrent_assets', 'accounts_receivable', 'equity_attributable_to_noncontrolling_interest', 'fixed_assets', 'current_assets', 'liabilities_and_equity', 'liabilities', 'equity_attributable_to_parent', 'other_current_assets', 'current_liabilities', 'long_term_investments', 'noncurrent_liabilities', 'other_noncurrent_assets', 'commitments_and_contingencies']

CASH = ['net_cash_flow_from_investing_activities_continuing', 'net_cash_flow_from_operating_activities_continuing', 'net_cash_flow_from_financing_activities', 'exchange_gains_losses', 'net_cash_flow', 'net_cash_flow_from_investing_activities', 'net_cash_flow_from_financing_activities_continuing', 'net_cash_flow_from_operating_activities', 'net_cash_flow_continuing']

ALL = [['income', ['common_stock_dividends', 'income_loss_before_equity_method_investments', 'net_income_loss_available_to_common_stockholders_basic', 'net_income_loss_attributable_to_parent', 'net_income_loss', 'participating_securities_distributed_and_undistributed_earnings_loss_basic', 'income_loss_from_equity_method_investments', 'selling_general_and_administrative_expenses', 'diluted_earnings_per_share', 'basic_earnings_per_share', 'net_income_loss_attributable_to_noncontrolling_interest', 'income_tax_expense_benefit', 'income_loss_from_continuing_operations_after_tax', 'preferred_stock_dividends_and_other_adjustments', 'income_loss_from_continuing_operations_before_tax', 'costs_and_expenses', 'interest_expense_operating']],
       ['comprehensive', ['comprehensive_income_loss_attributable_to_noncontrolling_interest', 'other_comprehensive_income_loss', 'comprehensive_income_loss', 'comprehensive_income_loss_attributable_to_parent']
],
 ['balance', ['equity', 'assets', 'noncurrent_assets', 'accounts_receivable', 'equity_attributable_to_noncontrolling_interest', 'fixed_assets', 'current_assets', 'liabilities_and_equity', 'liabilities', 'equity_attributable_to_parent', 'other_current_assets', 'current_liabilities', 'long_term_investments', 'noncurrent_liabilities', 'other_noncurrent_assets', 'commitments_and_contingencies']
], 
['cash', ['net_cash_flow_from_investing_activities_continuing', 'net_cash_flow_from_operating_activities_continuing', 'net_cash_flow_from_financing_activities', 'exchange_gains_losses', 'net_cash_flow', 'net_cash_flow_from_investing_activities', 'net_cash_flow_from_financing_activities_continuing', 'net_cash_flow_from_operating_activities', 'net_cash_flow_continuing']
]]

BIG =[['comprehensive', ['comprehensive_income_loss']]]

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
            #print(item['financials']['comprehensive_income'])
            if code in item['financials']['comprehensive_income']:
                final.append([to_unix_1pm_est(item['filing_date']), item['financials']['comprehensive_income'][code]['value']])
    return final


def merge_dataframes(df1: pd.DataFrame, df2: pd.DataFrame) -> pd.DataFrame:
    # Check if both dataframes are empty
    if df1.empty and df2.empty:
        return pd.DataFrame()

    # If one of the dataframes is empty, return the non-empty one
    if df1.empty:
        return df2.reset_index(drop=True)
    if df2.empty:
        return df1.reset_index(drop=True)

    # Merge on 'timestamp', using an outer join to keep all timestamps
    merged = pd.merge(df1, df2, on='timestamp', how='outer')

    # Sort by timestamp
    merged = merged.sort_values('timestamp').reset_index(drop=True)

    # Fill missing values with 0
    merged = merged.fillna(0)

    # Replace zeros with the value from above if possible
    for column in merged.columns:
        if column != 'timestamp':  # Skip the timestamp column
            # Use .ffill() and then convert objects to their appropriate types
            merged[column] = merged[column].replace(0, pd.NA).ffill().fillna(0)
            merged[column] = merged[column].infer_objects()

    return merged

# Ticker is all caps string: "NVDA"
#Sheet is one of four strings "balance", "income", "cash", "comprehensive"
#code is the financial data you want "revenue"
#period is either "Q" or "A"
def get_data(ticker, sheet, code, period):
    if period == 'Q':
        BASE_URL = 'https://api.polygon.io/vX/reference/financials?ticker=' + ticker + '&filing_date.gte=2021-01-01&timeframe=quarterly&order=asc&limit=100&sort=filing_date&apiKey=1282oQR0Wchq4TZXfjuXyEJqF6fmNvXX' 
    elif period == "A":
        BASE_URL = 'https://api.polygon.io/vX/reference/financials?ticker=' + ticker + '&filing_date.gte=2021-01-01&timeframe=annual&order=asc&limit=100&sort=filing_date&apiKey=1282oQR0Wchq4TZXfjuXyEJqF6fmNvXX' 
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
        #print('comprehensive')
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

def get_full_quarterly(ticker, sheet, code, seen_times):
    # print(ticker)
    Annual = pd.DataFrame(get_data(ticker, sheet, code, 'A'))
    Quarterly = pd.DataFrame(get_data(ticker, sheet, code, 'Q'))
    # print('why')
    # print([ticker, sheet, code])
    # print(get_data(ticker, sheet, code, 'Q'))
    
    if Annual.empty or Quarterly.empty:
        return pd.DataFrame()
    #print("here_full")
    # print(Annual)
    # print(Quarterly)
    # print(ticker)
    try: 
        Annual.columns = ['timestamp', 'annual']
    except IndexError:
        pass
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
    if sheet == 'balance':
        data = impute_q4_from_annual_stock(combined)
    else:
        data = impute_q4_from_annual_flow(combined)
    data = data.rename(columns={'quarterly': ticker})
    data = data.drop(columns=['annual'])
    threshold = 43200  # seconds, 12 hours
    # print(data)
    # print(list(seen_times)[:10])
    # print(len(data))
    # df = data[~data['timestamp'].apply(
    #     lambda ts: any(abs(int(ts) - int(seen)) <= threshold for seen in seen_times)
    # )]
    return data
    # print(len(df))
    # assert False
    return df

#SEEN TIME PLEASE FIX
def create_dataframe(stocks, seen_time):
    dataframes = {}
    for sheet in ALL:
        for datatype in sheet[1]:
            dataframes[datatype] = get_full_quarterly(stocks[0], sheet[0], datatype, seen_time)
    i = 1
    while i < len(stocks):
        for sheet in ALL:
            for datatype in sheet[1]:
                try: 
                    #print(get_full_quarterly(stocks[i], sheet[0], datatype, seen_time))
                    temp = get_full_quarterly(stocks[i], sheet[0], datatype, seen_time)
                    data = merge_dataframes(dataframes[datatype], temp)
                    dataframes[datatype] = data
                except ValueError:
                    print(stocks[i], 'missing some data, not completed')
        i += 1
    return dataframes

if __name__ == "__main__":
    # Example usage
    #print(get_data('AAPL', 'comprehensive', 'comprehensive_income_loss', 'Q'))
    #print(get_full_quarterly('AAPL', 'comprehensive', 'net_income_loss', 0))
    stocks = ["GOOGL", 'NVDA', "AAPL", "MSFT"]  # Replace with your actual list of stocks
    seen_time = 100 
    #print(get_full_quarterly('AAPL', 'comprehensive', 'comprehensive_income_loss', 100))
    dataframes = create_dataframe(stocks, seen_time)
    print(dataframes)


