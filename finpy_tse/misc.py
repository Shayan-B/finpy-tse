import pandas as pd
import requests
import jdatetime
import datetime
import calendar
from IPython.display import clear_output
from .util import __Check_JDate_Validity__, headers

def Get_USD_RIAL(start_date = '1395-01-01', end_date='1400-12-29', ignore_date = False, show_weekday = False, double_date = False):
    # check date validity --------------------------------------------------------------------------------------------------------------
    if(not ignore_date):
        start_date = __Check_JDate_Validity__(start_date,key_word="'START'")
        if(start_date==None):
            return
        end_date = __Check_JDate_Validity__(end_date,key_word="'END'")
        if(end_date==None):
            return
        start = jdatetime.date(year=int(start_date.split('-')[0]), month=int(start_date.split('-')[1]), day=int(start_date.split('-')[2]))
        end = jdatetime.date(year=int(end_date.split('-')[0]), month=int(end_date.split('-')[1]), day=int(end_date.split('-')[2]))
        if(start>end):
            print('Start date must be a day before end date!')
            return
    #---------------------------------------------------------------------------------------------------------------------------------------
    r = requests.get('https://platform.tgju.org/fa/tvdata/history?symbol=PRICE_DOLLAR_RL&resolution=1D', headers=headers)
    df_data = r.json()
    try:
        df_data = pd.DataFrame({'Date':df_data['t'],'Open':df_data['o'],'High':df_data['h'],'Low':df_data['l'],'Close':df_data['c'],})
        df_data['Date'] = df_data['Date'].apply(lambda x: datetime.datetime.fromtimestamp(x))
        df_data = df_data.set_index('Date')
        df_data.index = df_data.index.to_period("D")
        df_data.index=df_data.index.to_series().astype(str)
        df_data = df_data.reset_index()
        df_data['Date'] = pd.to_datetime(df_data['Date'])
        df_data['Weekday']=df_data['Date'].dt.weekday
        df_data['Weekday'] = df_data['Weekday'].apply(lambda x: calendar.day_name[x])
        df_data['J-Date']=df_data['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_data = df_data.set_index('J-Date')
        df_data=df_data[['Date','Weekday','Open','High','Low','Close']]
        if(not show_weekday):
            df_data.drop(columns=['Weekday'],inplace=True)
        if(not double_date):
            df_data.drop(columns=['Date'],inplace=True)
        if(not ignore_date):
            df_data = df_data[start_date:end_date]
    except:
        print('WARNING: USD/RIAL data is not up-to-date! Check the following links to find out what is going on!')
        print('https://www.tgju.org/profile/price_dollar_rl/technical')
        print('https://www.tgju.org/profile/price_dollar_rl/history')
        url = 'https://api.accessban.com/v1/market/indicator/summary-table-data/price_dollar_rl' # get existing history
        r = requests.get(url, headers=headers)
        df_data = pd.DataFrame(r.json()['data'])
        df_data.columns = ['Open','Low','High','Close','4','3','Date','7']
        df_data = df_data[['Date','Open','High','Low','Close']]
        df_data['Date'] = pd.to_datetime(df_data['Date'])
        df_data['Weekday']=df_data['Date'].dt.weekday
        df_data['Weekday'] = df_data['Weekday'].apply(lambda x: calendar.day_name[x])
        df_data['J-Date']=df_data['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_data = df_data.set_index('J-Date')
        df_data=df_data[['Date','Weekday','Open','High','Low','Close']]
        cols = ['Open','High','Low','Close']
        df_data['Open'] = df_data['Open'].apply(lambda x: x.replace(',',''))
        df_data['High'] = df_data['High'].apply(lambda x: x.replace(',',''))
        df_data['Low'] = df_data['Low'].apply(lambda x: x.replace(',',''))
        df_data['Close'] = df_data['Close'].apply(lambda x: x.replace(',',''))
        df_data[cols] = df_data[cols].apply(pd.to_numeric).astype('int64')
        df_data = df_data[df_data['Open']!=0]
        df_data = df_data.iloc[::-1]
        if(not show_weekday):
            df_data.drop(columns=['Weekday'],inplace=True)
        if(not double_date):
            df_data.drop(columns=['Date'],inplace=True)
        if(not ignore_date):
            df_data = df_data[start_date: end_date]
    return df_data

################################################################################################################################################################################
################################################################################################################################################################################
