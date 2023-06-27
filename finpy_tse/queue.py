import pandas as pd

import requests
import jdatetime
import calendar
from IPython.display import clear_output

from .util import __Check_JDate_Validity__, __Get_TSE_WebID__, __Get_Day_MarketClose_BQ_SQ__, headers


def Get_Queue_History(stock = 'وخارزم', start_date = '1400-09-15', end_date='1400-12-29', show_per_capita = True, show_weekday = False, double_date = False, show_progress = True):
    """
    دریافت ارزش صف خرید یا فروش یک سهم در زمان بسته شدن بازار، در روزهای معاملاتی بین تاریخ شروع و پایان
    اگر فقط به داده های یک روز مشخص نیاز دارید از تاریخ شروع و پایان یکسان استفاده کنید
    """
    # a function to get price data from a given page ----------------------------------------------------------------------------------
    def get_price_data_forintraday(ticker_no):
        r = requests.get(f'http://old.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={ticker_no}&Top=999999&A=0', headers=headers)
        df_history=pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Final','Close','Open','Y-Final','Value','Volume','No']
        #split data into defined columns
        df_history[columns] = df_history[0].str.split("@",expand=True)
        # drop old column 0
        df_history.drop(columns=[0],inplace=True)
        df_history.dropna(inplace=True)
        df_history['Date']=pd.to_datetime(df_history['Date'])
        df_history['WEB-ID'] = ticker_no
        df_history = df_history.set_index('Date')
        return df_history
    # check date validity --------------------------------------------------------------------------------------------------------------
    failed_jdates = []
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
    #-----------------------------------------------------------------------------------------------------------------------------------
    # find web-ids 
    ticker_no_df = __Get_TSE_WebID__(stock)
    if(type(ticker_no_df)==bool):
        return
    # create an empty dataframe:
    df_history = pd.DataFrame({},columns=['Date','High','Low','Final','Close','Open','Y-Final','Value','Volume','No','WEB-ID']).set_index('Date')
    # loop to get data from different pages of a ticker:
    for index, row in (ticker_no_df.reset_index()).iterrows():
        try:
            df_temp = get_price_data_forintraday(ticker_no = row['WEB-ID'])
            df_history = pd.concat([df_history,df_temp])
        except:
            pass
    # sort index and reverse the order for more processes:
    df_history = df_history.sort_index(ascending=True)
    df_history = df_history.reset_index()
    df_history['J-Date']=df_history['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_history['Weekday']=df_history['Date'].dt.weekday
    df_history['Weekday'] = df_history['Weekday'].apply(lambda x: calendar.day_name[x])
    df_history = df_history.set_index('J-Date')
    df_history = df_history[start_date:end_date]
    j_date_list = df_history.index.to_list()
    ticker_no_list = df_history['WEB-ID'].to_list()
    # now we have valid Jalali date list, let's loop over and concat:
    no_days = len(j_date_list)
    if(no_days==0):
        print('There is no trading day between start and end date for this stock!')
        return
    else:
        df_bq_sq_val = pd.DataFrame(columns=['J-Date','Day_UL','Day_LL','Time','BQ_Value','SQ_Value','BQPC','SQPC']).set_index(['J-Date'])
        day_counter = 1
        for j_date in j_date_list:
            try:
                df_bq_sq_val = pd.concat([df_bq_sq_val,__Get_Day_MarketClose_BQ_SQ__(ticker_no_list[day_counter-1], j_date)], axis=0)
            except:
                failed_jdates.append(j_date)
            if(show_progress):
                clear_output(wait=True)
                print('Progress : ', f'{round((day_counter)/no_days*100,1)} %')
            day_counter+=1
    df_bq_sq_val = pd.concat([df_bq_sq_val, df_history[['Value','Date','Weekday']]], axis=1, join="inner") 
    cols = ['Day_UL','Day_LL','Value','BQ_Value','SQ_Value','BQPC','SQPC']
    df_bq_sq_val[cols] = df_bq_sq_val[cols].apply(pd.to_numeric).astype('int64')
    # re-arrange columns order:
    df_bq_sq_val = df_bq_sq_val[['Date','Weekday','Day_UL','Day_LL','Value','Time','BQ_Value','SQ_Value','BQPC','SQPC']]
    if(not show_per_capita):
        df_bq_sq_val.drop(columns=['BQPC','SQPC'],inplace=True)
    if(not show_weekday):
        df_bq_sq_val.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_bq_sq_val.drop(columns=['Date'],inplace=True)
    # warning for failed dates:
    if(len(failed_jdates)!=0):
        print('WARNING: The following days data is not available on TSE website, even if those are trading days!')
        print(failed_jdates)
    return df_bq_sq_val

################################################################################################################################################################################
################################################################################################################################################################################