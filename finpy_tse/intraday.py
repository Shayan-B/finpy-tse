import pandas as pd
import requests
import jdatetime
from IPython.display import clear_output

from.util import __Check_JDate_Validity__, __Get_TSE_WebID__, __Get_Day_LOB__, headers

def __Get_Day_IntradayTrades__(ticker_no, j_date):
    #convert to desired Cristian data format
    year, month, day = j_date.split('-')
    date = jdatetime.date(int(year), int(month), int(day)).togregorian()
    date = f'{date.year:04}{date.month:02}{date.day:02}'
    # request and process
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    page = requests.get(f'http://cdn.tsetmc.com/api/Trade/GetTradeHistory/{ticker_no}/{date}/false', headers=headers)
    df_intraday = (pd.DataFrame(page.json()['tradeHistory'])).iloc[:,2:6]
    df_intraday = df_intraday.sort_values(by='nTran')
    df_intraday.drop(columns=['nTran'],inplace=True)
    df_intraday.columns = ['Time','Volume','Price']
    df_intraday['Time'] = df_intraday['Time'].astype('str').apply(lambda x: '0'+x[0]+':'+x[1:3]+':'+x[3:]  if len(x)==5 else x[:2]+':'+x[2:4]+':'+x[4:])
    df_intraday['J-Date'] = j_date
    df_intraday = df_intraday.set_index(['J-Date','Time'])
    return df_intraday

################################################################################################################################################################################
################################################################################################################################################################################

def Get_IntradayTrades_History(stock = 'وخارزم', start_date = '1400-09-15', end_date='1400-12-29', jalali_date = True, combined_datatime = False, show_progress = True):
    """
    دریافت سابقه ریز معاملات یک سهم در روزهای معاملاتی بین تاریخ شروع و پایان
    اگر فقط به داده های یک روز مشخص نیاز دارید از تاریخ شروع و پایان یکسان استفاده کنید
    توجه داشته باشید که معاملات باطل شده نماد در خروجی این تابع نمایش داده نمی شود
    """
    # a function to get price data from a given page ----------------------------------------------------------------------------------
    failed_jdates = []
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
        df_intraday = pd.DataFrame(columns=['J-Date','Time','Volume','Price']).set_index(['J-Date','Time'])
        day_counter = 1
        for j_date in j_date_list:
            try:
                df_intraday = pd.concat([df_intraday,__Get_Day_IntradayTrades__(ticker_no_list[day_counter-1], j_date)], axis=0)
            except:
                failed_jdates.append(j_date)
            if(show_progress):
                clear_output(wait=True)
                print('Progress : ', f'{round((day_counter)/no_days*100,1)} %')
            day_counter+=1
    # other settings -------------------------------------------------------------------------------------------------------------
    if(jalali_date):
        if(combined_datatime):
            df_intraday = df_intraday.reset_index()
            # add date to data frame:
            df_intraday['Date'] = df_intraday['J-Date'].apply(lambda x: jdatetime.date(int(x[:4]),int(x[5:7]),int(x[8:])).togregorian())
            df_intraday['DateTime'] = pd.to_datetime(df_intraday['Date'].astype(str) +' '+ df_intraday['Time'].astype(str))
            print('Combining Jalali date and time takes a few more seconds!')
            df_intraday['J-DateTime'] = df_intraday['DateTime'].apply(lambda x: str(jdatetime.datetime.fromgregorian(datetime=x)))
            df_intraday.drop(columns=['DateTime','Date','J-Date','Time'],inplace=True)
            df_intraday = df_intraday.set_index(['J-DateTime'])
    else:
        if(combined_datatime):
            df_intraday = df_intraday.reset_index()
            df_intraday['Date'] = df_intraday['J-Date'].apply(lambda x: jdatetime.date(int(x[:4]),int(x[5:7]),int(x[8:])).togregorian())
            df_intraday['DateTime'] = pd.to_datetime(df_intraday['Date'].astype(str) +' '+ df_intraday['Time'].astype(str))
            df_intraday.drop(columns=['Date','J-Date','Time'],inplace=True)
            df_intraday = df_intraday.set_index(['DateTime'])
        else:
            df_intraday = df_intraday.reset_index()
            df_intraday['Date'] = df_intraday['J-Date'].apply(lambda x: jdatetime.date(int(x[:4]),int(x[5:7]),int(x[8:])).togregorian())
            df_intraday.drop(columns=['J-Date'],inplace=True)
            df_intraday = df_intraday.set_index(['Date','Time'])
    df_intraday[['Volume','Price']] = df_intraday[['Volume','Price']].astype('int64')
    # warning for failed dates:
    if(len(failed_jdates)!=0):
        print('WARNING: The following days data is not available on TSE website, even if those are trading days!')
        print(failed_jdates)
    return df_intraday

################################################################################################################################################################################
################################################################################################################################################################################

def Get_IntradayOB_History(stock = 'کرمان', start_date = '1400-08-01', end_date='1400-08-01', jalali_date = True, combined_datatime = False, show_progress = True):
    """
    دریافت اطلاعات عرضه تقاضای اردر بوک برای یک سهم، در روزهای معاملاتی بین تاریخ شروع و پایان
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
        df_lob = pd.DataFrame(columns=['J-Date','Time','Depth','Sell_No','Sell_Vol','Sell_Price','Buy_Price','Buy_Vol','Buy_No',\
                                       'Day_LL','Day_UL','Date']).set_index(['J-Date','Time','Depth'])
        day_counter = 1
        for j_date in j_date_list:
            try:
                df_lob = pd.concat([df_lob,__Get_Day_LOB__(ticker_no_list[day_counter-1], j_date)], axis=0)
            except:
                failed_jdates.append(j_date)
            if(show_progress):
                clear_output(wait=True)
                print('Progress : ', f'{round((day_counter)/no_days*100,1)} %')
            day_counter+=1
    if(jalali_date):
        if(combined_datatime):
            df_lob = df_lob.reset_index()
            df_lob['DateTime'] = pd.to_datetime(df_lob['Date'].astype(str) +' '+ df_lob['Time'].astype(str))
            print('Combining Jalali date and time takes a few more seconds!')
            df_lob['J-DateTime'] = df_lob['DateTime'].apply(lambda x: str(jdatetime.datetime.fromgregorian(datetime=x)))
            df_lob.drop(columns=['DateTime','Date','J-Date','Time'],inplace=True)
            df_lob = df_lob.set_index(['J-DateTime','Depth'])
        else:
            df_lob.drop(columns=['Date'],inplace=True)
    else:
        if(combined_datatime):
            df_lob = df_lob.reset_index()
            df_lob['DateTime'] = pd.to_datetime(df_lob['Date'].astype(str) +' '+ df_lob['Time'].astype(str))
            df_lob.drop(columns=['Date','J-Date','Time'],inplace=True)
            df_lob = df_lob.set_index(['DateTime','Depth'])
        else:
            df_lob = df_lob.reset_index()
            df_lob.drop(columns=['J-Date'],inplace=True)
            df_lob = df_lob.set_index(['Date','Time','Depth'])
    if(len(failed_jdates)!=0):
        print('WARNING: The following days data is not available on TSE website, even if those are trading days!')
        print(failed_jdates)
    return df_lob

################################################################################################################################################################################
################################################################################################################################################################################