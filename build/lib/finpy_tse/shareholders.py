import pandas as pd
import requests
import jdatetime
from IPython.display import clear_output
import urllib3
urllib3.disable_warnings()
from bs4 import BeautifulSoup
from persiantools import characters
import calendar


from .util import __Save_List__, __Get_TSE_WebID__, __Check_JDate_Validity__, get_tse_webid, headers


def Get_ShareHoldersInfo(ticker = 'خودرو'):
    """
    دریافت آخرین وضعیت سهامداران بالای 1% نماد مورد نظر 
    """
    # find web-ids 
    ticker_no_df = __Get_TSE_WebID__(ticker)
    if(type(ticker_no_df)==bool):
        return
    ticker_no_df.reset_index(inplace=True)
    ticker_no_df = ticker_no_df[ticker_no_df['Active']==1]
    wid = ticker_no_df['WEB-ID'].values[0]
    market = ticker_no_df['Market'].values[0]

    # get isin id
    r = requests.get(f'http://old.tsetmc.com/Loader.aspx?Partree=15131M&i={wid}', headers=headers)
    isin_id = pd.read_html(r.text)[0].iloc[7,1]

    # get shareholders data
    r = requests.get(f'http://old.tsetmc.com/Loader.aspx?Partree=15131T&c={isin_id}', headers=headers)
    soup = BeautifulSoup(r.text, 'html.parser')
    table = soup.findAll("table", {"class": "table1"})[0].findAll("tr", {"class": "sh"})

    # extarct data and create dataframe
    name_list = []
    out_list = []
    per_list = []
    change_list = []

    for i in range(len(table)):
        name_list.append(characters.ar_to_fa(table[i].findAll("td")[0].text))
        out_list.append(int((table[i].findAll("td")[1].findAll('div')[0].attrs['title']).replace(',','')))
        per_list.append(float(table[i].findAll("td")[2].text))
        try:
            change_list.append(int((table[i].findAll("td")[3].text).replace(',','')))
        except:
            change_list.append(int((table[i].findAll("td")[3].findAll('div')[0].attrs['title']).replace(',','')))

    df_sh = pd.DataFrame({'Name':name_list, 'ShareNo':out_list, 'SharePct':per_list, 'Changes':change_list})
    df_sh['Ticker'] = ticker
    df_sh['Market'] = market
    df_sh.set_index(['Ticker','Market','Name'], inplace=True)
    return df_sh

################################################################################################################################################################################
################################################################################################################################################################################

def get_ri_history(stock:str = 'خودرو', 
                   start_date:str = '1400-01-01', 
                   end_date:str = '1401-01-01', 
                   ignore_date:bool = False, 
                   show_weekday:bool = False, 
                   double_date:bool = False) -> pd.DataFrame:
    
    """
    Takes ticker or firm's full name and returns a Pandas dataframe that contains the following columns:
    
    J-Date: Jalali date, as index
    Date: Gregorian date
    Weekday: Name of weekdays
    No_Buy_R: Number of buy trades executed by retails 
    No_Buy_I: Number of buy trades executed by institutionals 
    No_Sell_R: Number of sell trades executed by retails 
    No_Sell_I: Number of sell trades executed by institutionals
    Vol_Buy_R: Total volume are bought by retails
    Vol_Buy_I: Total volume are bought by institutionals
    Vol_Sell_R: Total volume are sold by retails
    Vol_Sell_I: Total volume are sold by institutionals
    Val_Buy_R: Total value in IRAN's Rial that are bought by retails
    Val_Buy_I: Total value in IRAN's Rial that are bought by institutionals
    Val_Sell_R: Total value in IRAN's Rial that are sold by retails
    Val_Sell_I: Total value in IRAN's Rial that are sold by institutionals
    Ticker: Ticker/Symbol
    Name: Firm's full name
    Market: Market, the stock is traded in
    
    * All data are taken from the new website of Tehran Stock Exchange.
    
    :param stock: (str) Ticker or firm's full name. 
    :param start_date: (str) Jalali date for starting day of historical retail-institutional data in YYYY-MM-DD format.
    :param end_date: (str) Jalali date for ending day of historical retail-institutional data in YYYY-MM-DD format.
    :param ignore_date: (bool) Ignores start_date and end_date and returns all available retail-institutional history, if set to True.
    :param show_weekday: (bool) Shows weekdays in the output, if set to True.
    :param double_date: (bool) Shows Gregorian date in the output, if set to True.
    :return: (pd.DataFrame) A dataframe that contains J-Date as index and Date, Weekday, No_Buy_R, No_Buy_I, No_Sell_R, No_Sell_I, Vol_Buy_R, Vol_Buy_I, Vol_Sell_R, Vol_Sell_I, Val_Buy_R, Val_Buy_I, Val_Sell_R, Val_Sell_I, Ticker, Name, Market.
    """
    
    # basic request and data cleaning function for historical retail-institutional data of a ticker for a given market:
    def get_ri_data(ticker_no, ticker, name, market):
        r = requests.get(f'http://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{ticker_no}',headers=headers)
        df_RI_tab = pd.DataFrame(r.json()['clientType'])
        cols = ['Date','WebID','Vol_Buy_R','Vol_Buy_I','Val_Buy_R','Val_Buy_I','No_Buy_I','Vol_Sell_R','No_Buy_R','Vol_Sell_I','Val_Sell_R','Val_Sell_I','No_Sell_I','No_Sell_R']
        df_RI_tab.columns = cols
        df_RI_tab = df_RI_tab[['Date','No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R','Vol_Sell_I','Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I']]
        df_RI_tab['Date'] = df_RI_tab['Date'].apply(lambda x: str(x))
        cols = ['No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R','Vol_Sell_I','Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I']
        df_RI_tab[cols] = df_RI_tab[cols].astype('int64')
        df_RI_tab['Date']=pd.to_datetime(df_RI_tab['Date'])
        df_RI_tab['Ticker'] = ticker
        df_RI_tab['Name'] = name
        df_RI_tab['Market'] = market
        df_RI_tab = df_RI_tab.set_index('Date')
        return df_RI_tab
    
    # check to see if the entry start and end dates are valid or not:
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

    # search for WebIDs:
    ticker_no_df = get_tse_webid(stock)
    if(type(ticker_no_df)==bool):
        return
    
    # create an empty dataframe:   
    df_RI_tab = pd.DataFrame({},columns=['Date','No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R',
                                         'Vol_Sell_I','Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I','Ticker','Name','Market']).set_index('Date')
   
    # loop to get data from different pages of a ticker:
    for index, row in (ticker_no_df.reset_index()).iterrows():
        try:
            df_temp = get_ri_data(ticker_no = row['WebID'], ticker = row['Ticker'], name = row['Name'], market = row['Market'])
            df_RI_tab = pd.concat([df_RI_tab,df_temp])
        except:
            pass
        
    # sort date index 
    df_RI_tab = df_RI_tab.sort_index(ascending=True)
    df_RI_tab = df_RI_tab.reset_index()
    
    # add weekdays and Jalali date:
    df_RI_tab['Weekday']=df_RI_tab['Date'].dt.weekday
    df_RI_tab['Weekday'] = df_RI_tab['Weekday'].apply(lambda x: calendar.day_name[x])
    df_RI_tab['J-Date']=df_RI_tab['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_RI_tab.set_index(df_RI_tab['J-Date'],inplace = True)
    df_RI_tab = df_RI_tab.set_index('J-Date')
    
    # rearrange columns and convert some columns to numeric:
    df_RI_tab=df_RI_tab[['Date','Weekday','No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R','Vol_Sell_I',
                         'Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I','Ticker','Name','Market']]
    cols = ['No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R','Vol_Sell_I','Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I']
    df_RI_tab[cols] = df_RI_tab[cols].apply(pd.to_numeric, axis=1)
    
    # drop weekdays if not requested:
    if(not show_weekday):
        df_RI_tab.drop(columns=['Weekday'],inplace=True)
        
    # drop Gregorian date if not requested:
    if(not double_date):
        df_RI_tab.drop(columns=['Date'],inplace=True)
        
    # # slice requested time window, if requested:
    if(not ignore_date):
        df_RI_tab = df_RI_tab[start_date:end_date]
        
    return df_RI_tab


def Get_RI_History(stock = 'خودرو', start_date = '1400-01-01', end_date='1401-01-01', ignore_date = False, show_weekday = False, double_date = False, alt = False):
    """
    دریافت سابقه اطلاعات حقیقی-حقوقی یک سهم در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه حقیقی-حقوقی بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    """
    # a function to get ri data from a given page ----------------------------------------------------------------------------------
    def get_ri_data(ticker_no,ticker,name, data_part):
        if(alt):
            r = requests.get(f'http://cdn.tsetmc.com/api/ClientType/GetClientTypeHistory/{ticker_no}',headers=headers)
            df_RI_tab = pd.DataFrame(r.json()['clientType'])
            cols = ['Date','WebID','Vol_Buy_R','Vol_Buy_I','Val_Buy_R','Val_Buy_I','No_Buy_I','Vol_Sell_R','No_Buy_R','Vol_Sell_I','Val_Sell_R','Val_Sell_I','No_Sell_I','No_Sell_R']
            df_RI_tab.columns = cols
            df_RI_tab = df_RI_tab[['Date','No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R','Vol_Sell_I','Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I']]
            df_RI_tab['Date'] = df_RI_tab['Date'].apply(lambda x: str(x))
            cols = ['No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R','Vol_Sell_I','Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I']
            df_RI_tab[cols] = df_RI_tab[cols].astype('int64')
        else:
            r = requests.get(f'http://www.tsetmc.com/tsev2/data/clienttype.aspx?i={ticker_no}', headers=headers)
            df_RI_tab=pd.DataFrame(r.text.split(';'))
            # define columns
            columns=['Date','No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R','Vol_Sell_I','Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I']
            # split data into defined columns
            df_RI_tab[columns] = df_RI_tab[0].str.split(",",expand=True)
            # drop old column 0
            df_RI_tab.drop(columns=[0],inplace=True)
        df_RI_tab['Date']=pd.to_datetime(df_RI_tab['Date'])
        df_RI_tab['Ticker'] = ticker
        df_RI_tab['Name'] = name
        df_RI_tab['Market'] = data_part
        df_RI_tab = df_RI_tab.set_index('Date')
        return df_RI_tab
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
    # find web-ids 
    ticker_no_df = __Get_TSE_WebID__(stock)
    if(type(ticker_no_df)==bool):
        return
    # create an empty dataframe:   
    df_RI_tab = pd.DataFrame({},columns=['Date','No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R',
                                         'Vol_Sell_I','Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I','Ticker','Name','Market']).set_index('Date')
    # loop to get data from different pages of a ticker:
    for index, row in (ticker_no_df.reset_index()).iterrows():
        try:
            df_temp = get_ri_data(ticker_no = row['WEB-ID'],ticker = row['Ticker'],name = row['Name'],data_part = row['Market'])
            df_RI_tab = pd.concat([df_RI_tab,df_temp])
        except:
            pass
    # sort index and reverse the order for more processes:
    df_RI_tab = df_RI_tab.sort_index(ascending=True)
    df_RI_tab = df_RI_tab.reset_index()
    # determining week days:
    df_RI_tab['Weekday']=df_RI_tab['Date'].dt.weekday
    df_RI_tab['Weekday'] = df_RI_tab['Weekday'].apply(lambda x: calendar.day_name[x])
    df_RI_tab['J-Date']=df_RI_tab['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_RI_tab.set_index(df_RI_tab['J-Date'],inplace = True)
    df_RI_tab = df_RI_tab.set_index('J-Date')
    # rearrange columns:
    df_RI_tab=df_RI_tab[['Date','Weekday','No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R','Vol_Sell_I',
                         'Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I','Ticker','Name','Market']]
    cols = ['No_Buy_R','No_Buy_I','No_Sell_R','No_Sell_I','Vol_Buy_R','Vol_Buy_I','Vol_Sell_R','Vol_Sell_I','Val_Buy_R','Val_Buy_I','Val_Sell_R','Val_Sell_I']
    df_RI_tab[cols] = df_RI_tab[cols].apply(pd.to_numeric, axis=1)
    if(not show_weekday):
        df_RI_tab.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_RI_tab.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_RI_tab = df_RI_tab[start_date:end_date]
    return df_RI_tab

################################################################################################################################################################################
################################################################################################################################################################################
