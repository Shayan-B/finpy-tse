from .util import __Check_JDate_Validity__, __Get_TSE_Sector_WebID__, headers
import jdatetime
import requests
import pandas as pd
import calendar


def Get_CWI_History(start_date = '1395-01-01', end_date='1400-12-29', ignore_date = False, just_adj_close = False, show_weekday = False, double_date = False):
    """
    دریافت سابقه شاخص کل در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص کل بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    """
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
    sector_web_id = 32097828799138957
    # get only close chart data for sector index:
    r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{sector_web_id}', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.json()['indexB2'])[['dEven','xNivInuClMresIbs']]
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: str(x))
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: x[:4]+'-'+x[4:6]+'-'+x[-2:])
    df_sector_cl['dEven'] = pd.to_datetime(df_sector_cl['dEven'])
    df_sector_cl.rename(columns={"dEven": "Date", "xNivInuClMresIbs":"Adj Close"}, inplace=True)
    df_sector_cl['J-Date']=df_sector_cl['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl

################################################################################################################################################################################
################################################################################################################################################################################

def Get_EWI_History(start_date = '1395-01-01', end_date='1400-12-29', ignore_date = False, just_adj_close = True, show_weekday = False, double_date = False):
    """
    دریافت سابقه شاخص هم وزن در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص هم وزن بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    """
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
    sector_web_id = 67130298613737946
    # get only close chart data for sector index:
    r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{sector_web_id}', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.json()['indexB2'])[['dEven','xNivInuClMresIbs']]
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: str(x))
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: x[:4]+'-'+x[4:6]+'-'+x[-2:])
    df_sector_cl['dEven'] = pd.to_datetime(df_sector_cl['dEven'])
    df_sector_cl.rename(columns={"dEven": "Date", "xNivInuClMresIbs":"Adj Close"}, inplace=True)
    df_sector_cl['J-Date']=df_sector_cl['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl

################################################################################################################################################################################
################################################################################################################################################################################


def Get_SectorIndex_History(sector = 'خودرو', start_date='1395-01-01', end_date='1400-12-29', ignore_date = False, \
                            just_adj_close = False, show_weekday = False, double_date = False):
    """
    دریافت سابقه شاخص گروه صنعت مد نظر در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص گروه صنعت مد نظر بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    قابلیت دریافت فقط مقدار پایانی روز برای شاخص گروه صنعت مد نظر
    """
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
    # get sector web-id ---------------------------------------------------------------------------------------------------------------------
    try:
        sector_web_id = __Get_TSE_Sector_WebID__(sector_name = sector)
    except:
        print('Please Enter a Valid Name for Sector Index!')
        return
    if(sector_web_id == None):
        return
    r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{sector_web_id}', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.json()['indexB2'])[['dEven','xNivInuClMresIbs']]
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: str(x))
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: x[:4]+'-'+x[4:6]+'-'+x[-2:])
    df_sector_cl['dEven'] = pd.to_datetime(df_sector_cl['dEven'])
    df_sector_cl.rename(columns={"dEven": "Date", "xNivInuClMresIbs":"Adj Close"}, inplace=True)
    df_sector_cl['J-Date']=df_sector_cl['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl


################################################################################################################################################################################
################################################################################################################################################################################

def Get_CWPI_History(start_date='1395-01-01', end_date='1400-12-29', ignore_date = False, just_adj_close = False, show_weekday = False, double_date = False):
    """
    CWPI: Cap-Weighted Price Index = TEPIX = شاخص قیمت (وزنی-ارزشی)
    دریافت سابقه شاخص قیمت در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص قیمت بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    قابلیت دریافت فقط مقدار پایانی روز برای شاخص قیمت
    """
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
    # get sector web-id ---------------------------------------------------------------------------------------------------------------------
    sector_web_id = 5798407779416661
    r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{sector_web_id}', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.json()['indexB2'])[['dEven','xNivInuClMresIbs']]
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: str(x))
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: x[:4]+'-'+x[4:6]+'-'+x[-2:])
    df_sector_cl['dEven'] = pd.to_datetime(df_sector_cl['dEven'])
    df_sector_cl.rename(columns={"dEven": "Date", "xNivInuClMresIbs":"Adj Close"}, inplace=True)
    df_sector_cl['J-Date']=df_sector_cl['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl

################################################################################################################################################################################
################################################################################################################################################################################

def Get_EWPI_History(start_date='1395-01-01', end_date='1400-12-29', ignore_date = False, just_adj_close = False, show_weekday = False, double_date = False):
    """
    EWPI: Equal-Weighted Price Index = شاخص قیمت (هم وزن)
    دریافت سابقه شاخص قیمت هم وزن در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص قیمت هم وزن بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    قابلیت دریافت فقط مقدار پایانی روز برای شاخص قیمت هم وزن
    """
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
    # get sector web-id ---------------------------------------------------------------------------------------------------------------------
    sector_web_id = 8384385859414435
    # get only close chart data for sector index:
    r_cl = requests.get(f'http://tsetmc.com/tsev2/chart/data/Index.aspx?i={sector_web_id}&t=value', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.text.split(';'))
    columns=['J-Date','Adj Close']
    df_sector_cl[columns] = df_sector_cl[0].str.split(",",expand=True)
    df_sector_cl.drop(columns=[0],inplace=True)
    df_sector_cl['J-Date'] = df_sector_cl['J-Date'].apply(lambda x: str(jdatetime.date(int(x.split('/')[0]),int(x.split('/')[1]),int(x.split('/')[2]))))
    df_sector_cl['Date'] = df_sector_cl['J-Date'].apply(lambda x: jdatetime.date(int(x[:4]),int(x[5:7]),int(x[8:])).togregorian())  
    df_sector_cl['Date'] = pd.to_datetime(df_sector_cl['Date'])
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://www.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl

################################################################################################################################################################################
################################################################################################################################################################################

def Get_FFI_History(start_date='1395-01-01', end_date='1400-12-29', ignore_date = False, just_adj_close = False, show_weekday = False, double_date = False):
    """
    FFI: Free-Float Index = شاخص شناور آزاد
    دریافت سابقه شاخص شناور آزاد در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص شناور آزاد بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    قابلیت دریافت فقط مقدار پایانی روز برای شاخص شناور آزاد
    """
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
    # get sector web-id ---------------------------------------------------------------------------------------------------------------------
    sector_web_id = 49579049405614711
    r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{sector_web_id}', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.json()['indexB2'])[['dEven','xNivInuClMresIbs']]
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: str(x))
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: x[:4]+'-'+x[4:6]+'-'+x[-2:])
    df_sector_cl['dEven'] = pd.to_datetime(df_sector_cl['dEven'])
    df_sector_cl.rename(columns={"dEven": "Date", "xNivInuClMresIbs":"Adj Close"}, inplace=True)
    df_sector_cl['J-Date']=df_sector_cl['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl

################################################################################################################################################################################
################################################################################################################################################################################

def Get_MKT1I_History(start_date='1395-01-01', end_date='1400-12-29', ignore_date = False, just_adj_close = False, show_weekday = False, double_date = False):
    """
    MKT1I: First Market Index = شاخص بازار اول
    دریافت سابقه شاخص بازار اول بورس در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص بازار اول بورس بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    قابلیت دریافت فقط مقدار پایانی روز برای شاخص بازار اول بورس
    """
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
    # get sector web-id ---------------------------------------------------------------------------------------------------------------------
    sector_web_id = 62752761908615603
    r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{sector_web_id}', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.json()['indexB2'])[['dEven','xNivInuClMresIbs']]
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: str(x))
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: x[:4]+'-'+x[4:6]+'-'+x[-2:])
    df_sector_cl['dEven'] = pd.to_datetime(df_sector_cl['dEven'])
    df_sector_cl.rename(columns={"dEven": "Date", "xNivInuClMresIbs":"Adj Close"}, inplace=True)
    df_sector_cl['J-Date']=df_sector_cl['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl

################################################################################################################################################################################
################################################################################################################################################################################

def Get_MKT2I_History(start_date='1395-01-01', end_date='1400-12-29', ignore_date = False, just_adj_close = False, show_weekday = False, double_date = False):
    """
    MKT2I: Second Market Index = شاخص بازار دوم
    دریافت سابقه شاخص بازار دوم بورس در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص بازار دوم بورس بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    قابلیت دریافت فقط مقدار پایانی روز برای شاخص بازار دوم بورس
    """
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
    # get sector web-id ---------------------------------------------------------------------------------------------------------------------
    sector_web_id = 71704845530629737
    # get only close chart data for sector index:
    r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{sector_web_id}', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.json()['indexB2'])[['dEven','xNivInuClMresIbs']]
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: str(x))
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: x[:4]+'-'+x[4:6]+'-'+x[-2:])
    df_sector_cl['dEven'] = pd.to_datetime(df_sector_cl['dEven'])
    df_sector_cl.rename(columns={"dEven": "Date", "xNivInuClMresIbs":"Adj Close"}, inplace=True)
    df_sector_cl['J-Date']=df_sector_cl['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl

################################################################################################################################################################################
################################################################################################################################################################################

def Get_INDI_History(start_date='1395-01-01', end_date='1400-12-29', ignore_date = False, just_adj_close = False, show_weekday = False, double_date = False):
    """
    INDI: Industry Index = شاخص صنعت
    دریافت سابقه شاخص صنعت بورس در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص صنعت بورس بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    قابلیت دریافت فقط مقدار پایانی روز برای شاخص صنعت بورس
    """
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
    # get sector web-id ---------------------------------------------------------------------------------------------------------------------
    sector_web_id = 43754960038275285
    # get only close chart data for sector index:
    r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{sector_web_id}', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.json()['indexB2'])[['dEven','xNivInuClMresIbs']]
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: str(x))
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: x[:4]+'-'+x[4:6]+'-'+x[-2:])
    df_sector_cl['dEven'] = pd.to_datetime(df_sector_cl['dEven'])
    df_sector_cl.rename(columns={"dEven": "Date", "xNivInuClMresIbs":"Adj Close"}, inplace=True)
    df_sector_cl['J-Date']=df_sector_cl['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl

################################################################################################################################################################################
################################################################################################################################################################################

def Get_LCI30_History(start_date='1395-01-01', end_date='1400-12-29', ignore_date = False, just_adj_close = False, show_weekday = False, double_date = False):
    """
    30LCI: 30 Large-Cap Index = شاخص 30 شرکت بزرگ بورس
    دریافت سابقه شاخص 30 شرکت بزرگ بورس در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص 30 شرکت بزرگ بورس بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    قابلیت دریافت فقط مقدار پایانی روز برای شاخص 30 شرکت بزرگ بورس
    """
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
    # get sector web-id ---------------------------------------------------------------------------------------------------------------------
    sector_web_id = 10523825119011581
    # get only close chart data for sector index:
    r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{sector_web_id}', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.json()['indexB2'])[['dEven','xNivInuClMresIbs']]
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: str(x))
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: x[:4]+'-'+x[4:6]+'-'+x[-2:])
    df_sector_cl['dEven'] = pd.to_datetime(df_sector_cl['dEven'])
    df_sector_cl.rename(columns={"dEven": "Date", "xNivInuClMresIbs":"Adj Close"}, inplace=True)
    df_sector_cl['J-Date']=df_sector_cl['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl

################################################################################################################################################################################
################################################################################################################################################################################

def Get_ACT50_History(start_date='1395-01-01', end_date='1400-12-29', ignore_date = False, just_adj_close = False, show_weekday = False, double_date = False):
    """
    ACT50: 50 Most Active Stocks Index = شاخص 50 شرکت فعال بورس
    دریافت سابقه شاخص 50 شرکت فعال بورس در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه شاخص 50 شرکت فعال بورس بدون توجه به تاریخ شروع و پایان
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    قابلیت دریافت فقط مقدار پایانی روز برای شاخص 50 شرکت فعال بورس
    """
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
    # get sector web-id ---------------------------------------------------------------------------------------------------------------------
    sector_web_id = 46342955726788357
    # get only close chart data for sector index:
    r_cl = requests.get(f'http://cdn.tsetmc.com/api/Index/GetIndexB2History/{sector_web_id}', headers=headers)
    df_sector_cl = pd.DataFrame(r_cl.json()['indexB2'])[['dEven','xNivInuClMresIbs']]
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: str(x))
    df_sector_cl['dEven'] = df_sector_cl['dEven'].apply(lambda x: x[:4]+'-'+x[4:6]+'-'+x[-2:])
    df_sector_cl['dEven'] = pd.to_datetime(df_sector_cl['dEven'])
    df_sector_cl.rename(columns={"dEven": "Date", "xNivInuClMresIbs":"Adj Close"}, inplace=True)
    df_sector_cl['J-Date']=df_sector_cl['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
    df_sector_cl['Weekday']=df_sector_cl['Date'].dt.weekday
    df_sector_cl['Weekday'] = df_sector_cl['Weekday'].apply(lambda x: calendar.day_name[x])
    df_sector_cl = df_sector_cl.set_index('J-Date')
    df_sector_cl = df_sector_cl[['Date','Weekday','Adj Close']]
    df_sector_cl['Adj Close'] = pd.to_numeric(df_sector_cl['Adj Close'])
    if(not just_adj_close):
        r = requests.get(f'http://old.tsetmc.com/tsev2/chart/data/IndexFinancial.aspx?i={sector_web_id}&t=ph', headers=headers)
        df_sector = pd.DataFrame(r.text.split(';'))
        columns=['Date','High','Low','Open','Close','Volume','D']
        # split data into defined columns
        df_sector[columns] = df_sector[0].str.split(",",expand=True)
        df_sector.drop(columns=[0,'D'],inplace=True)
        df_sector['Date']=pd.to_datetime(df_sector['Date'])
        df_sector['J-Date']=df_sector['Date'].apply(lambda x: str(jdatetime.date.fromgregorian(date=x.date())))
        df_sector = df_sector.set_index('J-Date')
        df_sector.drop(columns=['Date'],inplace=True)
        # now concat:
        df_sector_cl = pd.concat([df_sector,df_sector_cl],axis=1).dropna()
        df_sector_cl = df_sector_cl[['Date','Weekday','Open','High','Low','Close','Adj Close','Volume']]
        cols = ['Open','High','Low','Close','Adj Close','Volume']
        df_sector_cl[cols] = df_sector_cl[cols].apply(pd.to_numeric, axis=1)
        df_sector_cl['Volume'] = df_sector_cl['Volume'].astype('int64')
    if(not show_weekday):
        df_sector_cl.drop(columns=['Weekday'],inplace=True)
    if(not double_date):
        df_sector_cl.drop(columns=['Date'],inplace=True)
    # slice requested time window:
    if(not ignore_date):
        df_sector_cl = df_sector_cl[start_date:end_date]    
    return df_sector_cl

################################################################################################################################################################################
################################################################################################################################################################################
