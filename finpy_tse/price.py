import pandas as pd
import requests
import jdatetime
from IPython.display import clear_output
import urllib3

urllib3.disable_warnings()
from bs4 import BeautifulSoup
import time
import aiohttp
import asyncio
from unsync import unsync
import tracemalloc
from persiantools import characters
import calendar

from .util import (
    headers,
    __Check_JDate_Validity__,
    get_tse_webid,
    __Get_TSE_WebID__,
    URL,
)


class purl(URL):
    def __init__(self, asset_type=None, **kwargs):
        super().__init__(asset_type, **kwargs)

    def make_url(self):
        if self.atype == "up_lo_limit":
            ticker_no = self.kwargs["ticker_no"]
            date = self.kwargs["date"]
            self.url = f"http://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{ticker_no}/{date}"
        elif self.atype == "lob":
            ticker_no = self.kwargs["ticker_no"]
            date = self.kwargs["date"]
            self.url = f"http://cdn.tsetmc.com/api/BestLimits/{ticker_no}/{date}"
        elif self.atype == "up_lo_thresh":
            ticker_no = self.kwargs["ticker_no"]
            date = self.kwargs["date"]
            self.url = f"http://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{ticker_no}/{date}"
        elif self.atype == "search_G":
            sector_name = self.kwargs["sector_name"]
            self.url = (
                f"https://www.google.com/search?q={sector_name} tsetmc اطلاعات شاخص"
            )
        elif self.atype == "search_TSE":
            srch_key = self.kwargs["srch_key"]
            self.url = (
                f"http://cdn.tsetmc.com/api/Instrument/GetInstrumentSearch/{srch_key}"
            )
        elif not self.atype:
            self.url = None

        return


class Ticker:
    def __init__(self, stock="شپنا"):
        if type(stock) == str:
            self.ticker = stock
            self.webid = self.__GET_TSE_WEBID__()
        elif type(stock) == list:
            self.ticker = stock
        url = purl()
        self.price_data = None
        self.ri_hist = None

    def get_price_hist(self):
        return

    def get_ri_history(self):
        return

    def get_60D(self):
        return

    def __GET_TSE_WEBID__(self):
        return


def __get_history_data_group_parallel__(stock_list):
    # a function for finding web-id of symbols or tickers -------------------------------------------
    def find_code(stock_list):
        if type(stock_list) != list:
            return False
        tracemalloc.start()

        @unsync
        # تابعی برای سرچ کردن لیستی از نام ها و برگرداندن لیستی از دیتا فریم های تمیزشده
        async def parallel_request(stock_list):
            async def get_data(session, stock):
                url = f"http://old.tsetmc.com/tsev2/data/search.aspx?skey={stock}"
                # ارسال درخواست
                async with session.get(url, headers=headers) as response:
                    data_id = await response.text()

                    # تبدیل به لیست کردن دیتای مورد نیاز
                    data = []
                    for i in data_id.split(";"):
                        try:
                            i = i.split(",")
                            data.append([i[0], i[1], i[2], i[7]])
                        except:
                            pass

                    # تمیزکردن دیتا
                    data = pd.DataFrame(
                        data, columns=["Ticker", "Name", "WEB-ID", "Active"]
                    )
                    data["Name"] = data["Name"].apply(
                        lambda x: "".join(x.split("\u200c")).strip()
                    )
                    data["Ticker"] = data["Ticker"].apply(
                        lambda x: "".join(x.split("\u200c")).strip()
                    )
                    data["Name-Split"] = data["Name"].apply(
                        lambda x: "".join(x.split()).strip()
                    )
                    data["Symbol-Split"] = data["Ticker"].apply(
                        lambda x: "".join(x.split()).strip()
                    )
                    data["Active"] = pd.to_numeric(data["Active"])
                    data = data.sort_values("Ticker")
                    data = pd.DataFrame(
                        data[["Name", "WEB-ID", "Name-Split", "Symbol-Split"]].values,
                        columns=["Name", "WEB-ID", "Name-Split", "Symbol-Split"],
                        index=pd.MultiIndex.from_frame(data[["Ticker", "Active"]]),
                    )

                    return data

            # مدیریت درخواست ها
            async with aiohttp.ClientSession() as session:
                tasks = []
                for stock in stock_list:
                    # فرستادن دیتای مورد نیاز برای ارسال درخواست به تابع بالا
                    task = asyncio.ensure_future(get_data(session, stock))
                    # اضافه کردن دیتافریم ها به لیست
                    tasks.append(task)
                view_counts = await asyncio.gather(*tasks)

            return view_counts

        def request(stock_list):
            view_counts = []
            for stock in stock_list:
                while True:
                    try:
                        data_id = requests.get(
                            f"http://old.tsetmc.com/tsev2/data/search.aspx?skey={stock}",
                            headers=headers,
                        ).text
                        break
                    except:
                        print("nn")
                        pass

                data = []
                for i in data_id.split(";"):
                    try:
                        i = i.split(",")
                        data.append([i[0], i[1], i[2], i[7]])
                    except:
                        pass

                # تمیزکردن دیتا
                data = pd.DataFrame(
                    data, columns=["Ticker", "Name", "WEB-ID", "Active"]
                )
                data["Name"] = data["Name"].apply(
                    lambda x: "".join(x.split("\u200c")).strip()
                )
                data["Ticker"] = data["Ticker"].apply(
                    lambda x: "".join(x.split("\u200c")).strip()
                )
                data["Name-Split"] = data["Name"].apply(
                    lambda x: "".join(x.split()).strip()
                )
                data["Symbol-Split"] = data["Ticker"].apply(
                    lambda x: "".join(x.split()).strip()
                )
                data["Active"] = pd.to_numeric(data["Active"])
                data = data.sort_values("Ticker")
                data = pd.DataFrame(
                    data[["Name", "WEB-ID", "Name-Split", "Symbol-Split"]].values,
                    columns=["Name", "WEB-ID", "Name-Split", "Symbol-Split"],
                    index=pd.MultiIndex.from_frame(data[["Ticker", "Active"]]),
                )

                view_counts.append(data)

            return view_counts

        # --------------------------------------------------------------------------------------------------------------------------------------
        # cleaning entry list

        # تمیزکردن لیست سهام واردشده
        list_first_name, stock_list_split = [], []
        for stock in stock_list:
            stock = characters.fa_to_ar("".join(stock.split("\u200c")).strip())
            list_first_name.append(stock.split()[0])
            stock_list_split.append("".join(stock.split()))

        # TSE گرفتن نتایج سرچ در
        while True:
            try:
                data = parallel_request(list_first_name).result()
                break
            except:
                print("n")
                pass

        # جداکردن نام های سهام مورد نیاز از نتایج سرچ
        df_symbols, df_names = pd.DataFrame(), pd.DataFrame()
        for i in list(zip(data, stock_list_split)):
            df_symbol = i[0][i[0]["Symbol-Split"] == i[1]]
            df_name = i[0][i[0]["Name-Split"] == i[1]]
            df_symbols = pd.concat([df_symbols, df_symbol])
            df_names = pd.concat([df_names, df_name])

        list_first_name_not, stock_list_split_not = [], []
        for i in range(len(data)):
            if len(data[i]) == 0:
                list_first_name_not.append(list_first_name[i])
                stock_list_split_not.append(stock_list_split[i])

        data = request(list_first_name_not)
        for i in list(zip(data, stock_list_split_not)):
            df_symbol = i[0][i[0]["Symbol-Split"] == i[1]]
            df_name = i[0][i[0]["Name-Split"] == i[1]]
            df_symbols = pd.concat([df_symbols, df_symbol])
            df_names = pd.concat([df_names, df_name])

        if len(df_names) > 0:
            # جداکردن لیست نمادهایی که نام آنها پیدا شده
            stock_list = [
                characters.fa_to_ar("".join(i.split("\u200c")).strip())
                for i in df_names.index[
                    ~df_names.index.get_level_values("Ticker").duplicated()
                ].get_level_values("Ticker")
            ]

            # TSE گرفتن نتایج سرچ در
            while True:
                try:
                    data = parallel_request(stock_list).result()
                    break
                except:
                    print("n")
                    pass

            # جداکردن نام های سهام مورد نیاز از نتایج سرچ
            for i in list(zip(data, stock_list)):
                df_symbol = i[0][i[0].index.get_level_values("Ticker") == i[1]]
                df_symbols = pd.concat([df_symbols, df_symbol])

            stock_list_not = []
            for i in range(len(data)):
                if len(data[i]) == 0:
                    stock_list_not.append(stock_list[i])

            data = request(stock_list_not)
            for i in list(zip(data, stock_list_not)):
                df_symbol = i[0][i[0].index.get_level_values("Ticker") == i[1]]
                df_symbols = pd.concat([df_symbols, df_symbol])

        if len(df_symbols) == 0:
            return False

        return df_symbols.drop(["Name-Split", "Symbol-Split"], axis=1)

    def get_price(dict_code):
        tracemalloc.start()

        @unsync
        # تابعی برای سرچ کردن لیستی از نام ها و برگرداندن لیستی از دیتا فریم های تمیزشده
        async def parallel_request(list_code):
            async def get_data(session, code):
                url = f"http://old.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={code}&Top=999999&A=0"
                # ارسال درخواست
                async with session.get(url, headers=headers) as response:
                    data_id = await response.text()
                    return [data_id, response.status]

            # مدیریت درخواست ها
            async with aiohttp.ClientSession() as session:
                tasks = []
                for code in list_code:
                    # فرستادن دیتای مورد نیاز برای ارسال درخواست به تابع بالا
                    task = asyncio.ensure_future(get_data(session, code))
                    # اضافه کردن دیتافریم ها به لیست
                    tasks.append(task)
                view_counts = await asyncio.gather(*tasks)

            return view_counts

        data = {}
        while True:
            while True:
                try:
                    res = parallel_request(list(dict_code.values())).result()
                    break
                except:
                    pass

            for i in list(zip(res, dict_code.keys())):
                try:
                    if i[0][1] == 200:
                        data[i[1]] = i[0][0]
                        del dict_code[i[1]]
                except:
                    pass

            if len(dict_code) == 0:
                break

        data = dict(sorted(data.items()))

        return data

    df_total = find_code(stock_list)
    # print('get codes')

    df_total["price"] = ""
    dict_code = {}
    for i in range(len(df_total)):
        dict_code[i] = df_total.iloc[i, 1]

    data = get_price(dict_code)

    for index in data.keys():
        df_total.iloc[index, 2] = data[index]

    return df_total


# a function to get price data from a given page ----------------------------------------------------------------------------------
def __process_price_data__(ticker_no, ticker, r, data_part):
    df_history = pd.DataFrame(r.split(";"))
    columns = [
        "Date",
        "High",
        "Low",
        "Final",
        "Close",
        "Open",
        "Y-Final",
        "Value",
        "Volume",
        "No",
    ]
    # split data into defined columns
    try:
        df_history[columns] = df_history[0].str.split("@", expand=True)
    except ValueError:
        # print(ticker, "Not Found")
        read_stat = 0
        return ticker, read_stat
    # drop old column 0
    read_stat = 1
    df_history.drop(columns=[0], inplace=True)
    df_history.dropna(inplace=True)
    df_history["Date"] = pd.to_datetime(df_history["Date"])
    df_history["Ticker"] = ticker
    df_history["Part"] = data_part
    df_history = df_history.set_index("Date")
    return df_history, read_stat


# ----------------------------------------------------------------------------------------------------------------------------------
# process the data: responses might be duplicate
def __build_price_panel_seg__(
    df_response, param, save_excel=True, save_path="D:/FinPy-TSE Data/Price Panel/"
):
    # remove empty responses:
    df_response = df_response[df_response["price"] != ""]
    # drop duplicate indexes (repetitive indexes)
    df_response = pd.concat(
        [
            df_response[~df_response["WEB-ID"].duplicated(keep=False)],
            df_response[df_response["WEB-ID"].duplicated(keep="first")],
        ],
        axis=0,
    )
    # convert ticker from Arabic to Farsi:
    df_response = df_response.reset_index()
    df_response["Ticker"] = df_response["Ticker"].apply(
        lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
    )
    df_response = df_response.set_index(["Ticker", "Active"])
    df_response
    # now loop over and process the data ----------------------------------------------------------------------------------------------
    lost_tickers = []
    for ticker, ticker_no_df in df_response.groupby(level=0):
        # create an empty dataframe:
        # print("Processing ", ticker)
        df_history = pd.DataFrame(
            {},
            columns=[
                "Date",
                "High",
                "Low",
                "Final",
                "Close",
                "Open",
                "Y-Final",
                "Value",
                "Volume",
                "No",
                "Ticker",
                "Part",
            ],
        ).set_index("Date")
        # loop to get data from different pages of a ticker:
        for index, row in (ticker_no_df.reset_index()).iterrows():
            df_temp, read_stat = __process_price_data__(
                ticker_no=row["WEB-ID"],
                ticker=row["Ticker"],
                r=row["price"],
                data_part=index + 1,
            )
            if read_stat == 0:
                lost_tickers.append(df_temp)
                break
            else:
                df_history = pd.concat([df_history, df_temp])
        # sort index and reverse the order for more processes:
        if read_stat == 0:
            continue
        # print(df_history.info())
        df_history = df_history.sort_index(ascending=True)
        df_history = df_history.reset_index()
        # determining week days:
        # try:
        df_history["Weekday"] = df_history["Date"].dt.weekday
        # except AttributeError:
        #     print(df_history["Date"], df_history["Date"].info())
        df_history["Weekday"] = df_history["Weekday"].apply(
            lambda x: calendar.day_name[x]
        )
        df_history["J-Date"] = df_history["Date"].apply(
            lambda x: str(jdatetime.date.fromgregorian(date=x.date()))
        )
        df_history = df_history.set_index("J-Date")
        # rearrange columns:
        df_history = df_history[
            [
                "Date",
                "Weekday",
                "Y-Final",
                "Open",
                "High",
                "Low",
                "Close",
                "Final",
                "Volume",
                "Value",
                "No",
                "Ticker",
                "Part",
            ]
        ]
        cols = [
            "Y-Final",
            "Open",
            "High",
            "Low",
            "Close",
            "Final",
            "Volume",
            "No",
            "Value",
            "Part",
        ]
        df_history[cols] = df_history[cols].apply(pd.to_numeric, axis=1)
        # Y-Final for new part of data could be 0 or 1000, we need to replace them with yesterday's final price:
        df_history["Final(+1)"] = df_history["Final"].shift(
            +1
        )  # final prices shifted forward by one day
        df_history["Part(+1)"] = df_history["Part"].shift(
            +1
        )  # market shifted forward by one day
        df_history["temp"] = df_history.apply(
            lambda x: x["Y-Final"]
            if ((x["Y-Final"] != 0) and (x["Y-Final"] != 1000))
            else (
                x["Y-Final"]
                if ((x["Part(+1)"] == x["Part"]) or (pd.isnull(x["Final(+1)"])))
                else x["Final(+1)"]
            ),
            axis=1,
        )
        df_history["Y-Final"] = df_history["temp"]
        df_history.drop(columns=["Final(+1)", "temp", "Part(+1)"], inplace=True)

        for col in cols:
            df_history[col] = df_history[col].apply(
                lambda x: int(x)
            )  # convert to int because we do not have less than Rial
        # Adjust price data:
        df_history["COEF"] = (
            df_history["Y-Final"].shift(-1) / df_history["Final"]
        ).fillna(1.0)
        df_history["ADJ-COEF"] = df_history.iloc[::-1]["COEF"].cumprod().iloc[::-1]
        df_history["Adj Open"] = (df_history["Open"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj High"] = (df_history["High"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj Low"] = (df_history["Low"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj Close"] = (df_history["Close"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj Final"] = (df_history["Final"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history.drop(columns=["COEF", "ADJ-COEF"], inplace=True)
        # re-arrange again:
        df_history = df_history[
            [
                "Date",
                "Weekday",
                "Open",
                "High",
                "Low",
                "Close",
                "Final",
                "Volume",
                "Value",
                "No",
                "Adj Open",
                "Adj High",
                "Adj Close",
                "Adj Final",
                "Ticker",
                "Part",
            ]
        ]
        # ----------------------------------------------------------------------------------------------------------------------------------------------
        # save data in a given directory:
        if save_excel:
            try:
                df_history.to_excel(save_path + row["Ticker"].strip() + ".xlsx")
            except:
                pass
        # separate required column for price panel: Adj Final
        df_panel_temp = df_history.reset_index().set_index("Date")
        df_panel_temp = df_panel_temp[[param]]
        df_panel_temp.columns = [row["Ticker"].strip()]
        try:
            df_panel = pd.concat([df_panel, df_panel_temp], axis=1)
        except:  # for first time
            df_panel = df_panel_temp.copy()
    if len(lost_tickers) > 0:
        print("lost tickers are:", lost_tickers)
    return df_panel, lost_tickers


################################################################################################################################################################################
################################################################################################################################################################################


def get_price_history(
    stock: str = "خودرو",
    start_date: str = "1400-01-01",
    end_date: str = "1401-01-01",
    ignore_date: bool = False,
    adjust_price: bool = False,
    show_weekday: bool = False,
    double_date: bool = False,
) -> pd.DataFrame:
    """
    Takes ticker or firm's full name and returns a Pandas dataframe that contains the following columns:

    J-Date: Jalali date, as index
    Date: Gregorian date
    Weekday: Name of weekdays
    Open: Opening price of day
    High: Maximum price of day
    Low: Minimum price of day
    Close: Closing price of day (آخرین قیمت)
    Final: Weighted closing price of day (قیمت پایانی)
    Volume: Traded volume of day
    Value: Traded value of day in IRAN's Rial
    No: Number of trades in a day
    Ticker: Ticker/Symbol
    Name: Firm's full name
    Market: Market, the stock is traded in
    Adj Open: Adjusted opening price of day for stock-splits and dividends
    Adj High: Adjusted maximum price of day for stock-splits and dividends
    Adj Low: Adjusted minimum price of day for stock-splits and dividends
    Adj Close: Adjusted closing price of day for stock-splits and dividends
    Adj Final: Adjusted weighted closing price of day for stock-splits and dividends

     * All data are taken from the new website of Tehran Stock Exchange.

    :param stock: (str) Ticker or firm's full name.
    :param start_date: (str) Jalali date for starting day of historical price data in YYYY-MM-DD format.
    :param end_date: (str) Jalali date for ending day of historical price data in YYYY-MM-DD format.
    :param ignore_date: (bool) Ignores start_date and end_date and returns all available price history, if set to True.
    :param adjust_price: (bool) Adjusts price for stock-splits and dividends, if set to True.
    :param show_weekday: (bool) Shows weekdays in the output, if set to True.
    :param double_date: (bool) Shows Gregorian date in the output, if set to True.
    :return: (pd.DataFrame) A dataframe that contains J-Date as index and Date, Weekday, Open, High, Low, Close, Final, Volume, Value, No, Ticker, Name, Market, Adj Open, Adj High, Adj Low, Adj Close, Adj Final.
    """

    # basic request and data cleaning function for historical price data of a ticker for a given market
    def get_price_data(ticker_no, ticker, name, market):
        r = requests.get(
            f"http://cdn.tsetmc.com/api/ClosingPrice/GetClosingPriceDailyList/{ticker_no}/0",
            headers=headers,
        )
        df_history = pd.DataFrame(r.json()["closingPriceDaily"])
        columns = [
            "Date",
            "High",
            "Low",
            "Final",
            "Close",
            "Open",
            "Y-Final",
            "Value",
            "Volume",
            "No",
        ]
        df_history = df_history[
            [
                "dEven",
                "priceMax",
                "priceMin",
                "pClosing",
                "pDrCotVal",
                "priceFirst",
                "priceYesterday",
                "qTotCap",
                "qTotTran5J",
                "zTotTran",
            ]
        ]
        df_history.columns = [
            "Date",
            "High",
            "Low",
            "Final",
            "Close",
            "Open",
            "Y-Final",
            "Value",
            "Volume",
            "No",
        ]
        df_history["Date"] = df_history["Date"].apply(lambda x: str(x))
        df_history["Date"] = df_history["Date"].apply(
            lambda x: f"{x[:4]}-{x[4:6]}-{x[-2:]}"
        )
        df_history["Date"] = pd.to_datetime(df_history["Date"])
        df_history = df_history[df_history["No"] != 0]
        df_history["Ticker"] = ticker
        df_history["Name"] = name
        df_history["Market"] = market
        df_history = df_history.set_index("Date")
        return df_history

    # check to see if the entry start and end dates are valid or not
    if not ignore_date:
        start_date = __Check_JDate_Validity__(start_date, key_word="'START'")
        if start_date == None:
            return
        end_date = __Check_JDate_Validity__(end_date, key_word="'END'")
        if end_date == None:
            return
        start = jdatetime.date(
            year=int(start_date.split("-")[0]),
            month=int(start_date.split("-")[1]),
            day=int(start_date.split("-")[2]),
        )
        end = jdatetime.date(
            year=int(end_date.split("-")[0]),
            month=int(end_date.split("-")[1]),
            day=int(end_date.split("-")[2]),
        )
        if start > end:
            print("Start date must be a day before end date!")
            return

    # search for WebIDs
    ticker_no_df = get_tse_webid(stock)
    if type(ticker_no_df) == bool:
        return

    # create an empty dataframe:
    df_history = pd.DataFrame(
        {},
        columns=[
            "Date",
            "High",
            "Low",
            "Final",
            "Close",
            "Open",
            "Y-Final",
            "Value",
            "Volume",
            "No",
            "Ticker",
            "Name",
            "Market",
        ],
    ).set_index("Date")

    # loop to get data from different pages of a ticker:
    for index, row in (ticker_no_df.reset_index()).iterrows():
        try:
            df_temp = get_price_data(
                ticker_no=row["WebID"],
                ticker=row["Ticker"],
                name=row["Name"],
                market=row["Market"],
            )
            df_history = pd.concat([df_history, df_temp])
        except:
            pass

    # sort based on dated index:
    df_history = df_history.sort_index(ascending=True)
    df_history = df_history.reset_index()

    # add weekdays and j-date columns:
    df_history["Weekday"] = df_history["Date"].dt.weekday
    df_history["Weekday"] = df_history["Weekday"].apply(lambda x: calendar.day_name[x])
    df_history["J-Date"] = df_history["Date"].apply(
        lambda x: str(jdatetime.date.fromgregorian(date=x.date()))
    )
    df_history = df_history.set_index("J-Date")

    # rearrange columns and convert some columns to numeric
    df_history = df_history[
        [
            "Date",
            "Weekday",
            "Y-Final",
            "Open",
            "High",
            "Low",
            "Close",
            "Final",
            "Volume",
            "Value",
            "No",
            "Ticker",
            "Name",
            "Market",
        ]
    ]
    cols = ["Y-Final", "Open", "High", "Low", "Close", "Final", "Volume", "No", "Value"]
    df_history[cols] = df_history[cols].apply(pd.to_numeric, axis=1)

    # find stock moves between markets and adjust for nominal price in the new market, if necessary
    df_history["Final(+1)"] = df_history["Final"].shift(+1)
    df_history["Market(+1)"] = df_history["Market"].shift(+1)
    df_history["temp"] = df_history.apply(
        lambda x: x["Y-Final"]
        if ((x["Y-Final"] != 0) and (x["Y-Final"] != 1000))
        else (
            x["Y-Final"]
            if ((x["Market(+1)"] == x["Market"]) or (pd.isnull(x["Final(+1)"])))
            else x["Final(+1)"]
        ),
        axis=1,
    )
    df_history["Y-Final"] = df_history["temp"]
    df_history.drop(columns=["Final(+1)", "temp", "Market(+1)"], inplace=True)

    # convert numbers to int because we do not have less than Rial, just for clean outputs!
    for col in cols:
        df_history[col] = df_history[col].apply(lambda x: int(x))

    # Adjust price data, if requested:
    if adjust_price:
        df_history["COEF"] = (
            df_history["Y-Final"].shift(-1) / df_history["Final"]
        ).fillna(1.0)
        df_history["ADJ-COEF"] = df_history.iloc[::-1]["COEF"].cumprod().iloc[::-1]
        df_history["Adj Open"] = (df_history["Open"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj High"] = (df_history["High"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj Low"] = (df_history["Low"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj Close"] = (df_history["Close"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj Final"] = (df_history["Final"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history.drop(columns=["COEF", "ADJ-COEF"], inplace=True)

    # drop weekdays if not requested
    if not show_weekday:
        df_history.drop(columns=["Weekday"], inplace=True)

    # drop Gregorian date if not requested
    if not double_date:
        df_history.drop(columns=["Date"], inplace=True)

    # drop yesterday's final price!
    df_history.drop(columns=["Y-Final"], inplace=True)

    # slice requested time window, if requested:
    if not ignore_date:
        df_history = df_history[start_date:end_date]

    return df_history


def Get_Price_History(
    stock="خودرو",
    start_date="1400-01-01",
    end_date="1401-01-01",
    ignore_date=False,
    adjust_price=False,
    show_weekday=False,
    double_date=False,
):
    """
    دریافت سابقه قیمت یک سهم در روزهای معاملاتی بین تاریخ شروع و پایان
    قابلیت دریافت همه سابقه قیمت بدون توجه به تاریخ شروع و پایان
    قابلیت تعدیل قیمت برای سود نقدی و افزایش سرمایه با احتساب آورده
    قابلیت ارائه تاریخ میلادی علاوه بر تاریخ شمسی، قابلیت نمایش روزهای هفته
    """

    # a function to get price data from a given page ----------------------------------------------------------------------------------
    def get_price_data(ticker_no, ticker, name, data_part):
        r = requests.get(
            f"http://old.tsetmc.com/tsev2/data/InstTradeHistory.aspx?i={ticker_no}&Top=999999&A=0",
            headers=headers,
        )
        df_history = pd.DataFrame(r.text.split(";"))
        columns = [
            "Date",
            "High",
            "Low",
            "Final",
            "Close",
            "Open",
            "Y-Final",
            "Value",
            "Volume",
            "No",
        ]
        # split data into defined columns
        df_history[columns] = df_history[0].str.split("@", expand=True)
        # drop old column 0
        df_history.drop(columns=[0], inplace=True)
        df_history.dropna(inplace=True)
        df_history["Date"] = pd.to_datetime(df_history["Date"])
        df_history["Ticker"] = ticker
        df_history["Name"] = name
        df_history["Market"] = data_part
        df_history = df_history.set_index("Date")
        return df_history

    # ----------------------------------------------------------------------------------------------------------------------------------
    # check date validity
    if not ignore_date:
        start_date = __Check_JDate_Validity__(start_date, key_word="'START'")
        if start_date == None:
            return
        end_date = __Check_JDate_Validity__(end_date, key_word="'END'")
        if end_date == None:
            return
        start = jdatetime.date(
            year=int(start_date.split("-")[0]),
            month=int(start_date.split("-")[1]),
            day=int(start_date.split("-")[2]),
        )
        end = jdatetime.date(
            year=int(end_date.split("-")[0]),
            month=int(end_date.split("-")[1]),
            day=int(end_date.split("-")[2]),
        )
        if start > end:
            print("Start date must be a day before end date!")
            return
    # ---------------------------------------------------------------------------------------------------------------------------------------
    # find web-ids
    ticker_no_df = __Get_TSE_WebID__(stock)
    if type(ticker_no_df) == bool:
        return
    # create an empty dataframe:
    df_history = pd.DataFrame(
        {},
        columns=[
            "Date",
            "High",
            "Low",
            "Final",
            "Close",
            "Open",
            "Y-Final",
            "Value",
            "Volume",
            "No",
            "Ticker",
            "Name",
            "Market",
        ],
    ).set_index("Date")
    # loop to get data from different pages of a ticker:
    for index, row in (ticker_no_df.reset_index()).iterrows():
        try:
            df_temp = get_price_data(
                ticker_no=row["WEB-ID"],
                ticker=row["Ticker"],
                name=row["Name"],
                data_part=row["Market"],
            )
            df_history = pd.concat([df_history, df_temp])
        except:
            pass
    # sort index and reverse the order for more processes:
    df_history = df_history.sort_index(ascending=True)
    df_history = df_history.reset_index()
    # determining week days:
    df_history["Weekday"] = df_history["Date"].dt.weekday
    df_history["Weekday"] = df_history["Weekday"].apply(lambda x: calendar.day_name[x])
    df_history["J-Date"] = df_history["Date"].apply(
        lambda x: str(jdatetime.date.fromgregorian(date=x.date()))
    )
    df_history = df_history.set_index("J-Date")
    # rearrange columns:
    df_history = df_history[
        [
            "Date",
            "Weekday",
            "Y-Final",
            "Open",
            "High",
            "Low",
            "Close",
            "Final",
            "Volume",
            "Value",
            "No",
            "Ticker",
            "Name",
            "Market",
        ]
    ]
    cols = ["Y-Final", "Open", "High", "Low", "Close", "Final", "Volume", "No", "Value"]
    df_history[cols] = df_history[cols].apply(pd.to_numeric, axis=1)
    # ----------------------------------------------------------------------------------------------------------------------
    # Y-Final for new part of data could be 0 or 1000, we need to replace them with yesterday's final price:
    df_history["Final(+1)"] = df_history["Final"].shift(
        +1
    )  # final prices shifted forward by one day
    df_history["Market(+1)"] = df_history["Market"].shift(
        +1
    )  # market shifted forward by one day
    df_history["temp"] = df_history.apply(
        lambda x: x["Y-Final"]
        if ((x["Y-Final"] != 0) and (x["Y-Final"] != 1000))
        else (
            x["Y-Final"]
            if ((x["Market(+1)"] == x["Market"]) or (pd.isnull(x["Final(+1)"])))
            else x["Final(+1)"]
        ),
        axis=1,
    )
    df_history["Y-Final"] = df_history["temp"]
    df_history.drop(columns=["Final(+1)", "temp", "Market(+1)"], inplace=True)
    # -----------------------------------------------------------------------------------------------------------------------
    for col in cols:
        df_history[col] = df_history[col].apply(
            lambda x: int(x)
        )  # convert to int because we do not have less than Rial
    # --------------------------------------------------------------------------------------------------------------------
    # Adjust price data:
    if adjust_price:
        df_history["COEF"] = (
            df_history["Y-Final"].shift(-1) / df_history["Final"]
        ).fillna(1.0)
        df_history["ADJ-COEF"] = df_history.iloc[::-1]["COEF"].cumprod().iloc[::-1]
        df_history["Adj Open"] = (df_history["Open"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj High"] = (df_history["High"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj Low"] = (df_history["Low"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj Close"] = (df_history["Close"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history["Adj Final"] = (df_history["Final"] * df_history["ADJ-COEF"]).apply(
            lambda x: int(x)
        )
        df_history.drop(columns=["COEF", "ADJ-COEF"], inplace=True)
    if not show_weekday:
        df_history.drop(columns=["Weekday"], inplace=True)
    if not double_date:
        df_history.drop(columns=["Date"], inplace=True)
    df_history.drop(columns=["Y-Final"], inplace=True)
    # slice requested time window:
    if not ignore_date:
        df_history = df_history[start_date:end_date]
    return df_history


################################################################################################################################################################################
################################################################################################################################################################################


def Build_PricePanel(
    stock_list,
    param="Adj Final",
    jalali_date=True,
    save_excel=True,
    save_path="D:/FinPy-TSE Data/Price Panel/",
):
    if param not in ["Final", "Adj Final"]:
        print(
            'Invalid Input Error for "param": Valid inputs are "Final" and "Adj Final"'
        )
        return
    segment_size = 25
    # check save path:
    if save_excel:
        if save_path[-1] != "/":
            save_path = save_path + "/"
        today_j_date = jdatetime.datetime.now().strftime("%Y-%m-%d")
        df_save_test = pd.DataFrame({"Stocks": stock_list})
        try:
            df_save_test.to_excel(save_path + today_j_date + " Price Panel" + ".xlsx")
        except:
            print("Save path does not exist, Please Enter a Valid Destination Path!")
            return
    # segment data using given segment size:
    segmented_stock_list = [
        stock_list[i : i + segment_size]
        for i in range(0, len(stock_list), segment_size)
    ]
    no_segments = len(segmented_stock_list)
    # START -----------------------------------------------------------------------------------------------------------------------------------
    start_time = time.time()
    for i in range(no_segments):
        target_stock_list = segmented_stock_list[i]
        # request for data
        clear_output(wait=True)
        if save_excel:
            print(
                "Reading Data : ",
                f"{round((i)/no_segments*100,1)} %",
                "   Processing and Saving Data : ",
                f"{round((i)/no_segments*100,1)} %",
            )
        else:
            print(
                "Reading Data : ",
                f"{round((i)/no_segments*100,1)} %",
                "   Processing Data : ",
                f"{round((i)/no_segments*100,1)} %",
            )
        text_resp = __get_history_data_group_parallel__(target_stock_list)
        clear_output(wait=True)
        if save_excel:
            print(
                "Reading Data : ",
                f"{round((i+1)/no_segments*100,1)} %",
                "   Processing and Saving Data : ",
                f"{round((i)/no_segments*100,1)} %",
            )
        else:
            print(
                "Reading Data : ",
                f"{round((i+1)/no_segments*100,1)} %",
                "   Processing Data : ",
                f"{round((i)/no_segments*100,1)} %",
            )
        # process the data:
        if i == 0:
            df_panel, lost_ticks = __build_price_panel_seg__(
                df_response=text_resp,
                param=param,
                save_excel=save_excel,
                save_path=save_path,
            )
        else:
            df_temp_, lost_ticks_ = __build_price_panel_seg__(
                df_response=text_resp,
                param=param,
                save_excel=save_excel,
                save_path=save_path,
            )
            df_panel = pd.concat(
                [df_panel, df_temp_],
                axis=1,
            )
            if not lost_ticks:
                print("lost_ticks_", lost_ticks_)
                lost_ticks = lost_ticks_.copy()
            else:
                lost_ticks.extend(lost_ticks_)
                print("lost_ticks: ", lost_ticks)
        clear_output(wait=True)
        if save_excel:
            print(
                "Reading Data : ",
                f"{round((i+1)/no_segments*100,1)} %",
                "   Processing and Saving Data : ",
                f"{round((i+1)/no_segments*100,1)} %",
            )
        else:
            print(
                "Reading Data : ",
                f"{round((i+1)/no_segments*100,1)} %",
                "   Processing Data : ",
                f"{round((i+1)/no_segments*100,1)} %",
            )
    if lost_ticks:
        print(
            f"{len(lost_ticks)} tickers has been lost during the data gatehring process: ",
            lost_ticks,
        )
    # END -----------------------------------------------------------------------------------------------------------------------------------
    # add jalali date and drop date if necessary
    if jalali_date:
        df_panel = df_panel.reset_index()
        df_panel["J-Date"] = df_panel["Date"].apply(
            lambda x: str(jdatetime.date.fromgregorian(date=x.date()))
        )
        df_panel = df_panel.set_index("J-Date")
        df_panel.drop(columns=["Date"], inplace=True)
    # save options:
    if save_excel:
        today_j_date = jdatetime.datetime.now().strftime("%Y-%m-%d")
        try:
            df_panel.to_excel(save_path + today_j_date + " Price Panel" + ".xlsx")
        except:
            print(
                'Save path does not exist, you can handle saving this data by returned dataframe as Excel using ".to_excel()", if you will!'
            )
    # final messages to user: time of running:
    end_time = time.time()
    print(
        str(int(round(end_time - start_time, 0)))
        + " Seconds Took to Gather and Process Your Requested Data"
    )
    return df_panel, lost_ticks


################################################################################################################################################################################
################################################################################################################################################################################


def Get_60D_PriceHistory(
    stock_list,
    adjust_price=True,
    show_progress=True,
    save_excel=False,
    save_path="D:/FinPy-TSE Data/MarketWatch",
):
    # read stocks IDs from TSE webpages:
    def get_data_optimaize(codes):
        tracemalloc.start()

        @unsync
        async def get_data_parallel(codes):
            counter = 1
            async with aiohttp.ClientSession() as session:
                tasks = []
                for code in codes:
                    task = asyncio.ensure_future(get_session(session, code))
                    tasks.append(task)
                view_counts = await asyncio.gather(*tasks)
                for i in view_counts:
                    if counter == 1:
                        df_final = i.copy()
                    else:
                        df_final = pd.concat([df_final, i])
                    counter += 1
            return df_final

        async def get_session(session, code):
            url = f"http://old.tsetmc.com/Loader.aspx?Partree=15131M&i={code}"
            async with session.get(url, headers=headers) as response:
                try:
                    data_text = await response.text()
                    soup = BeautifulSoup(data_text, "html.parser")
                    table = soup.findAll("table", {"class": "table1"})
                    df_id = pd.read_html(str(table[0]))[0]
                    ticker = (df_id.iloc[5, 1].split("-")[0]).strip()
                    df_id = pd.DataFrame({"WEB-ID": [code], "Ticker": [ticker]})
                    return df_id
                except:
                    failed_tickers_code.append(code)
                    return pd.DataFrame()
            return

        return get_data_parallel(codes).result()

    # ---------------------------------------------------------------------------------
    start_time = time.time()
    if show_progress:
        clear_output(wait=True)
        print(f"STEP 1/4: Gathering historical price data of last 60 trading days ...")
    # send request:
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
    }
    r = requests.get(
        "http://old.tsetmc.com/tsev2/data/ClosingPriceAll.aspx", headers=headers
    )

    # clean the data:
    hist_60_days = pd.DataFrame(r.text.split(";"))
    hist_60_days.columns = ["Data"]
    hist_60_days["WEB-ID"] = hist_60_days["Data"].apply(
        lambda x: x.split(",")[0] if (x.count(",") == 10) else None
    )
    hist_60_days["Data"] = hist_60_days["Data"].apply(
        lambda x: x.replace(x.split(",")[0] + ",", "") if (x.count(",") == 10) else x
    )
    temp_data_df = hist_60_days["Data"].str.split(",", expand=True)
    cols = [
        "n",
        "Final",
        "Close",
        "No",
        "Volume",
        "Value",
        "Low",
        "High",
        "Y-Final",
        "Open",
    ]
    temp_data_df.columns = cols
    temp_data_df = temp_data_df[
        [
            "n",
            "Y-Final",
            "Open",
            "High",
            "Low",
            "Close",
            "Final",
            "Volume",
            "Value",
            "No",
        ]
    ]

    # concat and fill 'None' web ids
    hist_60_days = pd.concat([hist_60_days[["WEB-ID"]], temp_data_df], axis=1)
    hist_60_days["WEB-ID"] = hist_60_days["WEB-ID"].fillna(method="ffill")
    hist_60_days = hist_60_days.apply(pd.to_numeric)
    hist_60_days = hist_60_days.sort_values(by=["n", "WEB-ID"], ascending=[True, True])

    # find trading days j-dates and join:
    if show_progress:
        clear_output(wait=True)
        print(f"STEP 2/4: Adding J-date to the historical data ...")
    r_trading_days = requests.get(
        f"http://cdn.tsetmc.com/api/Index/GetIndexB2History/32097828799138957",
        headers=headers,
    )
    df_trading_days = pd.DataFrame(r_trading_days.json()["indexB2"])[
        ["dEven", "xNivInuClMresIbs"]
    ]
    df_trading_days["dEven"] = df_trading_days["dEven"].apply(lambda x: str(x))
    df_trading_days["dEven"] = df_trading_days["dEven"].apply(
        lambda x: x[:4] + "-" + x[4:6] + "-" + x[-2:]
    )
    df_trading_days["dEven"] = pd.to_datetime(df_trading_days["dEven"])
    df_trading_days["J-Date"] = df_trading_days["dEven"].apply(
        lambda x: str(jdatetime.date.fromgregorian(date=x.date()))
    )
    df_trading_days = df_trading_days.set_index("J-Date")[::-1].reset_index()[
        ["J-Date"]
    ]
    df_trading_days = df_trading_days[:60]
    df_trading_days.index.name = "n"
    df_trading_days = df_trading_days.reset_index()
    hist_60_days = pd.merge(hist_60_days, df_trading_days, on="n")

    if show_progress:
        clear_output(wait=True)
        print(f"STEP 3/4: Adding ticker names from market watch ...")
    # adding ticker names:
    r = requests.get(
        "http://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx", headers=headers
    )
    main_text = r.text
    Mkt_df = pd.DataFrame((main_text.split("@")[2]).split(";"))
    Mkt_df = Mkt_df[0].str.split(",", expand=True)
    Mkt_df = Mkt_df.iloc[:, :23]
    Mkt_df.columns = [
        "WEB-ID",
        "Ticker-Code",
        "Ticker",
        "Name",
        "Time",
        "Open",
        "Final",
        "Close",
        "No",
        "Volume",
        "Value",
        "Low",
        "High",
        "Y-Final",
        "EPS",
        "Base-Vol",
        "Unknown1",
        "Unknown2",
        "Sector",
        "Day_UL",
        "Day_LL",
        "Share-No",
        "Mkt-ID",
    ]
    Mkt_df = Mkt_df[["WEB-ID", "Ticker"]]
    Mkt_df["Ticker"] = Mkt_df["Ticker"].apply(
        lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
    )
    Mkt_df["WEB-ID"] = Mkt_df["WEB-ID"].apply(lambda x: int(x.strip()))

    # re-arrange columns and drop non-trading days:
    hist_60_days = hist_60_days[hist_60_days["Volume"] != 0]
    hist_60_days = pd.merge(hist_60_days, Mkt_df, on="WEB-ID", how="left")

    # ------------------------------------------------------------------------------------------------------------------------------------------
    if show_progress:
        clear_output(wait=True)
        print(
            f"STEP 4/4: Finding and adding ticker names that are not available in the market watch ..."
        )
    # find web-ids with unknown ticker:
    missing_df = hist_60_days[hist_60_days["Ticker"].isnull()]
    missing_df = missing_df.iloc[:, :-1]
    accepted_df = hist_60_days[~hist_60_days["Ticker"].isnull()]

    # gathering detailed data:
    continue_loop = True
    df_final = pd.DataFrame()
    missing_webids = missing_df["WEB-ID"].unique().tolist()
    failed_tickers_code = []
    while continue_loop:
        df_temp = get_data_optimaize(codes=missing_webids)
        if len(failed_tickers_code) > 0:  # failed tickers
            missing_webids = failed_tickers_code
            failed_tickers_code = []
            df_final = pd.concat([df_final, df_temp])
        else:
            df_final = pd.concat([df_final, df_temp])
            continue_loop = False
    df_final["Ticker"] = df_final["Ticker"].apply(
        lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
    )
    df_final["Ticker"] = df_final["Ticker"].apply(lambda x: (x.split("-")[0]).strip())

    # filter and concat:
    missing_df = pd.merge(missing_df, df_final, on="WEB-ID", how="left")
    hist_60_days = pd.concat([accepted_df, missing_df])
    hist_60_days = hist_60_days.sort_values(
        by=["Ticker", "J-Date"], ascending=[True, True]
    )
    hist_60_days.drop(columns=["WEB-ID", "n"], inplace=True)
    hist_60_days.set_index(["Ticker", "J-Date"], inplace=True)

    # filter-out requested stocks:
    stock_list_60D = hist_60_days.index.get_level_values("Ticker").unique().tolist()
    missing_stocks_list = [
        stock for stock in stock_list if (stock not in stock_list_60D)
    ]  # report this
    available_stocks_list = [stock for stock in stock_list if (stock in stock_list_60D)]
    hist_60_days = hist_60_days.loc[available_stocks_list]
    adj_info_df = pd.DataFrame()

    if adjust_price:
        if show_progress:
            clear_output(wait=True)
            print("Data Gathering Progress: 100%,  Adjusting Prices ...")
        # adjust price:
        market_changed_tickers = []
        df_adjusted_60D_requested = pd.DataFrame()

        for ticker in available_stocks_list:
            # separate ticker data and start adjusting:
            ticker_60D_hist_df = hist_60_days.loc[ticker].copy()
            if (ticker_60D_hist_df["Y-Final"][0] == 0) or (
                ticker_60D_hist_df["Y-Final"][0] == 100
            ):
                # if zero or 100 price happens at the begining of the data, the ticker might be changed its market!
                market_changed_tickers.append(ticker)
            else:
                # adjust the price
                ticker_60D_hist_df["COEF"] = (
                    ticker_60D_hist_df["Y-Final"].shift(-1)
                    / ticker_60D_hist_df["Final"]
                ).fillna(1.0)
                ticker_60D_hist_df["ADJ-COEF"] = (
                    ticker_60D_hist_df.iloc[::-1]["COEF"].cumprod().iloc[::-1]
                )
                ticker_60D_hist_df["Adj Open"] = (
                    ticker_60D_hist_df["Open"] * ticker_60D_hist_df["ADJ-COEF"]
                ).apply(lambda x: int(x))
                ticker_60D_hist_df["Adj High"] = (
                    ticker_60D_hist_df["High"] * ticker_60D_hist_df["ADJ-COEF"]
                ).apply(lambda x: int(x))
                ticker_60D_hist_df["Adj Low"] = (
                    ticker_60D_hist_df["Low"] * ticker_60D_hist_df["ADJ-COEF"]
                ).apply(lambda x: int(x))
                ticker_60D_hist_df["Adj Close"] = (
                    ticker_60D_hist_df["Close"] * ticker_60D_hist_df["ADJ-COEF"]
                ).apply(lambda x: int(x))
                ticker_60D_hist_df["Adj Final"] = (
                    ticker_60D_hist_df["Final"] * ticker_60D_hist_df["ADJ-COEF"]
                ).apply(lambda x: int(x))
                ticker_adj_coeff = ticker_60D_hist_df["ADJ-COEF"][0]
                ticker_adj_jdate = ticker_60D_hist_df.index[0]
                ticker_60D_hist_df.drop(
                    columns=["Y-Final", "COEF", "ADJ-COEF"], inplace=True
                )
                ticker_60D_hist_df["Ticker"] = ticker
                ticker_60D_hist_df = ticker_60D_hist_df.reset_index().set_index(
                    ["Ticker", "J-Date"]
                )
                df_adjusted_60D_requested = pd.concat(
                    [df_adjusted_60D_requested, ticker_60D_hist_df]
                )
                # saving adjust data for preceding days:
                ticker_adj_info_df = pd.DataFrame(
                    {
                        "Ticker": [ticker],
                        "ADJ-JDate": [ticker_adj_jdate],
                        "ADJ-Coef": [ticker_adj_coeff],
                    }
                )
                adj_info_df = pd.concat([adj_info_df, ticker_adj_info_df])
        # clean the adjust info df:
        adj_info_df = adj_info_df[adj_info_df["ADJ-Coef"] != 1.0]
        adj_info_df.set_index("Ticker", inplace=True)
        missing_stocks_list.extend(market_changed_tickers)
        hist_60_days = df_adjusted_60D_requested.copy()

    end_time = time.time()
    if show_progress:
        clear_output(wait=True)
        print(
            "Progress : 100 % , Done in "
            + str(int(round(end_time - start_time, 0)))
            + " seconds!"
        )

    if save_excel:
        # modify save path if necessary:
        if save_path[-1] != "/":
            save_path = save_path + "/"
        today_j_date = jdatetime.datetime.now().strftime("%Y-%m-%d")
        name = today_j_date + " 60D_History.xlsx"
        try:
            hist_60_days.to_excel(save_path + name)
            print("File saved in the specificed directory as: ", name)
        except:
            print(
                'Save path does not exist, you can handle saving this data by returned dataframe as Excel using ".to_excel()", if you will!'
            )
    return hist_60_days, adj_info_df, missing_stocks_list


###########################################################################################################################################################
###########################################################################################################################################################
