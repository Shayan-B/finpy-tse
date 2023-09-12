import pandas as pd
import requests
import jdatetime
from IPython.display import clear_output
from persiantools import characters
import re
import datetime


headers = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
}


def __Check_JDate_Validity__(date, key_word):
    try:
        if len(date.split("-")[0]) == 4:
            date = jdatetime.date(
                year=int(date.split("-")[0]),
                month=int(date.split("-")[1]),
                day=int(date.split("-")[2]),
            )
            date = f"{date.year:04}-{date.month:02}-{date.day:02}"
            return date
        else:
            print(f"Please enter valid {key_word} date in YYYY-MM-DD format")
    except:
        if len(date) == 10:
            print(f"Please enter valid {key_word} date")
            return
        else:
            print(f"Please enter valid {key_word} date in YYYY-MM-DD format")


################################################################################################################################################################################
################################################################################################################################################################################
def get_tse_webid(stock: str = "پترول") -> pd.DataFrame:
    """
    Takes ticker or firm's full name, does a live search in TSE new website and returns a multi-index Pandas dataframe that contains the following columns:

    Ticker: Symbol in the Tehran Stock Exchange.
    Active: 1 shows the market in which the stock is currently trading.
    Name: firm's full name in the relevant market.
    WebID: A numeric code that can be used for building request links and crawling the financial data of the given stock.
    Market: Market name in Tehran Stock Exchange, markets the stock was traded in and is trading now (بورس، فرابورس، پایه زرد، پایه نارنجی، پایه قرمز).

    :param stock: (str) Ticker or firm's full name.
    :return: (pd.DataFrame) A dataframe that contains Ticker, Active, WebID, Name and Market columns for the requested stock.
    """

    # basic search function: searches for and cleans the search results
    def srch_req(srch_key):
        srch_page = requests.get(
            f"http://cdn.tsetmc.com/api/Instrument/GetInstrumentSearch/{srch_key}",
            headers=headers,
        )
        srch_res = pd.DataFrame(srch_page.json()["instrumentSearch"])
        srch_res = srch_res[["lVal18AFC", "lVal30", "insCode", "lastDate", "cgrValCot"]]
        srch_res.columns = ["Ticker", "Name", "WebID", "Active", "Market"]
        srch_res["Name"] = srch_res["Name"].apply(
            lambda x: characters.ar_to_fa(
                " ".join([i.strip() for i in x.split("\u200c")]).strip()
            )
        )
        srch_res["Ticker"] = srch_res["Ticker"].apply(
            lambda x: characters.ar_to_fa("".join(x.split("\u200c")).strip())
        )
        srch_res["NameSplit"] = srch_res["Name"].apply(
            lambda x: "".join(x.split()).strip()
        )
        srch_res["SymbolSplit"] = srch_res["Ticker"].apply(
            lambda x: "".join(x.split()).strip()
        )
        srch_res["Active"] = pd.to_numeric(srch_res["Active"])
        srch_res = srch_res.sort_values("Ticker")
        srch_res = pd.DataFrame(
            srch_res[["Name", "WebID", "NameSplit", "SymbolSplit", "Market"]].values,
            columns=["Name", "WebID", "NameSplit", "SymbolSplit", "Market"],
            index=pd.MultiIndex.from_frame(srch_res[["Ticker", "Active"]]),
        )
        return srch_res

    # checking function inputs
    if type(stock) != str:
        print("Please Enetr a Valid Ticker or Name!")
        return False

    # special case that can not be found using ticker: convert ticker to full name!
    if stock == "آ س پ":
        stock = "آ.س.پ"

    # generating search keys
    stock = characters.ar_to_fa("".join(stock.split("\u200c")).strip())
    first_name = stock.split()[0]
    stock = "".join(stock.split())

    # start searching using keys, cleaning data, checking search results and handling special cases (ticker or full name)
    data = srch_req(first_name)
    df_symbol = data[data["SymbolSplit"] == stock]
    df_name = data[data["NameSplit"] == stock]

    # matching search results with search key, cleaning the data and adding market data
    if len(df_symbol) > 0:
        df_symbol = df_symbol.sort_index(level=1, ascending=False).drop(
            ["NameSplit", "SymbolSplit"], axis=1
        )
        df_symbol["Market"] = df_symbol["Market"].apply(
            lambda x: re.sub("[0-9]", "", x)
        )
        df_symbol["Market"] = df_symbol["Market"].map(
            {
                "N": "بورس",
                "Z": "فرابورس",
                "D": "فرابورس",
                "A": "پایه زرد",
                "P": "پایه زرد",
                "C": "پایه نارنجی",
                "L": "پایه قرمز",
                "W": "کوچک و متوسط فرابورس",
                "V": "کوچک و متوسط فرابورس",
            }
        )
        df_symbol["Market"] = df_symbol["Market"].fillna("نامعلوم")
        return df_symbol
    elif len(df_name) > 0:
        symbol = df_name.index[0][0]
        data = srch_req(symbol)
        symbol = characters.ar_to_fa("".join(symbol.split("\u200c")).strip())
        df_symbol = data[data.index.get_level_values("Ticker") == symbol]
        if len(df_symbol) > 0:
            df_symbol = df_symbol.sort_index(level=1, ascending=False).drop(
                ["NameSplit", "SymbolSplit"], axis=1
            )
            df_symbol["Market"] = df_symbol["Market"].apply(
                lambda x: re.sub("[0-9]", "", x)
            )
            df_symbol["Market"] = df_symbol["Market"].map(
                {
                    "N": "بورس",
                    "Z": "فرابورس",
                    "D": "فرابورس",
                    "A": "پایه زرد",
                    "P": "پایه زرد",
                    "C": "پایه نارنجی",
                    "L": "پایه قرمز",
                    "W": "کوچک و متوسط فرابورس",
                    "V": "کوچک و متوسط فرابورس",
                }
            )
            df_symbol["Market"] = df_symbol["Market"].fillna("نامعلوم")
            return df_symbol

    # invalid entry
    print("Please Enetr a Valid Ticker or Name!")

    return False


################################################################################################################################################################################
################################################################################################################################################################################


def __Get_TSE_WebID__(stock):
    # search TSE function ------------------------------------------------------------------------------------------------------------
    def request(name):
        page = requests.get(
            f"http://old.tsetmc.com/tsev2/data/search.aspx?skey={name}", headers=headers
        )
        data = []
        for i in page.text.split(";"):
            try:
                i = i.split(",")
                data.append([i[0], i[1], i[2], i[7], i[-1]])
            except:
                pass
        data = pd.DataFrame(
            data, columns=["Ticker", "Name", "WEB-ID", "Active", "Market"]
        )
        data["Name"] = data["Name"].apply(
            lambda x: characters.ar_to_fa(
                " ".join([i.strip() for i in x.split("\u200c")]).strip()
            )
        )
        data["Ticker"] = data["Ticker"].apply(
            lambda x: characters.ar_to_fa("".join(x.split("\u200c")).strip())
        )
        data["Name-Split"] = data["Name"].apply(lambda x: "".join(x.split()).strip())
        data["Symbol-Split"] = data["Ticker"].apply(
            lambda x: "".join(x.split()).strip()
        )
        data["Active"] = pd.to_numeric(data["Active"])
        data = data.sort_values("Ticker")
        data = pd.DataFrame(
            data[["Name", "WEB-ID", "Name-Split", "Symbol-Split", "Market"]].values,
            columns=["Name", "WEB-ID", "Name-Split", "Symbol-Split", "Market"],
            index=pd.MultiIndex.from_frame(data[["Ticker", "Active"]]),
        )
        return data

    # ---------------------------------------------------------------------------------------------------------------------------------
    if type(stock) != str:
        print("Please Enetr a Valid Ticker or Name!")
        return False
    if stock == "آ س پ":
        stock = "آ.س.پ"
    # cleaning input search key
    stock = characters.ar_to_fa("".join(stock.split("\u200c")).strip())
    first_name = stock.split()[0]
    stock = "".join(stock.split())
    # search TSE and process:
    data = request(first_name)
    df_symbol = data[data["Symbol-Split"] == stock]
    df_name = data[data["Name-Split"] == stock]
    if len(df_symbol) > 0:
        df_symbol = df_symbol.sort_index(level=1, ascending=False).drop(
            ["Name-Split", "Symbol-Split"], axis=1
        )
        df_symbol["Market"] = df_symbol["Market"].apply(
            lambda x: re.sub("[0-9]", "", x)
        )
        df_symbol["Market"] = df_symbol["Market"].map(
            {
                "N": "بورس",
                "Z": "فرابورس",
                "D": "فرابورس",
                "A": "پایه زرد",
                "P": "پایه زرد",
                "C": "پایه نارنجی",
                "L": "پایه قرمز",
                "W": "کوچک و متوسط فرابورس",
                "V": "کوچک و متوسط فرابورس",
            }
        )
        df_symbol["Market"] = df_symbol["Market"].fillna("نامعلوم")
        return df_symbol
    elif len(df_name) > 0:
        symbol = df_name.index[0][0]
        data = request(symbol)
        symbol = characters.ar_to_fa("".join(symbol.split("\u200c")).strip())
        df_symbol = data[data.index.get_level_values("Ticker") == symbol]
        if len(df_symbol) > 0:
            df_symbol = df_symbol.sort_index(level=1, ascending=False).drop(
                ["Name-Split", "Symbol-Split"], axis=1
            )
            df_symbol["Market"] = df_symbol["Market"].apply(
                lambda x: re.sub("[0-9]", "", x)
            )
            df_symbol["Market"] = df_symbol["Market"].map(
                {
                    "N": "بورس",
                    "Z": "فرابورس",
                    "D": "فرابورس",
                    "A": "پایه زرد",
                    "P": "پایه زرد",
                    "C": "پایه نارنجی",
                    "L": "پایه قرمز",
                    "W": "کوچک و متوسط فرابورس",
                    "V": "کوچک و متوسط فرابورس",
                }
            )
            df_symbol["Market"] = df_symbol["Market"].fillna("نامعلوم")
            return df_symbol
    print("Please Enetr a Valid Ticker or Name!")
    return False


################################################################################################################################################################################
################################################################################################################################################################################


def __Get_TSE_Sector_WebID__(sector_name):
    sector_list = [
        "زراعت",
        "ذغال سنگ",
        "کانی فلزی",
        "سایر معادن",
        "منسوجات",
        "محصولات چرمی",
        "محصولات چوبی",
        "محصولات کاغذی",
        "انتشار و چاپ",
        "فرآورده های نفتی",
        "لاستیک",
        "فلزات اساسی",
        "محصولات فلزی",
        "ماشین آلات",
        "دستگاه های برقی",
        "وسایل ارتباطی",
        "خودرو",
        "قند و شکر",
        "چند رشته ای",
        "تامین آب، برق و گاز",
        "غذایی",
        "دارویی",
        "شیمیایی",
        "خرده فروشی",
        "کاشی و سرامیک",
        "سیمان",
        "کانی غیر فلزی",
        "سرمایه گذاری",
        "بانک",
        "سایر مالی",
        "حمل و نقل",
        "رادیویی",
        "مالی",
        "اداره بازارهای مالی",
        "انبوه سازی",
        "رایانه",
        "اطلاعات و ارتباطات",
        "فنی مهندسی",
        "استخراج نفت",
        "بیمه و بازنشستگی",
    ]
    sector_web_id = [
        34408080767216529,
        19219679288446732,
        13235969998952202,
        62691002126902464,
        59288237226302898,
        69306841376553334,
        58440550086834602,
        30106839080444358,
        25766336681098389,
        12331083953323969,
        36469751685735891,
        32453344048876642,
        1123534346391630,
        11451389074113298,
        33878047680249697,
        24733701189547084,
        20213770409093165,
        21948907150049163,
        40355846462826897,
        54843635503648458,
        15508900928481581,
        3615666621538524,
        33626672012415176,
        65986638607018835,
        57616105980228781,
        70077233737515808,
        14651627750314021,
        34295935482222451,
        72002976013856737,
        25163959460949732,
        24187097921483699,
        41867092385281437,
        61247168213690670,
        61985386521682984,
        4654922806626448,
        8900726085939949,
        18780171241610744,
        47233872677452574,
        65675836323214668,
        59105676994811497,
    ]
    df_index_lookup = pd.DataFrame(
        {"Sector": sector_list, "Web-ID": sector_web_id}
    ).set_index("Sector")

    # try search keyy with available look-up table and find web-id:
    try:
        sector_web_id = df_index_lookup.loc[sector_name]["Web-ID"]
    except:
        sector_name = characters.fa_to_ar(sector_name)
        page = requests.get(
            f"https://www.google.com/search?q={sector_name} tsetmc اطلاعات شاخص",
            headers=headers,
        )
        code = page.text.split(
            "http://www.tsetmc.com/Loader.aspx%3FParTree%3D15131J%26i%3D"
        )[1]
        code = code.split("&")[0]
        # check google acquired code with reference table
        if len(df_index_lookup[df_index_lookup["Web-ID"] == int(code)]) == 1:
            sector_web_id = int(code)
        else:
            print("Invalid sector name! Please try again with correct sector name!")
            return
    return sector_web_id


################################################################################################################################################################################
################################################################################################################################################################################


def __Get_Day_MarketClose_BQ_SQ__(ticker_no, j_date):
    # convert to desired Cristian data format
    year, month, day = j_date.split("-")
    date = jdatetime.date(int(year), int(month), int(day)).togregorian()
    date = f"{date.year:04}{date.month:02}{date.day:02}"
    # get day upper and lower band prices:
    page = requests.get(
        f"http://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{ticker_no}/{date}",
        headers=headers,
    )
    df_ub_lb = pd.DataFrame(page.json()["staticThreshold"])
    day_ub = df_ub_lb.iloc[-1]["psGelStaMax"]  # day upper band price
    day_lb = df_ub_lb.iloc[-1]["psGelStaMin"]  # day lower band price
    # get LOB data:
    page = requests.get(
        f"http://cdn.tsetmc.com/api/BestLimits/{ticker_no}/{date}", headers=headers
    )
    data = pd.DataFrame(page.json()["bestLimitsHistory"])
    # find last orders before 12:30:00 (market close)
    time = 123000
    bq, sq, bq_percap, sq_percap = 0.0, 0.0, 0.0, 0.0
    while time > 122900:
        end_lob = data[data["hEven"] == time]
        end_lob = end_lob.sort_values(by="number", ascending=True).iloc[:1, 5:-1]
        end_lob.columns = [
            "Vol_Buy",
            "No_Buy",
            "Price_Buy",
            "Price_Sell",
            "No_Sell",
            "Vol_Sell",
        ]
        end_lob = end_lob[
            ["No_Sell", "Vol_Sell", "Price_Sell", "Price_Buy", "No_Buy", "Vol_Buy"]
        ]
        if len(end_lob) == 0:  # go one second back and try again
            time -= 1
            if int(str(time)[-2:]) > 59:
                a = int(str(time)[:-2] + "59")
        else:
            # check buy and sell queue and do calculations
            if end_lob.iloc[0]["Price_Sell"] == day_lb:
                sq = day_lb * end_lob.iloc[0]["Vol_Sell"]
                sq_percap = sq / end_lob.iloc[0]["No_Sell"]
            if end_lob.iloc[0]["Price_Buy"] == day_ub:
                bq = day_ub * end_lob.iloc[0]["Vol_Buy"]
                bq_percap = bq / end_lob.iloc[0]["No_Buy"]
            break
    df_sq_bq = pd.DataFrame(
        {
            "J-Date": [j_date],
            "Day_UL": [int(day_lb)],
            "Day_LL": [int(day_ub)],
            "Time": [str(time)[:2] + ":" + str(time)[2:4] + ":" + str(time)[-2:]],
            "BQ_Value": [int(bq)],
            "SQ_Value": [int(sq)],
            "BQPC": [int(round(bq_percap, 0))],
            "SQPC": [int(round(sq_percap, 0))],
        }
    )
    df_sq_bq = df_sq_bq.set_index("J-Date")
    return df_sq_bq


################################################################################################################################################################################
################################################################################################################################################################################


def __Get_Day_LOB__(ticker_no, j_date):
    # convert to desired Cristian data format
    year, month, day = j_date.split("-")
    date = jdatetime.date(int(year), int(month), int(day)).togregorian()
    date = f"{date.year:04}{date.month:02}{date.day:02}"
    # get day upper and lower band prices:
    page = requests.get(
        f"http://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{ticker_no}/{date}",
        headers=headers,
    )
    df_ub_lb = pd.DataFrame(page.json()["staticThreshold"])
    day_ub = df_ub_lb.iloc[-1]["psGelStaMax"]  # day upper band price
    day_lb = df_ub_lb.iloc[-1]["psGelStaMin"]  # day lower band price
    # get LOB data:
    page = requests.get(
        f"http://cdn.tsetmc.com/api/BestLimits/{ticker_no}/{date}", headers=headers
    )
    data = pd.DataFrame(page.json()["bestLimitsHistory"])
    data.drop(columns=["idn", "dEven", "refID", "insCode"], inplace=True)
    data = data.sort_values(["hEven", "number"], ascending=(True, True))
    data = data[
        (data["hEven"] >= 84500) & (data["hEven"] < 123000)
    ]  # filter out 8:30 to 12:35
    data.columns = [
        "Time",
        "Depth",
        "Buy_Vol",
        "Buy_No",
        "Buy_Price",
        "Sell_Price",
        "Sell_No",
        "Sell_Vol",
    ]
    data["J-Date"] = j_date
    data["Date"] = date
    data["Date"] = pd.to_datetime(data["Date"])
    # re-arrange columns:
    data["Time"] = (
        data["Time"]
        .astype("str")
        .apply(
            lambda x: datetime.time(
                hour=int(x[0]), minute=int(x[1:3]), second=int(x[3:])
            )
            if len(x) == 5
            else datetime.time(hour=int(x[:2]), minute=int(x[2:4]), second=int(x[4:]))
        )
    )
    data["Day_UL"] = day_ub
    data["Day_LL"] = day_lb
    data = data[
        [
            "J-Date",
            "Time",
            "Depth",
            "Sell_No",
            "Sell_Vol",
            "Sell_Price",
            "Buy_Price",
            "Buy_Vol",
            "Buy_No",
            "Day_LL",
            "Day_UL",
            "Date",
        ]
    ]
    data = data.set_index(["J-Date", "Time", "Depth"])
    return data


def __Save_List__(
    df_data,
    bourse,
    farabourse,
    payeh,
    detailed_list,
    save_excel,
    save_csv,
    save_path="D:/FinPy-TSE Data/",
):
    # find today's j-date ti use in name of the file
    today_j_date = jdatetime.datetime.now().strftime("%Y-%m-%d")
    # select name:
    if bourse:
        if farabourse:
            if payeh:
                if detailed_list:
                    name = today_j_date + " detailed_stocklist_bfp"
                else:
                    name = today_j_date + " stocklist_bfp"
            else:
                if detailed_list:
                    name = today_j_date + " detailed_stocklist_bf"
                else:
                    name = today_j_date + " stocklist_bf"
        else:
            if payeh:
                if detailed_list:
                    name = today_j_date + " detailed_stocklist_bp"
                else:
                    name = today_j_date + " stocklist_bp"
            else:
                if detailed_list:
                    name = today_j_date + " detailed_stocklist_b"
                else:
                    name = today_j_date + " stocklist_b"
    else:
        if farabourse:
            if payeh:
                if detailed_list:
                    name = today_j_date + " detailed_stocklist_fp"
                else:
                    name = today_j_date + " stocklist_fp"
            else:
                if detailed_list:
                    name = today_j_date + " detailed_stocklist_f"
                else:
                    name = today_j_date + " stocklist_f"
        else:
            if payeh:
                if detailed_list:
                    name = today_j_date + " detailed_stocklist_p"
                else:
                    name = today_j_date + " stocklist_p"
            else:
                name = None
    # ------------------------------------------------
    # modify save path if necessary:
    if save_path[-1] != "/":
        save_path = save_path + "/"
    # save Excel file:
    if save_excel:
        try:
            df_data.to_excel(save_path + name + ".xlsx")
            print("File saved in the specificed directory as: ", name + ".xlsx")
        except:
            print(
                'Save path does not exist, you can handle saving this data by returned dataframe as Excel using ".to_excel()", if you will!'
            )
    # save Excel file:
    if save_csv:
        try:
            df_data.to_csv(save_path + name + ".csv")
            print("File saved in the specificed directory as: ", name + ".csv")
        except:
            print(
                'Save path does not exist, you can handle saving this data by returned dataframe as CSV using ".to_csv()", if you will!'
            )
    return


class URL:
    def __init__(self, asset_type=None, **kwargs):
        self.base_url = None
        self.header = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36"
        }
        if asset_type:
            self.atype = asset_type
        else:
            self.atype = None
        self.kwargs = kwargs
        self.make_url()

    def make_url(self):
        pass
        # if self.atype == "up_lo_limit":
        #     ticker_no = self.kwargs["ticker_no"]
        #     date = self.kwargs["date"]
        #     self.url = f"http://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{ticker_no}/{date}"
        # elif self.atype == "lob":
        #     ticker_no = self.kwargs["ticker_no"]
        #     date = self.kwargs["date"]
        #     self.url = f"http://cdn.tsetmc.com/api/BestLimits/{ticker_no}/{date}"
        # elif self.atype == "op_lo_thresh":
        #     ticker_no = self.kwargs["ticker_no"]
        #     date = self.kwargs["date"]
        #     self.url = f"http://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{ticker_no}/{date}"
        # elif self.atype == "search_G":
        #     sector_name = self.kwargs["sector_name"]
        #     self.url = (
        #         f"https://www.google.com/search?q={sector_name} tsetmc اطلاعات شاخص"
        #     )
        # elif self.atype == "search_TSE":
        #     srch_key = self.kwargs["srch_key"]
        #     self.url = (
        #         f"http://cdn.tsetmc.com/api/Instrument/GetInstrumentSearch/{srch_key}"
        #     )
        # elif not self.atype:
        #     self.url = None
        ##------- OR I CAN DO THE FOLLOWING ------------##
        # match self.atype:
        #     case "up_lo_limit":
        #         ticker_no = self.kwargs["ticker_no"]
        #         date = self.kwargs["date"]
        #         self.url  = f"http://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{ticker_no}/{date}"
        #     case "lob":
        #         ticker_no = self.kwargs["ticker_no"]
        #         date = self.kwargs["date"]
        #         self.url = f"http://cdn.tsetmc.com/api/BestLimits/{ticker_no}/{date}"
        #     case "op_lo_thresh":
        #         ticker_no = self.kwargs["ticker_no"]
        #         date = self.kwargs["date"]
        #         self.url = f"http://cdn.tsetmc.com/api/MarketData/GetStaticThreshold/{ticker_no}/{date}"
        #     case "searchG":
        #         sector_name = self.kwargs["sector_name"]
        #         self.url = f'https://www.google.com/search?q={sector_name} tsetmc اطلاعات شاخص'
        #     case "srachTSE":
        #         srch_key = self.kwargs["srch_key"]
        #         self.url = f'http://cdn.tsetmc.com/api/Instrument/GetInstrumentSearch/{srch_key}'


class dates:
    def __init__(self, pdate, keyword):
        self.keyword = keyword
        self.date = pdate
        self.date = self.__Check_JDate_Validity__()

    def __Check_JDate_Validity__(self):
        try:
            if len(self.date.split("-")[0]) == 4:
                t_date = self.build_date(self.date)
                # t_date = f"{t_date.year:04}-{t_date.month:02}-{t_date.day:02}" I THOUGH THIS WAS UNNECESSARY
                return t_date
            else:
                print(f"Please enter valid {self.keyword} date in YYYY-MM-DD format")
        except:
            if len(self.date) == 10:
                print(f"Please enter valid {self.keyword} date")
            else:
                print(f"Please enter valid {self.keyword} date in YYYY-MM-DD format")
        return 

    def build_date(self, temp_date):
        return jdatetime.date(
            year=int(temp_date.split("-")[0]),
            month=int(temp_date.split("-")[1]),
            day=int(temp_date.split("-")[2]),
        )
