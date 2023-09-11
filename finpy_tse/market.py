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

from .util import __Save_List__, __Get_TSE_WebID__, headers, URL


class murl(URL):
    def __init__(self, asset_type=None, **kwargs):
        super().__init__(asset_type, **kwargs)

    def make_url(self):
        if self.atype == "market_watch":
            self.url = "http://old.tsetmc.com/tsev2/data/ClientTypeAll.aspx"
        elif self.atype == "market_ob":
            self.url = "http://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx"
        elif self.atype == "partree":
            self.url = "http://old.tsetmc.com/Loader.aspx?ParTree=111C1213"
        elif self.atype == "bourse_list":
            self.url = (
                "http://old.tsetmc.com/Loader.aspx?ParTree=15131J&i=32097828799138957"
            )
        elif self.atype == "fara_list":
            self.url = (
                "http://old.tsetmc.com/Loader.aspx?ParTree=15131J&i=43685683301327984"
            )
        elif not self.atype:
            self.url = None

        return


class Market:
    def __init__(self) -> None:
        pass


def Get_MarketWatch(save_excel=True, save_path="D:/FinPy-TSE Data/MarketWatch"):
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # GET MARKET RETAIL AND INSTITUTIONAL DATA
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    r = requests.get(
        "http://old.tsetmc.com/tsev2/data/ClientTypeAll.aspx", headers=headers
    )
    Mkt_RI_df = pd.DataFrame(r.text.split(";"))
    Mkt_RI_df = Mkt_RI_df[0].str.split(",", expand=True)
    # assign names to columns:
    Mkt_RI_df.columns = [
        "WEB-ID",
        "No_Buy_R",
        "No_Buy_I",
        "Vol_Buy_R",
        "Vol_Buy_I",
        "No_Sell_R",
        "No_Sell_I",
        "Vol_Sell_R",
        "Vol_Sell_I",
    ]
    # convert columns to numeric type:
    cols = [
        "No_Buy_R",
        "No_Buy_I",
        "Vol_Buy_R",
        "Vol_Buy_I",
        "No_Sell_R",
        "No_Sell_I",
        "Vol_Sell_R",
        "Vol_Sell_I",
    ]
    Mkt_RI_df[cols] = Mkt_RI_df[cols].apply(pd.to_numeric, axis=1)
    Mkt_RI_df["WEB-ID"] = Mkt_RI_df["WEB-ID"].apply(lambda x: x.strip())
    Mkt_RI_df = Mkt_RI_df.set_index("WEB-ID")
    # re-arrange the order of columns:
    Mkt_RI_df = Mkt_RI_df[
        [
            "No_Buy_R",
            "No_Buy_I",
            "No_Sell_R",
            "No_Sell_I",
            "Vol_Buy_R",
            "Vol_Buy_I",
            "Vol_Sell_R",
            "Vol_Sell_I",
        ]
    ]
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # GET MARKET WATCH PRICE AND OB DATA
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
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
    # re-arrange columns and drop some columns:
    Mkt_df = Mkt_df[
        [
            "WEB-ID",
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
            "Sector",
            "Day_UL",
            "Day_LL",
            "Share-No",
            "Mkt-ID",
        ]
    ]
    # Just keep: 300 Bourse, 303 Fara-Bourse, 305 Sandoogh, 309 Payeh, 400 H-Bourse, 403 H-FaraBourse, 404 H-Payeh
    Mkt_ID_list = ["300", "303", "305", "309", "400", "403", "404"]
    Mkt_df = Mkt_df[Mkt_df["Mkt-ID"].isin(Mkt_ID_list)]
    Mkt_df["Market"] = Mkt_df["Mkt-ID"].map(
        {
            "300": "بورس",
            "303": "فرابورس",
            "305": "صندوق قابل معامله",
            "309": "پایه",
            "400": "حق تقدم بورس",
            "403": "حق تقدم فرابورس",
            "404": "حق تقدم پایه",
        }
    )
    Mkt_df.drop(
        columns=["Mkt-ID"], inplace=True
    )  # we do not need Mkt-ID column anymore
    # assign sector names:
    r = requests.get(
        "http://old.tsetmc.com/Loader.aspx?ParTree=111C1213", headers=headers
    )
    sectro_lookup = (pd.read_html(r.text)[0]).iloc[1:, :]
    # convert from Arabic to Farsi and remove half-space
    sectro_lookup[1] = sectro_lookup[1].apply(
        lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
    )
    sectro_lookup[1] = sectro_lookup[1].apply(lambda x: x.replace("\u200c", " "))
    sectro_lookup[1] = sectro_lookup[1].apply(lambda x: x.strip())
    Mkt_df["Sector"] = Mkt_df["Sector"].map(dict(sectro_lookup[[0, 1]].values))
    # modify format of columns:
    cols = [
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
        "Day_UL",
        "Day_LL",
        "Share-No",
    ]
    Mkt_df[cols] = Mkt_df[cols].apply(pd.to_numeric, axis=1)
    Mkt_df["Time"] = Mkt_df["Time"].apply(
        lambda x: x[:-4] + ":" + x[-4:-2] + ":" + x[-2:]
    )
    Mkt_df["Ticker"] = Mkt_df["Ticker"].apply(
        lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
    )
    Mkt_df["Name"] = Mkt_df["Name"].apply(
        lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
    )
    Mkt_df["Name"] = Mkt_df["Name"].apply(lambda x: x.replace("\u200c", " "))
    # calculate some new columns
    Mkt_df["Close(%)"] = round(
        (Mkt_df["Close"] - Mkt_df["Y-Final"]) / Mkt_df["Y-Final"] * 100, 2
    )
    Mkt_df["Final(%)"] = round(
        (Mkt_df["Final"] - Mkt_df["Y-Final"]) / Mkt_df["Y-Final"] * 100, 2
    )
    Mkt_df["Market Cap"] = round(Mkt_df["Share-No"] * Mkt_df["Final"], 2)
    # set index
    Mkt_df["WEB-ID"] = Mkt_df["WEB-ID"].apply(lambda x: x.strip())
    Mkt_df = Mkt_df.set_index("WEB-ID")
    # ------------------------------------------------------------------------------------------------------------------------------------------
    # reading OB (order book) and cleaning the data
    OB_df = pd.DataFrame((main_text.split("@")[3]).split(";"))
    OB_df = OB_df[0].str.split(",", expand=True)
    OB_df.columns = [
        "WEB-ID",
        "OB-Depth",
        "Sell-No",
        "Buy-No",
        "Buy-Price",
        "Sell-Price",
        "Buy-Vol",
        "Sell-Vol",
    ]
    OB_df = OB_df[
        [
            "WEB-ID",
            "OB-Depth",
            "Sell-No",
            "Sell-Vol",
            "Sell-Price",
            "Buy-Price",
            "Buy-Vol",
            "Buy-No",
        ]
    ]
    # extract top row of order book = OB1
    OB1_df = (OB_df[OB_df["OB-Depth"] == "1"]).copy()  # just keep top row of OB
    OB1_df.drop(
        columns=["OB-Depth"], inplace=True
    )  # we do not need this column anymore
    # set WEB-ID as index for future joining operations:
    OB1_df["WEB-ID"] = OB1_df["WEB-ID"].apply(lambda x: x.strip())
    OB1_df = OB1_df.set_index("WEB-ID")
    # convert columns to numeric format:
    cols = ["Sell-No", "Sell-Vol", "Sell-Price", "Buy-Price", "Buy-Vol", "Buy-No"]
    OB1_df[cols] = OB1_df[cols].apply(pd.to_numeric, axis=1)
    # join OB1_df to Mkt_df
    Mkt_df = Mkt_df.join(OB1_df)
    # calculate buy/sell queue value
    bq_value = Mkt_df.apply(
        lambda x: int(x["Buy-Vol"] * x["Buy-Price"])
        if (x["Buy-Price"] == x["Day_UL"])
        else 0,
        axis=1,
    )
    sq_value = Mkt_df.apply(
        lambda x: int(x["Sell-Vol"] * x["Sell-Price"])
        if (x["Sell-Price"] == x["Day_LL"])
        else 0,
        axis=1,
    )
    Mkt_df = pd.concat(
        [
            Mkt_df,
            pd.DataFrame(bq_value, columns=["BQ-Value"]),
            pd.DataFrame(sq_value, columns=["SQ-Value"]),
        ],
        axis=1,
    )
    # calculate buy/sell queue average per-capita:
    bq_pc_avg = Mkt_df.apply(
        lambda x: int(round(x["BQ-Value"] / x["Buy-No"], 0))
        if ((x["BQ-Value"] != 0) and (x["Buy-No"] != 0))
        else 0,
        axis=1,
    )
    sq_pc_avg = Mkt_df.apply(
        lambda x: int(round(x["SQ-Value"] / x["Sell-No"], 0))
        if ((x["SQ-Value"] != 0) and (x["Sell-No"] != 0))
        else 0,
        axis=1,
    )
    Mkt_df = pd.concat(
        [
            Mkt_df,
            pd.DataFrame(bq_pc_avg, columns=["BQPC"]),
            pd.DataFrame(sq_pc_avg, columns=["SQPC"]),
        ],
        axis=1,
    )
    # just keep tickers with Value grater than zero! = traded stocks:
    # Mkt_df = Mkt_df[Mkt_df['Value']!=0]
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # JOIN DATA
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    final_df = Mkt_df.join(Mkt_RI_df)
    # add trade types:
    final_df["Trade Type"] = pd.DataFrame(
        final_df["Ticker"].apply(
            lambda x: "تابلو"
            if ((not x[-1].isdigit()) or (x in ["انرژی1", "انرژی2", "انرژی3"]))
            else (
                "بلوکی"
                if (x[-1] == "2")
                else (
                    "عمده"
                    if (x[-1] == "4")
                    else ("جبرانی" if (x[-1] == "3") else "تابلو")
                )
            )
        )
    )
    # add update Jalali date and time:
    jdatetime_download = jdatetime.datetime.today().strftime("%Y-%m-%d %H:%M:%S")
    final_df["Download"] = jdatetime_download
    # just keep necessary columns and re-arrange theor order:
    final_df = final_df[
        [
            "Ticker",
            "Trade Type",
            "Time",
            "Open",
            "High",
            "Low",
            "Close",
            "Final",
            "Close(%)",
            "Final(%)",
            "Day_UL",
            "Day_LL",
            "Value",
            "BQ-Value",
            "SQ-Value",
            "BQPC",
            "SQPC",
            "Volume",
            "Vol_Buy_R",
            "Vol_Buy_I",
            "Vol_Sell_R",
            "Vol_Sell_I",
            "No",
            "No_Buy_R",
            "No_Buy_I",
            "No_Sell_R",
            "No_Sell_I",
            "Name",
            "Market",
            "Sector",
            "Share-No",
            "Base-Vol",
            "Market Cap",
            "EPS",
            "Download",
        ]
    ]
    final_df = final_df.set_index("Ticker")
    # convert columns to int64 data type:
    """cols = ['Open','High','Low','Close','Final','Day_UL', 'Day_LL','Value', 'BQ-Value', 'SQ-Value', 'BQPC', 'SQPC',
            'Volume','Vol_Buy_R', 'Vol_Buy_I', 'Vol_Sell_R', 'Vol_Sell_I','No','No_Buy_R', 'No_Buy_I', 'No_Sell_R', 'No_Sell_I',
            'Share-No','Base-Vol','Market Cap']
    final_df[cols] = final_df[cols].astype('int64')"""
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # PROCESS ORDER BOOK DATA IF REQUESTED
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    final_OB_df = (Mkt_df[["Ticker", "Day_LL", "Day_UL"]]).join(
        OB_df.set_index("WEB-ID")
    )
    # convert columns to numeric int64
    cols = [
        "Day_LL",
        "Day_UL",
        "OB-Depth",
        "Sell-No",
        "Sell-Vol",
        "Sell-Price",
        "Buy-Price",
        "Buy-Vol",
        "Buy-No",
    ]
    final_OB_df[cols] = final_OB_df[cols].astype("int64")
    # sort using tickers and order book depth:
    final_OB_df = final_OB_df.sort_values(
        ["Ticker", "OB-Depth"], ascending=(True, True)
    )
    final_OB_df = final_OB_df.set_index(["Ticker", "Day_LL", "Day_UL", "OB-Depth"])
    # add Jalali date and time:
    final_OB_df["Download"] = jdatetime_download
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    # SAVE OPTIONS AND RETURNS
    # --------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
    if save_excel:
        try:
            if save_path[-1] != "/":
                save_path = save_path + "/"
            mkt_watch_file_name = "MarketWatch " + jdatetime.datetime.today().strftime(
                "%Y-%m-%d %H-%M-%S"
            )
            OB_file_name = "OrderBook " + jdatetime.datetime.today().strftime(
                "%Y-%m-%d %H-%M-%S"
            )
            final_OB_df.to_excel(save_path + OB_file_name + ".xlsx")
            final_df.to_excel(save_path + mkt_watch_file_name + ".xlsx")
        except:
            print(
                'Save path does not exist, you can handle saving this data by returned dataframe as Excel using ".to_excel()", if you will!'
            )
    return final_df, final_OB_df


################################################################################################################################################################################
################################################################################################################################################################################


def Build_Market_StockList(
    bourse=True,
    farabourse=True,
    payeh=True,
    detailed_list=True,
    show_progress=True,
    save_excel=True,
    save_csv=True,
    save_path="D:/FinPy-TSE Data/",
):
    if not bourse and not farabourse and not payeh:
        print("Choose at least one market!")
        return
    start_time = time.time()
    http = urllib3.PoolManager()
    look_up = pd.DataFrame({"Ticker": [], "Name": [], "WEB-ID": [], "Market": []})
    # --------------------------------------------------------------------------------------------------
    if bourse:
        if show_progress:
            clear_output(wait=True)
            print("Gathering Bourse market stock list ...")
        r = http.request(
            "GET",
            "http://old.tsetmc.com/Loader.aspx?ParTree=15131J&i=32097828799138957",
        )
        soup = BeautifulSoup(r.data.decode("utf-8"), "html.parser")
        table = soup.findAll("table", {"class": "table1"})
        stock_list = table[0].find_all("a")
        ticker = []
        web_id = []
        name = []
        for stock in stock_list:
            ticker.append(stock.text)
            name.append(stock.attrs["title"])
            web_id.append(stock.attrs["href"].split("&i=")[1])
        bourse_lookup = pd.DataFrame({"Ticker": ticker, "Name": name, "WEB-ID": web_id})
        bourse_lookup["Market"] = "بورس"
        look_up = pd.concat([look_up, bourse_lookup], axis=0)
    # --------------------------------------------------------------------------------------------------
    if farabourse:
        if show_progress:
            clear_output(wait=True)
            print("Gathering Fara-Bourse market stock list ...")
        r = http.request(
            "GET",
            "http://old.tsetmc.com/Loader.aspx?ParTree=15131J&i=43685683301327984",
        )
        soup = BeautifulSoup(r.data.decode("utf-8"), "html.parser")
        table = soup.findAll("table", {"class": "table1"})
        stock_list = table[0].find_all("a")
        ticker = []
        web_id = []
        name = []
        for stock in stock_list:
            ticker.append(stock.text)
            name.append(stock.attrs["title"])
            web_id.append(stock.attrs["href"].split("&i=")[1])
        farabourse_lookup = pd.DataFrame(
            {"Ticker": ticker, "Name": name, "WEB-ID": web_id}
        )
        farabourse_lookup["Market"] = "فرابورس"
        look_up = pd.concat([look_up, farabourse_lookup], axis=0)
    # --------------------------------------------------------------------------------------------------
    if payeh:
        if show_progress:
            clear_output(wait=True)
            print("Gathering Payeh market stock list ...")
        url = "https://www.ifb.ir/StockQoute.aspx"
        header = {"__EVENTTARGET": "exportbtn"}
        response = requests.post(url, header, verify=False)
        table = pd.read_html(response.content.decode("utf-8"))[0]
        payeh_lookup = table.iloc[2:, :3]
        payeh_lookup.columns = ["Ticker", "Name", "Market"]
        payeh_lookup = payeh_lookup[
            payeh_lookup["Market"].isin(
                ["تابلو پایه زرد", "تابلو پایه نارنجی", "تابلو پایه قرمز"]
            )
        ]
        payeh_lookup["Market"] = payeh_lookup["Market"].apply(
            lambda x: (x.replace("تابلو", "")).strip()
        )
        # drop other than normal trades:
        payeh_lookup = payeh_lookup[
            payeh_lookup["Ticker"].apply(lambda x: x[-1].isdigit()) == False
        ]
        # drop hagh-taghaddom!
        payeh_lookup = payeh_lookup[
            payeh_lookup["Ticker"].apply(lambda x: x.strip()[-1] != "ح")
        ]
        look_up = pd.concat([look_up, payeh_lookup], axis=0)
    # ---------------------------------------------------------------------------------------------------
    if not detailed_list:
        # convert tickers and names to farsi characters
        look_up["Ticker"] = look_up["Ticker"].apply(
            lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
        )
        look_up["Name"] = look_up["Name"].apply(
            lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
        )
        look_up["Name"] = look_up["Name"].apply(lambda x: x.replace("\u200c", " "))
        look_up = look_up.set_index("Ticker")
        look_up.drop(columns=["WEB-ID"], inplace=True)
        if show_progress:
            clear_output(wait=True)
        # save file if necessary
        if save_excel | save_csv:
            __Save_List__(
                df_data=look_up,
                bourse=bourse,
                farabourse=bourse,
                payeh=payeh,
                detailed_list=detailed_list,
                save_excel=save_excel,
                save_csv=save_csv,
                save_path=save_path,
            )
        return look_up
    else:
        if show_progress:
            clear_output(wait=True)
            print("Searching Payeh market stocks web-pages ...")
        # rearrange columns
        look_up["Ticker"] = look_up["Ticker"].apply(
            lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
        )
        look_up["Ticker"] = look_up["Ticker"].apply(lambda x: x.replace("\u200c", " "))
        look_up["Name"] = look_up["Name"].apply(
            lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
        )
        look_up["Name"] = look_up["Name"].apply(lambda x: x.replace("\u200c", " "))
        look_up = look_up.set_index("Ticker")
        look_up = look_up[["Name", "Market", "WEB-ID"]]
        if payeh:
            # some minor changes in payeh_lookup
            payeh_lookup["Ticker"] = payeh_lookup["Ticker"].apply(
                lambda x: characters.ar_to_fa(x)
            )
            payeh_lookup = payeh_lookup.set_index("Ticker")
            # look for payeh market web-ids from market watch
            r = requests.get(
                "http://old.tsetmc.com/tsev2/data/MarketWatchPlus.aspx", headers=headers
            )
            mkt_watch = pd.DataFrame((r.text.split("@")[2]).split(";"))
            mkt_watch = mkt_watch[0].str.split(",", expand=True)
            mkt_watch = mkt_watch[[0, 2]]
            mkt_watch.columns = ["WEB-ID", "Ticker"]
            mkt_watch["Ticker"] = mkt_watch["Ticker"].apply(
                lambda x: characters.ar_to_fa(x)
            )
            mkt_watch = mkt_watch.set_index("Ticker")
            # join based on payeh_lookup
            payeh_lookup = payeh_lookup.join(mkt_watch)
            with_web_id = (payeh_lookup[payeh_lookup["WEB-ID"].notnull()]).copy()
            no_web_id = (payeh_lookup[payeh_lookup["WEB-ID"].isnull()]).copy()
            no_web_id.drop(columns=["WEB-ID"], inplace=True)
            # search from google for no web-id stocks:
            web_id = []
            no_stocks = len(no_web_id)
            counter = 1
            for index, row in no_web_id.iterrows():
                if show_progress:
                    clear_output(wait=True)
                    print(
                        "Searching Payeh market stocks web-pages: ",
                        f"{round((counter)/no_stocks*100,1)} %",
                    )
                # search with ticker, if you find nothing, then search with name
                code_df = __Get_TSE_WebID__(index)
                print(code_df.head())
                code_df = code_df.reset_index()
                try:
                    web_id.append(code_df[code_df["Active"] == 1].iloc[0]["WEB-ID"])
                    counter += 1
                except:
                    web_id.append(code_df[code_df["Active"] == 0].iloc[0]["WEB-ID"])
                    counter += 1
                    pass
            # add new codes to dataframe
            no_web_id["WEB-ID"] = web_id
            # build payeh dataframe with web-ids again:
            payeh_lookup = pd.concat([with_web_id, no_web_id])
            # add to bourse and fara-bourse:
            look_up = pd.concat([look_up[look_up["WEB-ID"].notnull()], payeh_lookup])
            look_up["Name"] = look_up["Name"].apply(lambda x: characters.ar_to_fa(x))

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
                        # rotate data frame:
                        df_id = df_id.T
                        df_id.columns = df_id.iloc[0]
                        df_id = df_id[1:]
                        df_current_stock = look_up[look_up["WEB-ID"] == code]
                        df_id["Ticker"] = df_current_stock.index[0]
                        df_id["Market"] = df_current_stock["Market"][0]
                        df_id["WEB-ID"] = df_current_stock["WEB-ID"][0]
                        return df_id
                    except:
                        failed_tickers_code.append(code)
                        return pd.DataFrame()
                return

            return get_data_parallel(codes).result()

        no_stocks = len(look_up)
        if show_progress:
            clear_output(wait=True)
            print(f"Gathering detailed data for {no_stocks} stocks ...")

        # gathering detailed data:
        continue_loop = True
        df_final = pd.DataFrame()
        web_id_list = look_up["WEB-ID"].to_list()
        failed_tickers_code = []
        while continue_loop:
            df_temp = get_data_optimaize(codes=web_id_list)
            if len(failed_tickers_code) > 0:  # failed tickers
                web_id_list = failed_tickers_code
                failed_tickers_code = []
                df_final = pd.concat([df_final, df_temp])
            else:
                df_final = pd.concat([df_final, df_temp])
                continue_loop = False

        df_final.columns = [
            "Ticker(12)",
            "Ticker(5)",
            "Name(EN)",
            "Ticker(4)",
            "Name",
            "Comment",
            "Ticker(30)",
            "Company Code(12)",
            "Panel",
            "Panel Code",
            "Sector Code",
            "Sector",
            "Sub-Sector Code",
            "Sub-Sector",
            "Ticker",
            "Market",
            "WEB-ID",
        ]
        df_final["Comment"] = df_final["Comment"].apply(
            lambda x: x.split("-")[1] if (len(x.split("-")) > 1) else "-"
        )
        df_final = df_final[
            [
                "Ticker",
                "Name",
                "Market",
                "Panel",
                "Sector",
                "Sub-Sector",
                "Comment",
                "Name(EN)",
                "Company Code(12)",
                "Ticker(4)",
                "Ticker(5)",
                "Ticker(12)",
                "Sector Code",
                "Sub-Sector Code",
                "Panel Code",
                "WEB-ID",
            ]
        ]
        # change arabic letter to farsi letters nad drop half-spaces:
        df_final["Ticker"] = df_final["Ticker"].apply(
            lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
        )
        df_final["Name"] = df_final["Name"].apply(
            lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
        )
        df_final["Panel"] = df_final["Panel"].apply(
            lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
        )
        df_final["Sector"] = df_final["Sector"].apply(
            lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
        )
        df_final["Sub-Sector"] = df_final["Sub-Sector"].apply(
            lambda x: (str(x).replace("ي", "ی")).replace("ك", "ک")
        )
        df_final["Ticker"] = df_final["Ticker"].apply(
            lambda x: (x.replace("\u200c", " ")).strip()
        )
        df_final["Name"] = df_final["Name"].apply(
            lambda x: (x.replace("\u200c", " ")).strip()
        )
        df_final["Panel"] = df_final["Panel"].apply(
            lambda x: (x.replace("\u200c", " ")).strip()
        )
        df_final["Sector"] = df_final["Sector"].apply(
            lambda x: (x.replace("\u200c", " ")).strip()
        )
        df_final["Sub-Sector"] = df_final["Sub-Sector"].apply(
            lambda x: (x.replace("\u200c", " ")).strip()
        )

        df_final = df_final.set_index("Ticker")
        df_final.drop(columns=["WEB-ID"], inplace=True)
        end_time = time.time()
        if show_progress:
            clear_output(wait=True)
            print(
                "Progress : 100 % , Done in "
                + str(int(round(end_time - start_time, 0)))
                + " seconds!"
            )
        # print(str(int(round(end_time - start_time,0)))+' seconds took to gather detailed data')
        # -------------------------------------------------------------------------------------------------------------------------------------
        # save file if necessary
        if save_excel | save_csv:
            __Save_List__(
                df_data=df_final,
                bourse=bourse,
                farabourse=bourse,
                payeh=payeh,
                detailed_list=detailed_list,
                save_excel=save_excel,
                save_csv=save_csv,
                save_path=save_path,
            )
        return df_final


################################################################################################################################################################################
################################################################################################################################################################################
