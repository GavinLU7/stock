# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.
import multiprocessing
import os

import pandas as pd
import numpy as np
import statistics
import threading
import multiprocessing

max_cpu = os.cpu_count() - 4

Trading_volume = "거래량(주)"
symbol = "Symbol"
daily_5 = "직전 5일 대비 당일 거래량"
daily_20 = "직전 20일 대비 당일 거래량"
modified_price = "수정고가(원)"
sp = "sp"
mv_std_20 = "직전 20일 수익률 표준편차"
mv_std_60 = "직전 60일 수익률 표준편차"

def no_zero_data(df, col):
    return df.loc[df[col] == 0].index.tolist()

def readCSVFile(path):
    return pd.read_csv(path, index_col=0)

def splitSymbol(df:pd.DataFrame, columns):
    symbolList = df[columns].tolist()
    symbolList = list(set(symbolList))
    symbolList.sort()
    return symbolList

def splitTradingVolumeDataBySymbol(df:pd.DataFrame, colume):
    return df.groupby(colume)

def sumPreTradingVolumeXdays(data, index, days):
    return statistics.mean(data[index-days:index])

def dailyTradingVolumeByDays(groupSymbolTradingV, days):
    symbolsDailyTradingVolumeByXDays = []
    for symbolTradingV in groupSymbolTradingV:
        TradingVList = symbolTradingV[1][Trading_volume].tolist()
        lenth = len(TradingVList)
        if lenth < days:
            days = lenth
            print("No consecutive {} days".format(days))
        dailyTradingVolumeByXDays = [0 for index in range(days)]
        for index in range(days, lenth):
            dailyTradingVolumeByXDays.append(TradingVList[index] / sumPreTradingVolumeXdays(TradingVList, index, days))
        symbolsDailyTradingVolumeByXDays.append(dailyTradingVolumeByXDays)

    return symbolsDailyTradingVolumeByXDays

def transForm(listForm):
    newForm = []
    for datalist in listForm:
        newForm.append(datalist)
    return newForm

def calculXDaysRateoRe(moified_price_list, index):
    preDay = moified_price_list[index - 1]
    return (moified_price_list[index] - preDay)/preDay

def calculXDaysRateoReStdDev(rateoRe, index, days):
    sub_list = rateoRe[index-days:index]
    return np.std(sub_list, ddof=1)

def threadXDaysRateoReStdDevByMulTread(df, days):
    global symbol
    groups = df.groupby(symbol)
    return_list = []
    for group in groups:
        modified_price_list = group[1][modified_price].tolist()
        lenth = len(modified_price_list)
        rateoRe_list = []
        group_list = []
        if lenth > days + 1:
            rateoRe_list.extend([0])
            group_list.extend([0 for index in range(days + 1)])
            for index in range(1, lenth):
                rateoRe_list.append(calculXDaysRateoRe(modified_price_list, index))
            for index in range(days + 1, lenth):
                group_list.append(calculXDaysRateoReStdDev(rateoRe_list, index, days))
            return_list.extend(group_list)

        else:
            group_list.extend([0 for index in range(lenth)])
            return_list.extend(group_list)
    return return_list
def run_function(data_list):
    return threadXDaysRateoReStdDevByMulTread(data_list[0], data_list[1])

def xDaysRateoReStdDevByMulThread(df:pd.DataFrame, xDay):
    global symbol
    threads = []
    xDaysRateoReStdDevList = []
    multiThread = 16
    groups = df.groupby(symbol)
    length = len(groups.indices)
    mod = length % multiThread

    if mod != 0:
        round1 = length // multiThread + 1
    else:
        round1 = length // multiThread

    dfs = [pd.DataFrame([]) for index in range(multiThread)]

    count = 0
    for group in groups:
        circle = count // round1
        print("{} circle {} time".format(circle, count % round1))
        count += 1
        dfs[circle] = pd.concat([dfs[circle], group[1]])

    # manager = Manager()
    # return_list = manager.list()
    new_dfs = []
    for i in range(len(dfs)):
        new_dfs.append((dfs[i], xDay))
    pool = multiprocessing.Pool(16)
    res = pool.map(run_function, new_dfs)
    pool.close()
    pool.join()

    return_list = [token for rlist in res for token in rlist]

    return return_list

if __name__ == '__main__':

    filePath = "new_5daily_20daily.csv"
    # filePath1 = "new.csv"
    # filePath = "df_final3.csv"
    # filePath = "test.csv"

    # csvFilePath = "new.csv"
    df = readCSVFile(filePath)
    # df1 = pd.read_csv(filePath1)
    # df20 = df1[mv_std_20]
    # df20 = pd.DataFrame(df20, columns=[mv_std_20])

    # df60 = df1[mv_std_60]
    # df60 = pd.DataFrame(df60, columns=[mv_std_60])

    # df = pd.concat([df, df20, df60], axis=1)
    # df.to_csv("new.csv", index=False)

    # print()


    print("File load finished!")
    print("---------"*3)
    df = df[[symbol, modified_price]]
    print("Extract Data")
    print("---------"*3)
    print("Test Outlier Value!")
    count = df[modified_price].isnull().sum()
    print("{} datas Outlier Value!".format(count))
    print("---------"*3)
    print("Start Calculation")
    xDaysRateoReStdDevByMulThreadList_20 = xDaysRateoReStdDevByMulThread(df, 20)
    print()
    # xDaysRateoReStdDevByMulThreadList_20 = threadXDaysRateoReStdDevByMulTread(df, 20)
    # df20 = pd.DataFrame(xDaysRateoReStdDevByMulThreadList_20, columns=[mv_std_20])
    # print("20 Day Rate of Re Std Dev is OK")

    # xDaysRateoReStdDevByMulThreadList_60 = threadXDaysRateoReStdDevByMulTread(df, 60)
    # df60 = pd.DataFrame(xDaysRateoReStdDevByMulThreadList_60, columns=[mv_std_60])
    # print("60 Day Rate of Re Std Dev is OK")


    # print("---------"*3)
    # print("Start to transform")
    # xDaysRateoReStdDevByMulThreadList_60 = transForm(xDaysRateoReStdDevByMulThreadList_60)
    # print("Transform dome.")
    # print("---------"*3)
    # print("Start to be DataFrame")
    # print("DataFrame Done")
    # print("---------"*3)




    """
    symbolList = splitSymbol(df, "Symbol")



    dfbyGroups = PEdf.groupby("Symbol")
    for oneGroup in dfbyGroups:
        a = oneGroup[1]
        calculateXdaysStdDev(a, 20)
    """

    # noTrading_volume_lsit = no_zero_data(df, Trading_volume)
    # print("No Trading Volume list got")
    # df = df.drop(noTrading_volume_lsit)
    # print("Delete No Trading Volume.")
    # df = df.reset_index()
    # print("Reset index")
    # df = df.rename(columns={"index":"old_id"})
    # print("Rename index")
    # dfsplit = df[[Symbol, Trading_volume]]



    # csvFilePath = "delete_0_noTrading.csv"
    # csvFile = readCSVFile(csvFilePath)

    # symbolList = splitSymbol(df, Symbol)
    # print("symbol list split finished.")


    # symbol_num = len(symbol)
    # tradingVolumeDataGroup = splitTradingVolumeDataBySymbol(dfsplit, Symbol)
    # print("Split Trading Volume Data")
    # dailyTradingVolumeBy5Days = dailyTradingVolumeByDays(tradingVolumeDataGroup, 5)
    # print("Daily Trading Volume 5 days")
    # dailyTradingVolumeBy20Days = dailyTradingVolumeByDays(tradingVolumeDataGroup, 20)
    # print("Daily Trading Volume 20 days")
    # dailyTradingVolumeBy5Days = transForm(dailyTradingVolumeBy5Days)
    # print("TransForm 5 days")
    # dailyTradingVolumeBy20Days = transForm(dailyTradingVolumeBy20Days)
    # print("TransForm 20 days")
    # df5 = pd.DataFrame(dailyTradingVolumeBy5Days, columns=[daily_5])
    # print("df5")
    # df20 = pd.DataFrame(dailyTradingVolumeBy20Days, columns=[daily_20])
    # print("df20")
    # df = pd.concat([df, df20, df60], axis=1)
    # print("df OK!")
    # df.to_csv("new.csv", index=False)
    # noTrading_volume_lsit = csvFile.loc[csvFile[Trading_volume] == 0].index.tolist()
    # csvFile = csvFile.drop(noTrading_volume_lsit)
    # csvFile = csvFile.reset_index()
    # csvFile = csvFile.drop(columns="index")
    # csvFile = csvFile.rename(columns={"Unnamed: 0" : "old_id"})
    # csvFile.to_csv("delete_0_noTrading.csv")

    print("finished!")



# See PyCharm help at https://www.jetbrains.com/help/pycharm/
