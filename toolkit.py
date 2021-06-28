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

def mergeCol(df):
    return 0



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

def splitDFGroup(df:pd.DataFrame, num_class):
    multiThread = num_class
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

    return dfs

def xDaysRateoReStdDevByMulThread(df:pd.DataFrame, xDay):
    global symbol
    threads = []
    multiThread = 16

    dfs = splitDFGroup(df, multiThread)

    new_dfs = []
    for i in range(len(dfs)):
        new_dfs.append((dfs[i], xDay))
    pool = multiprocessing.Pool(16)
    res = pool.map(run_function, new_dfs)
    pool.close()
    pool.join()

    return_list = [token for rlist in res for token in rlist]

    return return_list


def mulThreadRunFunction(dfs, args, func):
    global symbol
    threads = []
    multiThread = 16
    print(func)
    new_dfs = []
    for i in range(len(dfs)):
        new_dfs.append((dfs[i], args))
    pool = multiprocessing.Pool(16)
    res = pool.map(func, new_dfs)
    pool.close()
    pool.join()

    return_list = [token for rlist in res for token in rlist]

    return return_list
