import pandas as pd

from toolkit import *
import trading_calendars as tc
from datetime import timedelta, date
from datetime import datetime

dateData = "date"




def find_pre_x_month_end(dd:date, xMonth):
    oneDay = timedelta(days=1)
    XKRX = tc.get_calendar("XKRX")

    if dd.month - xMonth + 1 > 0:
        month_end = date(dd.year, dd.month - xMonth + 1, 1) - oneDay
    else:
        month_end = date(dd.year - 1, dd.month - xMonth + 13, 1) - oneDay
    while not XKRX.is_session(pd.Timestamp(month_end)):
        month_end -= oneDay
    return month_end



def find_next_x_month_end(dd:date, xMonth):
    oneDay = timedelta(days=1)
    XKRX = tc.get_calendar("XKRX")

    if dd.month + xMonth <= 12:
        month_end = date(dd.year, dd.month + xMonth, 1) - oneDay
    else:
        month_end = date(dd.year + 1, dd.month + xMonth - 12, 1) - oneDay
    while not XKRX.is_session(pd.Timestamp(month_end)):
        month_end -= oneDay
    return month_end

def init_zero(df:pd.Series, xMonth):
    df_first_day = df[df.index[0]]
    first_day = date(df_first_day.year, df_first_day.month, df_first_day.day)
    month_end = find_next_x_month_end(first_day, xMonth)
    df_end = df[df.index[len(df.index)-1]]
    end_day = date(df_end.year, df_end.month, df_end.day)
    if month_end >= end_day:
        return [0 for index in range(df.shape[0])]
    else:
        a = df[df == pd.to_datetime(str(month_end))].index.to_list()[0]
        return [0 for index in range(a+1)]


def mom_x_m(df, xMonth):
    groups = df.groupby(symbol)
    return_list = []
    for group in groups:
        mom_x_g = init_zero(group[1][dateData], xMonth)
        # len(mom_x_g) is no
        if len(mom_x_g) == group[1].shape[0]:
            print("Symbol is too sort! only {}".format(len(mom_x_g)))
            return_list.extend(mom_x_g)
        else:
            for i in range(len(mom_x_g), len(group[1])):
                pre_month_end = find_pre_x_month_end(group[1][dateData][i], xMonth)
                modified_price_pre_x_month_end = group[1][modified_price][group[1][dateData] == pd.to_datetime(str(pre_month_end))].to_list()[0]
                modified_price_today = group[1][modified_price][i]
                mom_x_g.append(modified_price_today/modified_price_pre_x_month_end-1)

            return_list.extend(mom_x_g)
    print("finished 1!")
    return return_list


def multi_mom_x_m(data_list):
    return mom_x_m(data_list[0], data_list[1])






if __name__ == "__main__":


    # df = readCSVFile("test.csv")
    df = readCSVFile("new_with_mv_std.csv")

    print("File load finished!")
    print("---------"*3)
    df = df[[dateData, symbol, modified_price]]
    print("Extract Data")
    print("---------"*3)
    print("Test Outlier Value!")
    count = df[modified_price].isnull().sum()
    print("{} datas Outlier Value!".format(count))
    print("---------"*3)
    print("Start Calculation")
    # dfSplit = splitDFGroup(df, 16)
    # df_date = df[date].apply(str)
    # df_date = df.date
    df.date = pd.to_datetime(df.date, format="%Y-%m-%d")
    dfs = splitDFGroup(df, 16)

    mom_1_m = mulThreadRunFunction(dfs, 1, multi_mom_x_m)
    mom_6_m = mulThreadRunFunction(dfs, 6, multi_mom_x_m)
    mom_12_m = mulThreadRunFunction(dfs, 12, multi_mom_x_m)


    year = df.date.dt.year
    month = df.date.dt.month
    task = df.date[0]
    targetTask = str(task.year) + "-" + str(task.month+1) + "-" + str(task.day)
    count = df[df.date.dt.year == task.year and df.date.dt.month == task.month]
    print()
    


