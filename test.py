import pandas as pd

original = 'df_final3.csv'
new = 'new1.csv'
Trading_volume = "거래량(주)"
daily_5 = "직전 5일 대비 당일 거래량"
daily_20 = "직전 20일 대비 당일 거래량"
unname = "Unnamed: 0"

def fusion():

    return




if __name__ == "__main__":

    dfOriginal = pd.read_csv(original)
    dfNew = pd.read_csv(new)

    is_0_row = dfOriginal.loc[dfOriginal[Trading_volume] == 0]
    indexList = is_0_row.index.tolist()
    row = is_0_row.shape[0]
    list0 = [0 for index in range(row)]
    df5 = pd.DataFrame(list0, columns=[daily_5])
    df20 = pd.DataFrame(list0, columns=[daily_20])
    s = pd.Series(indexList)
    df5 = df5.set_index([s], "id")
    df20 = df20.set_index([s], "id")

    is_0_row = pd.concat([is_0_row, df5, df20], axis=1, ignore_index=False)
    is_0_row = is_0_row.rename(columns={unname:"old_id"})
    no_0_row = pd.read_csv("new1.csv", index_col=False)
    # no_0_row = no_0_row.drop([unname], axis=1)

    no_0_row_index = no_0_row["old_id"]
    no_0_row = no_0_row.set_index([no_0_row_index], "id")
    df = pd.concat([no_0_row, is_0_row])
    df = df.sort_index()
    # df.to_csv("new_5daily_20daily.csv")
    print("finished")