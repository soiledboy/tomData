def loseNewActivity():
    import pandas as pd
    print('Hello Sir! Excellent to see you.... O.o')
    df_old = pd.read_csv("/home/tier1marketspace/youtuberReport/scripts/data/losersDiff_total2Meta.csv")
    df_new = pd.read_csv("/home/tier1marketspace/youtuberReport/scripts/data/losers_total2Meta.csv")
    unique_new = (df_new['product_id']).unique()
    df_new2 = pd.DataFrame(unique_new, columns = ['product_id'])
    unique_old = (df_old['product_id']).unique()
    df_old2 = pd.DataFrame(unique_old, columns = ['product_id'])
    df_new2['key1'] = 1
    df_old2['key2'] = 1
    df_new3 = pd.merge(df_new2, df_old2, on=['product_id'], how = 'left')
    df_new3 = df_new3[~(df_new3.key2 == df_new3.key1)]
    df_new3 = df_new3.drop(['key1','key2'], axis=1)
    df_newActivity = df_new.merge(df_new3, on=['product_id'])
    df_newActivity['View'] = df_newActivity['View'].replace(['All Time'],'Today')
    df_newActivity.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/losers_todayMeta.csv', encoding='utf-8', index=False)