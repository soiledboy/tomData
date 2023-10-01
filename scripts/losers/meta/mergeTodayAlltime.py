def concatAllTimeToday():
    import pandas as pd
    print('Hello Sir! Excellent to see you.... O.o')
    df_newToday = pd.read_csv("/home/tier1marketspace/youtuberReport/scripts/data/losers_todayMeta.csv")
    df_allTime = pd.read_csv("/home/tier1marketspace/youtuberReport/scripts/data/losers_total2Meta.csv")
    df_losers_final = pd.concat([df_allTime, df_newToday], ignore_index=True)
    df_losers_final_date = pd.to_datetime(df_losers_final['Date']).dt.normalize()
    df_losers_final = df_losers_final.drop(columns=['Date'])
    df_losers_final.insert(8, 'Date', df_losers_final_date)
    df_losers_final.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/losers_finalMeta.csv',date_format='%Y-%m-%d', encoding='utf-8', index=False)