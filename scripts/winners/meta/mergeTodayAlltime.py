def concatAllTimeToday():
    import pandas as pd
    print('Hello Sir! Excellent to see you.... O.o')
    df_newToday = pd.read_csv("/home/tier1marketspace/youtuberReport/scripts/data/winners_today.csv")
    df_allTime = pd.read_csv("/home/tier1marketspace/youtuberReport/scripts/data/winners_total2.csv")
    df_winners_final = pd.concat([df_allTime, df_newToday], ignore_index=True, sort=True)
    df_winners_final_date = pd.to_datetime(df_winners_final['Date']).dt.normalize()
    df_winners_final = df_winners_final.drop(columns=['Date'])
    df_winners_final.insert(8, 'Date', df_winners_final_date)


    pd.options.mode.chained_assignment = None  # default='warn'
    df_winners_final["alltimeId"] = df_winners_final["product_id"].astype(str) + df_winners_final["Period"].astype(str)
    df_alltime= df_winners_final.drop_duplicates(subset='alltimeId', keep='first')
    alltime_ids = df_alltime["alltimeId"].values.tolist()
    alltime_ids = [x for x in alltime_ids if x != 'nan']
    alltimeDict = {}
    for id in alltime_ids:
        df_forloop = df_winners_final[df_winners_final.alltimeId == id]
        df_forloop.Date = pd.to_datetime(df_forloop.Date, dayfirst=True)
        df_forloop = df_forloop.sort_values('Date').reset_index(drop=True)
        maxrow = df_forloop.loc[df_forloop['Market'].idxmax()]
        if maxrow.Date == df_forloop ['Date'].iloc[-1]:
            alltimeDict[id] = 'On'
        else:
            alltimeDict[id]='Off'
    final = pd.Series(alltimeDict)
    final2 = final.to_frame()
    final2.reset_index(inplace=True)
    final2.columns = ['alltimeId', 'alltime']
    df_winners_final = pd.merge(df_winners_final,
                          final2,
                          on ='alltimeId',
                          how ='inner')

    df_winners_final.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_final.csv',date_format='%Y-%m-%d', encoding='utf-8', index=False)
concatAllTimeToday()