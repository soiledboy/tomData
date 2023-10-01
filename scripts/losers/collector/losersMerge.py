def loseMerge():
    import pandas as pd
    df1 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/losers_totalCollector.csv')
    df2 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/losersmetaDataCollector.csv')
    df3 = df1.merge(df2, how='outer', on='tcg_product_id', left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
    df3['View'] = 'All Time'
    df3['Release Date']= pd.to_datetime(df3['updated_at'])
    df3.columns = ['Date', 'Change ($)','Date (1)','Price (1)','High', 'Date (2)','Price (2)','Low','Market','Mid','Name','Set Number','Change (%)','Period','product_id','Set Name','Edition','tcg_product_id','Release Date2','Rarity','imageUrl','tcgplayerUrl','View','Release Date']
    df4 = df3.drop(columns=['Release Date2','View'])
    df4['View'] = 'All Time'
    print("merge successful")
    df4.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/losers_total2Collector.csv', encoding='utf-8', index=False)
