def winMerge():
    import pandas as pd
    import numpy as np
    df1 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_total.csv')
    df2 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaData.csv')
    df3 = df1.merge(df2, how='outer', on='tcg_product_id', left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
    df3['View'] = 'All Activity'
    df3['Release Date']= pd.to_datetime(df3['updated_at'])
    df3.columns = ['Date', 'Change ($)','Date (1)','Price (1)','High', 'Date (2)','Price (2)','Low','Market','Mid','Name','Set Number','Change (%)','Period','product_id','Set Name','Edition','tcg_product_id','Release Date2','Rarity','imageUrl','tcgplayerUrl','View','Release Date']
    df4 = df3.drop(columns=['Release Date2','View'])
    df4['View'] = 'All Activity'
    print("merge successful")

    conditions = [
        (df4['Price (1)'] <=1 ),
        (df4['Price (1)'] >1 ) & (df4['Price (1)'] <= 5),
        (df4['Price (1)'] >5 ) & (df4['Price (1)'] <= 10),
        (df4['Price (1)'] >10 ) & (df4['Price (1)'] <= 20),
        (df4['Price (1)'] >20 ) & (df4['Price (1)'] <= 50),
        (df4['Price (1)'] >50 ) & (df4['Price (1)'] <= 100),
        (df4['Price (1)'] >100 ) & (df4['Price (1)'] <= 200),
        (df4['Price (1)'] >200 ),
    ]
    outputs = [
        '< $1', '$1-5', '$5-10', '$10-20', '$20-50', '$50-100', '$100-200','> $200'
    ]

    res = np.select(conditions, outputs,'Other')

    df4['Price Bucket'] = pd.Series(res)

    df4.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_total2.csv', encoding='utf-8', index=False)
winMerge()