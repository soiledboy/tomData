# imports
import psycopg2
import pandas.io.sql as psql
import pandas as pd
from queries.queryLine_win import query_marketTable
import numpy as np
from google.cloud import storage
import os

# connect to Database


conn = psycopg2.connect(dbname='d4glguq0as3fe6', user='ucjrvhda1pfbv1', password='pa045091d3e285537e8a31eb863c11df1dfa42659a9a447ce231b18f0070892a1', host='ec2-34-195-123-119.compute-1.amazonaws.com', port='5432', sslmode='require')
cur = conn.cursor()
print('Connection Established')


# begin Winners Query's


df07 = psql.read_sql(query_marketTable, conn, params={'range': '7'})
df07['period'] = '7'
df30 = psql.read_sql(query_marketTable, conn, params={'range': '30'})
df30['period'] = '30'
df90 = psql.read_sql(query_marketTable, conn, params={'range': '90'})
df90['period'] = '90'
df180 = psql.read_sql(query_marketTable, conn, params={'range': '180'})
df180['period'] = '180'


#Begin Winners Difference Query's


df08 = psql.read_sql(query_marketTable, conn, params={'range': '8'})
df08['period'] = '7'
df31 = psql.read_sql(qquery_marketTable, conn, params={'range': '31'})
df31['period'] = '30'
df91 = psql.read_sql(query_marketTable, conn, params={'range': '91'})
df91['period'] = '90'
df181 = psql.read_sql(query_marketTable, conn, params={'range': '181'})
df181['period'] = '180'


# Winners exports
# Combine into one dataframe


df_winners_total  = pd.concat([df07, df30, df90, df180], ignore_index=True, sort=True)


# Remove <$3 differences & export to csv


df0 = df_winners_total.loc[((df_winners_total['first_price'] < 1) & (df_winners_total['difference'] >= .5)),:]
df1 = df_winners_total.loc[((df_winners_total['first_price'] > 1) & (df_winners_total['first_price'] <= 7) & (df_winners_total['difference'] >= 1)),:]
df2 = df_winners_total.loc[((df_winners_total['first_price'] > 7) & (df_winners_total['first_price'] <= 15) & (df_winners_total['difference'] >= 2)),:]
df3 = df_winners_total.loc[((df_winners_total['first_price'] > 15) & (df_winners_total['first_price'] <= 20) & (df_winners_total['difference'] >= 3)),:]
df4 = df_winners_total.loc[((df_winners_total['first_price'] > 20) & (df_winners_total['first_price'] <= 30) & (df_winners_total['difference'] >= 4)),:]
df5 = df_winners_total.loc[((df_winners_total['first_price'] > 30) & (df_winners_total['first_price'] <= 60) & (df_winners_total['difference'] >= 5)),:]
df6 = df_winners_total.loc[((df_winners_total['first_price'] > 60) & (df_winners_total['first_price'] <= 100) & (df_winners_total['difference'] >= 10)),:]
df7 = df_winners_total.loc[((df_winners_total['first_price'] > 100) & (df_winners_total['difference'] >= 20)),:]

df_winners_total = pd.concat([df0, df1, df2, df3, df4,df5,df6,df7], ignore_index=True)

df_winners_total.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_total.csv', encoding='utf-8', index=False)


# create df and remove duplicates for use in tcgplayer api script to grab additional info on cards & export to csv


metaData_pop = df_winners_total.drop_duplicates(subset='tcg_product_id', keep='first')
metaData_pop.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaData_pop.csv', encoding='utf-8', index=False)


# Winners Diff exports
# Combine into one dataframe


df_winners_total_diff = pd.concat([df08, df31, df91, df181], ignore_index=True, sort=True)


# Remove <$3 differences & export to csv


df8 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] < 1) & (df_winners_total_diff['difference'] >= .5)),:]
df9 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 1) & (df_winners_total_diff['first_price'] <= 7) & (df_winners_total_diff['difference'] >= 1)),:]
df10 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 7) & (df_winners_total_diff['first_price'] <= 15) & (df_winners_total_diff['difference'] >= 2)),:]
df11 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 15) & (df_winners_total_diff['first_price'] <= 20) & (df_winners_total_diff['difference'] >= 3)),:]
df12 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 20) & (df_winners_total_diff['first_price'] <= 30) & (df_winners_total_diff['difference'] >= 4)),:]
df13 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 30) & (df_winners_total_diff['first_price'] <= 60) & (df_winners_total_diff['difference'] >= 5)),:]
df14 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 60) & (df_winners_total_diff['first_price'] <= 100) & (df_winners_total_diff['difference'] >= 10)),:]
df15 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 100) & (df_winners_total_diff['difference'] >= 20)),:]

df_winners_total_diff = pd.concat([df8, df9, df10, df11, df12, df13, df14, df15], ignore_index=True)

df_winners_total_diff.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_totalDiff.csv', encoding='utf-8', index=False)


# create df and remove duplicates for use in tcgplayer api script to grab additional info on cards & export to csv


metaDataDiff_pop = df_winners_total_diff.drop_duplicates(subset='tcg_product_id', keep='first')
metaDataDiff_pop.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaDataDiff_pop.csv', encoding='utf-8', index=False)


# close connection to database


cur.close()
conn.close()
print('Connection Closed')

# winnersMerge.py
df1 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_total.csv')
df2 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaData.csv')
df3 = df1.merge(df2, how='outer', on='tcg_product_id', left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
df3['View'] = 'All Time'
df3['Release Date']= pd.to_datetime(df3['updated_at'])
df3.columns = ['Date', 'Change ($)','Date (1)','Price (1)','High', 'Date (2)','Price (2)','Low','Market','Mid','Name','Set Number','Change (%)','Period','product_id','Set Name','Edition','tcg_product_id','Release Date2','Rarity','imageUrl','tcgplayerUrl','View','Release Date']
df4 = df3.drop(columns=['Release Date2','View'])
df4['View'] = 'All Time'
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

# winnersMergeDiff.py

df1 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_totalDiff.csv')
df2 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaDataDiff.csv')
df3 = df1.merge(df2, how='outer', on='tcg_product_id', left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
df3['View'] = 'Today'
df3['Release Date']= pd.to_datetime(df3['updated_at'])
df3.columns = ['Date', 'Change ($)','Date (1)','Price (1)','High', 'Date (2)','Price (2)','Low','Market','Mid','Name','Set Number','Change (%)','Period','product_id','Set Name','Edition','tcg_product_id','Release Date2','Rarity','imageUrl','tcgplayerUrl','View','Release Date']
df4 = df3.drop(columns=['Release Date2'])
print("merge successful")
df4.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winnersDiff_total2.csv', encoding='utf-8', index=False)


# winnersNewActivityMerge.py
print('Hello Sir! Excellent to see you.... O.o')
df_old = pd.read_csv("/home/tier1marketspace/youtuberReport/scripts/data/winnersDiff_total2.csv")
df_new = pd.read_csv("/home/tier1marketspace/youtuberReport/scripts/data/winners_total2.csv")
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
df_newActivity.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_today.csv', encoding='utf-8', index=False)

#mergeTodayAlltime.py
print('Hello Sir! Excellent to see you.... O.o')
df_newToday = pd.read_csv("/home/tier1marketspace/youtuberReport/scripts/data/winners_today.csv")
df_allTime = pd.read_csv("/home/tier1marketspace/youtuberReport/scripts/data/winners_total2.csv")
df_winners_final = pd.concat([df_allTime, df_newToday], ignore_index=True)
df_winners_final_date = pd.to_datetime(df_winners_final['Date']).dt.normalize()
df_winners_final = df_winners_final.drop(columns=['Date'])
df_winners_final.insert(8, 'Date', df_winners_final_date)
df_winners_final.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_final.csv',date_format='%Y-%m-%d', encoding='utf-8', index=False)


#googleCloud.py


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/tier1marketspace/youtuberReport/fourth-splice-313717-968799892cd2.json"

storage_client = storage.Client()

buckets = list(storage_client.list_buckets())

bucket = storage_client.get_bucket("tom-market-report")
blob = bucket.blob('meta/winners_final.csv')

blob.upload_from_filename('/home/tier1marketspace/youtuberReport/scripts/data/winners_final.csv')

print(buckets)



