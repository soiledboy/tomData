# imports
import psycopg2
import pandas.io.sql as psql
import pandas as pd
from queries.queryLine_win import query_marketTable
import numpy as np
from google.cloud import storage
import os
import requests
# connect to Database


conn = psycopg2.connect(dbname='d4glguq0as3fe6', user='ucjrvhda1pfbv1', password='pa045091d3e285537e8a31eb863c11df1dfa42659a9a447ce231b18f0070892a1', host='ec2-34-195-123-119.compute-1.amazonaws.com', port='5432', sslmode='require')
cur = conn.cursor()
print('Connection Established')


# begin Winners Query's
print("working on winners querys")

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
df31 = psql.read_sql(query_marketTable, conn, params={'range': '31'})
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

df_winners_total.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_total_marketTable.csv', encoding='utf-8', index=False)


# create df and remove duplicates for use in tcgplayer api script to grab additional info on cards & export to csv


metaData_pop = df_winners_total.drop_duplicates(subset='tcg_product_id', keep='first')
#metaData_pop.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaData_pop.csv', encoding='utf-8', index=False)


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

df_winners_total_diff.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_totalDiff_marketTable.csv', encoding='utf-8', index=False)


# create df and remove duplicates for use in tcgplayer api script to grab additional info on cards & export to csv


metaDataDiff_pop = df_winners_total_diff.drop_duplicates(subset='tcg_product_id', keep='first')
#metaDataDiff_pop.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaDataDiff_pop.csv', encoding='utf-8', index=False)


# close connection to database
cur.close()
conn.close()
print('Connection Closed')

print("working on meta data")
#Meta Data
url = 'https://api.tcgplayer.com/token'
res = requests.post(url,
data=dict(
grant_type='client_credentials',
client_id='1E702034-6BE9-44CB-A4AD-23AF260417E2',
client_secret='7FA38F49-67B4-481D-A9F1-2BDB9810B2D7'))
data = res.json()
access_token = data['access_token']
pd.set_option('display.max_colwidth', 1000)
rarityL = []

imageUrlL= []
tcgplayerUrlL =[]
tcgplayer_productIdL = []
for row in metaData_pop.itertuples():
    productId = str(row.tcg_product_id)
    print('Working')
    url = "https://api.tcgplayer.com/catalog/products/%s?getExtendedFields=true" % (productId)
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers = {"Authorization": "bearer %s" % (access_token)})
    json_data = response.json()
    a = (json_data['results'])
    df_data = pd.DataFrame(a)
    dataDict= df_data.to_dict(orient='list')
    imageUrl = (dataDict['imageUrl'])
    imageUrl = (imageUrl[0])
    tcgplayerUrl = (dataDict['url'])
    tcgplayerUrl = (tcgplayerUrl[0])
    b = (json_data['results'][0]['extendedData'])
    df_extendedData = pd.DataFrame(b)
    df_extendedData = df_extendedData.transpose()
    new_header = df_extendedData.iloc[0] #grab the first row for the header
    df_extendedData = df_extendedData.iloc[-1:] #take the data less the header row
    df_extendedData.columns = new_header #set the header row as the df header
    extendedDict= df_extendedData.to_dict(orient='list')
    rarity = (extendedDict['Rarity'])
    rarity = (rarity[0])
    imageUrlL.append(imageUrl)
    tcgplayerUrlL.append(tcgplayerUrl)
    tcgplayer_productIdL.append(productId)
    rarityL.append(rarity)

df_metaData = pd.DataFrame({
'rarity': rarityL,
'imageUrl': imageUrlL,
'tcgplayerUrl': tcgplayerUrlL,
'tcg_product_id': tcgplayer_productIdL,
})
#df_metaData= db_df.drop_duplicates()
df_metaData.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaData_marketTable.csv', encoding='utf-8', index=False)

# winnersMerge.py
df1 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_total_marketTable.csv')
df2 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaData_marketTable.csv')
df3 = df1.merge(df2, how='outer', on='tcg_product_id', left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
df3['View'] = 'All Time'
df3['Release Date']= pd.to_datetime(df3['updated_at'])
df3.columns = ['Change ($)','Date (1)','Price (1)', 'Date (2)','Price (2)','Name','Set Number','Change (%)','Period','product_id','SetName','Edition','tcg_product_id','Release Date2','Rarity','imageUrl','tcgplayerUrl','View','Release Date']
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

winners_total2 = df4

winners_total2.head(10)
rarityL2 = []
imageUrlL2= []
tcgplayerUrlL2 =[]
tcgplayer_productIdL2 = []
print("working on meta data DIFF")
for row in metaDataDiff_pop.itertuples():
    productId = str(row.tcg_product_id)
    print('Working')
    url = "https://api.tcgplayer.com/catalog/products/%s?getExtendedFields=true" % (productId)
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers = {"Authorization": "bearer %s" % (access_token)})
    json_data = response.json()
    a = (json_data['results'])
    df_data = pd.DataFrame(a)
    dataDict= df_data.to_dict(orient='list')
    imageUrl = (dataDict['imageUrl'])
    imageUrl = (imageUrl[0])
    tcgplayerUrl = (dataDict['url'])
    tcgplayerUrl = (tcgplayerUrl[0])
    b = (json_data['results'][0]['extendedData'])
    df_extendedData = pd.DataFrame(b)
    df_extendedData = df_extendedData.transpose()
    new_header = df_extendedData.iloc[0] #grab the first row for the header
    df_extendedData = df_extendedData.iloc[-1:] #take the data less the header row
    df_extendedData.columns = new_header #set the header row as the df header
    extendedDict= df_extendedData.to_dict(orient='list')
    rarity = (extendedDict['Rarity'])
    rarity = (rarity[0])
    imageUrlL2.append(imageUrl)
    tcgplayerUrlL2.append(tcgplayerUrl)
    tcgplayer_productIdL2.append(productId)
    rarityL2.append(rarity)

df_metaData = pd.DataFrame({
'rarity': rarityL2,
'imageUrl': imageUrlL2,
'tcgplayerUrl': tcgplayerUrlL2,
'tcg_product_id': tcgplayer_productIdL2,
})

df_metaData.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaDataDiff_marketTable.csv', encoding='utf-8', index=False)
print("working on merge")
# winnersMergeDiff.py
df1 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_totalDiff_marketTable.csv')
df2 = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaDataDiff_marketTable.csv')
df3 = df1.merge(df2, how='outer', on='tcg_product_id', left_on=None, right_on=None, left_index=False, right_index=False, sort=False, suffixes=('_x', '_y'), copy=True, indicator=False, validate=None)
df3['View'] = 'Today'
df3['Release Date']= pd.to_datetime(df3['updated_at'])
df3.columns = ['Change ($)','Date (1)','Price (1)', 'Date (2)','Price (2)','Name','Set Number','Change (%)','Period','product_id','SetName','Edition','tcg_product_id','Release Date2','Rarity','imageUrl','tcgplayerUrl','View','Release Date']
winnersDiff_total2 = df3.drop(columns=['Release Date2'])
print("merge successful")

# winnersNewActivityMerge.py
print('Hello Sir! Excellent to see you.... O.o')
df_old = winnersDiff_total2
df_new = winners_total2
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
print("working on merge all time")
#mergeTodayAlltime.py
print('Hello Sir! Excellent to see you.... O.o')
df_newToday = df_newActivity
df_allTime = winners_total2
df_winners_final = pd.concat([df_allTime, df_newToday], ignore_index=True)
df_winners_final = df_winners_final.drop_duplicates(subset=['tcg_product_id','Period'], keep='first')

# identify partial string
discard = ["Duelist League"]

# drop rows that contain the partial string "Duelist League"
df_winners_final = df_winners_final[~df_winners_final.SetName.str.contains('Duelist League'.join(discard))]

df_winners_final.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/marketTable.csv',date_format='%Y-%m-%d', encoding='utf-8', index=False)

print("sending to cloud....O3O3O3o3oOO3o33")

#googleCloud.py


os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="/home/tier1marketspace/youtuberReport/fourth-splice-313717-968799892cd2.json"

storage_client = storage.Client()

buckets = list(storage_client.list_buckets())

bucket = storage_client.get_bucket("tom-market-report")
blob = bucket.blob('meta/marketTable.csv')

blob.upload_from_filename('/home/tier1marketspace/youtuberReport/scripts/data/marketTable.csv')

print(buckets)

print("all done sir! O.o let me know if you need anything else")


#Export to csv for google sheets

#df_winners_final[df_winners_final.Period == '7']

#cust_sell = mainDf[mainDf.Type == 'S']
#cust_buy = mainDf[mainDf.Type == 'P']



#ATH7 = df_winners_final[df_winners_final.Period == '7']

#ATH30 = df_winners_final[df_winners_final.Period == '7']

#ATH90 = df_winners_final[df_winners_final.Period == '7']

#ATH180 = df_winners_final[df_winners_final.Period == '7']

ALL7 = df_winners_final[df_winners_final.Period == '7']

ALL30 = df_winners_final[df_winners_final.Period == '30']

ALL90 = df_winners_final[df_winners_final.Period == '90']

ALL180 = df_winners_final[df_winners_final.Period == '180']


ALL7.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/ALL7.csv',date_format='%Y-%m-%d', encoding='utf-8', index=False)

ALL30.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/ALL30.csv',date_format='%Y-%m-%d', encoding='utf-8', index=False)

ALL90.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/ALL90.csv',date_format='%Y-%m-%d', encoding='utf-8', index=False)

ALL180.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/ALL180.csv',date_format='%Y-%m-%d', encoding='utf-8', index=False)

















































