def setData():
    import psycopg2
    import pandas.io.sql as psql
    from queries.set_price import querySet, querySetCards
    import pandas as pd

    conn = psycopg2.connect(dbname='d4glguq0as3fe6', user='ucjrvhda1pfbv1', password='pa045091d3e285537e8a31eb863c11df1dfa42659a9a447ce231b18f0070892a1', host='ec2-34-195-123-119.compute-1.amazonaws.com', port='5432', sslmode='require')
    cur = conn.cursor()
    df1 = psql.read_sql(querySet, conn)
    df = psql.read_sql(querySetCards, conn)
    df.date=pd.to_datetime(df.date)#Coaerce Date to Datetime
    df2 = df.groupby('name')['date'].agg(['min','max']).rename(columns={'min':'first','max':'last'}).stack().reset_index()
    df2.columns = ['name', 'price','date']
    df2.set_index(df2.date, inplace=True)#Set Date as index
    df2.columns = ['name', 'price','date']
    df2.index.name = None
    df3 = df.merge(df2, how='outer')
    df4 = df3.dropna()
    df4 = df4.reset_index(drop = True)
    df4['Set Number'] = df4['number']
    df5 = df4.drop(columns=['abbreviation','number'])
    df_first = df5[df5['price']=='first']
    df_first.columns = ['Date', 'Market','product_id','Name','Set Name','price', 'Set Number']
    df_last = df5[df5['price']=='last']
    df_last.columns = ['Date', 'Market','product_id','Name','Set Name','price', 'Set Number']
    df_last2 = df_last.drop(columns=['Name','Set Number','price', 'Set Name'])
    df6 = df_first.merge(df_last2, left_on='product_id', right_on='product_id')
    df6.columns = ['Date (1)','Price (1)','Product Id','Name','Set Name','price','Set Number', 'Date (2)','Price (2)']
    df7 = df6.drop(columns=['price','Product Id'])
    df7['$ Change'] = ( df7['Price (2)'] - df7['Price (1)'] )
    df8= df7.round(2)
    df9 = pd.DataFrame({'Name':['Name'],'Set Number':['Set Number'],'Set Name':['Set Name'],'Price (1)':['Price (1)'],'Date (1)':['Date (1)'],'Price (2)':['Price (2)'],'Date (2)':['Date (2)'],'$ Change':['$ Change']}, index=[0])
    df9 = df9.append(df8,sort= False)
    df9 = df9.reset_index(drop=True)
    df9['Date (1)'] = df9['Date (1)'].astype(str)
    df9['Date (2)'] = df9['Date (2)'].astype(str)
    df9['Date (1)'] = df9['Date (1)'].str[:11]
    df9['Date (2)'] = df9['Date (2)'].str[:11]
    df10 = df9.iloc[1: , :]
    df10.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/setDataCards_final.csv', encoding='utf-8', index=False)
    df1.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/setData.csv', encoding='utf-8', index=False)
    cur.close()
    conn.close()
    print('Connection Closed')
setData()


