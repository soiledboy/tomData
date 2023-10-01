def winData():
    import psycopg2
    import pandas.io.sql as psql
    import pandas as pd
    from queries.queryLine_win import query

    #Connect to Database
    conn = psycopg2.connect(dbname='d4glguq0as3fe6', user='ucjrvhda1pfbv1', password='pa045091d3e285537e8a31eb863c11df1dfa42659a9a447ce231b18f0070892a1', host='ec2-34-195-123-119.compute-1.amazonaws.com', port='5432', sslmode='require')
    cur = conn.cursor()
    print('Connection Established')

    #Begin Query's
    print('working on 7' )
    df07 = psql.read_sql(query, conn, params={'range': '7'})
    df07['period'] = '7'

    print('working on 30' )
    df30 = psql.read_sql(query, conn, params={'range': '30'})
    df30['period'] = '30'

    print('working on 90' )
    df90 = psql.read_sql(query, conn, params={'range': '90'})
    df90['period'] = '90'

    print('working on 180' )
    df180 = psql.read_sql(query, conn, params={'range': '180'})
    df180['period'] = '180'

    #Combine into one dataframe
    win = pd.concat([df07, df30, df90, df180], ignore_index=True, sort=True)

    df0 = win.loc[((win['first_price'] < 1) & (win['difference'] >= .75)),:]
    df1 = win.loc[((win['first_price'] > 1) & (win['first_price'] <= 7) & (win['difference'] >= 1.5)),:]
    df2 = win.loc[((win['first_price'] > 7) & (win['first_price'] <= 15) & (win['difference'] >= 2)),:]
    df3 = win.loc[((win['first_price'] > 15) & (win['first_price'] <= 20) & (win['difference'] >= 3)),:]
    df4 = win.loc[((win['first_price'] > 20) & (win['first_price'] <= 30) & (win['difference'] >= 4)),:]
    df5 = win.loc[((win['first_price'] > 30) & (win['first_price'] <= 60) & (win['difference'] >= 5)),:]
    df6 = win.loc[((win['first_price'] > 60) & (win['first_price'] <= 100) & (win['difference'] >= 10)),:]
    df7 = win.loc[((win['first_price'] > 100) & (win['difference'] >= 20)),:]

    df_winners_total = pd.concat([df0, df1, df2, df3, df4, df5, df6, df7], ignore_index=True)

    # identify partial string
    discard = ["Duelist League"]

    # drop rows that contain the partial string "Duelist League"
    df_winners_total = df_winners_total[~df_winners_total.set_name.str.contains('Duelist League'.join(discard))]

    #export to csv
    df_winners_total.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_total.csv', encoding='utf-8', index=False)

    #create df and remove duplicates for use in tcgplayer api script to grab additional info on cards
    df_forLoop = df_winners_total.drop_duplicates(subset='tcg_product_id', keep='first')
    #export to csv
    df_forLoop.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaData_pop.csv', encoding='utf-8', index=False)

    #Begin Difference Query's
    print('working on 8' )
    df08 = psql.read_sql(query, conn, params={'range': '8'})
    df08['period'] = '7'

    print('working on 31' )
    df31 = psql.read_sql(query, conn, params={'range': '31'})
    df31['period'] = '30'

    print('working on 91' )
    df91 = psql.read_sql(query, conn, params={'range': '91'})
    df91['period'] = '90'

    print('working on 181' )
    df181 = psql.read_sql(query, conn, params={'range': '181'})
    df181['period'] = '180'


    #Close connection to database
    cur.close()
    conn.close()
    print('Connection Closed')

    #Combine into one dataframe
    df_winners_total_diff = pd.concat([df08, df31, df91, df181], ignore_index=True, sort=True)

    df8 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] < 1) & (df_winners_total_diff['difference'] >= .75)),:]
    df9 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 1) & (df_winners_total_diff['first_price'] <= 7) & (df_winners_total_diff['difference'] >= 1.5)),:]
    df10 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 7) & (df_winners_total_diff['first_price'] <= 15) & (df_winners_total_diff['difference'] >= 2)),:]
    df11 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 15) & (df_winners_total_diff['first_price'] <= 20) & (df_winners_total_diff['difference'] >= 3)),:]
    df12 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 20) & (df_winners_total_diff['first_price'] <= 30) & (df_winners_total_diff['difference'] >= 4)),:]
    df13 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 30) & (df_winners_total_diff['first_price'] <= 60) & (df_winners_total_diff['difference'] >= 5)),:]
    df14 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 60) & (df_winners_total_diff['first_price'] <= 100) & (df_winners_total_diff['difference'] >= 10)),:]
    df15 = df_winners_total_diff.loc[((df_winners_total_diff['first_price'] > 100) & (df_winners_total_diff['difference'] >= 20)),:]

    df_winners_total_diff = pd.concat([df8, df9, df10, df11, df12, df13, df14, df15], ignore_index=True)

    # drop rows that contain the partial string "Duelist League"
    df_winners_total_diff = df_winners_total_diff[~df_winners_total_diff.set_name.str.contains('Duelist League'.join(discard))]

    df_winners_total_diff.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_totalDiff.csv', encoding='utf-8', index=False)

    df_newActivity2 = df_winners_total_diff.drop_duplicates(subset='tcg_product_id', keep='first')
    df_newActivity2.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaDataDiff_pop.csv', encoding='utf-8', index=False)


