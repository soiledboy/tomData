def loseDataDiff():
    import psycopg2
    import pandas.io.sql as psql
    import pandas as pd
    from queries.queryLine_losediffCollector import query07
    from queries.queryLine_losediffCollector import query30
    from queries.queryLine_losediffCollector import query90
    from queries.queryLine_losediffCollector import query180
    # 7 days


    conn = psycopg2.connect(dbname='d4glguq0as3fe6', user='ucjrvhda1pfbv1', password='pa045091d3e285537e8a31eb863c11df1dfa42659a9a447ce231b18f0070892a1', host='ec2-34-195-123-119.compute-1.amazonaws.com', port='5432', sslmode='require')
    cur = conn.cursor()
    print('Connection Established 7 days')
    df = psql.read_sql(query07, conn)
    df = df[~(df['difference']  >= -5)]
    df['period'] = '7'
    df07 = df
    cur.close()
    conn.close()
    print('Connection Closed 7 days')

    # 30 days


    conn = psycopg2.connect(dbname='d4glguq0as3fe6', user='ucjrvhda1pfbv1', password='pa045091d3e285537e8a31eb863c11df1dfa42659a9a447ce231b18f0070892a1', host='ec2-34-195-123-119.compute-1.amazonaws.com', port='5432', sslmode='require')
    cur = conn.cursor()
    print('Connection Established 30 days')
    df = psql.read_sql(query30, conn)
    df = df[~(df['difference']  >= -5)]
    df['period'] = '30'
    df30 = df
    cur.close()
    conn.close()
    print('Connection Closed 30 days')

    # 90 days


    conn = psycopg2.connect(dbname='d4glguq0as3fe6', user='ucjrvhda1pfbv1', password='pa045091d3e285537e8a31eb863c11df1dfa42659a9a447ce231b18f0070892a1', host='ec2-34-195-123-119.compute-1.amazonaws.com', port='5432', sslmode='require')
    cur = conn.cursor()
    print('Connection Established 90 days')
    df = psql.read_sql(query90, conn)
    df = df[~(df['difference']  >= -5)]
    df['period'] = '90'
    df90 = df
    cur.close()
    conn.close()
    print('Connection Closed 90 days')

    # 30 days


    conn = psycopg2.connect(dbname='d4glguq0as3fe6', user='ucjrvhda1pfbv1', password='pa045091d3e285537e8a31eb863c11df1dfa42659a9a447ce231b18f0070892a1', host='ec2-34-195-123-119.compute-1.amazonaws.com', port='5432', sslmode='require')
    cur = conn.cursor()
    print('Connection Established 180 days')
    df = psql.read_sql(query180, conn)
    df = df[~(df['difference']  >= -5)]
    df['period'] = '180'

    df180 = df
    cur.close()
    conn.close()
    print('Connection Closed 180 days')

    df_lose = pd.concat([df07, df30, df90, df180], ignore_index=True, sort=True)
    df_lose.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/losers_totalDiffCollector.csv', encoding='utf-8', index=False)

    df_newActivity = df_lose.drop_duplicates(subset='tcg_product_id', keep='first')
    df_newActivity.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/losersmetaDataDiff_popCollector.csv', encoding='utf-8', index=False)









