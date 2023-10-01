def winMetaData():
    import requests
    import pandas as pd

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
    df_forLoop= pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaData_pop.csv')
    #df_forLoop= pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/test.csv')
    for row in df_forLoop.itertuples():
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
    df_metaData.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaData.csv', encoding='utf-8', index=False)
winMetaData()