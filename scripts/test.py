import requests
import json
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
attributeL = []
cardTypeL = []
descriptionL = []
imageUrlL= []
tcgplayerUrlL =[]
tcgplayer_productIdL = []
def Convert(string):
    li = list(string.split(" "))
    return li
df = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/winners_total.csv')
#df_forLoop = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/test.csv')
df_forLoop = df.drop_duplicates(subset='tcg_product_id', keep='first')
for row in df_forLoop.itertuples():
    productId = str(row.tcg_product_id)
    print("card %s" % (productId))
    url = "https://api.tcgplayer.com/catalog/products/%s?getExtendedFields=true" % (productId)
    headers = {"Accept": "application/json"}
    response = requests.request("GET", url, headers = {"Authorization": "bearer %s" % (access_token)})
    json_data = response.json()
    a = (json_data['results'])
    for b in a:
        data = json.dumps(a)
        df_data = pd.read_json(data)
        df_data.to_dict(orient='list')
        imageUrl = (df_data.get('imageUrl'))
        imageUrl = imageUrl.to_string(buf=None, na_rep='NaN', float_format=None, header=False, index=False, length=False, dtype=False, name=False)
        url = (df_data.get('url'))
        url = url.to_string(buf=None, na_rep='NaN', float_format=None, header=False, index=False, length=False, dtype=False, name=False)
        tcgplayer_productId = (df_data.get('productId'))
        tcgplayer_productId = productId
        l = (b['extendedData'])
        for x in l:
            extendedData = json.dumps(l)
            df_extendedData = pd.read_json(extendedData)
            df_extendedData = df_extendedData.transpose()
            new_header = df_extendedData.iloc[0] #grab the first row for the header
            df_extendedData = df_extendedData.iloc[-1:] #take the data less the header row
            df_extendedData.columns = new_header #set the header row as the df header
            df_extendedData.to_dict(orient='list')
            rarity = (df_extendedData.get('Rarity'))
            rarity = rarity.to_string(buf=None, na_rep='NaN', float_format=None, header=False, index=False, length=False, dtype=False, name=False)
            cardType = (df_extendedData.get('Card Type'))
            if cardType is None:
                cardType="N/A"
                attribute = "N/A"
                description = "N/A"
                continue
            cardType = cardType.to_string(buf=None, na_rep='NaN', float_format=None, header=False, index=False, length=False, dtype=False, name=False)
            attribute = (df_extendedData.get('Attribute'))
            if attribute is None:
                attribute ="N/A"
                description = "N/A"
                continue
            attribute = attribute.to_string(buf=None, na_rep='NaN', float_format=None, header=False, index=False, length=False, dtype=False, name=False)
    imageUrlL.append(imageUrl)
    tcgplayerUrlL.append(url)
    tcgplayer_productIdL.append(productId)
    rarityL.append(rarity)
    cardTypeL.append(cardType)
    attributeL.append(attribute)

df_metaData = pd.DataFrame({
'rarity': rarityL,
'attribute': attributeL,
'cardType': cardTypeL,
'imageUrl': imageUrlL,
'tcgplayerUrl': tcgplayerUrlL,
'tcg_player_productId': tcgplayer_productIdL,
})
#df_metaData= db_df.drop_duplicates()
df_metaData.to_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/metaData.csv', encoding='utf-8', index=False)