import gspread
#import pygsheets
import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

df_winners_final = pd.read_csv(r'/home/tier1marketspace/youtuberReport/scripts/data/marketTable.csv')

df_winners_final = df_winners_final.fillna('')

df_winners_final = df_winners_final.sort_values('Period', ascending=False)

df_winners_final = df_winners_final.drop_duplicates(subset=['tcg_product_id'], keep='first')





ALL7 = df_winners_final[(df_winners_final['Period'] == 7)]

ALL30 = df_winners_final[(df_winners_final['Period'] == 30)]

ALL90 = df_winners_final[(df_winners_final['Period'] == 90)]

ALL180 = df_winners_final[(df_winners_final['Period'] == 180)]



































SCOPES = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
credentials = ServiceAccountCredentials.from_json_keyfile_name("market-spreadsheet-18100df5195e.json", SCOPES)
connection = gspread.authorize(credentials)


# 7 days

worksheet7 = connection.open("Tier One Marketspace - Market Report").get_worksheet(1)

worksheet7.update([ALL7.columns.values.tolist()] + ALL7.values.tolist())

#30 Days

worksheet30 = connection.open("Tier One Marketspace - Market Report").get_worksheet(2)

worksheet30.update([ALL30.columns.values.tolist()] + ALL30.values.tolist())

#90 Days

worksheet90 = connection.open("Tier One Marketspace - Market Report").get_worksheet(3)

worksheet90.update([ALL90.columns.values.tolist()] + ALL90.values.tolist())

#180 Days

worksheet180 = connection.open("Tier One Marketspace - Market Report").get_worksheet(4)

worksheet180.update([ALL180.columns.values.tolist()] + ALL180.values.tolist())













print("Done updating, check the spreadsheet now")