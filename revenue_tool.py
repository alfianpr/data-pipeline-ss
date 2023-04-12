import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials
import numpy as np
from datetime import date
from dateutil.relativedelta import relativedelta
from cred import cred

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vR0pfMgEgoaH5MV38SxPYONIoNQVyj0Y2j2Aiz_ROGt3Xjcd8zmtMuOHBetfs-U2UKmenZe1c7u5Yp4/pub?gid=1676381659&single=true&output=csv")

credentials = ServiceAccountCredentials.from_json_keyfile_dict(cred, scope)
gc = gspread.authorize(credentials)

# clean column

replace = {' ' : '_', '-' : '_', '.' : '_', '?' : '', '[' : '', ']' : '', '(' : '', ')' : '', ':' : ''}

def clean_col (col, clean_col):
  col.columns = col.columns.str.lower()
  for i, j in clean_col.items():
    col.columns = col.columns.str.replace(i, j)
  return col

df_2 = clean_col(df, replace)
df_3 = df_2.loc[df_2["new_business"] == "WKE Business"]
df_4 = df_3[["customer/project_name", "januari_2023", "februari_2023", "maret_2023", 
            "april_2023", "mei_2023", "juni_2023", "juli_2023", "agustus_2023", 
            "september_2023", "oktober_2023", "november_2023", "desember_2023"]]

print(df_4)

# df_prodev_2 = df_prodev_2.drop(["timestamp", "delegation_date"], axis=1)
# #df_prodel_2["actual_project_cogs"] = df_prodel_2["actual_project_cogs"].fillna(0)

# # build df_raw
# id = []
# for i in range(len(df_prodev_2["project_name"])):
#     id.append("PR"+str(format(i+1, '05d')))

# df_prodev_2["project_id"] = id
# df_prodev_2["timestamp"] = pd.Timestamp('now')
# #new_df = pd.concat((new_df_1, df_clean), axis=0)

# spreadsheet_key = '1TORE-3APd2dtazoD7wjkg-P2XuFaGo2PmLRk1K4Nui8'
# wks_name = 'clean'
# d2g.upload(df_prodev_2, spreadsheet_key, wks_name, clean=True, credentials=credentials, row_names=False)