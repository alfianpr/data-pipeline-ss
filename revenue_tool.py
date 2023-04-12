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
convert_date = {
  "januari_2023":"1/1/2023",
  "februari_2023":"2/1/2023",
  "maret_2023":"3/1/2023", 
  "april_2023":"4/1/2023",
  "mei_2023":"5/1/2023",
  "juni_2023":"6/1/2023",
  "juli_2023":"7/1/2023",
  "agustus_2023":"8/1/2023", 
  "september_2023":"9/1/2023",
  "oktober_2023":"10/1/2023",
  "november_2023":"11/1/2023",
  "desember_2023":"12/1/2023"
}

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
df_5 = df_4.melt(id_vars="customer/project_name",
                  var_name="month",
                  value_name="revenue").sort_values(["customer/project_name"]).reset_index(drop=True)
df_6 = df_5.fillna(0)
df_7 = df_6.replace({"month": convert_date})
df_7["month"] = pd.to_datetime(df_7["month"])
df_7["revenue"] = df_7["revenue"].astype(int)

print(df_7)
spreadsheet_key = '1TORE-3APd2dtazoD7wjkg-P2XuFaGo2PmLRk1K4Nui8'
wks_name = "rct_auto"
d2g.upload(df_7, spreadsheet_key, wks_name, credentials=credentials, row_names=False)
