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

df_prodev = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRhcAWkWXIjp2XVAsnTLw13QGg6Ot9D_HBf_FMCA42qIWf034T8oKOgV6cTBJS29tfJRPHPyQ4DQJ6s/pub?gid=1335506491&single=true&output=csv")
df_prodel = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRhcAWkWXIjp2XVAsnTLw13QGg6Ot9D_HBf_FMCA42qIWf034T8oKOgV6cTBJS29tfJRPHPyQ4DQJ6s/pub?gid=802309967&single=true&output=csv")
df_implementator = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vR0pfMgEgoaH5MV38SxPYONIoNQVyj0Y2j2Aiz_ROGt3Xjcd8zmtMuOHBetfs-U2UKmenZe1c7u5Yp4/pub?gid=2086891367&single=true&output=csv")

SCHEMA = {
    "last_record" : "datetime64[ns]",
    "date_leads_acquired" : "datetime64[ns]",
    "delegation_date" : "datetime64[ns]",
    "actual_main_activity_start_date" : "datetime64[ns]",
    "planned_main_activity_start_date" : "datetime64[ns]",
    "actual_main_activity_end_date" : "datetime64[ns]",
    "planned_main_activity_end_date" : "datetime64[ns]",
    "actual_final_report_submitted_date" : "datetime64[ns]",
    "planned_final_report_submitted_date" : "datetime64[ns]",
    "actual_project_archived_date" : "datetime64[ns]",
    "planned_project_archived_date" : "datetime64[ns]",
    "actual_project_cogs" : "Int64"
}

credentials = ServiceAccountCredentials.from_json_keyfile_dict(cred, scope)
gc = gspread.authorize(credentials)

# clean column

replace = {' ' : '_', '-' : '_', '.' : '_', '?' : '', '[' : '', ']' : '', '(' : '', ')' : '', ':' : ''}

def clean_col (col, clean_col):
  col.columns = col.columns.str.lower()
  for i, j in clean_col.items():
    col.columns = col.columns.str.replace(i, j)
  return col

df_prodel_2 = clean_col(df_prodel, replace)
df_prodev_2 = clean_col(df_prodev, replace)
df_prodel_2 = df_prodel_2.drop(["unnamed_2", "unnamed_37", "project_name"], axis=1)
df_prodev_2 = df_prodev_2.drop(["timestamp", "delegation_date"], axis=1)
df_prodel_2 = df_prodel_2.rename({"timestamp":"last_record"}, axis=1)
df_implementator_2 = clean_col(df_implementator, replace)
df_implementator_2["in_implementator"] = "True"
#df_prodel_2["actual_project_cogs"] = df_prodel_2["actual_project_cogs"].fillna(0)

# build df_raw
id = []
for i in range(len(df_prodev_2["project_name"])):
    id.append("PR"+str(format(i+1, '05d')))

df_prodev_2["project_id"] = id
new_df = pd.merge(pd.merge(df_prodev_2, df_prodel_2, on="project_id", how="outer"), df_implementator_2, on="project_id", how="outer")

new_df = new_df.astype(SCHEMA)
new_df["timestamp"] = pd.Timestamp('now')

spreadsheet_key = '1TORE-3APd2dtazoD7wjkg-P2XuFaGo2PmLRk1K4Nui8'
wks_name = 'clean'
d2g.upload(new_df, spreadsheet_key, wks_name, clean=True, credentials=credentials, row_names=False)