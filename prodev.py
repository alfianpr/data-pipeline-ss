import pandas as pd
from pandas.io import gbq
from datetime import datetime


df = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRhcAWkWXIjp2XVAsnTLw13QGg6Ot9D_HBf_FMCA42qIWf034T8oKOgV6cTBJS29tfJRPHPyQ4DQJ6s/pub?gid=484546428&single=true&output=csv")
pd.set_option('display.max_columns', None)

#cleaning columns
replace = {' ' : '_', '-' : '_', '.' : '_', '?' : '', '[' : '', ']' : '', '(' : '', ')' : ''}

def clean_col (col, clean_col):
  col.columns = col.columns.str.lower()
  for i, j in clean_col.items():
    col.columns = col.columns.str.replace(i, j)
  return col

df = df.rename({"Timestamp":"last_record"}, axis=1)
df["timestamp"] = pd.to_datetime("now")

df_2 = clean_col(df, replace)
df_2 = df_2.astype(str)

def timestamp_convert(date_to_convert):
     return datetime.strptime(date_to_convert, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d %H:%M:%S.%f')

df_2['timestamp'] = df_2['timestamp'].apply(timestamp_convert)
df_2['timestamp'] = df_2['timestamp'].astype("datetime64[ns]")

df_2.to_gbq (
    destination_table = "strategic_service.prodev",
    project_id = "waste4change-362106",
    if_exists = "append"
)