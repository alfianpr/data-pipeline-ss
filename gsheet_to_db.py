import pandas as pd
from pandas.io import gbq
from datetime import datetime

RAW = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRhcAWkWXIjp2XVAsnTLw13QGg6Ot9D_HBf_FMCA42qIWf034T8oKOgV6cTBJS29tfJRPHPyQ4DQJ6s/pub?gid=1990926458&single=true&output=csv"

df = pd.read_csv(RAW)
df = df.rename({"Timestamp":"last_record"}, axis=1)
df_drop = df.drop(["Project Name", "Date Leads Acquired", 
                    "Delegation Date", "Main Activity Start Date",
                    "Main Activity End Date", "Final Report Submitted Date", 
                    "Project Archived Date"], axis=1)

def phase(start, finish, phase):
    df_phase = df[["Project Name", start, finish]]
    df_phase = pd.concat([df_phase, df_drop], axis=1)
    df_phase = df_phase.dropna(subset=["Project Name"])
    df_phase["Phase"] = phase
    df_phase.rename(columns = {"Project Name":"Project", start:"Start", finish:"Finish"}, inplace=True)
    df_phase["Start"] = pd.to_datetime(df_phase["Start"])
    df_phase["Finish"] = pd.to_datetime(df_phase["Finish"])
    return df_phase

#Create Phase Columns
df_development = phase(start="Date Leads Acquired", finish="Delegation Date", phase="Development")
df_preparation = phase(start="Delegation Date", finish="Main Activity Start Date", phase="Preparation")
df_active = phase(start="Main Activity Start Date", finish="Main Activity End Date", phase="Active")
df_reporting = phase(start="Main Activity End Date", finish="Final Report Submitted Date", phase="Reporting")
df_closing = phase(start="Final Report Submitted Date", finish="Project Archived Date", phase="Closing")
df_gantt = pd.concat(
                [df_development, df_preparation, df_active, df_reporting, df_closing], 
                ignore_index=True)

df_gantt["timestamp"] = pd.to_datetime("now")

#cleaning columns
replace = {' ' : '_', '-' : '_', '.' : '_', '?' : '', '[' : '', ']' : '', '(' : '', ')' : ''}

#df_gantt = df.fillna(0)

def clean_col (col, clean_col):
  col.columns = col.columns.str.lower()
  for i, j in clean_col.items():
    col.columns = col.columns.str.replace(i, j)
  return col

df_2 = clean_col(df_gantt, replace)
df_2 = df_2.astype(str) #apply data types

def timestamp_convert(date_to_convert):
     return datetime.strptime(date_to_convert, '%Y-%m-%d %H:%M:%S.%f').strftime('%Y-%m-%d')

df_2['timestamp'] = df_2['timestamp'].apply(timestamp_convert)
df_2["timestamp"] = df_2["timestamp"].astype("datetime64[ns]")

datetime = ["start", "finish"]
for i in datetime:
  df_2[i]= pd.to_datetime(df_2[i],format='%Y-%m-%d', errors='coerce')

df_2.to_gbq (
    destination_table = "strategic_service.greenlit",
    project_id = "waste4change-362106",
    if_exists = "replace"
)