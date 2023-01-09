import pandas as pd
import gspread
from df2gspread import df2gspread as d2g
from oauth2client.service_account import ServiceAccountCredentials

scope = ['https://spreadsheets.google.com/feeds',
         'https://www.googleapis.com/auth/drive']

df_prodev = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRhcAWkWXIjp2XVAsnTLw13QGg6Ot9D_HBf_FMCA42qIWf034T8oKOgV6cTBJS29tfJRPHPyQ4DQJ6s/pub?gid=1335506491&single=true&output=csv")
df_prodel = pd.read_csv("https://docs.google.com/spreadsheets/d/e/2PACX-1vRhcAWkWXIjp2XVAsnTLw13QGg6Ot9D_HBf_FMCA42qIWf034T8oKOgV6cTBJS29tfJRPHPyQ4DQJ6s/pub?gid=802309967&single=true&output=csv")

cred = {
    "type": "service_account",
    "project_id": "waste4change-362106",
    "private_key_id": "5ac2727f921a423460d13366c455b5066775eeff",
    "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQDPmQNPYXeb9g9b\nn+8OtHm4nGBhx+Bo6x7WOmWu8vHD/12kyc/ZH7LCR2Txl2mwzJgjpPcc0JGhDfp4\naMXFyd9+RDuRXcVvWpTalfo9UxT5nohivCHpeteHldIkLph6LyU2CJW8a9ktZsNN\n7YToHmK6L2xnSEHAy0zz7ySbqdM5mKHQrsM9c6bZn7JCv1yWoEgHmI4znvEMu2LY\nptDZYceemHYV2JzJ0w9cSvlBxicIW352HU2QrJVKZ4sO1dK0E68m9dzwlu28pr2l\nkiI2iyhqtG7GCLK8fOLPCHT3aFDBzUrxQEb4e+Q8By/myDRnWxYVqxOQozJ2l/ra\nBGaClk61AgMBAAECggEAGqQ13wmQmoTE6srHGJmDp3P2EIZgn8ZQwkhjRUTrTu5m\nO6AkmuYtAC6+XxzB1Q0klyp0BhAkKEmNc8dqXhwuoNsr09d3X3d98+FcaGNRpUk5\nkoY3SenYYA+TlM4gBkonmdwXL0Od7uwps0YAkPNZUzCD1mtY+N9+RC3UAmAUeecH\n6mmwJsPMucIjRCT0ZHEfcACdX++0N78ipLeHtIYf64hFLa4DH0/k9Bw47ITChg4j\n3pGheSRXKJ2YswwyxEJIXp4ThFY5iZ73ysw5A6zZH6263jyvbqLHACklQS89UzWQ\n18B5dSI0lsG6/nC162kOzQ86gMKw7iYZ8nqvxlPUHwKBgQDrTam2SCwF8UCR8luS\njXJ3pU0ZVyLpsNmMeRsGuG1+4Rna19A2euJA4egthe8CrlXD51KYXe37CF7/X+pQ\neNFa03neywJRHpa89LVF96LQz95J0tS/VnOZDzAXtiZLVwzI+a79ReiOdSjQ+uTz\nDeZuajVxtGAjlyi2F89nWBOUQwKBgQDh24AQfYmiSnZw671GtrDuEpNezqnFyAxw\nXUIGbM+vGVIVjzxow5RAVPEQC3non1cbfzUeRxFTBNCDv/AgMQfHGJ5eWWTje2x5\n9qwS3r4R2uH/okQrTb2CF5/T+AYeXNRxbfJwp7RDKkEyIGMlOI2AUyw6kaMN34pL\nc9rsLBcdpwKBgGWk2MjrCElQ28mv3arC+01/1Hw7zdMRGAi8T9ZpWLNUXazRSRof\nnxjT/U2c65BG6rmDdYRoHuMvzImYT0mPxy/q5lw9abTM9+SL+LpOrMBy6t0M++Xj\nXHAjxWyYvJYS9mzinpq91iWjqowLtVbhDEdqDcgeWOFG7MksI+XKgDqtAoGAVwLZ\nM4EppPxK5PXs1XFMeGtvmvbDMQ8Au8kxxofk+vAmT2f/Pd0bIC2ZMTU6ogPA6zb2\nn9OBORqBukAXok9BT9KMMIz/8SN7hwEhCaQGVZqdtn4tBQHOIHA5gs/qi4f30Dsr\nS3tQVw/QTh9sEsrWuLlSML4qsw2LmcXocVQLE8ECgYAGPOp2LX3GRtbfoC+r9BF0\nMVixYUNa2dKLqg0Ynl9Si4kjJ21quTQGp4VB02tG0SVwLmCi473uOEyql5G14we/\n3CuK6QZX7ftWUZL9PkTyU6duVjqCFEgUin/uPVQNhALyStAyadTv34hCePtS+fub\nPtjAqNpZVt0UtusSvLssvg==\n-----END PRIVATE KEY-----\n",
    "client_email": "ss-413@waste4change-362106.iam.gserviceaccount.com",
    "client_id": "105120854566980528131",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/ss-413%40waste4change-362106.iam.gserviceaccount.com"
}

credentials = ServiceAccountCredentials.from_json_keyfile_dict(
    cred, scope)
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

# build df_raw
id = []
for i in range(len(df_prodev_2["project_name"])):
    id.append("PR"+str(format(i+1, '05d')))

df_prodev_2["project_id"] = id

new_df = pd.merge(df_prodel_2, df_prodev_2, on="project_id", how="inner")

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

}

new_df = new_df.astype(SCHEMA)

new_col = ["main_activity_start_date", "main_activity_end_date", "final_report_submitted_date", "project_archived_date"]
fill = {
    "actual_main_activity_start_date" : "planned_main_activity_start_date",
    "actual_main_activity_end_date" : "planned_main_activity_end_date",
    "actual_final_report_submitted_date" : "planned_final_report_submitted_date",
    "actual_project_archived_date": "planned_project_archived_date"
}

for i in range(len(new_col)):
    new_df[new_col[i]] = new_df[list(fill)[i]].fillna(new_df[list(fill.values())[i]])

df_drop = new_df.drop(["project_name", "date_leads_acquired", 
                    "delegation_date", "main_activity_start_date",
                    "main_activity_end_date", "final_report_submitted_date", 
                    "project_archived_date"], axis=1)

def phase(start, finish, phase):
    df_phase = new_df[["project_name", start, finish]]
    df_phase = pd.concat([df_phase, df_drop], axis=1)
    df_phase = df_phase.dropna(subset=["project_name"])
    df_phase["phase"] = phase
    df_phase.rename(columns = {start:"start", finish:"finish"}, inplace=True)
    df_phase["start"] = pd.to_datetime(df_phase["start"])
    df_phase["finish"] = pd.to_datetime(df_phase["finish"])
    return df_phase

#Create Phase Columns
df_development = phase(start="date_leads_acquired", finish="delegation_date", phase="Development")
df_preparation = phase(start="delegation_date", finish="main_activity_start_date", phase="Preparation")
df_active = phase(start="main_activity_start_date", finish="main_activity_end_date", phase="Active")
df_reporting = phase(start="main_activity_end_date", finish="final_report_submitted_date", phase="Reporting")
df_closing = phase(start="final_report_submitted_date", finish="project_archived_date", phase="Closing")
df_final = pd.concat(
                [df_development, df_preparation, df_active, df_reporting, df_closing], 
                ignore_index=True)


spreadsheet_key = '1UHxsZ0XP-HdietMUrX38H8bhCb47GsKdp7Lj8Xfy4xc'
wks_name = 'raw_2'
d2g.upload(df_final, spreadsheet_key, wks_name, credentials=credentials, row_names=False)