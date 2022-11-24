import pandas as pd
from pandas.io import gbq
from datetime import datetime

RAW = "https://docs.google.com/spreadsheets/d/e/2PACX-1vRhcAWkWXIjp2XVAsnTLw13QGg6Ot9D_HBf_FMCA42qIWf034T8oKOgV6cTBJS29tfJRPHPyQ4DQJ6s/pub?gid=1990926458&single=true&output=csv"

SCHEMA = {
    "project" : "object",
    "start" : "datetime64[ns]",
    "finish" : "datetime64[ns]",
    "project_id" : "object",
    "preparation_start_date" : "datetime64[ns]",
    "ready_to_deploy_date" : "datetime64[ns]",
    "project_end_date" : "datetime64[ns]",
    "service_type" : "object",
    "in_house_pic" : "object",
    "project_objectives" : "object",
    "project_key_results" : "object",
    "project_main_activities" : "object",
    "project_value" : "object",
    #"proposed_cogs" : "float64",
    "planned_project_start_date" : "datetime64[ns]",
    "planned_ready_to_deploy_date" : "datetime64[ns]",
    "planned_main_activity_start_date" : "datetime64[ns]",
    "planned_main_activity_end_date" : "datetime64[ns]",
    "planned_final_report_submitted_date" : "datetime64[ns]",
    "planned_project_end_date" : "datetime64[ns]",
    "planned_project_archived_date" : "datetime64[ns]",
    #"proposed_nsm_projection" : "float64",
    #"planned_project_strategic_index_profit_margin" : "float64",
    #"planned_project_strategic_index_replicability_to_market" : "float64",
    #"planned_project_strategic_index_influencing_decision_maker" : "float64",
    #"planned_project_strategic_index_contributes_to_swm_intelligence" : "float64",
    #"planned_project_strategic_index_catalyzing_behaviour_change" : "float64",
    #"planned_project_strategic_index_quantity_of_beneficiaries" : "float64",
    #"planned_project_strategic_index_ease_of_implementation" : "float64",
    #"planned_project_strategic_index_supports_nsm_financially" : "float64",
    #"planned_project_strategic_index_supports_nsm_strategically" : "float64",
    "email_address" : "object",
    "the_actual_data_is_available" : "object",
    "actual_project_start_date" : "datetime64[ns]",
    "actual_ready_to_deploy_date" : "datetime64[ns]",
    "actual_main_activity_start_date" : "datetime64[ns]",
    "actual_main_activity_end_date" : "datetime64[ns]",
    "actual_final_report_submitted_date" : "datetime64[ns]",
    "actual_project_end_date" : "datetime64[ns]",
    "actual_project_archived_date" : "datetime64[ns]",
    #"actual_nsm_projection" : "float64",
    #"actual_project_cogs" : "float64",
    "last_record" : "datetime64[ns]",
    "email_address_1" : "object",
    "client_organization" : "object",
    "new_client_organization_name" : "object",
    "project_name_1" : "object",
    "w4c_pic" : "object",
    "client_organization_type" : "object",
    "new_client_organization_type" : "object",
    "client_organization_industry" : "object",
    "new_client_organization_industry" : "object",
    #"client_organization_size" : "float64",
    "client_organization_location" : "object",
    "client_pic" : "object",
    "client_pic_position" : "object",
    "client_phone" : "object",
    "client_email" : "object",
    "lead_status" : "object",
    "service_type_1" : "object",
    "service_topic" : "object",
    "new_service_topic" : "object",
    #"chance" : "float64",
    "date_project_decided" : "datetime64[ns]",
    "date_proposal_sent" : "datetime64[ns]",
    #"number_of_proposal_revision" : "float64",
    #"operational_value" : "float64",
    #"non_ss_operational_value" : "float64",
    "in_house_consultant_involved" : "object",
    #"total_personnel_value_albert" : "float64",
    #"total_billed_days_albert" : "float64",
    "other_in_house_consultant" : "object",
    #"total_personnel_value_anin" : "float64",
    #"total_billed_days_anin" : "float64",
    "other_in_house_consultant_1" : "object",
    #"total_personnel_value_april" : "float64",
    #"total_billed_days_april" : "float64",
    "other_in_house_consultant_2" : "object",
    #"total_personnel_value_elma" : "float64",
    #"total_billed_days_elma" : "float64",
    "other_in_house_consultant_3" : "object",
    #"total_personnel_value_fitria" : "float64",
    #"total_billed_days_fitria" : "float64",
    "other_in_house_consultant_4" : "object",
    #"total_personnel_value_ica" : "float64",
    #"total_billed_days_ica" : "float64",
    "other_in_house_consultant_5" : "object",
    #"total_personnel_value_ifa" : "float64",
    #"total_billed_days_ifa" : "float64",
    "other_in_house_consultant_6" : "object",
    #"total_personnel_value_khai" : "float64",
    #"total_billed_days_khai" : "float64",
    "other_in_house_consultant_7" : "object",
    #"total_personnel_value_prita" : "float64",
    #"total_billed_days_prita" : "float64",
    "other_in_house_consultant_8" : "object",
    #"total_personnel_value_saka" : "float64",
    #"total_billed_days_saka" : "float64",
    "other_in_house_consultant_9" : "object",
    #"total_personnel_value_tantin" : "float64",
    #"total_billed_days_tantin" : "float64",
    "other_in_house_consultant_10" : "object",
    #"number_of_other_in_house_consultants" : "float64",
    "names_of_in_house_consultant" : "object",
    #"total_personnel_value_other" : "float64",
    #"total_billed_days_other" : "float64",
    "other_in_house_consultant_11" : "object",
    #"total_analyst_personnel_value" : "float64",
    #"total_analyst_billed_days" : "float64",
    #"total_associate_personnel_value" : "float64",
    #"total_associate_billed_days" : "float64",
    #"total_consultant_personnel_value" : "float64",
    #"total_consultant_billed_days" : "float64",
    "other_in_house_consultant_12" : "object",
    #"proposed_cogs_1" : "float64",
    "proposed_project_start_date" : "datetime64[ns]",
    "proposed_project_end_date" : "datetime64[ns]",
    #"nsm_projection" : "float64",
    #"project_strategic_index_projection_profit_margin" : "float64",
    #"project_strategic_index_projection_repicability_to_market" : "float64",
    #"project_strategic_index_projection_influencing_decision_maker" : "float64",
    #"project_strategic_index_projection_contributes_to_swm_intelligence" : "float64",
    #"project_strategic_index_projection_catalyzing_behaviour_change" : "float64",
    #"project_strategic_index_projection_quantity_of_beneficiaries" : "float64",
    #"project_strategic_index_projection_ease_of_implementation" : "float64",
    #"project_strategic_index_projection_supports_nsm_financially" : "float64",
    #"project_strategic_index_projection_supports_nsm_strategically" : "float64",
    "delegation_date_1" : "datetime64[ns]",
    "contract_status" : "object",
    "contract_signature_date" : "datetime64[ns]",
    #"terms_of_payment_quantity" : "float64",
    #"term_of_payment_value" : "float64",
    "milestone" : "object",
    "estimated_date_of_milestone_delivery" : "datetime64[ns]",
    "final_term_of_payment" : "object",
    #"term_of_payment_value_1" : "float64",
    "milestone_1" : "object",
    "estimated_date_of_milestone_delivery_1" : "datetime64[ns]",
    "final_term_of_payment_1" : "object",
    #"term_of_payment_value_2" : "float64",
    "milestone_2" : "object",
    "estimated_date_of_milestone_delivery_2" : "datetime64[ns]",
    "final_term_of_payment_2" : "object",
    #"term_of_payment_value_3" : "float64",
    "milestone_3" : "object",
    "estimated_date_of_milestone_delivery_3" : "datetime64[ns]",
    "final_term_of_payment_3" : "object",
    #"term_of_payment_value_4" : "float64",
    "milestone_4" : "object",
    "estimated_date_of_milestone_delivery_4" : "datetime64[ns]",
    "final_term_of_payment_4" : "object",
    "other_terms_of_payment" : "object",
    "date_leads_defined" : "datetime64[ns]",
    "client_name_organization" : "object",
    "project_team" : "object",
    #"cogs" : "float64",
    #"cash_in" : "float64",
    #"nsm" : "float64",
    "phase" : "object",
    "timestamp" : "datetime64[ns]"
}

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


df_2.to_gbq (
    destination_table = "strategic_service.greenlit",
    project_id = "waste4change-362106",
    if_exists = "replace"
)