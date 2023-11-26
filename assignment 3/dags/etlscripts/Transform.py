import pytz
import numpy as np
import pandas as pd
from io import StringIO
from datetime import datetime
from google.cloud import storage
from collections import OrderedDict

mountain_time_zone = pytz.timezone('US/Mountain')

# creating the global mapping for outcome types
outcomes_map = {'Rto-Adopt':1, 
                'Adoption':2, 
                'Euthanasia':3, 
                'Transfer':4,
                'Return to Owner':5, 
                'Died':6,
                'Disposal':7,
                'Missing': 8,
                'Relocate':9,
                'N/A':10,
                'Stolen':11}


def getcredentials():
    bucket = "data_center_lab3"
    credentials = {
                          "type": "service_account",
                          "project_id": "subtle-arcade-406301",
                          "private_key_id": "4f897fbf2728abbb83f6a21b5b0d8de9aaf78e01",
                          "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCxuI44YL22qyyV\nSJTuTyhS6xE878Yk3OBHnp56PPgKNfYv5hz75mck4vOOpqODq5zdmaWFgptD6LpV\n2AFa8RfvzjHDxC7RapJd+LwrrOQRNKDHd8kyUdVRBPOhAa3TKluJx+J/RkM6e4xl\nivPn5lRX2rUxo5JMWTe1dLYTtLYJBxPDm07+gF++Mr8MJwS5UL91Kg4oP9L0xQ7h\nmrvaZq4RaNegreY3jZst8fcaeyZV7fxPJlCTEFnnkbHywJSr+FL30x97VyLjbvgW\n2Ij9w2iSFCFFrYeD5sGVIRVlQ2o1UetooTD3XoY5ye1CQruzwFG1Ks0ft1pOWBzt\n5NSvkCPDAgMBAAECggEAEahcHRbjkpQ0VErHXVwpDgAEEPFj1wLJF+SXK18GaAFP\nihfLdm3AfsY728cLxQfWiJ/89aRx58/mP6Q8EjyVG8L5Z9GEs1Lq4wJVM+O0L09y\n0A7DQ0glMS0URcGEc1Aokyp0gx/IizV+UBJ7xGU6qRX2Xc26OfK9Wb6UXqXcVk4e\nhDv7UIEd68blRnAoDUvAVzdkDmoWVatnsCrehJ7JnwD7IZyxymCQOSwk1p0omTdK\nZwf5nIdVJse3P4W+X5O4Oo/sz6bllMc/RN//bEMn5wFzUOU3N9V8pr8Aj04qnanh\ny9EXs0oyjZbUZNQPtJgJ5Q/ImOMIBBsDqjqSKC9S3QKBgQDweIeRfKo9gqfhuLEP\nX/buBH0aXlWQGvVXJdIZDkBdL+lud4NtYC+UMK7L9xnRD8Pq6KoZKG6KxQLhCZ/L\nHa7d0g/n16mrAZyIRXQiNTKJfbH8ZK3YWyZ3erPSDw39MJY1axGckgxTApyxPeng\ntocoQQ4Tn4WNXRHKAI0jMWeDnQKBgQC9MqSTNGh8qVE6BI7icLs8R7NAa+MxvRob\nfh2lvxrOijvhwY7T2lJYxmdHak4AW8W2LT9P1L7gImLcV4t0A2cg4l7Z2astyTew\n3CJmQxublscNnEZZO/LPVTJgb/2P79tmgdxDVS+wB5IeF43j3kXSYuys5MHk4UW0\nM+JXpdkW3wKBgEFTvSCdCXKkaA8+KXvPyjlnJsVfE0eOy0/dinIcGQg31+aWuTGh\ni8tqXlIy0uSkr8jFZFBpV36DfwC7qJl5euPwCTomsIUwbuHmXGJeqVgIua2jfEbm\nnFChGPDb/iTmaigg6ivq0UZL012jQEI31AfvhG94SsPYoNiLxcvlxAVRAoGAAOLi\n2HmvX54fbkklLjRe/CpN0ahvuQTswWI221bo1jzZiPYyKBXmutm7FB+QW/oSFAdk\nO4iAmGYw1l1mntWBPyswTI7zeVRu+VoeoyH/WBw9tHed8L8SJRx+DuuXYXw7J+DF\n2l7kyUtPyOpNVNrussyZ1TmBGwRfIjf4AHcbbDsCgYBmy/jF94cabbUGAnKaqVt7\npMjmTPKYb2ssq810X1Rm7o7wjzXUAKn+DWAENEz22qMHhtX3YPwAUh/t0nnCkVt6\n8IkA5X7dq561j3uzjF15DXeAzgmSEjX8C94dqoC+zDKS50j2JWg4R3aQYYJC2AvJ\ndCJiRsV0Wn5rqpi8uQo76A==\n-----END PRIVATE KEY-----\n",
                          "client_email": "riya-61@subtle-arcade-406301.iam.gserviceaccount.com",
                          "client_id": "115490748950783181990",
                          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                          "token_uri": "https://oauth2.googleapis.com/token",
                          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/riya-61%40subtle-arcade-406301.iam.gserviceaccount.com",
                          "universe_domain": "googleapis.com"
                        }
    return credentials, bucket


def getdata(credentials_inf, gcp_bucket):
    gcp_filepath = 'data/{}/outcomes_{}.csv'

    client_info = storage.Client.from_service_account_info(credentials_inf)
    
    bucket_id = client_info.get_bucket(gcp_bucket)
    
    # Get the current date in the format YYYY-MM-DD
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    
    # Format the file path with the current date
    formatted_file_path = gcp_filepath.format(current_date, current_date)
    
    # Read the CSV file from GCP into a DataFrame
    blob = bucket_id.blob(formatted_file_path)
    csv_file = blob.download_as_text()
    data = pd.read_csv(StringIO(csv_file))

    return data


def write_data_to_gcs(dataframe, credentials_info, bucket_name, file_path):
    print(f"Writing data to GCS.....")

    client = storage.Client.from_service_account_info(credentials_info)
    csv_data = dataframe.to_csv(index=False)
    
    bucket = client.get_bucket(bucket_name)
    
    # current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    # formatted_file_path = file_path.format(current_date, current_date)
    
    blob = bucket.blob(file_path)
    blob.upload_from_string(csv_data, content_type='text/csv')
    print(f"Finished writing data to GCS.")


def prep_animal_tab_dim(data):
    print("Preparing Animal Dimensions Table Data")
    # extract columns only relevant to animal dim
    animal_dim_tab_data = data[['animal_id','name','date_of_birth', 'reprod', 'animal_type', 'breed', 'color','gender','datetime']].drop_duplicates()
    animal_dim_tab_data['animal_key'] = range(1, len(animal_dim_tab_data) + 1)
    return animal_dim_tab_data


def prep_date_tab_dim(data):
    date_dim_tab_data = data[['month_recorded', 'year_recorded']].drop_duplicates()
    date_dim_tab_data['date_key'] = range(1, len(date_dim_tab_data) + 1)
    return date_dim_tab_data


def prep_outcome_types_tab_dim(data):
    outcome_type_dim_tab_data = data[['outcome_type']].drop_duplicates()
    outcome_type_dim_tab_data['outcome_type_key'] = range(1, len(outcome_type_dim_tab_data) + 1)
    return outcome_type_dim_tab_data



def prep_outcomes_fct_tab(data, animal_dim_data, date_dim_data, outcome_type_dim_data):
     # Create or append data to the Outcomes_Fact table, linking to dimension tables
    df_fact = data.merge(date_dim_data, how='inner', left_on=['month_recorded','year_recorded'], right_on=['month_recorded','year_recorded'])
    df_fact = df_fact.merge(animal_dim_data, how='inner', left_on=['animal_id','animal_type','datetime'], right_on=['animal_id','animal_type','datetime'])
    df_fact = df_fact.merge(outcome_type_dim_data, how='inner', left_on='outcome_type', right_on='outcome_type')

    # Map the merged DataFrame columns to the table columns
    df_fact.rename(columns={
        'date_key': 'date_key',
        'animal_key': 'animal_key',
        'outcome_type_key': 'outcome_type_key'
    }, inplace=True)

    df_fact = df_fact[['date_key', 'animal_key', 'outcome_type_key']]
    return df_fact

def transform(data):
    transformed_data = data.copy()
    transformed_data['monthyear'] = pd.to_datetime(transformed_data['monthyear'])
    transformed_data['month_recorded'] = transformed_data['monthyear'].dt.month
    transformed_data['year_recorded'] = transformed_data['monthyear'].dt.year
    #transformed_data[['Month', 'Year']] = transformed_data['MonthYear'].str.split(' ', expand=True)
    transformed_data[['Name']] = transformed_data[['name']].fillna('Name_less')
    transformed_data.drop(['monthyear','age_upon_outcome'], axis=1, inplace = True)
    mapping = {
    'Animal ID': 'animal_id',
    'Name': 'name',
    'datetime': 'datetime',
    'date_of_birth': 'date_of_birth',
    'outcome_type': 'outcome_type',
    'animal_type': 'animal_type',
    'breed': 'breed',
    'color': 'color',
    'month_recorded': 'month_recorded',
    'year_recorded': 'year_recorded',
    'sex_upon_outcome': 'sex'
    }
    transformed_data.rename(columns=mapping, inplace=True)
    transformed_data[['reprod', 'gender']] = transformed_data.sex.str.split(' ', expand=True)
    transformed_data.drop(columns = ['sex'], inplace=True)
    #store this into a temporary table for us to populate the fact table later
    return transformed_data

def transform_data():
    credentials, bucket = getcredentials()

    rawdata = getdata(credentials, bucket)
    
    rawdata = transform(rawdata)
    
    dim_animal_tab = prep_animal_tab_dim(rawdata)
    dim_date_tab = prep_date_tab_dim(rawdata)
    dim_outcome_tab = prep_outcome_types_tab_dim(rawdata)

    outcomes_fact_tab = prep_outcomes_fct_tab(rawdata,dim_animal_tab,dim_date_tab,dim_outcome_tab)

    dim_animal_path = "transformed_data/dim_animal.csv"
    dim_dates_path = "transformed_data/dim_dates.csv"
    dim_outcome_types_path = "transformed_data/dim_outcome_types.csv"
    fct_outcomes_path = "transformed_data/fct_outcomes.csv"

    write_data_to_gcs(dim_animal_tab, credentials, bucket, dim_animal_path)
    write_data_to_gcs(dim_date_tab, credentials, bucket, dim_dates_path)
    write_data_to_gcs(dim_outcome_tab, credentials, bucket, dim_outcome_types_path)
    write_data_to_gcs(outcomes_fact_tab, credentials, bucket, fct_outcomes_path)
    