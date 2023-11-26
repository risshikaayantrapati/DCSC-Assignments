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
                          "project_id": "winter-joy-406123",
                          "private_key_id": "d74d0686045c6d771d03cb9a6a3568b006e9a0cc",
                          "private_key": "-----BEGIN PRIVATE KEY-----\nMIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC1tganLp7yA2L3\nz4jw8PWgqam8PA3XQyUT0JVaRdirnbXCh/G3THIwOIorNZmOsP7yWpucLcz0hzcF\nnNcoa0AX+DpIhpXfppaEVtmx4c0hHKKGUyHrk+zBspAaDYAM30YyIjUAvAO9RvLq\nMkr6hpJ5/EUPm2py3obgslAclQN7f4BiQj+CvOJb+JDmuG2x1oZ1mhE6B26G7Uwd\nnHqQLV/q6q0nUbz5y07/TZuN52V15wUJ6S3Ue1BaDYJKTEs8hrkDkyw33su3uCaH\nBNceBOwHn5fR+GHWcog9HAmArPaksVvXclqt0dDBKa9FkWHDNxdPK9Brdn13TPnR\nPc7prBxzAgMBAAECggEABqW9kZEhjG32SWWEt3fBLJr4VCQIR5czvIlVZWOHvSN8\nWjz56UA5Ly1qVJFV6EPuV7Rb2/dK96kYqLJnpplh016n1y5hPjjMadP5i8ncZLk0\n8uAIriMPtrhPEDztMctbOItK0BeQtXRqf4nOd2LD6gWCC0OevcwJOCAd0SXcZkCg\ni2yUkirTcsqnv23TL6al63Meemqwr1icafEA3hIpvn3KEhpRA7uJMkAdU07RSCYj\njsg+WNmzP0IzbY0i/aOeUPeVoB2576LTN3i/FdWMikxu6vlaUWMuMAOwf9zizHwi\nzawlol3i03QQfxFMPgI5qlwe/8qwOrQcUu/Rdyw2hQKBgQDch6Yia5WiFdN8YKfu\nCZCZ7pEOe1MKtKxj7g8KHipxufPhUc5XwAG2WbE3EcW/uJ0Iqz2d7BxqAjezsp2P\nUWEEjRNscbEO8f5shnPmHi5lIxugFYH3k8mv5ZjjifGEdHF/1n8U1ZZzRCM/YyJM\n3sWIb1RzVoXTMuLdp+19WNZypQKBgQDS8AFkmu2hzZTfVtxg9hQawHpBoh+jMKq/\n3xqsZKk0D5emhtR7DnvQ8RHXENuXb0vVFX7BSE47ZWbKnWNl03v8ebaEK6/XNu+B\nRX7Id/LjzkFQsSsorFEDI5P23oGAdQV5d9pOtKw1dryorR7OEN3dnS5nEyiB+Foy\neQ/IJMGfNwKBgQDQil4qcn4/llA1b9mdmeHqDtWRUkHG/+99WCNUuA3/GY9sZUWx\naVq1K8APiXjswhGNnxFXg22jOZGfFqs0WgpamWXiyOhcb67exY5X7/aDoV2AVpZe\nnpy8/2tC0LFZRhwGfboS45+wRKDoUkCfXJKDYHQF1a4beCVc4m4MeLPiGQKBgQDP\n1OHKekvArIoOM8sXTd4pLZRHrrF1XLIgMnZZfSSpwuMslJQuWurrx1pIiLeT0Xjq\nDi/ByLgsFZDd+YzB+0miTVnjiBfM+LeqqwpsAqMyiToZgzZ+8KkxapCTIFCAfMxU\nDh7uhV1XoBHqMAi2CDBR9liN/nZe+JAGQvmlvXF4qQKBgHg9nog6zaYVul59LLw7\nUgCnwFhdMf+0K9AZPEyMv93oHnyE3bGS4N8NxkSgDGFbwl/NVQNethv5b5IvwAV0\nfZwVnyX+i5+DCssNnI3v65U6alc719Y8X0celgO3284n59SwpHB9Gpj/qtuWmLq4\n3xpgP4EZ+QYx6vp159YF04tP\n-----END PRIVATE KEY-----\n",
                          "client_email": "manasak@winter-joy-406123.iam.gserviceaccount.com",
                          "client_id": "110599242930579426630",
                          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                          "token_uri": "https://oauth2.googleapis.com/token",
                          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/manasak%40winter-joy-406123.iam.gserviceaccount.com",
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
    