import pytz
import requests
import pandas as pd
from datetime import datetime
from google.cloud import storage


# Set the time zone to Mountain Time
mountain_time_zone = pytz.timezone('US/Mountain')


def extract_data_from_api(limit=50000, order='animal_id'):
    """
    Function to extract data from data.austintexas.gov API.
    """
    api_url = 'https://data.austintexas.gov/resource/9t4d-g238.json'
    
    api_key = '58778d3tul9ykaurce5wf29ek'
    
    headers = { 
        'accept': "application/json", 
        'apikey': api_key,
    }
    
    loop = 0
    data = []

    while loop < 157000:  # Iterating through all the records
        params = {
            '$limit': str(limit),
            '$offset': str(loop),
            '$order': order,
        }

        api_response = requests.get(api_url, headers=headers, params=params)
        print("response : ", api_response)
        latest_data = api_response.json()
        
        # Break the loop if there is no data further
        if not latest_data:
            break

        data.extend(latest_data)
        loop += limit

    return data


def create_dataframe(data):
    columns = [
        'animal_id', 'name', 'datetime', 'monthyear', 'date_of_birth',
        'outcome_type', 'animal_type', 'sex_upon_outcome', 'age_upon_outcome',
        'breed', 'color'
    ]

    df_list = []
    for entry in data:
        row_df = [entry.get(column, None) for column in columns]
        df_list.append(row_df)

    df = pd.DataFrame(df_list, columns=columns)
    return df


def upload_to_gcs(dataframe, bucket_name, file_path):
    """
    Upload a DataFrame to a Google Cloud Storage bucket using service account credentials.
    """
    print("Writing data to GCS.....")
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

    client_info = storage.Client.from_service_account_info(credentials)
    csv_df = dataframe.to_csv(index=False)
    
    bucket = client_info.get_bucket(bucket_name)
    
    current_date = datetime.now(mountain_time_zone).strftime('%Y-%m-%d')
    file_path_formatted = file_path.format(current_date, current_date)
    
    blob = bucket.blob(file_path_formatted)
    blob.upload_from_string(csv_df, content_type='text/csv')
    print(f"Completed writing data to GCS with date: {current_date}.")


def main():
    data_extracted = extract_data_from_api(limit=50000, order='animal_id')
    shelter_data = create_dataframe(data_extracted)

    gcs_bucket_name = 'data_center_lab3'
    gcs_file_path = 'data/{}/outcomes_{}.csv'

    upload_to_gcs(shelter_data, gcs_bucket_name, gcs_file_path)
    