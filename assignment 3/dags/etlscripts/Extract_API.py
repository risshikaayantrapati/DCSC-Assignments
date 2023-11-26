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
    