import psycopg2
import pandas as pd
from io import StringIO
from google.cloud import storage
from sqlalchemy import create_engine


class GCPDataLoader:

    def __init__(self):
        self.bucket_name = 'data_center_lab3'

    def getcredentials(self):
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
        return credentials

    def connect_to_gcp_and_get_data(self, file_name):
        gcp_file_path = f'transformed_data/{file_name}'

        credentials_info = self.getcredentials()
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.get_bucket(self.bucket_name)

        # Read the CSV file from GCP into a DataFrame
        blob = bucket.blob(gcp_file_path)
        csv_data = blob.download_as_text()
        df = pd.read_csv(StringIO(csv_data))

        return df

    def get_data(self, file_name):
        df = self.connect_to_gcp_and_get_data(file_name)
        return df


class PostgresDataLoader:

    def __init__(self):
        self.db_config = {
            'dbname': 'shelter_outcomes_db',
            'user': 'postgres',
            'password': 'pgadmin',
            'host': '34.27.8.179',
            'port': '5432',
        }

    def get_queries(self, table_name):
        
        drop_table_query = f"DROP TABLE IF EXISTS {table_name};"

        if table_name =="animaldimension":
            query = """CREATE TABLE IF NOT EXISTS animaldimension (
                            animal_key INT PRIMARY KEY,
                            animal_id VARCHAR,
                            name VARCHAR,
                            dob DATE,
                            reprod VARCHAR,
                            gender VARCHAR, 
                            animal_type VARCHAR NOT NULL,
                            breed VARCHAR,
                            color VARCHAR,
                            datetime TIMESTAMP
                        );
                        """
            alter_table_query = """ALTER TABLE animaldimension
                                ADD CONSTRAINT animal_key_unique UNIQUE (animal_key);
                                """
        elif table_name =="outcomedimension":
            query = """CREATE TABLE IF NOT EXISTS outcomedimension (
                            outcome_type_key INT PRIMARY KEY,
                            outcome_type VARCHAR NOT NULL
                        );
                        """
            alter_table_query = """ALTER TABLE outcomedimension
                                ADD CONSTRAINT outcometype_key_unique UNIQUE (outcome_type_key);
                                """
        elif table_name =="datedimension":
            query = """CREATE TABLE IF NOT EXISTS datedimension (
                            date_key INT PRIMARY KEY,
                            year_recorded INT2  NOT NULL,
                            month_recorded INT2  NOT NULL
                        );
                        """
            alter_table_query = """ALTER TABLE datedimension
                                ADD CONSTRAINT date_key_unique UNIQUE (date_key);
                                """
        else:
            query = """CREATE TABLE IF NOT EXISTS outcomesfact (
                            outcome_id SERIAL PRIMARY KEY,
                            animal_key INT,
                            date_key INT,
                            outcome_type_key INT,
                            FOREIGN KEY (animal_key) REFERENCES animaldimension(animal_key),
                            FOREIGN KEY (date_key) REFERENCES datedimension(date_key),
                            FOREIGN KEY (outcome_type_key) REFERENCES outcomedimension(outcome_type_key)
                        );
                        """
            alter_table_query = ";"
        return f"{drop_table_query}\n{query}\n{alter_table_query}".strip()
        #return f"{query}"

    def connect_to_postgres(self):
        connection = psycopg2.connect(**self.db_config)
        return connection

    def create_table(self, connection, table_query):
        print("Executing Create Table Queries...")
        cursor = connection.cursor()
        cursor.execute(table_query)
        connection.commit()
        cursor.close()
        print("Finished creating tables...")

    def load_data_into_postgres(self, connection, gcp_data, table_name):
        cursor = connection.cursor()
        print(f"Dropping Table {table_name}")
        truncate_table = f"DROP TABLE {table_name};"
        cursor.execute(truncate_table)
        connection.commit()
        cursor.close()
        
        print(f"Loading data into PostgreSQL for table {table_name}")
        # Specify the PostgreSQL engine explicitly
        engine = create_engine(
            f"postgresql+psycopg2://{self.db_config['user']}:{self.db_config['password']}@{self.db_config['host']}:{self.db_config['port']}/{self.db_config['dbname']}"
        )

        # Write the DataFrame to PostgreSQL using the specified engine
        gcp_data.to_sql(table_name, engine, if_exists='replace', index=False)

        print(f"Number of rows inserted for table {table_name}: {len(gcp_data)}")
        
def load_data_to_postgres(file_name, table_name):
    gcp_loader = GCPDataLoader()
    table_data_df = gcp_loader.get_data(file_name)

    postgres_dataloader = PostgresDataLoader()
    table_query = postgres_dataloader.get_queries(table_name)
    postgres_connection = postgres_dataloader.connect_to_postgres()

    postgres_dataloader.create_table(postgres_connection, table_query)
    postgres_dataloader.load_data_into_postgres(postgres_connection, table_data_df, table_name)
    