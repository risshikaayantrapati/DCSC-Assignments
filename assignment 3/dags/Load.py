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
        return credentials

    def connect_to_gcp_and_get_data(self, file_name):
        gcp_file_path = f'transformed_data/{file_name}'

        credentials_info = self.getcredentials()
        client = storage.Client.from_service_account_info(credentials_info)
        bucket = client.get_bucket(self.bucket_name)

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
    