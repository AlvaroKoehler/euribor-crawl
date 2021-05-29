#!/usr/bin/python
import psycopg2
from psycopg2.extras import execute_values
from config import get_db_config

# Big Query
# https://cloud.google.com/bigquery/docs/authentication/service-account-file
from google.cloud import bigquery
from google.oauth2 import service_account

class Postgres:
    def __init__(self):
        try:
            self.params = get_db_config()
            self.conn = psycopg2.connect(**self.params)
            self.cur = self.conn.cursor()
        except (Exception, psycopg2.DatabaseError) as error:
            raise(error)
    
    def query(self, query):
        self.cur.execute(query)
        
    def close(self):
        if self.conn:
            self.conn.commit()
            self.conn.close()

    def insert_many_dict(self, table_name, keys, values):
        try:
            query = "INSERT INTO {} ({}) VALUES %s".format(table_name,','.join(keys))
            execute_values(self.cur, query, values)
            
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            self.conn.commit()

    def insert_dict(self,table_name, data):
        try:
            keys = data.keys()
            columns = ','.join(keys)
            values = ','.join(['%({})s'.format(k) for k in keys])
            query = 'insert into {} ({}) values ({})'.format(table_name, columns, values)
            self.cur.execute(query, data)
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
        finally:
            self.conn.commit()

    
class BigQuery:
    def __init__(self):
        
        try:
            self.bigquery = bigquery
            self.params = get_db_config(section='gcp')
            credentials = service_account.Credentials.from_service_account_file(
                self.params['credentials_file_path'], 
                scopes=["https://www.googleapis.com/auth/cloud-platform"],
            )
            self.client = bigquery.Client(
                credentials=credentials, 
                project=credentials.project_id,
            )         

        except Exception as error:
            raise(error)
    
    def insert_many_dict(self, table_id, list_of_dicts):
        '''
        STREAM DATA NOT FREE
        https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-gcs-json
        '''
        errors = self.client.insert_rows_json(table_id, list_of_dicts) 
        if errors == []:
            print("New rows have been added.")
        else:
            print("Encountered errors while inserting rows: {}".format(errors))

    def insert_dataframe(self, table_id, df, dict_schema):
        '''
        https://cloud.google.com/bigquery/docs/samples/bigquery-load-table-dataframe
        '''
        job_config = bigquery.LoadJobConfig(
            schema=dict_schema,
            write_disposition="WRITE_TRUNCATE"
        )

        job=self.client.load_table_from_dataframe(
            df,
            table_id,
            job_config
        )
        job.result()
        table = self.client.get_table(table_id)  # Make an API request.
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )