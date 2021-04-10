#!/usr/bin/python
import psycopg2
from psycopg2.extras import execute_values
from config import get_db_config 


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