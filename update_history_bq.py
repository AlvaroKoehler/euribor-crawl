#!/usr/bin/python
from google.cloud import bigquery
from google.cloud.bigquery.enums import SqlTypeNames
from google.cloud.bigquery.schema import SchemaField
from db_manager import BigQuery
from utils import get_timestamp
import euribor
import pandas as pd 


TABLE_NAME = 'orchard-euribor.orchard.euribor'

if __name__ == '__main__':
    ec = euribor.EuriborCrawl()
    db = BigQuery()
    # Build history 
    items = ec.build_history()
    # We have to add a new column: time stamp insertion 
    now = get_timestamp()
    items['date_insertion'] = now
    items['date_insertion'] = pd.to_datetime(items['date_insertion'])
    schema=[
        db.bigquery.SchemaField("eur_date", db.bigquery.enums.SqlTypeNames.TIMESTAMP),
        db.bigquery.SchemaField("eur_1w", db.bigquery.enums.SqlTypeNames.NUMERIC),
        db.bigquery.SchemaField("eur_1m", db.bigquery.enums.SqlTypeNames.NUMERIC),
        db.bigquery.SchemaField("eur_3m", db.bigquery.enums.SqlTypeNames.NUMERIC),
        db.bigquery.SchemaField("eur_6m", db.bigquery.enums.SqlTypeNames.NUMERIC),
        db.bigquery.SchemaField("eur_12m", db.bigquery.enums.SqlTypeNames.NUMERIC),
        db.bigquery.SchemaField("eur_year", db.bigquery.enums.SqlTypeNames.NUMERIC),
        db.bigquery.SchemaField("eur_month", db.bigquery.enums.SqlTypeNames.NUMERIC),
        db.bigquery.SchemaField("date_insertion", db.bigquery.enums.SqlTypeNames.TIMESTAMP),
    ]
    db.insert_dataframe(TABLE_NAME,items,schema)
    
