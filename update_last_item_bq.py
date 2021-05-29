#!/usr/bin/python
import pandas as pd 
from euribor import EuriborCrawl
from db_manager import BigQuery
from utils import get_timestamp


# TODO: Take out this from the code 
TABLE_NAME = 'orchard-euribor.orchard.euribor'

def update_last_item():
    # TODO: Check if last working day has been already included into the system
    ec = EuriborCrawl()
    db = BigQuery()
    last_item = ec.get_last_euribor_rate_dict()
    last_item_pd = {k:[v] for k,v in last_item.items()}
    now = get_timestamp()
    last_item_pd['date_insertion'] = now
    last_item_pd['date_insertion'] = pd.to_datetime(last_item_pd['date_insertion'])
    df_last_item = pd.DataFrame.from_dict(last_item_pd)
    df_last_item['eur_date'] = pd.to_datetime(df_last_item['eur_date'])
    
    
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
    db.insert_dataframe(TABLE_NAME, df_last_item, schema)
    


if __name__ == '__main__':
    update_last_item()