#!/usr/bin/python
from db_manager import Postgres
from utils import get_timestamp, from_pd_to_jsons
import euribor


TABLE_NAME = 'orchard.euribor'

if __name__ == '__main__':
    ec = euribor.EuriborCrawl()
    db = Postgres()
    # Build history 
    items = ec.build_history()
    # We have to add a new column: time stamp insertion 
    now = get_timestamp()
    items['date_insertion'] = now

    # Transform to json 
    items_json = from_pd_to_jsons(items)
    # Get keys example
    first_item = items_json[0]
    keys = [key for key in first_item.keys()]
    # Get values 
    values = [[value for value in entry.values()] for entry in items_json]
    # Insert them into the DB 
    db.insert_many_dict(TABLE_NAME, keys, values)
    db.close()

