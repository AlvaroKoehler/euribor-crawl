#!/usr/bin/python
import euribor
from db_manager import Postgre

TABLE_NAME = 'indexes.euribor'

if __name__ == '__main__':
    ec = euribor.EuriborCrawl()
    db = Postgre()
    # Build history 
    items = ec.build_history()

    # Transform to json 
    items_json = euribor.from_pd_to_jsons(items)
    # Get keys example and give them the format we will use 
    # eur_date, eur_1w, eur_1m ...
    first_item = items_json[0]
    keys = ['_'.join(['eur',(i)]) for i in first_item.keys()]
    # Get values 
    values = [[value for value in entry.values()] for entry in items_json]
    # Insert them into the DB 
    db.insert_many_dict(TABLE_NAME, keys, values)
    db.close()

