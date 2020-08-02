#!/usr/bin/python
from euribor import EuriborCrawl
from db_manager import Postgre

TABLE_NAME = 'indexes.euribor'

def update_last_item():
    ec = EuriborCrawl()
    db = Postgre()
    last_item = ec.get_last_euribor_rate()
    db.insert_dict(TABLE_NAME, last_item)
    db.close()


if __name__ == '__main__':
    update_last_item()