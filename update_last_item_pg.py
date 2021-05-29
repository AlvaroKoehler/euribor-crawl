#!/usr/bin/python
from euribor import EuriborCrawl
from db_manager import Postgres

TABLE_NAME = 'orchard.euribor'

def update_last_item():
    # TODO: Check if last working day has been already included into the system
    ec = EuriborCrawl()
    db = Postgres()
    last_item = ec.get_last_euribor_rate_dict()
    db.insert_dict(TABLE_NAME, last_item)
    db.close()


if __name__ == '__main__':
    update_last_item()