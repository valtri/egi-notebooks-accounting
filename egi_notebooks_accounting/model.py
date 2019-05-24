import logging
import os

import peewee

# default file for the notebooks database
DEFAULT_NOTEBOOK_DB = 'notebooks.sqlite'
# environment variable to get the notebooks database file
NOTEBOOK_DB_ENV = 'NOTEBOOK_DB'

db = peewee.SqliteDatabase(None)


class Notebook(peewee.Model):
    uid = peewee.CharField()
    username = peewee.CharField()
    start = peewee.FloatField(null=True)
    end = peewee.FloatField(null=True)
    processed = peewee.BooleanField(default=False)
    record = peewee.TextField(null=True)

    class Meta:
        database = db


def init_db(db_file=''):
    if not db_file:
        db_file = os.environ.get(NOTEBOOK_DB_ENV, DEFAULT_NOTEBOOK_DB)
    logging.debug('Creating DB at %s', db_file)
    db.init(db_file)
    db.connect()
    db.create_tables([Notebook])
    db.close()
    return db
