import sqlite3
import pandas as pd
import json
from sqlalchemy import create_engine
from dbtools import config, pull_from_calibre, update_db

def parse_sidecar(data, book):
    bookmarks = pd.DataFrame(columns = ['datetime', 'chapter', 'text', 'pos0', 'pos1'])
    highlights = pd.DataFrame(columns = ['datetime', 'chapter', 'text', 'pos0', 'pos1'])

    # parse bookmarks from json provided by calibre database
    for i in data['bookmarks'].values():
        if 'text' in i.keys():
            bookmarks = bookmarks.append(
                {
                    'datetime' : i['datetime'],
                    'chapter': i['chapter'],
                    'text': i['text'],
                    'pos0': i['pos0'],
                    'pos1': i['pos1']
                },
                ignore_index = True
            )

    # as above but with highlights
    for i in data['highlight'].values():
        for j in i.values():
            highlights = highlights.append(
                {
                    'datetime' : j['datetime'],
                    'chapter' : j['chapter'],
                    'text' : j['text'],
                    'pos0' : j['pos0'],
                    'pos1' : j['pos1'],
                },
                ignore_index = True
            )

    # combine highlights and bookmarks, redefine as "annotations"
    a = highlights.merge(bookmarks, on=['datetime', 'chapter', 'pos0', 'pos1'], how='outer').rename(columns={'text_x': 'highlight', 'text_y': 'note', 'pos0': 'location'})[['highlight', 'note', 'location', 'chapter', 'datetime']]
    a['book'] = book

    return a

def parse_all_sidecars(df):
    # define columns for annotations df
    annotations = pd.DataFrame(columns = ['book', 'highlight', 'note', 'location', 'chapter', 'datetime'])

    for index, row in df.iterrows():
        val = row['value']
        data = json.loads(val)

        a = parse_sidecar(data, row['book'])

        # store all the annotations for current book
        annotations = pd.concat([annotations, a]).reset_index(drop = True)

    return annotations

def run_all(path):
    # get data from Calibre database
    tables = pull_from_calibre(path)

    tables['annotations'] = parse_all_sidecars(tables['sdr'])

    print('Connecting to the PostgreSQL database...')
    # read connection parameters
    params = config()
    con = create_engine("postgresql+psycopg2://{}:{}@{}:{}/{}".format(params['user'], params['password'], params['host'], params['port'], params['database']))

    update_db(tables, con)
