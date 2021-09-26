import pandas as pd
import json
from dbtools import pull_from_calibre
import re

# TODO: parse tags in notes
# create dataframe of tags with private keys to relate to annotations.
# annotations should also have private keys.
# just replace entire database interaction code with django rest api.
def parse_note(highlight, note):
    
    if highlight in note:
        note = re.sub("Page [0-9]+ ", '', note)
        note = re.sub(" @ [0-9]+[-][0-9]+[-][0-9]+ [0-9]+[:][0-9]+[:][0-9]+", '', note)
        note = note.replace(highlight, '')
    
    return note

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
    
    a.loc[pd.notna(a['note']), 'note'] = [parse_note(h, n) for h, n in a[pd.notna(a['note'])][['highlight', 'note']].itertuples(index=False)]
    print(a['note'])
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

