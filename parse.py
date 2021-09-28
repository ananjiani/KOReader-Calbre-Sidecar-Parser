import pandas as pd
import json
from dbtools import pull_from_calibre
import re


def parse_note(highlight, annotation):
    if highlight in annotation:
        annotation = re.sub(r'Page [0-9]+ ', '', annotation)
        annotation = re.sub(
            r'@ [0-9]+[-][0-9]+[-][0-9]+ [0-9]+[:][0-9]+[:][0-9]+', '', annotation)
        annotation = annotation.replace(highlight, '')

    tags = tuple([s.replace('#','') for s in re.findall(r'#[\w-]+', annotation)])
    for tag in tags:
        annotation = annotation.replace(f'#{tag}', '')
    annotation = annotation.strip()
    return annotation, tags


def parse_sidecar(data, book):
    bookmarks = pd.DataFrame(
        columns=['datetime', 'chapter', 'text', 'pos0', 'pos1'])
    highlights = pd.DataFrame(
        columns=['datetime', 'chapter', 'text', 'pos0', 'pos1'])

    # parse bookmarks from json provided by calibre database
    for i in data['bookmarks'].values():
        if 'text' in i.keys():
            bookmarks = bookmarks.append(
                {
                    'datetime': i['datetime'],
                    'chapter': i['chapter'],
                    'text': i['text'],
                    'pos0': i['pos0'],
                    'pos1': i['pos1']
                },
                ignore_index=True
            )

    # as above but with highlights
    for i in data['highlight'].values():
        for j in i.values():
            highlights = highlights.append(
                {
                    'datetime': j['datetime'],
                    'chapter': j['chapter'],
                    'text': j['text'],
                    'pos0': j['pos0'],
                    'pos1': j['pos1'],
                },
                ignore_index=True
            )

    # combine highlights and bookmarks, redefine as "annotations"
    note = highlights.merge(bookmarks, on=['datetime', 'chapter', 'pos0', 'pos1'], how='outer').rename(columns={
        'text_x': 'highlight', 'text_y': 'annotation', 'pos0': 'location'})[['highlight', 'annotation', 'location', 'chapter', 'datetime']]

    note['annotation'] = note['annotation'].fillna('')
    note['tags'] = None
    note.loc[pd.notna(note['annotation']), ['annotation', 'tags']] = [parse_note(h, a) for h, a in note[pd.notna(
        note['annotation'])][['highlight', 'annotation']].itertuples(index=False)]

    note['book'] = book

    return note


def parse_all_sidecars(df):
    # define columns for annotations df
    notes = pd.DataFrame(
        columns=['book', 'highlight', 'annotation', 'location', 'chapter', 'datetime', 'tags'])

    for index, row in df.iterrows():
        val = row['value']
        data = json.loads(val)

        a = parse_sidecar(data, row['book'])

        # store all the notes for current book
        notes = pd.concat([notes, a]).reset_index(drop=True)

    notes['annotation'] = notes['annotation'].fillna('')
    return notes
