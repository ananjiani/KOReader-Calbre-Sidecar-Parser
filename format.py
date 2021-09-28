import requests
import pandas as pd
import json
from parse import parse_all_sidecars



def create_if_not_exists(df, url, col):
    """Compares items in dataframe against items already in the API and creates items from the dataframe that are not in the API.

    Args:
        df (pd.Dataframe): A dataframe of items from Calibre to POST to API.
        url (str): url to request
        col: unique column names to use for comparing with API.

    Returns:
        item_ids (pd.Dataframe): A dataframe containing the ID of each item from Calibre mapped to its ID assigned by the API. Useful for establishing relationships between items.

    """

    r = requests.get(url)
    r_df = pd.DataFrame.from_records(r.json())

    # compare items already in API with ones being added.
    if not r_df.empty:
        for c in ['author', 'tags']:
            if c in r_df.columns:
                r_df[c] = r_df[c].apply(sorted).apply(tuple)

        orig_columns = list(df.columns)
        orig_columns.remove('id')
        merged = df.merge(r_df, how='left', on=col, indicator=True).rename(
            columns={'id_x': 'cid', 'id_y': 'rid'}) # cid is calibre id, rid is request id

        in_api = merged[merged['_merge'] == 'both']        
        item_ids = in_api[['cid', 'rid']]

        # condition_to_update = (True)
        # patch_dfs = []
        # for s in dupe_cols:
        #     in_api[in_api[f'{s}_x'] == in_api[f'{s}_y']]

            
        items_to_add = merged[merged['_merge'] == 'left_only']

        if not items_to_add.empty:
            dupe_cols = [x for x in orig_columns if x not in col]

            col_to_rename = {}
            col_to_drop = []
            for s in dupe_cols:
                col_to_rename[f'{s}_x'] = s
                col_to_drop.append(f'{s}_y')
            items_to_add = items_to_add.rename(columns=col_to_rename).drop(columns=col_to_drop)[orig_columns + ['cid']]
    else:
        item_ids = pd.DataFrame(columns=['cid', 'rid'])
        items_to_add = df.rename(columns={'id': 'cid'})

    for index, row in items_to_add.iterrows():
        # Construct Payload
        payload = {}
        for i in items_to_add.columns:
            payload[i] = row[i]
        r = requests.post(url, data=payload)
        r_json = r.json()
        print(r_json)
        # store ids
        item_ids = item_ids.append({
            'cid': row['cid'],
            'rid': r_json['id']
        }, ignore_index=True)

    item_ids['rid'] = item_ids['rid'].astype(int)

    return item_ids

def prepare_author(df):

    def format_author(name):
        names = name.split(',', 1)
        return names[0].strip(), names[1].strip()

    df['lname'], df['fname'] = zip(
        *[format_author(name) for name in df['sort']])

    return df[['id', 'fname', 'lname']]

def prepare_note_tags(df):
    df = df['tags'].explode('tags').dropna().drop_duplicates()
    return df.to_frame(name='name')

def post_note_tags(df, url):
    url += 'note_tag/'
    df['id'] = df['name']
    return create_if_not_exists(df, url, ['name']).rename(columns={'cid' : 'name', 'rid' : 'id'})

def convert_tags_to_ids(tags, tag_ids):
    ret_tags = []
    print(tag_ids)
    for t in tags:
        ret_tags.append(tag_ids.loc[tag_ids['name'] == t, ['id']].iloc[0].iloc[0])
    ret_tags = sorted(ret_tags)

    return tuple(ret_tags)

def prepare_notes(df, tag_ids):
    df['tags'] = [convert_tags_to_ids(t, tag_ids) for t in df['tags']]
    df['id'] = None
    return df

def post_notes(df, url):
    url += 'note/'
    
    return create_if_not_exists(df, url, ['book', 'highlight', 'chapter'])

def prepare_all(in_tables):
    tables = {
        'author': prepare_author(in_tables['author']),
        'book_tag': in_tables['tags'].apply(lambda x: x.str.strip() if x.dtype == "object" else x),
        'book': in_tables['books'],
        'note_tag': '',
        'note': ''
    }

    url = 'http://localhost:8000/brain2_api/'

    # POST authors, store assigned ids
    author_ids = create_if_not_exists(
        tables['author'], 'http://localhost:8000/brain2_api/author/', ['fname', 'lname'])

    # replace author ids from calibre with author ids from API
    b_a_link = pd.merge(
        in_tables['books_authors_link'],
        author_ids,
        left_on='author',
        right_on='cid',
        how='left'
    )[['book', 'rid']].rename(columns={'rid': 'author'}).sort_values(['book', 'author'])
    # collapse rows down to a single book and a tuple of author ids
    b_a_link = b_a_link.groupby(['book'])['author'].apply(tuple).to_frame()

    # POST book tags
    book_tag_ids = create_if_not_exists(
        tables['book_tag'], 'http://localhost:8000/brain2_api/book_tag/', ['name'])

    # as above but with the tags
    b_t_link = pd.merge(
        in_tables['books_tags_link'], 
        book_tag_ids, 
        left_on='tag',
        right_on='cid', 
        how='left'
    )[['book', 'rid']].rename(columns={'rid': 'tag'}).sort_values(['book', 'tag'])
    b_t_link = b_t_link.groupby(['book'])['tag'].apply(tuple).to_frame()

    # combine book author ids and tags ids into one
    b_a_t_link = pd.merge(b_a_link, b_t_link, on=['book'], how='outer')

    # prepare book data for payload
    books = pd.merge(tables['book'], b_a_t_link, left_on=['id'], right_on='book', how='outer').rename(
        columns={'pubdate': 'published', 'tag': 'tags'})
    books['published'] = pd.to_datetime(
        books['published'], infer_datetime_format=True, errors='coerce').dt.strftime('%Y-%m-%d')
    books.loc[pd.isna(books['published']), 'published'] = '0001-01-01'
    books.loc[pd.isna(books['tags']), 'tags'] = (None)

    # POST books
    book_ids = create_if_not_exists(
        books, 'http://localhost:8000/brain2_api/book/', ['title', 'author'])

    notes = parse_all_sidecars(in_tables['sdr'])

    note_tags = prepare_note_tags(notes)
    note_tag_ids = post_note_tags(note_tags, url)

    notes = prepare_notes(notes, note_tag_ids)
    notes = notes.merge(book_ids, how='left', left_on='book', right_on='cid').drop(columns=['book', 'cid']).rename(columns={'rid' : 'book'})
    note_ids = post_notes(notes, url)

    
