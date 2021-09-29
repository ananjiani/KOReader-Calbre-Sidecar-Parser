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

def post_author(df, url):
    return create_if_not_exists(df, url + 'author/', ['fname', 'lname'])

def post_book_tags(df, url):
    return create_if_not_exists(df, url + 'book_tag/', ['name'])

def prepare_book(tables, url, author_ids, tag_ids):
    # replace author ids from calibre with author ids from API
    b_a_link = pd.merge(
        tables['books_authors_link'],
        author_ids,
        left_on='author',
        right_on='cid',
        how='left'
    )[['book', 'rid']].rename(columns={'rid': 'author'}).sort_values(['book', 'author'])
    # collapse rows down to a single book and a tuple of author ids
    b_a_link = b_a_link.groupby(['book'])['author'].apply(tuple).to_frame()

    # as above but with the tags
    b_t_link = pd.merge(
        tables['books_tags_link'], 
        tag_ids, 
        left_on='tag',
        right_on='cid', 
        how='left'
    )[['book', 'rid']].rename(columns={'rid': 'tag'}).sort_values(['book', 'tag'])
    b_t_link = b_t_link.groupby(['book'])['tag'].apply(tuple).to_frame()

    # combine book author ids and tags ids into one
    b_a_t_link = pd.merge(b_a_link, b_t_link, on=['book'], how='outer')

    # prepare book data for payload
    books = pd.merge(tables['books'], b_a_t_link, left_on=['id'], right_on='book', how='outer').rename(
        columns={'pubdate': 'published', 'tag': 'tags'})
    books['published'] = pd.to_datetime(
        books['published'], infer_datetime_format=True, errors='coerce').dt.strftime('%Y-%m-%d')
    books.loc[pd.isna(books['published']), 'published'] = '0001-01-01'
    books.loc[pd.isna(books['tags']), 'tags'] = (None)

    return books

def post_book(df, url):
    return create_if_not_exists(df, url + 'book/', ['title', 'author'])

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
    df['linked_notes'] = None
    return df

def post_notes(df, url):
    url += 'note/'
    
    return create_if_not_exists(df, url, ['book', 'highlight', 'chapter'])

def post_all(tables, url):
    
    tables['author'] = prepare_author(tables['author'])
    tables['book_tag'] = tables['tags'].apply(lambda x: x.str.strip() if x.dtype == "object" else x)

    # POST authors, store assigned ids
    author_ids = post_author(tables['author'], url)

    # POST book tags
    book_tag_ids = post_book_tags(tables['book_tag'], url)

    books = prepare_book(tables, url, author_ids, book_tag_ids)
    
    # POST books
    book_ids = post_book(books, url)

    notes = parse_all_sidecars(tables['sdr'])

    # POST note tags
    note_tags = prepare_note_tags(notes)
    note_tag_ids = post_note_tags(note_tags, url)

    # POST notes
    notes = prepare_notes(notes, note_tag_ids)
    notes = notes.merge(book_ids, how='left', left_on='book', right_on='cid').drop(columns=['book', 'cid']).rename(columns={'rid' : 'book'})
    post_notes(notes, url)

    
