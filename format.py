import requests
import pandas as pd
import json


def prepare_author(df):

    def format_author(name):
        names = name.split(',', 1)
        return names[0].strip(), names[1].strip()

    df['lname'], df['fname'] = zip(
        *[format_author(name) for name in df['sort']])

    return df[['id', 'fname', 'lname']]


def create_if_not_exists(df, url, col):
    """Compares items in dataframe against items already in the API and creates items from the dataframe that are not in the API.

    Args:
        df (pd.Dataframe): A dataframe of items from Calibre to POST to API.
        url (str): url to request
        col: column names from dataframe to get data from to construct POST payload.

    Returns:
        item_ids (pd.Dataframe): A dataframe containing the ID of each item from Calibre mapped to its ID assigned by the API. Useful for establishing relationships between items.

    """

    r = requests.get(url)
    r_df = pd.DataFrame.from_records(r.json())

    # compare items already in API with ones being added.
    if not r_df.empty:
        if 'author' in col and 'tags' in col:
            r_df['author'] = r_df['author'].apply(tuple)
            r_df['tags'] = r_df['tags'].apply(tuple)
        merged = df.merge(r_df, how='left', on=col, indicator=True).rename(
            columns={'id_x': 'cid', 'id_y': 'rid'})  # cid is calibre id, rid is request id
        # create a dataframe containing calibre's id for the item, and the id

        # generated for the item from the API
        item_ids = merged[merged['_merge'] == 'both'][['cid', 'rid']]
        # dataframe of items to POST
        items_to_add = merged[merged['_merge'] == 'left_only'][col + ['cid']]
    else:
        item_ids = pd.DataFrame(columns=['cid', 'rid'])
        items_to_add = df.rename(columns={'id': 'cid'})

    for index, row in items_to_add.iterrows():
        # Construct Payload
        payload = {}
        for i in col:
            payload[i] = row[i]
        r = requests.post(url, data=payload)

        # store ids
        item_ids = item_ids.append({
            'cid': row['cid'],
            'rid': r.json()['id']
        }, ignore_index=True)

    item_ids['rid'] = item_ids['rid'].astype(int)

    return item_ids


def prepare_all(in_tables):
    tables = {
        'author': prepare_author(in_tables['author']),
        'book_tag': in_tables['tags'].apply(lambda x: x.str.strip() if x.dtype == "object" else x),
        'book': in_tables['books'],
        'note_tag': '',
        'note': ''
    }

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
    )[['book', 'rid']].rename(columns={'rid': 'author'})
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
    )[['book', 'rid']].rename(columns={'rid': 'tag'})
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
        books, 'http://localhost:8000/brain2_api/book/', ['title', 'published', 'author', 'tags'])
    
