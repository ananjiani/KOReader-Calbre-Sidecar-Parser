import requests
import pandas as pd
import json

def prepare_author(df):
    
    def format_author(name):
        names = name.split(',', 1)
        return names[0].strip(), names[1].strip()
    
    df['lname'], df['fname'] = zip(*[format_author(name) for name in df['sort']])

    return df[['id', 'fname', 'lname']]


def prepare_all(in_tables):
    tables = {
        'author' : prepare_author(in_tables['author']),
        'book_tag' : in_tables['tags'].apply(lambda x: x.str.strip() if x.dtype == "object" else x),
        'book' : in_tables['books'],
        'note_tag' : '',
        'note' : ''
    }

    def create_if_not_exists(df, url, col):
        print(url)
        print(col)
        r = requests.get(url)
        r_df = pd.DataFrame.from_records(r.json())
        print(r_df.dtypes)
        if not r_df.empty:
            print(col)
            if 'author' in col and 'tags' in col:
                print('here')
                r_df['author'] = r_df['author'].apply(tuple)
                r_df['tags'] = r_df['tags'].apply(tuple)
            merged = df.merge(r_df, how='left', on=col, indicator=True).rename(columns={'id_x' : 'cid', 'id_y' : 'rid'}) #cid is calibre id, rid is request id
            item_ids = merged[merged['_merge'] == 'both'][['cid','rid']]
            items_to_add = merged[merged['_merge'] == 'left_only'][col + ['cid']]
        else:
            item_ids = pd.DataFrame(columns=['cid', 'rid'])
            items_to_add = df.rename(columns={'id' : 'cid'})

        for index, row in items_to_add.iterrows():
            payload = {}
            for i in col:
                payload[i] = row[i]
            r = requests.post(url, data = payload)
            print(payload)
            print(r.json())
            item_ids = item_ids.append({
                'cid' : row['cid'],
                'rid' : r.json()['id']
            }, ignore_index=True)
        item_ids['rid'] = item_ids['rid'].astype(int)

        return item_ids
    # authors = tables['author']
    # url = 'http://localhost:8000/brain2_api/author/'
    # r = requests.get(url)

    # r_authors = pd.DataFrame.from_records(r.json())
    # merged = authors.merge(r_authors, how='left', on=['fname', 'lname'], indicator=True).rename(columns={'id_x' : 'cid', 'id_y' : 'rid'}) #cid is calibre id, rid is request id
    # author_ids = merged[merged['_merge'] == 'both'][['cid','rid']]
    # authors_to_add = merged[merged['_merge'] == 'left_only'][['fname', 'lname', 'cid']]

    # for index, row in authors_to_add.iterrows():
    #     payload = {
    #         'fname' : row['fname'].strip(),
    #         'lname' : row['lname'].strip()
    #     }
    #     r = requests.post(url, data = payload)
    #     author_ids = author_ids.append({
    #         'cid' : row['cid'],
    #         'rid' : r.json()['id']
    #     }, ignore_index=True)

    # author_ids['rid'] = author_ids['rid'].astype(int)

    author_ids = create_if_not_exists(tables['author'], 'http://localhost:8000/brain2_api/author/', ['fname', 'lname'])
    
    b_a_link = pd.merge(in_tables['books_authors_link'], author_ids, left_on='author', right_on='cid', how='left')[['book', 'rid']].rename(columns={'rid': 'author'})

    # book_tags = tables['book_tag']
    # url = 'http://localhost:8000/brain2_api/book_tag/'
    # r = requests.get(url)
    
    # r_book_tags = pd.DataFrame.from_records(r.json())
    # merged = book_tags.merge(r_book_tags, how='left', on=['name'], indicator=True).rename(columns={'id_x' : 'cid', 'id_y' : 'rid'})
    # book_tag_ids = merged[merged['_merge'] == 'both'][['cid','rid']]
    # book_tags_to_add = merged[merged['_merge'] == 'left_only'][['name', 'cid']]

    book_tag_ids = create_if_not_exists(tables['book_tag'], 'http://localhost:8000/brain2_api/book_tag/', ['name'])

    b_t_link = pd.merge(in_tables['books_tags_link'], book_tag_ids, left_on='tag', right_on='cid', how='left')[['book', 'rid']].rename(columns={'rid': 'tag'})


    b_a_link = b_a_link.groupby(['book'])['author'].apply(tuple).to_frame()
    b_t_link = b_t_link.groupby(['book'])['tag'].apply(tuple).to_frame()

    b_a_t_link = pd.merge(b_a_link, b_t_link, on=['book'], how='outer')
    books = pd.merge(tables['book'], b_a_t_link, left_on=['id'], right_on='book', how='outer').rename(columns={'pubdate': 'published', 'tag': 'tags'})
    books['published'] = pd.to_datetime(books['published'], infer_datetime_format=True, errors='coerce').dt.strftime('%Y-%m-%d')
    books.loc[pd.isna(books['published']), 'published'] = '0001-01-01'
    books.loc[pd.isna(books['tags']), 'tags'] = (None)
    print(books['published'])
    book_ids = create_if_not_exists(books, 'http://localhost:8000/brain2_api/book/', ['title', 'published', 'author', 'tags'])
    print(book_ids)
    # print(b_a_t_link)
    # # for book_id, df in b_a_link:
    # #     print(book_id)
    # #     for author_id in df['author']:
    # #         print(author_id)