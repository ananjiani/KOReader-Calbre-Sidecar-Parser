import sqlite3
import pandas as pd
import json
import psycopg2

# conn = psycopg2.connect(
#     host="localhost",
#     database="books",
#     user="postgres"
# )

con = sqlite3.connect("/home/ammar/Calibre Library/metadata.db")


books_df = pd.read_sql_query("SELECT id, title, sort, author_sort, pubdate, uuid FROM books", con).rename(columns={'author_sort': 'author'})
sdr_df = pd.read_sql_query("SELECT book, value FROM custom_column_6", con)
books_tags_link_df = pd.read_sql_query("SELECT book, tag FROM books_tags_link", con)
tags_df = pd.read_sql_query("SELECT id, name FROM tags", con)

annotations = pd.DataFrame(columns = ['book', 'highlight', 'note', 'location', 'chapter', 'datetime'])
for index, row in sdr_df.iterrows():
    val = row['value']
    data = json.loads(val)
    bookmarks = pd.DataFrame(columns = ['datetime', 'chapter', 'text', 'pos0', 'pos1'])
    highlights = pd.DataFrame(columns = ['datetime', 'chapter', 'text', 'pos0', 'pos1'])

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

    a = highlights.merge(bookmarks, on=['datetime', 'chapter', 'pos0', 'pos1'], how='outer').rename(columns={'text_x': 'highlight', 'text_y': 'note', 'pos0': 'location'})[['highlight', 'note', 'location', 'chapter', 'datetime']]
    a['book'] = row['book']

    annotations = pd.concat([annotations, a]).reset_index(drop = True)

# print(pd.merge(books_df, annotations, left_on='id', right_on='book', how='outer'))

con.close()

b_con = sqlite3.connect("books.db")

cur_dfs = {
    'books': pd.read_sql_query("SELECT * FROM books", b_con),
    'annotations': pd.read_sql_query("SELECT * FROM annotations", b_con),
    'books_tags_link': pd.read_sql_query("SELECT * FROM books_tags_link", b_con),
    'tags': pd.read_sql_query("SELECT * FROM tags", b_con)
}

final_dfs = {
    'books' : pd.concat([cur_dfs['books'].reset_index(drop = True), books_df]).drop_duplicates(keep=False),
    'annotations' : pd.concat([cur_dfs['annotations'].reset_index(drop = True), annotations]).drop_duplicates(keep=False),
    'books_tags_link' : pd.concat([cur_dfs['books_tags_link'].reset_index(drop = True), books_tags_link_df]).drop_duplicates(keep=False),
    'tags' : pd.concat([cur_dfs['tags'].reset_index(drop = True), tags_df]).drop_duplicates(keep=False)
}

for k, v in final_dfs.items():
    print(k, v)
    v.to_sql(k, b_con, if_exists = 'append', index = False)


