from configparser import ConfigParser
import psycopg2
import sqlite3
import pandas as pd

def pull_from_calibre(path):
    con = sqlite3.connect(path)
    tables = {
        'author' : pd.read_sql_query("SELECT id, sort FROM authors", con),
        'books' : pd.read_sql_query("SELECT id, title, pubdate FROM books", con),
        'books_authors_link' : pd.read_sql_query("SELECT id, book, author FROM books_authors_link", con),
        'sdr' : pd.read_sql_query("SELECT id, book, value FROM custom_column_6", con),
        'books_tags_link' : pd.read_sql_query("SELECT book, tag FROM books_tags_link", con),
        'tags' : pd.read_sql_query("SELECT id, name FROM tags", con)
    }
    con.close()

    return tables

def update_db(tables, con): 
    # get the current values in the database for each table. 
    cur_dfs = {
    'books': pd.read_sql_query("SELECT * FROM books", con),
    'annotations': pd.read_sql_query("SELECT * FROM annotations", con),
    'books_tags_link': pd.read_sql_query("SELECT * FROM books_tags_link", con),
    'tags': pd.read_sql_query("SELECT * FROM tags", con)
    }

    # add the values from calibre
    final_dfs = {
        'books' : pd.concat([cur_dfs['books'].reset_index(drop = True), tables['books']]).drop_duplicates(keep=False),
        'annotations' : pd.concat([cur_dfs['annotations'].reset_index(drop = True), tables['annotations']]).drop_duplicates(keep=False),
        'books_tags_link' : pd.concat([cur_dfs['books_tags_link'].reset_index(drop = True), tables['books_tags_link']]).drop_duplicates(keep=False),
        'tags' : pd.concat([cur_dfs['tags'].reset_index(drop = True), tables['tags']]).drop_duplicates(keep=False)
    }

    # send dataframe to postgres table
    for k, v in final_dfs.items():
        print(k, v)
        v.to_sql(k, con, if_exists = 'append', index = False)
    