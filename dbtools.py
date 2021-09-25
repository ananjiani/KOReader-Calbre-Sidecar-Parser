from configparser import ConfigParser
import psycopg2
import sqlite3
import pandas as pd


def config(filename='database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        # create a cursor
        cur = conn.cursor()
        
	# execute a statement
        print('PostgreSQL database version:')
        cur.execute('SELECT version()')

        # display the PostgreSQL database server version
        db_version = cur.fetchone()
        print(db_version)

        cur.execute('SELECT * from books')
        print("Result ", cur.fetchall())
       
	# close the communication with the PostgreSQL
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()
            print('Database connection closed.')

def pull_from_calibre(path):
    con = sqlite3.connect(path)
    tables = {
        'books' : pd.read_sql_query("SELECT id, title, sort, author_sort, pubdate, uuid FROM books", con).rename(columns={'author_sort': 'author'}),
        'sdr' : pd.read_sql_query("SELECT book, value FROM custom_column_6", con),
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
    