from parse import pull_from_calibre
from format import post_all
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

path = config['CONFIG']['PATH']
url = config['CONFIG']['URL']

post_all(pull_from_calibre(path), url)
