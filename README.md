# KOReader Calibre Sidecar Parser

Pulls data from Calibre local database, and parses sidecar generated by this [KOReader Calibre Plugin](https://git.sr.ht/~harmtemolder/koreader-calibre-plugin)

Designed to send data to a self hosted [brain2](https://github.com/AmmarNanjiani/brain2) through it's REST API.
# Usage

## Prerequisites
- Python installed.
- A device with [KOReader](https://github.com/koreader/koreader) installed. 
- [Calibre](https://github.com/kovidgoyal/calibre) with the [KOReader plugin](https://git.sr.ht/~harmtemolder/koreader-calibre-plugin).
- A running local instance of [brain2](https://github.com/AmmarNanjiani/brain2).

## Steps
1. Modify `config.ini` with the path to Calibre's metadata sqlite3 database, and if it is different from the default, modify the URL for the API. 
2. (Optional but recommended) create and enter a [virtual environment](https://docs.python.org/3/library/venv.html)
3. Run in a command line `pip install -r requirements.txt` in the project's root folder.
4. Run `python main.py` to send the date through the API.
5. You're done! Just run `python main.py` any time you want to sync in the future.

# Roadmap
- Pare this repository down to just the parsing code. Interaction with [brain2](https://github.com/AmmarNanjiani/brain2) should be integrated with that project.
