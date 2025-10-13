# config.py
DB_PATH = "data/knmi_data.db"
KNMI_BASE_URL = "https://www.daggegevens.knmi.nl/klimatologie/daggegevens"
DOWNLOAD_DIR = "data"

xlBasisfile = 'P13.xlsx'

dbFile = 'data/KNMI_P13.db'

# naam van de tabbladen, worden ook de namen van de tabellen in de SQLite database
tables = ['P13_locations','locations_stations', 'KNMI_stations', 'KNMI_file_types']

