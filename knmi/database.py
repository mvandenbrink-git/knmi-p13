# knmi/database.py
import sqlite3

class KNMI_queries:
    create_location_table_query = '''
        CREATE TABLE IF NOT EXISTS P13_locations (
        LocationId INTEGER PRIMARY KEY AUTOINCREMENT,
        LocationName TEXT NOT NULL
        );
        '''
    create_stations_table_query = '''
        CREATE TABLE IF NOT EXISTS KNMI_stations (
        StationId INTEGER PRIMARY KEY AUTOINCREMENT,
        Name TEXT NOT NULL,
        Parameter TEXT NOT NULL,
        Code INTEGER,
        Url TEXT,
        FileTypeId INTEGER,
        Timestep TEXT
        );
        '''
    create_locations_stations_table_query = '''
        CREATE TABLE IF NOT EXISTS locations_stations (
        LocationStatId INTEGER PRIMARY KEY AUTOINCREMENT,
        LocationId INTEGER NOT NULL,
        Parameter TEXT NOT NULL,
        StationId INTEGER NOT NULL,
        valid_from TEXT,
        valid_through TEXT
        );
        '''
    
    create_KNMI_file_types_table_query = '''
        CREATE TABLE IF NOT EXISTS KNMI_file_types (
        FileTypeId INTEGER PRIMARY KEY AUTOINCREMENT,
        skip_rows INTEGER,
        date_column TEXT,
        date_format TEXT,
        P_param TEXT,
        P_conversion REAL,
        E_param TEXT,
        E_conversion REAL
        );
    '''
    create_KNMI_data_table_query = '''
        CREATE TABLE IF NOT EXISTS KNMI_data (
        ObsId INTEGER PRIMARY KEY AUTOINCREMENT,
        StationId INTEGER NOT NULL,
        Parameter TEXT NOT NULL,
        Timestamp REAL NOT NULL,
        Value REAL,
        Quality TEXT
        );
        '''

class KNMIDatabase(KNMI_queries):

    
    def __init__(self, db_path):
        self.db_path = db_path

    def create_database(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            try:
                # Start a transaction
                cursor.execute("BEGIN;")

                cursor.execute(self.create_location_table_query)
                cursor.execute(self.create_stations_table_query)
                cursor.execute(self.create_locations_stations_table_query)
                cursor.execute(self.create_KNMI_data_table_query)
                cursor.execute(self.create_KNMI_file_types_table_query )

                # Commit the changes
                conn.commit()
                print("Tabellen aangemaakt")

            except Exception as e:
                # If an error occurs, rollback the transaction
                conn.rollback()
                print(f"Fout: {e}")

    def create_insert_query(tble, columns, values):
        """ hulpfunctie die een string produceert waarmee een record aan de database wordt toegevoegd.
        """
    
        cols = []
        vals = []
    
        for c,v in zip(columns, values):
            if isinstance(v,str) or isinstance(v,pd.Timestamp):
                sv = str(v).strip()
                if len(sv) > 0:
                    vals.append(sv)
                    cols.append(c)
            elif not pd.isna(v):
                vals.append(v)
                cols.append(c)
        
        if len(cols) > 1:
            str_cols = tuple(cols)
            str_vals = tuple(vals)
        else:
            str_cols = '(' + cols[0] + ')'
            str_vals = '("' + vals[0] + '")'
        
        return f'INSERT INTO {tble} {str_cols} VALUES {str_vals};'

    def fill_database(self):
        """Functie die de metadata uit het Excelbestand xlBasisFile leest en in de juiste tabellen in de database zet.
            Deze functie doet niets met de data-tabel.
        """
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
    
            for table in tables:
                df = pd.read_excel(xlBasisfile,sheet_name = table, index_col = 0)
            #columns = '(' + ','.join(df.columns) + ')'
        
                try:
                    # Start a transaction
                    cursor.execute("BEGIN;")
            
                    for values in df.itertuples(name = None):
                
                        str_insert = create_insert_query(table, df.columns, values[1:])
                        cursor.execute(str_insert)
                
                    # Commit the changes
                    conn.commit()
                    print(f"Tabel {table} gevuld.")
        
                except Exception as e:
                    conn.rollback()
                    print(f"Fout bij het vullen van tabel {table}: {e} \n" + str_insert)

    def reset_database(self):
        """Leegt de database."""
            with sqlite3.connect(self.db_path) as conn:
                conn.execute("DELETE FROM measurements")
                conn.commit()

        def get_locations(self):
            """Geeft lijst van unieke locaties/stations."""
            with sqlite3.connect(self.db_path) as conn:
                c = conn.cursor()
                return c.execute("SELECT DISTINCT station_id FROM measurements").fetchall()

    def get_table(self, station_id):
        """Geeft alle metingen voor een station."""
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            return c.execute("SELECT * FROM measurements WHERE station_id = ?", (station_id,)).fetchall()
