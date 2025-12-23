# knmi/database.py
import sqlite3
import pandas as pd
from pathlib import Path
import numpy as np

class KNMI_queries:

    db_tables = ['P13_locations','locations_stations', 'KNMI_stations', 'KNMI_file_types']
    db_parameters = ['P','E']

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
    
    qry_stations = "SELECT * FROM KNMI_stations;"
    qry_stations_by_location = "SELECT StationID, Parameter, valid_from, valid_through FROM locations_stations WHERE LocationID = ?;"
    qry_locations = "SELECT * FROM P13_locations;"
    qry_date_range = '''SELECT date(min(Timestamp)) AS first_date, date(max(Timestamp)) AS last_date 
                            FROM KNMI_data 
                            WHERE StationId = ? AND Parameter = ?;
                     '''
    qry_file_types = 'SELECT * from KNMI_file_types;'
    qry_insert_data = 'INSERT INTO KNMI_data (StationId, Parameter, Timestamp, Value) VALUES (?,?,?,?);'
    qry_delete_table = 'DROP TABLE IF EXISTS (?);'
    qry_station_data = '''SELECT date(Timestamp) AS date, value, quality 
                             FROM KNMI_data 
                             WHERE StationId = ? AND Parameter = ? AND Timestamp BETWEEN ? AND ?;
                       '''

class KNMIDatabase(KNMI_queries):

    
    def __init__(self, db_path, xlDefinitionFile):
        self.db_path = db_path
        self.db_def_file = xlDefinitionFile

    def exists(self):
        return Path(self.db_path).exists()

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

    def create_insert_query(self, tble, columns, values):
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
    
            for table in self.db_tables:
                df = pd.read_excel(self.db_def_file,sheet_name = table, index_col = 0)
            #columns = '(' + ','.join(df.columns) + ')'
        
                try:
                    # Start a transaction
                    cursor.execute("BEGIN;")
            
                    for values in df.itertuples(name = None):
                
                        str_insert = self.create_insert_query(table, df.columns, values[1:])
                        cursor.execute(str_insert)
                
                    # Commit the changes
                    conn.commit()
                    print(f"Tabel {table} gevuld.")
        
                except Exception as e:
                    conn.rollback()
                    print(f"Fout bij het vullen van tabel {table}: {e} \n" + str_insert)
    
    def get_locations(self):
        
        with sqlite3.connect(self.db_path) as conn:
            #cur = conn.cursor()
            #cur.execute()
            return pd.read_sql_query(self.qry_locations, conn, index_col = 'LocationId')
        
    def get_stations(self):
        
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(self.qry_stations, conn, index_col = 'StationId')
        
    def get_filetypes(self):
        with sqlite3.connect(self.db_path) as conn:
            return pd.read_sql_query(self.qry_file_types, conn, index_col = 'FileTypeId')
        
    def get_date_range(self, StationID, ParameterID):
        with sqlite3.connect(self.db_path) as conn:
            qry_result = pd.read_sql_query(self.qry_date_range, conn, params=(StationID, ParameterID))
        return qry_result['first_date'].squeeze(), qry_result['last_date'].squeeze()
        
    def get_location_stations(self, locations = 'all'):
        """
        Geeft de meest recente waarneming uit de database, per locatie en parameter.
        
        :param locations: String of list van P13 locaties. Default: 'all'
        """
        # get a list of LocationIDs for the query
        locs = self.get_locations()
        if locations == 'all':
            locs = locs.index
        elif isinstance(locations, list):
            locs = locs.loc[locs['LocationName'].isin(locations)].index
        elif isinstance(locations, str):
            locs = locs.loc[locs['LocationName'] == locations].index
        else:
            raise TypeError(f'Location: Unsupported object type, expected str or list, not {type(locations)}')
            
        if len(locs) == 0:
            raise ValueError(f"None of given locations found in database.")
        
        # retrieve data for the selected locations
        qry_result = pd.DataFrame() 
        with sqlite3.connect(self.db_path) as conn:      
            for loc in locs:
                qry_result = pd.concat([qry_result, pd.read_sql_query(self.qry_stations_by_location, conn, 
                                                                      params = [str(loc)])])

        # retrieve info about the availability of timeseries data
        obs_dates = []
        for i,stat_param in qry_result.iterrows():
            obs_dates.append(self.get_date_range(str(stat_param['StationId']), str(stat_param['Parameter'])))
        
        qry_result['first_date'] = [d[0] for d in obs_dates]
        qry_result['last_date'] =  [d[1] for d in obs_dates]

        # match observation validity and availability and return the date range of valid and available observations
        # for eacht (station, paramater) pair
        qry_result['obs_from'] = np.fmax(pd.to_datetime(qry_result['valid_from'], format = '%d-%m-%Y'), 
                                         pd.to_datetime(qry_result['first_date'], format = '%Y-%m-%d'))
        qry_result['obs_through'] = np.fmin(pd.to_datetime(qry_result['valid_through'], format = '%d-%m-%Y'), 
                                         pd.to_datetime(qry_result['last_date'], format = '%Y-%m-%d'))

        
        return qry_result
    
    def add_data(self, StatID, Parameter, timeseries):
        """
        Adds one or more records to the database
        
        :param StatID: ID of the station where the records will be added
        :param Parameter: ID of the parameter to assign the data to
        :param timeseries: Pandas Series object with a datetime index containing one or more records to add
        """
        n = len(timeseries)
        all_records = list(zip([StatID]*n,
                                [Parameter]*n,
                                timeseries.index.to_julian_date(),
                                timeseries
                              ))
        
        with sqlite3.connect(self.db_path) as conn:           
            try:
                cursor = conn.cursor()
                cursor.execute("BEGIN;")
                cursor.executemany(self.qry_insert_data,all_records)
                conn.commit()

                return 0  # succes
            
            except Exception as e:
                conn.rollback()

                return -1 # failed

    def get_station_timeseries(self, StatID, Param, start_date, end_date):
        
        start = pd.to_datetime(start_date).to_julian_date()
        end = pd.to_datetime(end_date).to_julian_date()

        with sqlite3.connect(self.db_path) as conn:
            qry_result = pd.read_sql_query(self.qry_station_data, conn, params = (StatID, Param, start, end))
            qry_result['date'] = pd.to_datetime(qry_result['date'], format = '%Y-%m-%d')

        return qry_result.set_index('date').sort_index()

    def get_location_timeseries(self, LocationID, Param, start_date, end_date):
        """
        Returns the timeseries of a parameter for a specific location
        
        :param LocationID: Name of the location (string)
        :param Param: Name of the parameter ('P' or 'E')
        :param start_date: first date of the timeseries (string of date like)
        :param end_date: last date of the timeseries (string of date like)

        :returns: pandas dataframe with a datetime-index and two columns 'Value', 'Quality'
        """
        start = pd.to_datetime(start_date)
        end = pd.to_datetime(end_date)

        stats = self.get_location_stations(LocationID)

        result = pd.DataFrame()
        for i,stat in stats[stats['Parameter'] == Param].iterrows():
            res_station = self.get_station_timeseries(stat['StationId'],Param,
                                                                   max(start, stat['obs_from']),
                                                                   min(end,stat['obs_through']))
            if len(res_station) != 0:
                result = pd.concat([result,res_station])
        return result.sort_index()

    