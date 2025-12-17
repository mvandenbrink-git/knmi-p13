class P13_data_object:
    
    def __init__(self, dbFile, LocIdx, startyear, endyear, startmonth, endmonth):
        self.locations = LocIdx
        self.startyear = startyear
        self.endyear = endyear
        self.startmonth = startmonth
        self.endmonth = endmonth
        self.dbFile = dbFile
    
    def LocationParameterSeries (self,LocIdx, param):
        
        qry_stations = f'''SELECT StationId, valid_from, valid_through 
                            FROM locations_stations
                            WHERE LocationId = {LocIdx} AND Parameter = "{param}"; 
                       '''
        
        qry_data = f'''SELECT date(Timestamp) as date, Value FROM KNMI_data 
                          WHERE StationId = ? AND Parameter = "{param}" AND Timestamp BETWEEN ? AND ?;
                    '''
    
        with sqlite3.connect('data/KNMI_P13.db') as conn:
            stations = pd.read_sql_query(qry_stations,conn,
                                         parse_dates = {'valid_from':'%d-%m-%Y', 'valid_through':'%d-%m-%Y'}
                                        )
            s = pd.Series()
            for r in stations.itertuples():
                last_requested_date = pd.to_datetime({'year':self.endyear,'month': 12, 'day':31})
                if pd.isna(r.valid_through):
                    last_valid_date = pd.Timestamp.today()
                else:
                    last_valid_date = r.valid_through
                
                lastdate = min(last_requested_date,last_valid_date)
                
                first_requested_date = pd.to_datetime({'year':self.startyear,'month': 1, 'day': 1})
                first_valid_date = r.valid_from
                
                firstdate = max(first_requested_date, first_valid_date)
                
                data = pd.read_sql_query(qry_data,conn,
                                         parse_dates = ['date'],
                                         params = (r.StationId,firstdate.to_julian_date(), lastdate.to_julian_date()),
                                         index_col = 'date'
                                        ).squeeze()
                s.append(data,verify_integrity = True)
            
        s = s.sort_index()
        s = s.loc[s.index.month >= self.startmonth | s.index.month <= self.endmonth ]
        return s
            
        
    def series(self):
        pass
        
    def stats(self):
        pass
        
class KNMI_P13:
    
    def __init__ (self, DatabaseFile):
        self.dbFile = DatabaseFile
        
    def locations(self):
        select_query = "SELECT * FROM P13_locations;"
        with sqlite3.connect(self.dbFile) as conn:
            return pd.read_sql_query(select_query, conn, index_col = 'LocationId')
        
    
        
    def define_data(locations = 'all', startyear = 0, endyear = 0, startmonth = 4, endmonth = 9):
        
        start_yr = max(startyear,1906)
        end_yr = min(endyear, pd.Timestamp.today().year)
        
        locs = self.locations()
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
            
        return P13_data_object(self.dbFile, locs, start_yr, end_yr, startmonth, endmonth)
        
    #def add_station(self, parameter, station):
    #    p = str(parameter).upper()
    #    if isinstance(station, KNMI_station):
    #        if str(p) == 'P':
    #            self.P_stations.append(station)
    #        elif str(p) == 'E':
    #            self.E_stations.append(station)
    #        else:
    #            raise ValueError(f"Parameter '{parameter}' given, expected 'P' or 'E'.")
    #    else:
    #        raise TypeError(f'{station}: Unsupported object type, expected KNMI_station.')
            
    def __str__(self):
        return f'KNMI P13 object (database: {self.dbFile})'
    
    def __repr__(self):
        return f'KNMI_P13({self.dbFile})'
    

def download_KNMI_data(url, fdef,param,lastdate):
        # download file and read into data frame
        #print(fdef, type(fdef))
        df = pd.read_table(url.strip(), 
                           skiprows = fdef['skip_rows'], 
                           sep = ',', low_memory = False,
                           parse_dates = [fdef['date_column']], date_format = fdef['date_format']
                          )
        
        # trim the column names
        df = df.rename(columns = lambda x: x.strip())
        
        # select relevant parameter/column and convert to mm
        df = df[[fdef['date_column'], fdef[param + '_param']]].set_index(fdef['date_column']).squeeze()
        df = pd.to_numeric(df,errors = 'coerce') * fdef[param + '_conversion']
        
        # select new observations to add to the database
        if pd.isna(lastdate):
            df = df.loc[~pd.isna(df)]
        else:
            df = df.loc[df.index > lastdate]
          
        # return a pandas Series object
        return df

#
xlBasisfile = 'P13.xlsx'

dbFile = 'data/KNMI_P13.db'

# naam van de tabbladen, worden ook de namen van de tabellen in de SQLite database
tables = ['P13_locations','locations_stations', 'KNMI_stations', 'KNMI_file_types']

# https://www.freecodecamp.org/news/work-with-sqlite-in-python-handbook/
# https://www.sqlitetutorial.net/sqlite-date/




        
drop_tables = tables.copy()
drop_tables.append('KNMI_data')

with sqlite3.connect(dbFile) as conn:
    cursor = conn.cursor()
    
    for table in drop_tables:
        try:
            qry_delete_table = f'DROP TABLE IF EXISTS {table};'
            cursor.execute(qry_delete_table)
            
            print(f'Tabel {table} verwijderd.')
            conn.commit()
        except Exception as e:
            print(f'Fout: {e}')
            conn.rollback()
            
create_new_database()
fill_tables()

