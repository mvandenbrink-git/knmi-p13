# knmi/downloader.py
import pandas as pd

class KNMIDownloader:
    def __init__(self, db):
        self.db = db

    def download_KNMI_data(self, url, fdef,param,lastdate):
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

    def update_data(self):
        
        result = []
        print('Updating database...')

        stations =  self.db.get_stations()
        file_types = self.db.get_filetypes()
            
        for StatId in stations.index:
                
            # find date of last entry
            dmin, dmax = self.db.get_date_range(StatId, stations.loc[StatId,'Parameter'])
            ts = pd.to_datetime(dmax, format = 'ISO8601')
                
            new_data = self.download_KNMI_data(stations.loc[StatId,'Url'],
                                                  file_types.loc[stations.loc[StatId,'FileTypeId']],
                                                  stations.loc[StatId,'Parameter'],
                                                  ts
                                                 )
            
            if self.db.add_data(StatId, stations.loc[StatId,'Parameter'] ,new_data) == 0:
                result.append (f'{stations.loc[StatId,"Name"]}, {stations.loc[StatId,"Parameter"]}: {len(new_data)} records toegevoegd.')
            else:
                result.append (f'{stations.loc[StatId,"Name"]}, {stations.loc[StatId,"Parameter"]}: geen record toegevoegd.')
            
        return result
                
