# knmi/processor.py
import pandas as pd

def cumsum_with_min(s, minimum=0):
    sl = list(s)
    
    res = []
    for i in range(len(sl)):
        if i == 0:
            res.append(max(minimum,sl[i]))
        else:
            res.append(max(minimum,sl[i] +  res[-1]))

    if isinstance(s, pd.Series):
        return pd.Series(res, index=s.index)
    else:
        return res

class KNMIDataFilter:
    def __init__(self):
        self.locations = 'all'
        self.startyear = 0
        self.endyear = 0
        self.startmonth = 4
        self.endmonth = 9

    def set_filter (self, locations = 'all', startyear = 0, endyear = 0, startmonth = 4, endmonth = 9):
        self.locations = locations
        self.startyear = startyear
        self.endyear = endyear
        self.startmonth = startmonth
        self.endmonth = endmonth

    def clear_filter (self):
        self.locations = 'all'
        self.startyear = 0
        self.endyear = 0
        self.startmonth = 4
        self.endmonth = 9

    def __str__(self):
        start = str(self.startyear) if self.startyear != 0 else 'begin'
        end = str(self.endyear) if self.endyear != 0 else 'end'
        return f'KNMI data filter object (locations: {self.locations}, years: {start} - {end}, months: {self.startmonth} - {self.endmonth})'
    
    def __repr__(self):
        return f'KNMIDataFilter'

class KNMIProcessor:
    def __init__(self, db):
        self.db = db
        self.Filter = KNMIDataFilter()

    def set_filter(self, locations = 'all', startyear = 0, endyear = 0, startmonth = 4, endmonth = 9):
        self.Filter.set_filter(locations, startyear, endyear, startmonth, endmonth)

    def clear_filter(self):
        self.Filter.clear_filter()

    def filter_locations(self):
        locs = self.Filter.locations
        if locs == 'all':
            locs = list(self.db.get_locations()['LocationName'])
        return locs

    def get_filtered_PDseries(self, bottom = None):
        """
        Berekent een tijdreeks voor het cumulatief neerslagtekort, gebaseerd op de filter-instellingen
        
        :param bottom: bepaalt de laagst mogelijke waarde die de tijdreeks kan krijgen. De waarde 0 is gebruikelijk. Default: None
        """

        if bottom is None:
            bottom = float('-inf')
        else:
            bottom = float(bottom)

        startdate = str(self.Filter.startyear) + '-01-01'
        enddate = str(self.Filter.endyear) + '-12-31'
        all_dates = pd.date_range(startdate, enddate, freq='D')

        df_stations = pd.DataFrame({'date' : all_dates}).set_index('date')
        for loc in self.filter_locations():

            print(f'Processing location: {loc}')
            df_pd = pd.DataFrame({'date' : all_dates}).set_index('date')
            Pseries = self.db.get_location_timeseries(loc,'P',startdate,enddate)
            Eseries = self.db.get_location_timeseries(loc,'E',startdate,enddate)
            df_pd = df_pd.merge(Pseries['Value'],left_index=True, right_index= True).merge(Eseries['Value'], left_index=True, right_index=True, suffixes=('_P','_E'))

            df_pd = df_pd.loc[(df_pd.index.month >= self.Filter.startmonth) & (df_pd.index.month <= self.Filter.endmonth)]
            df_pd['pd_' + loc] = df_pd['Value_E'] - df_pd['Value_P']
            
            df_stations = df_stations.merge(df_pd['pd_' + loc], left_index=True, right_index=True)

        df_mean = df_stations.mean(axis=1)

        return df_mean.groupby(df_mean.index.year).apply(cumsum_with_min,bottom).droplevel(0)
