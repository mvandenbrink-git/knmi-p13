# knmi/processor.py
import pandas as pd

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

        df_stations = pd.DataFrame()

        startdate = str(self.Filter.startyear) + '01-01'
        enddate = str(self.Filter.endyear) + '12-31'

        for loc in self.filter_locations():
            Pseries = self.db.get_location_timeseries(loc,'P',startdate,enddate)
            Eseries = self.db.get_location_timeseries(loc,'E',startdate,enddate)
