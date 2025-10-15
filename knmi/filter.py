#knmi/filter.py

class KNMI_data_filter:
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

   