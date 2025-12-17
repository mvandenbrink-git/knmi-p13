# knmi/processor.py
import numpy as np

class KNMIProcessor:
    def __init__(self, db_path):
        self.db_path = db_path

    def gemiddelde_temperatuur(self, station_id):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            data = c.execute(
                "SELECT temperature FROM measurements WHERE station_id = ?", 
                (station_id,)
            ).fetchall()
        temps = [row[0] for row in data if row[0] is not None]
        return np.mean(temps) if temps else None

    def neerslag_som(self, station_id):
        with sqlite3.connect(self.db_path) as conn:
            c = conn.cursor()
            data = c.execute(
                "SELECT precipitation FROM measurements WHERE station_id = ?", 
                (station_id,)
            ).fetchall()
        prec = [row[0] for row in data if row[0] is not None]
        return np.sum(prec) if prec else 0
