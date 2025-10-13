# knmi/downloader.py
import requests
import os

class KNMIDownloader:
    def __init__(self, base_url, download_dir):
        self.base_url = base_url
        self.download_dir = download_dir

    def download_data(self, station_id, start_date, end_date):
        """Downloadt data van KNMI voor een station en datumrange."""
        url = f"{self.base_url}?stns={station_id}&start={start_date}&end={end_date}"
        response = requests.get(url)
        response.raise_for_status()
        file_path = os.path.join(self.download_dir, f"{station_id}_{start_date}_{end_date}.txt")
        with open(file_path, "w") as f:
            f.write(response.text)
        return file_path
