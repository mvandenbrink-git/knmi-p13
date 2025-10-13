# main.py
from knmi.downloader import KNMIDownloader
from knmi.database import KNMIDatabase
from knmi.processor import KNMIProcessor
from config import DB_PATH, KNMI_BASE_URL, DOWNLOAD_DIR

def main():
    # Instantieer classes
    downloader = KNMIDownloader(KNMI_BASE_URL, DOWNLOAD_DIR)
    db = KNMIDatabase(DB_PATH)
    processor = KNMIProcessor(DB_PATH)

    # Voorbeeld workflow
    db.create_database()
    file_path = downloader.download_data(station_id="260", start_date="20240101", end_date="20240131")
    
    # Stel hier parse-functie in (bijv. parse_knmi_file)
    parsed_data = parse_knmi_file(file_path)  # Zelf te implementeren
    db.fill_database(parsed_data)

    stations = db.get_locations()
    print("Stations:", stations)

    gem_temp = processor.gemiddelde_temperatuur("260")
    print(f"Gemiddelde temperatuur station 260: {gem_temp:.2f} °C")

if __name__ == "__main__":
    main()
