# main.py
from knmi.downloader import KNMIDownloader
from knmi.database import KNMIDatabase
from knmi.processor import KNMIProcessor
from config import xlBasisfile, dbFile 

def main():
    # Instantieer classes
    db = KNMIDatabase(dbFile, xlBasisfile)
    
    downloader = KNMIDownloader(db)
    processor = KNMIProcessor(db)

    # als de opgegeven database-file niet bestaat, wordt een nieuwe database aangemaakt
    if not db.exists():
        db.create_database()
        db.fill_database()

    # de database wordt (aan)gevuld met de meest recente data
    downloader.update_data()

    #stations = db.get_locations()
    #print("Stations:", stations)

    #gem_temp = processor.gemiddelde_temperatuur("260")
    #print(f"Gemiddelde temperatuur station 260: {gem_temp:.2f} °C")

if __name__ == "__main__":
    main()
