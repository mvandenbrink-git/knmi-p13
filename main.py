# main.py
from knmi.downloader import KNMIDownloader
from knmi.database import KNMIDatabase
from knmi.processor import KNMIProcessor
from config import xlBasisfile, dbFile 
import plotly.graph_objects as go
import pandas as pd

def build_graph(df_pd):
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=df_pd.index, y=df_pd.values, mode='lines+markers', name='Data'))
    fig.update_layout(title='KNMI Data Visualization', xaxis_title='Date', yaxis_title='Value')
    return fig

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
    #downloader.update_data()

    processor.set_filter(locations='all', startyear=2020, endyear=2024, startmonth=1, endmonth=12)

    data = processor.get_filtered_PDseries(bottom=0)

    fig = build_graph(data)
    fig.show()

    #external_stylesheets = [dbc.themes.FLATLY]

    #app = Dash(__name__, external_stylesheets=external_stylesheets, suppress_callback_exceptions=True)


if __name__ == "__main__":
    main()
