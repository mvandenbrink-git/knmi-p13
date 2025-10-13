# tests/test_database.py
import os
import pytest
from knmi.database import KNMIDatabase

@pytest.fixture
def test_db(tmp_path):
    db_path = tmp_path / "test.db"
    db = KNMIDatabase(db_path)
    db.create_database()
    return db

def test_create_and_insert(test_db):
    data = [("260", "2024-01-01", 5.0, 0.0)]
    test_db.fill_database(data)
    results = test_db.get_locations()
    assert ("260",) in results
