import sqlite3
from pathlib import Path
from src.config import DATABASE_URL

def db_path():
    return DATABASE_URL.split('///',1)[1] if 'sqlite' in DATABASE_URL else './data/cornerpro.sqlite'

def ensure_elo():
    con = sqlite3.connect(db_path()); cur = con.cursor()
    # ensure elo table exists
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS elo_ratings (
      fighter_id TEXT PRIMARY KEY,
      elo REAL,
      last_fight TEXT
    );
    """)
    # add missing fighters with default 1500
    cur.execute("""
      INSERT INTO elo_ratings(fighter_id, elo, last_fight)
      SELECT f.fighter_id, 1500.0, NULL
      FROM fighters f
      LEFT JOIN elo_ratings e ON e.fighter_id = f.fighter_id
      WHERE e.fighter_id IS NULL
    """)
    con.commit(); con.close()

if __name__ == "__main__":
    ensure_elo()
    print("Elo initialized for all fighters (default 1500).")
