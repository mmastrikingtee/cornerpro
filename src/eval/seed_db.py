from pathlib import Path
import sqlite3
from src.config import DATABASE_URL

def main():
    db_path = DATABASE_URL.split('///',1)[1] if 'sqlite' in DATABASE_URL else './data/cornerpro.sqlite'
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(db_path); cur = con.cursor()
    cur.executescript(Path('db/schema.sql').read_text(encoding='utf-8'))
    # seed a demo event if empty
    if cur.execute('SELECT COUNT(*) FROM events').fetchone()[0] == 0:
        cur.execute('INSERT INTO events VALUES (?,?,?,?,?)',
            ('ev_1','UFC','2099-01-01','UFC Sample Card','Las Vegas'))
        cur.executemany('INSERT INTO fighters VALUES (?,?,?,?,?,?)',[
            ('f_a','Sample A','1990-01-01','Orthodox',180,190),
            ('f_b','Sample B','1992-02-02','Southpaw',178,178),
        ])
        cur.execute('INSERT INTO bouts VALUES (?,?,?,?,?,?,?,?)',
            ('bt_1','ev_1','f_a','f_b','MW',3,NULL,NULL))
        cur.executemany('INSERT INTO elo_ratings VALUES (?,?,?)',[
            ('f_a',1500,''),('f_b',1500,'')
        ])
    con.commit(); con.close()
    print('Seeded demo data.')

if __name__ == '__main__':
    main()
