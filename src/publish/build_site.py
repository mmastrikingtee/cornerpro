from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from datetime import datetime
import sqlite3, os, urllib.parse
from src.config import DATABASE_URL

def _db():
    db_path = DATABASE_URL.split('///',1)[1] if 'sqlite' in DATABASE_URL else './data/cornerpro.sqlite'
    return sqlite3.connect(db_path)

def slug(s): 
    return urllib.parse.quote(s.lower().replace(' ','-'))

def build():
    out = Path('site/public'); out.mkdir(parents=True, exist_ok=True)
    env = Environment(loader=FileSystemLoader('site/templates'))
    # index
    (out/'index.html').write_text(
        env.get_template('index.html').render(timestamp=datetime.utcnow().isoformat()),
        encoding='utf-8'
    )
    # one card from DB
    con=_db(); cur=con.cursor()
    ev = cur.execute('SELECT event_id,org,event_date,name FROM events ORDER BY event_date LIMIT 1').fetchone()
    if ev:
        event_id, org, dt, name = ev
        rows = cur.execute('SELECT fa.name, fb.name, b.weight_class \
                            FROM bouts b JOIN fighters fa ON fa.fighter_id=b.fighter_a_id \
                                          JOIN fighters fb ON fb.fighter_id=b.fighter_b_id \
                            WHERE b.event_id=?',[event_id]).fetchall()
        rows = [ {'a_name':r[0], 'b_name':r[1], 'weight_class':r[2]} for r in rows ]
        card_html = env.get_template('card.html').render(event_name=name, event_date=dt, rows=rows)
        card_dir = out / 'cards' / f"{dt}-{org.lower()}-{slug(name)}"
        card_dir.mkdir(parents=True, exist_ok=True)
        (card_dir/'index.html').write_text(card_html, encoding='utf-8')
    con.close()

if __name__ == '__main__':
    build(); print('Built site to site/public/')
