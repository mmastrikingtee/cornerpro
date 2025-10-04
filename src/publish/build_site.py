from jinja2 import Environment, FileSystemLoader
from pathlib import Path
from datetime import datetime
import sqlite3, urllib.parse, re, csv
from src.config import DATABASE_URL

def _db():
    db_path = DATABASE_URL.split('///',1)[1] if 'sqlite' in DATABASE_URL else './data/cornerpro.sqlite'
    return sqlite3.connect(db_path)

def slug(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+","-", s).strip("-")
    return s[:120]  # keep paths short/safe on Windows

def load_predictions():
    pred = {}
    p = Path('data/processed/predictions.csv')
    if not p.exists(): return pred
    with p.open('r', encoding='utf-8') as f:
        r = csv.DictReader(f)
        for row in r:
            pred[row['bout_id']] = row
    return pred

def build():
    out = Path('site/public'); out.mkdir(parents=True, exist_ok=True)
    env = Environment(loader=FileSystemLoader('site/templates'))
    # index
    (out/'index.html').write_text(
        env.get_template('index.html').render(timestamp=datetime.utcnow().isoformat()),
        encoding='utf-8'
    )
    # card pages for all future events
    con=_db(); cur=con.cursor()
    preds = load_predictions()
    rows = cur.execute('SELECT event_id,org,event_date,name FROM events WHERE event_date>=date("now") ORDER BY event_date').fetchall()
    for (event_id, org, dt, name) in rows:
        bouts = cur.execute('SELECT b.bout_id, fa.name, fb.name, b.weight_class \
                             FROM bouts b JOIN fighters fa ON fa.fighter_id=b.fighter_a_id \
                                           JOIN fighters fb ON fb.fighter_id=b.fighter_b_id \
                             WHERE b.event_id=?',[event_id]).fetchall()
        out_rows=[]
        for (bout_id, a_name, b_name, weight) in bouts:
            pr = preds.get(bout_id)
            if pr:
                p_a = float(pr['p_a']); p_b = float(pr['p_b'])
                odds_a = int(pr['odds_a']); odds_b = int(pr['odds_b'])
            else:
                p_a = p_b = 0.5; odds_a = odds_b = -100
            out_rows.append({
                'bout_id': bout_id,
                'a_name': a_name, 'b_name': b_name,
                'weight_class': weight,
                'p_a': p_a, 'p_b': p_b,
                'odds_a': odds_a, 'odds_b': odds_b
            })
        # SAFE folder name: yyyy-mm-dd-org-event-slug
        card_dir = out / 'cards' / f"{dt}-{slug(org)}-{slug(name)}"
        card_dir.mkdir(parents=True, exist_ok=True)
        card_html = env.get_template('card.html').render(event_name=name, event_date=dt, rows=out_rows)
        (card_dir/'index.html').write_text(card_html, encoding='utf-8')
    con.close()

if __name__ == '__main__':
    build(); print('Built site to site/public/')
