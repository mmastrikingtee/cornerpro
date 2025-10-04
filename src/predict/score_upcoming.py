import math, sqlite3, json
from pathlib import Path
from datetime import datetime
from src.config import DATABASE_URL

OUT_CSV = Path("data/processed/predictions.csv")

def db_path():
    return DATABASE_URL.split('///',1)[1] if 'sqlite' in DATABASE_URL else './data/cornerpro.sqlite'

def elo_prob(ra, rb):
    # classic Elo expectation (base 10)
    return 1.0 / (1.0 + 10 ** ((rb - ra) / 400.0))

def fair_american(p):
    # Convert probability to American odds
    p = max(min(p, 0.999), 0.001)
    if p >= 0.5:
        return int(round(-100 * p / (1 - p)))
    else:
        return int(round(100 * (1 - p) / p))

def main():
    con = sqlite3.connect(db_path()); cur = con.cursor()
    # events (future only)
    today = datetime.utcnow().date().isoformat()
    events = cur.execute("SELECT event_id, name, event_date, org FROM events WHERE event_date >= ? ORDER BY event_date", [today]).fetchall()
    # join bouts with fighters & elo
    rows = []
    for ev_id, ev_name, ev_date, org in events:
        bouts = cur.execute("""
            SELECT b.bout_id, b.fighter_a_id, b.fighter_b_id, b.weight_class,
                   fa.name, fb.name,
                   COALESCE(ea.elo,1500), COALESCE(eb.elo,1500)
            FROM bouts b
            JOIN fighters fa ON fa.fighter_id=b.fighter_a_id
            JOIN fighters fb ON fb.fighter_id=b.fighter_b_id
            LEFT JOIN elo_ratings ea ON ea.fighter_id=b.fighter_a_id
            LEFT JOIN elo_ratings eb ON eb.fighter_id=b.fighter_b_id
            WHERE b.event_id = ?
        """, [ev_id]).fetchall()
        for (bout_id, a_id, b_id, weight, a_name, b_name, ra, rb) in bouts:
            p_a = elo_prob(ra, rb)
            p_b = 1 - p_a
            odds_a = fair_american(p_a)
            odds_b = fair_american(p_b)
            rows.append([ev_id, ev_name, ev_date, org, bout_id, a_id, b_id, a_name, b_name, weight, ra, rb, p_a, p_b, odds_a, odds_b])

    OUT_CSV.parent.mkdir(parents=True, exist_ok=True)
    # write simple CSV by hand to avoid extra deps
    with OUT_CSV.open("w", encoding="utf-8") as f:
        f.write("event_id,event_name,event_date,org,bout_id,a_id,b_id,a_name,b_name,weight,elo_a,elo_b,p_a,p_b,odds_a,odds_b\n")
        for r in rows:
            # escape commas in names if any (very rare)
            safe = [str(x).replace(",", " ") if isinstance(x,str) else x for x in r]
            f.write(",".join(map(str, safe)) + "\n")

    # also write a tiny JSON index for site if needed later
    api = Path("site/public/api"); api.mkdir(parents=True, exist_ok=True)
    cards = {}
    for r in rows:
        k = (r[0], r[1], r[2], r[3])  # (event_id, name, date, org)
        cards.setdefault(k, []).append({
            "bout_id": r[4], "a": r[7], "b": r[8],
            "p_a": r[12], "p_b": r[13],
            "odds_a": r[14], "odds_b": r[15],
            "weight": r[9]
        })
    out = []
    for (ev_id, name, date, org), fights in cards.items():
        out.append({"event_id": ev_id, "name": name, "date": date, "org": org, "fights": fights})
    Path("site/public/api/cards.json").write_text(json.dumps(out, indent=2), encoding="utf-8")

    con.close()
    print(f"Wrote {len(rows)} predictions to {OUT_CSV}")

if __name__ == "__main__":
    main()
