import argparse, time, re, sqlite3
from datetime import datetime, timezone
from pathlib import Path
import requests
import pandas as pd
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential
from src.config import DATABASE_URL

WIKI_LIST = "https://en.wikipedia.org/wiki/List_of_UFC_events"
HEADERS = {"User-Agent":"CornerPro/1.0 (+https://github.com/mmastrikingtee/cornerpro)"}

def db_path_from_url(url: str) -> str:
    return url.split('///',1)[1] if 'sqlite' in url else './data/cornerpro.sqlite'

def slug_fighter(name: str) -> str:
    return re.sub(r"[^a-z0-9]+","-", name.lower()).strip("-")

@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=1, max=6))
def get(url: str):
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r

def parse_event_table():
    r = get(WIKI_LIST)
    soup = BeautifulSoup(r.text, "lxml")
    h2s = soup.select("h2")
    idx = None
    for i,h in enumerate(h2s):
        if "Upcoming events" in h.get_text():
            idx = i
            break
    if idx is None:
        tables = pd.read_html(r.text)
        for t in tables:
            if {"Date","Event","Location"} <= set(t.columns):
                df = t; break
        else:
            raise RuntimeError("Could not find events table")
        df["Link"] = ""
        return df
    table = h2s[idx].find_next("table")
    df = pd.read_html(str(table))[0]
    links = []
    for row in table.select("tr")[1:]:
        a = row.select_one("a[href]")
        links.append("https://en.wikipedia.org"+a["href"] if a else "")
    df["Link"] = links
    return df

def to_iso(date_str: str) -> str:
    try:
        dt = datetime.strptime(date_str.strip(), "%B %d, %Y").replace(tzinfo=timezone.utc)
        return dt.date().isoformat()
    except Exception:
        return date_str.strip()

def upsert(con, table, rowdict, pk):
    cols = list(rowdict.keys())
    vals = [rowdict[c] for c in cols]
    placeholders = ",".join(["?"]*len(cols))
    cur = con.cursor()
    cur.execute(f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) "
                f"ON CONFLICT({pk}) DO UPDATE SET " +
                ",".join([f"{c}=excluded.{c}" for c in cols if c!=pk]), vals)
    con.commit()

def ingest(days_ahead: int = 120, max_events: int = 5, polite_delay: float = 1.0):
    dbp = db_path_from_url(DATABASE_URL)
    Path(dbp).parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(dbp); cur = con.cursor()
    schema = Path("db/schema.sql").read_text(encoding="utf-8")
    cur.executescript(schema); con.commit()

    events_df = parse_event_table()
    today = datetime.utcnow().date().isoformat()
    events_df["DateISO"] = events_df["Date"].astype(str).map(to_iso)
    future = events_df[events_df["DateISO"] >= today].head(max_events)

    for _, row in future.iterrows():
        date_iso = to_iso(str(row["Date"]))
        name = str(row["Event"])
        location = str(row.get("Location",""))
        link = str(row.get("Link",""))
        org = "UFC"
        event_id = f"UFC_{date_iso}_{re.sub(r'[^a-z0-9]+','-',name.lower()).strip('-')}"
        upsert(con, "events", {
            "event_id": event_id,
            "org": org,
            "event_date": date_iso,
            "name": name,
            "location": location
        }, "event_id")

        if not link:
            continue

        try:
            time.sleep(polite_delay)
            rr = get(link)
            subsoup = BeautifulSoup(rr.text, "lxml")
            card_table = None
            for h in subsoup.select("h2, h3"):
                if "Fight card" in h.get_text():
                    tbl = h.find_next("table")
                    if tbl: card_table = tbl; break
            if card_table is None:
                for tbl in subsoup.select("table.wikitable"):
                    ths = [th.get_text(strip=True) for th in tbl.select("th")]
                    joined = " ".join(ths).lower()
                    if ("weight" in joined) and ("vs" in joined or "fighter" in joined or "bout" in joined):
                        card_table = tbl; break
            if card_table is None:
                continue

            df = pd.read_html(str(card_table))[0]
            cols = [c.lower().strip() for c in df.columns]
            df.columns = cols
            if "fighter 1" in cols and "fighter 2" in cols:
                f1c, f2c = "fighter 1","fighter 2"
            elif "fighter1" in cols and "fighter2" in cols:
                f1c, f2c = "fighter1","fighter2"
            elif "red corner" in cols and "blue corner" in cols:
                f1c, f2c = "red corner","blue corner"
            elif "bout" in cols:
                names = df["bout"].astype(str).str.split(" vs ", n=1, expand=True)
                df["fighter_a"] = names[0]; df["fighter_b"] = names[1]
                f1c, f2c = "fighter_a","fighter_b"
            else:
                obj_cols = [c for c in cols if df[c].dtype=='O'][:2]
                if len(obj_cols) >= 2: f1c, f2c = obj_cols[0], obj_cols[1]
                else: continue

            wcol = None
            for cand in ["weight class","division","weight","wt"]:
                if cand in cols: wcol = cand; break

            for _, r in df.iterrows():
                a = str(r.get(f1c,"")).strip()
                b = str(r.get(f2c,"")).strip()
                if not a or not b or a == "-" or b == "-": continue
                weight = str(r.get(wcol,"")).strip() if wcol else ""
                a_id = slug_fighter(a); b_id = slug_fighter(b)

                upsert(con, "fighters", {
                    "fighter_id": a_id, "name": a, "dob": None, "stance": None, "height_cm": None, "reach_cm": None
                }, "fighter_id")
                upsert(con, "fighters", {
                    "fighter_id": b_id, "name": b, "dob": None, "stance": None, "height_cm": None, "reach_cm": None
                }, "fighter_id")

                bout_id = f"{event_id}_{a_id}_vs_{b_id}"
                upsert(con, "bouts", {
                    "bout_id": bout_id,
                    "event_id": event_id,
                    "fighter_a_id": a_id,
                    "fighter_b_id": b_id,
                    "weight_class": weight,
                    "scheduled_rounds": None,
                    "result": None,
                    "winner_id": None
                }, "bout_id")

        except Exception as e:
            print(f"[warn] could not parse {link}: {e}")
            continue

    con.close()
    print("Ingest complete.")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--days-ahead", type=int, default=120)
    args = ap.parse_args()
    ingest(days_ahead=args.days_ahead)
