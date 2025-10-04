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

def find_upcoming_table(soup: BeautifulSoup):
    for h in soup.select("h2, h3"):
        txt = h.get_text(" ", strip=True).lower()
        if any(k in txt for k in ["upcoming events", "scheduled events", "future events"]):
            t = h.find_next("table", class_="wikitable")
            if t: return t
    for t in soup.select("table.wikitable"):
        ths = [th.get_text(strip=True).lower() for th in t.select("th")]
        if "date" in ths and "event" in ths:
            return t
    return None

def parse_event_table():
    r = get(WIKI_LIST)
    soup = BeautifulSoup(r.text, "lxml")
    table = find_upcoming_table(soup)
    if table is None:
        raise RuntimeError("Could not find Upcoming/Scheduled events table on Wikipedia.")
    rows = []
    for tr in table.select("tr")[1:]:
        tds = tr.find_all("td")
        if len(tds) < 3:
            continue
        date_txt = tds[0].get_text(" ", strip=True)
        event_cell = tds[1]
        event_txt = event_cell.get_text(" ", strip=True)
        link_el = event_cell.find("a", href=True)
        link = ("https://en.wikipedia.org" + link_el["href"]) if link_el else ""
        location_txt = tds[2].get_text(" ", strip=True)
        rows.append({"Date": date_txt, "Event": event_txt, "Location": location_txt, "Link": link})
    return pd.DataFrame(rows)

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
    cur.execute(
        f"INSERT INTO {table} ({','.join(cols)}) VALUES ({placeholders}) "
        f"ON CONFLICT({pk}) DO UPDATE SET " +
        ",".join([f"{c}=excluded.{c}" for c in cols if c != pk]),
        vals
    )
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
                if "fight card" in h.get_text(" ", strip=True).lower():
                    tbl = h.find_next("table", class_="wikitable")
                    if tbl: card_table = tbl; break
            if card_table is None:
                for tbl in subsoup.select("table.wikitable"):
                    ths = [th.get_text(strip=True).lower() for th in tbl.select("th")]
                    joined = " ".join(ths).lower()
                    if ("weight" in joined) and (("vs" in joined) or ("fighter" in joined) or ("bout" in joined)):
                        card_table = tbl; break
            if card_table is None:
                continue

            bouts = []
            for tr in card_table.select("tr")[1:]:
                tds = tr.find_all(["td","th"])
                if len(tds) < 2: continue
                cells = [td.get_text(" ", strip=True) for td in tds]
                weight = ""
                a = b = ""
                if len(cells) >= 3:
                    if any(k in cells[0].lower() for k in ["weight","feather","bantam","fly","heavy","middle","light","welter","catch"]):
                        weight = cells[0]
                        a, b = cells[1], cells[2] if len(cells) >= 3 else ("","")
                    else:
                        if " vs " in cells[0]:
                            a, b = cells[0].split(" vs ", 1)
                        else:
                            a, b = cells[0], cells[1]
                if a and b:
                    bouts.append((a, b, weight))

            for a, b, weight in bouts:
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
