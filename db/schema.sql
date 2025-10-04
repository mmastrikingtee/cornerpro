CREATE TABLE IF NOT EXISTS fighters (
  fighter_id TEXT PRIMARY KEY,
  name TEXT, dob TEXT, stance TEXT, height_cm REAL, reach_cm REAL
);
CREATE TABLE IF NOT EXISTS events (
  event_id TEXT PRIMARY KEY,
  org TEXT, event_date TEXT, name TEXT, location TEXT
);
CREATE TABLE IF NOT EXISTS bouts (
  bout_id TEXT PRIMARY KEY,
  event_id TEXT, fighter_a_id TEXT, fighter_b_id TEXT,
  weight_class TEXT, scheduled_rounds INTEGER,
  result TEXT, winner_id TEXT
);
CREATE TABLE IF NOT EXISTS elo_ratings (
  fighter_id TEXT PRIMARY KEY, elo REAL, last_fight TEXT
);
