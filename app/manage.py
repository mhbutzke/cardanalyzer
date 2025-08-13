#!/usr/bin/env python3
import os, sys, time, json, argparse, datetime as dt
import httpx, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()
API = os.getenv("API_BASE_URL","https://api.sportmonks.com/v3/football")
KEY = os.getenv("SPORTMONKS_API_KEY")
DSN = os.getenv("DB_DSN","postgresql://card:card@localhost:5432/carddb")
TZ  = os.getenv("TZ","America/Sao_Paulo")

INCLUDE = ("participants:id,name,meta.location;"
           "referees;"
           "periods;"
           "events:id,minute,minute_extra,period_id,type_id,participant_id,player_id,related_player_id,sort_order,rescinded;"
           "statistics.type")
FILTERS = "eventTypes:14,15,16,17,19,20,21,fixtureStatisticTypes:34,52,56"

def http_get(client: httpx.Client, path: str, params: dict):
    assert KEY, "Defina SPORTMONKS_API_KEY no .env"
    url = API.rstrip("/") + "/" + path.lstrip("/")
    q = dict(params or {}); q.setdefault("api_token", KEY)
    for i in range(5):
        r = client.get(url, params=q)
        if r.status_code in (429,500,502,503,504):
            time.sleep((2**i)+0.1); continue
        r.raise_for_status(); return r.json()
    r.raise_for_status()

def upsert(conn, table, cols, rows, conflict_cols):
    if not rows: return
    cols_csv = ",".join(cols)
    excl = ",".join([f"{c}=EXCLUDED.{c}" for c in cols if c not in conflict_cols])
    sql = f"INSERT INTO {table} ({cols_csv}) VALUES %s ON CONFLICT ({','.join(conflict_cols)}) DO UPDATE SET {excl}"
    with conn.cursor() as cur: execute_values(cur, sql, rows)

def save_page_into_db(conn, page_data, league_id=None, season_id=None):
    fx_rows=[]; part_rows=[]; ref_rows=[]; fxr_rows=[]; ev_rows=[]; st_rows=[]
    for fx in page_data.get("data", []):
        if league_id and fx.get("league_id")!=league_id: continue
        if season_id and fx.get("season_id")!=season_id: continue
        fid = fx["id"]
        fx_rows.append((fid, fx.get("league_id"), fx.get("season_id"),
                        fx.get("starting_at"), fx.get("state_id"), fx.get("venue_id"),
                        fx.get("name"), json.dumps(fx)))
        # participants
        for p in (fx.get("participants") or {}).get("data", []):
            part_rows.append((p["id"], fid, p.get("team_id"),
                              (p.get("meta") or {}).get("location"), p.get("name")))
        # referees
        for r in (fx.get("referees") or {}).get("data", []):
            ref_rows.append((r["id"], r.get("name"), json.dumps(r)))
            fxr_rows.append((fid, r["id"]))
        # events
        for e in (fx.get("events") or {}).get("data", []):
            ev_rows.append((e["id"], fid, e.get("participant_id"), e.get("player_id"),
                            e.get("related_player_id"), e.get("type_id"), e.get("minute"),
                            e.get("minute_extra"), e.get("period_id"), e.get("sort_order"),
                            e.get("rescinded"), None, json.dumps(e)))
        # stats
        for s in (fx.get("statistics") or {}).get("data", []):
            st_rows.append((fid, s.get("participant_id"), s.get("type_id"), s.get("value")))
    upsert(conn,"fixtures",
           ["id","league_id","season_id","starting_at","state_id","venue_id","name","json_data"],
           fx_rows, ["id"])
    upsert(conn,"referees",["id","name","json_data"], ref_rows, ["id"])
    if fxr_rows:
        with conn.cursor() as cur:
            cur.execute("CREATE UNIQUE INDEX IF NOT EXISTS uq_fxr ON fixture_referees(fixture_id,referee_id);")
            execute_values(cur, "INSERT INTO fixture_referees(fixture_id,referee_id) VALUES %s ON CONFLICT DO NOTHING", fxr_rows)
    upsert(conn,"fixture_participants",["id","fixture_id","team_id","location","name"], part_rows, ["id"])
    upsert(conn,"events",
           ["id","fixture_id","participant_id","player_id","related_player_id","type_id","minute","minute_extra","period_id","sort_order","rescinded","attrs","json_data"],
           ev_rows, ["id"])
    upsert(conn,"fixture_statistics",["fixture_id","participant_id","type_id","value"], st_rows, ["fixture_id","participant_id","type_id"])

def fetch_between(league_id, start, end, season_id=None):
    with httpx.Client(timeout=30.0) as client, psycopg2.connect(DSN) as conn:
        page = 1
        while True:
            payload = {"include":INCLUDE, "filters":FILTERS, "per_page":200, "page":page}
            data = http_get(client, f"fixtures/between/{start}/{end}", payload)
            save_page_into_db(conn, data, league_id=league_id, season_id=season_id)
            conn.commit()
            if not (data.get("pagination") or {}).get("has_more"): break
            page += 1

def cmd_initdb():
    os.system(f'psql "{DSN}" -f sql/schema.sql')
    os.system(f'psql "{DSN}" -f sql/seed_known_types.sql')
    os.system(f'psql "{DSN}" -f sql/views.sql')
    print("OK initdb")

def cmd_seed():
    league_id = 648     # Série A (BR)
    season_id = 25184   # 2025
    ranges = [("2025-03-29","2025-07-06"), ("2025-07-07","2025-10-14"), ("2025-10-15","2025-12-21")]
    for s,e in ranges:
        print(f"Seed {s}..{e}")
        fetch_between(league_id, s, e, season_id)

def cmd_update_daily(days_back=3):
    today = dt.date.today()
    start = today - dt.timedelta(days=days_back)
    end   = today
    league_id = 648; season_id = 25184
    cur = start
    while cur <= end:
        s = cur.isoformat()
        print(f"Update {s}")
        fetch_between(league_id, s, s, season_id)
        cur += dt.timedelta(days=1)

def cmd_refresh_gold():
    print("Views simples não precisam refresh. (Use MVs se quiser).")

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["initdb","seed","update-daily","refresh-gold"])
    ap.add_argument("--days-back", type=int, default=3)
    args = ap.parse_args()
    if args.cmd=="initdb": cmd_initdb()
    elif args.cmd=="seed": cmd_seed()
    elif args.cmd=="update-daily": cmd_update_daily(args.days_back)
    elif args.cmd=="refresh-gold": cmd_refresh_gold()
