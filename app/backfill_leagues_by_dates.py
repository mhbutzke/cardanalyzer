#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import time
import logging
from datetime import datetime, timedelta, date

import httpx
import psycopg2
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('backfill_leagues_by_dates.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

LEAGUES = {
    648: "Série A Brasil",
    651: "Série B Brasil",
    654: "Copa do Brasil",
    636: "Primeira Divisão Argentina",
    1122: "Libertadores",
    1116: "Sudamericana",
}
YEARS = [2024, 2025]

IMPORTANT_EVENT_TYPES = [14, 15, 16, 17, 19, 20, 21]
IMPORTANT_STAT_TYPES = [34, 52, 56]

API_BASE = os.getenv("API_BASE_URL", "https://api.sportmonks.com/v3/football")
API_TOKEN = os.getenv("SPORTMONKS_API_KEY")
DB_DSN = os.getenv("DB_DSN")

KNOWN_SEASONS = {
    648: {2024: 23265, 2025: 25037},
    651: {2024: 25185},
}

# HTTP client global (HTTP/2, gzip)
SESSION = httpx.Client(http2=True, timeout=45, headers={
    "Accept": "application/json",
    "Accept-Encoding": "gzip",
    "User-Agent": "cardanalyzer-backfill/1.0"
})


def _get_pagination(data: dict) -> dict:
    meta = data.get("meta") or {}
    return data.get("pagination") or meta.get("pagination") or {}


def get_with_backoff(url: str, params: dict | None = None, max_retries: int = 4):
    params = params or {}
    params["api_token"] = API_TOKEN
    delay = 1.5
    for attempt in range(max_retries):
        try:
            resp = SESSION.get(url, params=params)
            # Respostas OK
            if resp.status_code == 200:
                return resp.json()
            # Rate limit / falhas temporárias
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_after = resp.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else delay
                logger.warning(f"{resp.status_code} - aguardando {wait:.1f}s")
                time.sleep(wait)
                delay = min(delay * 1.7, 15)
                continue
            # Outros erros
            logger.error(f"HTTP {resp.status_code}: {resp.text[:300]}")
            return None
        except Exception as e:
            logger.warning(f"Erro rede (tent {attempt + 1}): {e}")
            time.sleep(delay)
            delay = min(delay * 1.7, 15)
    return None


def generate_windows(year: int, window_days: int = 60):
    start = date(year, 1, 1)
    end = date(year, 12, 31)
    today = date.today()
    if end > today:
        end = today
    windows = []
    current_start = start
    while current_start <= end:
        current_end = min(end, current_start + timedelta(days=window_days - 1))
        windows.append((current_start, current_end))
        current_start = current_end + timedelta(days=1)
    return windows


def upsert_fixture(cur, fixture: dict):
    cur.execute(
        """
        INSERT INTO fixtures (id, league_id, season_id, starting_at, state_id, venue_id, name, json_data)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (id) DO UPDATE SET
            league_id = EXCLUDED.league_id,
            season_id = EXCLUDED.season_id,
            starting_at = EXCLUDED.starting_at,
            state_id = EXCLUDED.state_id,
            venue_id = EXCLUDED.venue_id,
            name = EXCLUDED.name,
            json_data = EXCLUDED.json_data
        """,
        (
            fixture["id"],
            fixture.get("league_id"),
            fixture.get("season_id"),
            fixture.get("starting_at"),
            fixture.get("state_id"),
            fixture.get("venue_id"),
            fixture.get("name", ""),
            json.dumps(fixture),
        ),
    )


def upsert_participants(cur, fixture_id: int, participants: list[dict]):
    rows = []
    for p in participants or []:
        team_id = p.get("id") or p.get("team_id")
        if not team_id:
            continue
        rows.append((
            fixture_id,
            team_id,
            (p.get("meta") or {}).get("location"),
            p.get("name", "")
        ))
    if rows:
        cur.executemany(
            """
            INSERT INTO fixture_participants (fixture_id, team_id, location, name)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fixture_id, team_id) DO UPDATE SET
                location = EXCLUDED.location,
                name = EXCLUDED.name
            """,
            rows
        )


def upsert_events(cur, fixture_id: int, events: list[dict]):
    for e in events or []:
        if not e.get("id"):
            continue
        if e.get("type_id") not in IMPORTANT_EVENT_TYPES:
            continue
        cur.execute(
            """
            INSERT INTO events (id, fixture_id, minute, minute_extra, period_id, type_id,
                                participant_id, player_id, related_player_id, sort_order, rescinded, json_data)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                minute = EXCLUDED.minute,
                minute_extra = EXCLUDED.minute_extra,
                period_id = EXCLUDED.period_id,
                type_id = EXCLUDED.type_id,
                participant_id = EXCLUDED.participant_id,
                player_id = EXCLUDED.player_id,
                related_player_id = EXCLUDED.related_player_id,
                sort_order = EXCLUDED.sort_order,
                rescinded = EXCLUDED.rescinded,
                json_data = EXCLUDED.json_data
            """,
            (
                e.get("id"),
                fixture_id,
                e.get("minute"),
                e.get("minute_extra"),
                e.get("period_id"),
                e.get("type_id"),
                e.get("participant_id"),
                e.get("player_id"),
                e.get("related_player_id"),
                e.get("sort_order"),
                e.get("rescinded", False),
                json.dumps(e),
            ),
        )


def upsert_statistics(cur, fixture_id: int, statistics: list[dict]):
    for s in statistics or []:
        if s.get("type_id") not in IMPORTANT_STAT_TYPES:
            continue
        cur.execute(
            """
            INSERT INTO fixture_statistics (fixture_id, type_id, participant_id, value)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (fixture_id, type_id, participant_id) DO UPDATE SET
                value = EXCLUDED.value
            """,
            (
                fixture_id,
                s.get("type_id"),
                s.get("participant_id"),
                s.get("value"),
            ),
        )


def fetch_fixture_ids_minimal(league_id: int, year: int, start_d: date, end_d: date) -> list[dict]:
    url = f"{API_BASE}/fixtures/between/{start_d.strftime('%Y-%m-%d')}/{end_d.strftime('%Y-%m-%d')}"
    season_id = KNOWN_SEASONS.get(league_id, {}).get(year)

    params = {
        "per_page": 200,
        "page": 1,
        "fields[fixtures]": "id,league_id,season_id,starting_at,state_id,venue_id,name"
    }
    # Filtro server-side suportado: leagues e seasons
    params["leagues"] = str(league_id)
    if season_id:
        params["seasons"] = str(season_id)

    fixtures = []
    while True:
        data = get_with_backoff(url, params)
        if not data or not data.get("data"):
            break
        fixtures.extend(data["data"])
        pagination = _get_pagination(data)
        if not pagination.get("has_more"):
            break
        params["page"] = params.get("page", 1) + 1
        time.sleep(0.3)
    return fixtures


def fetch_fixtures_details_multi(ids: list[int]) -> list[dict]:
    detailed = []
    for i in range(0, len(ids), 50):
        batch = ids[i:i+50]
        url = f"{API_BASE}/fixtures/multi/{','.join(str(x) for x in batch)}"
        params = {
            "include": "participants;events.type;statistics"
        }
        data = get_with_backoff(url, params)
        if data and data.get("data"):
            detailed.extend(data["data"])
        time.sleep(0.2)
    return detailed


def backfill_league_year(league_id: int, league_name: str, year: int) -> int:
    logger.info(f"🏆 {league_name} — Ano {year}")
    total_saved = 0

    windows = generate_windows(year, window_days=60)
    logger.info(f"   🔎 {len(windows)} janelas de busca (60d)")

    for idx, (start_d, end_d) in enumerate(windows, 1):
        logger.info(f"   [{idx}/{len(windows)}] {start_d} → {end_d}")
        minimal = fetch_fixture_ids_minimal(league_id, year, start_d, end_d)
        if not minimal:
            continue
        ids = [f["id"] for f in minimal]
        details = fetch_fixtures_details_multi(ids)
        if not details:
            continue
        try:
            conn = psycopg2.connect(DB_DSN)
            cur = conn.cursor()
            for fixture in details:
                upsert_fixture(cur, fixture)
                upsert_participants(cur, fixture["id"], fixture.get("participants"))
                upsert_events(cur, fixture["id"], fixture.get("events"))
                upsert_statistics(cur, fixture["id"], fixture.get("statistics"))
                total_saved += 1
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"   ❌ Erro ao salvar fixtures: {e}")
        time.sleep(0.5)

    logger.info(f"   ✅ Salvos/atualizados: {total_saved}")
    return total_saved


def run_backfill():
    if not API_TOKEN:
        logger.error("SPORTMONKS_API_KEY não configurada")
        return
    if not DB_DSN:
        logger.error("DB_DSN não configurado")
        return

    start_ts = time.time()
    grand_total = 0
    logger.info("🚀 BACKFILL — Ligas 2024 e 2025 por janelas otimizadas")
    for league_id, league_name in LEAGUES.items():
        for year in YEARS:
            saved = backfill_league_year(league_id, league_name, year)
            grand_total += saved
            time.sleep(1)
    elapsed = time.time() - start_ts
    logger.info(f"🎉 Concluído. Fixtures processados: {grand_total} em {elapsed:.1f}s")

    try:
        from complete_analysis import CompleteAnalysis
        analyzer = CompleteAnalysis()
        analyzer.run_complete_analysis()
    except Exception as e:
        logger.error(f"❌ Erro ao executar análise completa: {e}")


if __name__ == "__main__":
    run_backfill()
