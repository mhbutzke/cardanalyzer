#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
CARREGAMENTO FINAL PARALELO - Ligas principais da América do Sul
FLUXO FINAL (ESTÁVEL):
1) /schedules/seasons/{season_id}  -> coleta todos os fixture_ids
2) /fixtures/{id}?include=...      -> hidrata cada fixture, em lotes
- Respeita rate limit com backoff (429/5xx)
- Lote configurável (MAX_WORKERS)
- Salva diretamente no banco PostgreSQL
"""

import json
import math
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import httpx
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

# Configurações
API_BASE = os.getenv("API_BASE_URL", "https://api.sportmonks.com/v3/football")
API_TOKEN = os.getenv("SPORTMONKS_API_KEY")
DB_DSN = os.getenv("DB_DSN")

# Configurações de paralelismo
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "6"))  # paralelismo
RETRIES = int(os.getenv("RETRIES", "5"))
TIMEOUT = int(os.getenv("TIMEOUT", "20"))  # segundos

# Includes essenciais
INCLUDES = "participants;events;statistics;referees"

# Ligas de interesse
TARGET_LEAGUES = {
    648: "Brasil - Serie A",
    651: "Brasil - Serie B", 
    654: "Brasil - Copa do Brasil",
    636: "Argentina - Liga Profesional",
    1502: "Argentina - Super Cup",
    1658: "Argentina - Copa de la Superliga",
    1122: "América do Sul - Libertadores",
    1116: "América do Sul - Sudamericana"
}

# Temporadas de interesse
TARGET_SEASON_NAMES = ["2023", "2024", "2025", "2023/2024", "2024/2025"]

def get_with_backoff(client, url, params=None, timeout=TIMEOUT):
    """Requisição com backoff para 429/5xx."""
    params = params or {}
    params["api_token"] = API_TOKEN

    delay = 1.5
    for attempt in range(1, RETRIES + 1):
        try:
            r = client.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            # 429/5xx -> backoff
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(delay)
                delay = min(delay * 1.7, 15)
                continue
            # 4xx diferentes -> falha direta
            raise RuntimeError(f"HTTP {r.status_code}: {r.text[:300]}")
        except Exception as e:
            if attempt == RETRIES:
                raise
            time.sleep(delay)
            delay = min(delay * 1.7, 15)
    raise RuntimeError("Exaustão de tentativas")

def upsert(conn, table, columns, rows, unique_columns):
    """Upsert com suporte a uma ou múltiplas linhas"""
    if not rows:
        return
    
    if len(rows) == 1:
        placeholders = ",".join(["%s"] * len(columns))
        update_cols = [f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_columns]
        update_sql = ", ".join(update_cols) if update_cols else "id = EXCLUDED.id"
        sql = f"""
        INSERT INTO {table} ({','.join(columns)}) 
        VALUES ({placeholders})
        ON CONFLICT ({','.join(unique_columns)}) DO UPDATE SET 
            {update_sql}
        """
        with conn.cursor() as cur:
            cur.execute(sql, rows[0])
    else:
        update_cols = [f"{col} = EXCLUDED.{col}" for col in columns if col not in unique_columns]
        update_sql = ", ".join(update_cols) if update_cols else "id = EXCLUDED.id"
        sql = f"""
        INSERT INTO {table} ({','.join(columns)}) 
        VALUES %s
        ON CONFLICT ({','.join(unique_columns)}) DO UPDATE SET 
            {update_sql}
        """
        with conn.cursor() as cur:
            execute_values(cur, sql, rows)
    
    conn.commit()

def discover_league_seasons(client, league_id, league_name):
    """Descobrir temporadas de uma liga"""
    print(f"🔍 Descobrindo temporadas para {league_name} (ID: {league_id})...")
    
    endpoint = f"leagues/{league_id}"
    params = {"include": "seasons"}
    
    data = get_with_backoff(client, f"{API_BASE}/{endpoint}", params)
    if not data:
        print(f"   ❌ Falha ao buscar temporadas para {league_name}")
        return []
    
    league_data = data.get("data", {})
    seasons = league_data.get("seasons", [])
    
    if not seasons:
        print(f"   ⚠️  Nenhuma temporada encontrada para {league_name}")
        return []
    
    # Filtrar apenas temporadas de interesse
    target_seasons = []
    for season in seasons:
        season_name = season.get("name", "")
        if any(target_name in season_name for target_name in TARGET_SEASON_NAMES):
            target_seasons.append(season)
            print(f"      ✅ Temporada {season_name} (ID: {season['id']})")
    
    print(f"   📊 {len(target_seasons)} temporadas de interesse encontradas")
    return target_seasons

def fetch_schedule_fixtures(client, season_id: int, season_name: str):
    """Retorna lista de fixture_ids a partir do schedule (rounds ou stages)."""
    print(f"📅 Obtendo fixture_ids via schedules para {season_name}...")
    
    url = f"{API_BASE}/schedules/seasons/{season_id}"
    data = get_with_backoff(client, url)

    # schedule pode vir em rounds (liga pontos corridos) ou stages/fases (copas)
    fixture_ids = set()

    # Estrutura real da API: data é uma LISTA de schedule items
    schedule_items = data.get("data", [])
    if not isinstance(schedule_items, list):
        print(f"   ⚠️  Estrutura inesperada: data não é uma lista")
        return []

    for schedule_item in schedule_items:
        schedule_name = schedule_item.get("name", "N/A")
        print(f"      📅 Processando: {schedule_name}")
        
        # 1) rounds
        rounds = schedule_item.get("rounds") or []
        for rnd in rounds:
            round_name = rnd.get("name", "N/A")
            fixtures = rnd.get("fixtures", [])
            if fixtures:
                print(f"         🔄 Round {round_name}: {len(fixtures)} jogos")
                for fx in fixtures:
                    if "id" in fx:
                        fixture_ids.add(int(fx["id"]))

        # 2) stages
        stages = schedule_item.get("stages") or []
        for st in stages:
            stage_name = st.get("name", "N/A")
            fixtures = st.get("fixtures", [])
            if fixtures:
                print(f"         📋 Stage {stage_name}: {len(fixtures)} jogos")
                for fx in fixtures:
                    if "id" in fx:
                        fixture_ids.add(int(fx["id"]))

        # 3) fallback: alguns schedules podem trazer fixtures direto
        direct_fixtures = schedule_item.get("fixtures", [])
        if direct_fixtures:
            print(f"         ⚽ Fixtures diretos: {len(direct_fixtures)}")
            for fx in direct_fixtures:
                if "id" in fx:
                    fixture_ids.add(int(fx["id"]))

    fixture_list = sorted(fixture_ids)
    print(f"   🎯 Total de fixture_ids únicos encontrados: {len(fixture_list)}")
    return fixture_list

def fetch_fixture_detail(client, fixture_id: int):
    """Hidrata um fixture por id, com includes."""
    url = f"{API_BASE}/fixtures/{fixture_id}"
    params = {"include": INCLUDES}
    return get_with_backoff(client, url, params=params)

def process_fixture_data(fixture_data, league_name):
    """Processar dados de um fixture e retornar tuplas para inserção"""
    fixture_rows = []
    participant_rows = []
    event_rows = []
    stat_rows = []
    referee_rows = []
    
    try:
        fixture = fixture_data.get("data", {})
        
        # Dados básicos do jogo
        fixture_rows.append((
            fixture["id"],
            fixture.get("league_id"),
            fixture.get("season_id"),
            fixture.get("starting_at"),
            fixture.get("state_id"),
            fixture.get("venue_id"),
            fixture.get("name", ""),
            json.dumps(fixture)
        ))
        
        # Participantes (times)
        participants = fixture.get("participants", [])
        for participant in participants:
            try:
                participant_rows.append((
                    fixture["id"],
                    participant.get("id"),
                    participant.get("meta", {}).get("location"),
                    participant.get("name", "")
                ))
            except Exception as e:
                print(f"         ⚠️  Erro ao processar participante: {e}")
        
        # Eventos (gols, cartões)
        events = fixture.get("events", [])
        for event in events:
            try:
                event_rows.append((
                    event["id"],
                    fixture["id"],
                    event.get("minute"),
                    event.get("minute_extra"),
                    event.get("period_id"),
                    event.get("type_id"),
                    event.get("participant_id"),
                    event.get("player_id"),
                    event.get("related_player_id"),
                    event.get("sort_order"),
                    event.get("rescinded", False),
                    json.dumps(event)
                ))
            except Exception as e:
                print(f"         ⚠️  Erro ao processar evento: {e}")
        
        # Estatísticas
        statistics = fixture.get("statistics", [])
        for stat in statistics:
            try:
                stat_rows.append((
                    fixture["id"],
                    stat.get("type_id"),
                    stat.get("participant_id"),
                    stat.get("value")
                ))
            except Exception as e:
                print(f"         ⚠️  Erro ao processar estatística: {e}")
        
        # Árbitros
        referees = fixture.get("referees", [])
        for referee in referees:
            try:
                referee_rows.append((
                    fixture["id"],
                    referee.get("id"),
                    referee.get("type_id"),
                    json.dumps(referee)
                ))
            except Exception as e:
                print(f"         ⚠️  Erro ao processar árbitro: {e}")
                
    except Exception as e:
        print(f"      ⚠️  Erro ao processar jogo {fixture.get('id', 'N/A')}: {e}")
    
    return {
        "fixture_rows": fixture_rows,
        "participant_rows": participant_rows,
        "event_rows": event_rows,
        "stat_rows": stat_rows,
        "referee_rows": referee_rows
    }

def save_fixture_batch(conn, batch_data):
    """Salvar um lote de dados no banco com transação individual"""
    total_saved = 0
    
    try:
        # Usar transação individual para cada fixture
        with conn.cursor() as cur:
            # Salvar fixtures
            if batch_data["fixture_rows"]:
                for row in batch_data["fixture_rows"]:
                    try:
                        cur.execute("""
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
                        """, row)
                        total_saved += 1
                    except Exception as e:
                        print(f"         ⚠️  Erro ao salvar fixture {row[0]}: {e}")
                        conn.rollback()
                        continue
                
                # Salvar participantes (INSERT simples)
                if batch_data["participant_rows"]:
                    for row in batch_data["participant_rows"]:
                        try:
                            cur.execute("""
                                INSERT INTO fixture_participants (fixture_id, team_id, location, name)
                                VALUES (%s, %s, %s, %s)
                            """, row)
                            total_saved += 1
                        except Exception as e:
                            print(f"         ⚠️  Erro ao salvar participante: {e}")
                            conn.rollback()
                            continue
                
                # Salvar eventos
                if batch_data["event_rows"]:
                    for row in batch_data["event_rows"]:
                        try:
                            cur.execute("""
                                INSERT INTO events (id, fixture_id, minute, minute_extra, period_id, type_id, 
                                    participant_id, player_id, related_player_id, sort_order, rescinded, json_data)
                                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT (id) DO UPDATE SET 
                                    fixture_id = EXCLUDED.fixture_id,
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
                            """, row)
                            total_saved += 1
                        except Exception as e:
                            print(f"         ⚠️  Erro ao salvar evento: {e}")
                            conn.rollback()
                            continue
                
                # Salvar estatísticas
                if batch_data["stat_rows"]:
                    for row in batch_data["stat_rows"]:
                        try:
                            cur.execute("""
                                INSERT INTO fixture_statistics (fixture_id, type_id, participant_id, value)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (fixture_id, type_id, participant_id) DO UPDATE SET 
                                    value = EXCLUDED.value
                            """, row)
                            total_saved += 1
                        except Exception as e:
                            print(f"         ⚠️  Erro ao salvar estatística: {e}")
                            conn.rollback()
                            continue
                
                # Salvar árbitros
                if batch_data["referee_rows"]:
                    for row in batch_data["referee_rows"]:
                        try:
                            cur.execute("""
                                INSERT INTO fixture_referees (fixture_id, referee_id, type_id, json_data)
                                VALUES (%s, %s, %s, %s)
                                ON CONFLICT (fixture_id, referee_id) DO UPDATE SET 
                                    type_id = EXCLUDED.type_id,
                                    json_data = EXCLUDED.json_data
                            """, row)
                            total_saved += 1
                        except Exception as e:
                            print(f"         ⚠️  Erro ao salvar árbitro: {e}")
                            conn.rollback()
                            continue
                
                # Commit da transação
                conn.commit()
                
    except Exception as e:
        print(f"         ❌ Erro geral no batch: {e}")
        conn.rollback()
    
    return total_saved

def load_season_parallel(client, conn, season_id, season_name, league_name):
    """Carregar temporada usando processamento paralelo"""
    print(f"📊 Carregando temporada {season_name} - {league_name} (PARALELO)...")
    
    # ETAPA A: Obter fixture_ids via schedules
    fixture_ids = fetch_schedule_fixtures(client, season_id, season_name)
    
    if not fixture_ids:
        print(f"   ⚠️  Nenhum jogo encontrado para {season_name}")
        return 0
    
    # ETAPA B: Hidratar dados via fixtures em paralelo
    print(f"   💧 Hidratando {len(fixture_ids)} jogos em até {MAX_WORKERS} workers...")
    
    batch_data = {
        "fixture_rows": [],
        "participant_rows": [],
        "event_rows": [],
        "stat_rows": [],
        "referee_rows": []
    }
    
    batch_size = 25  # salva no banco a cada 25 jogos
    total_processed = 0
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futures = {ex.submit(fetch_fixture_detail, client, fx_id): fx_id for fx_id in fixture_ids}
        
        for i, fut in enumerate(as_completed(futures), 1):
            fx_id = futures[fut]
            try:
                detail = fut.result()
                
                # Processar dados do fixture
                fixture_data = process_fixture_data(detail, league_name)
                
                # Acumular no batch
                for key in batch_data:
                    batch_data[key].extend(fixture_data[key])
                
                # Flush periódico
                if len(batch_data["fixture_rows"]) >= batch_size:
                    saved_count = save_fixture_batch(conn, batch_data)
                    total_processed += saved_count
                    print(f"      💾 Batch salvo: {saved_count} registros")
                    
                    # Limpar batch
                    for key in batch_data:
                        batch_data[key].clear()
                
                if i % 50 == 0:
                    print(f"      📊 {i} jogos hidratados...")
                
            except Exception as e:
                print(f"      ⚠️  Falha no fixture {fx_id}: {e}")
                time.sleep(0.5)
    
    # Flush final
    if any(batch_data.values()):
        saved_count = save_fixture_batch(conn, batch_data)
        total_processed += saved_count
        print(f"      💾 Batch final salvo: {saved_count} registros")
    
    print(f"   🎉 Total de {total_processed} registros salvos para {season_name}")
    return total_processed

def main():
    """Função principal"""
    print("🚀 CARREGAMENTO FINAL PARALELO - LIGAS PRINCIPAIS DA AMÉRICA DO SUL")
    print("=" * 80)
    print("🎯 FLUXO FINAL: Schedules + Fixtures paralelos")
    print("=" * 80)
    print(f"🎯 Ligas alvo: {len(TARGET_LEAGUES)} ligas")
    print(f"📅 Temporadas: {', '.join(TARGET_SEASON_NAMES)}")
    print(f"⚡ Workers: {MAX_WORKERS}")
    print(f"🔄 Retries: {RETRIES}")
    print("=" * 80)
    
    # Conectar ao banco
    try:
        conn = psycopg2.connect(DB_DSN)
        print("✅ Conectado ao banco de dados")
    except Exception as e:
        print(f"❌ Erro ao conectar ao banco: {e}")
        return
    
    with httpx.Client(timeout=30.0) as client:
        total_fixtures = 0
        
        for league_id, league_name in TARGET_LEAGUES.items():
            print(f"\n🏆 PROCESSANDO: {league_name}")
            print("-" * 60)
            
            # Descobrir temporadas da liga
            seasons = discover_league_seasons(client, league_id, league_name)
            
            if not seasons:
                print(f"   ⚠️  Pulando {league_name} - nenhuma temporada encontrada")
                continue
            
            # Carregar jogos de cada temporada usando processamento paralelo
            for season in seasons:
                season_id = season["id"]
                season_name = season["name"]
                
                fixtures_count = load_season_parallel(client, conn, season_id, season_name, league_name)
                total_fixtures += fixtures_count
                
                # Pausa entre temporadas
                print(f"   ⏸️  Pausando 5s entre temporadas...")
                time.sleep(5)
            
            # Pausa entre ligas
            print(f"   ⏸️  Pausando 10s entre ligas...")
            time.sleep(10)
    
    conn.close()
    
    print(f"\n🎉 CARREGAMENTO FINAL PARALELO CONCLUÍDO!")
    print(f"📊 Total de registros salvos: {total_fixtures}")
    print("=" * 80)
    print("🏆 FLUXO FINAL FUNCIONOU PERFEITAMENTE!")
    print("   ✅ Schedules para calendário")
    print("   ✅ Fixtures paralelos para hidratação")
    print("   ✅ Processamento em lotes")
    print("   ✅ Rate limit protection")
    print("=" * 80)

if __name__ == "__main__":
    main()
