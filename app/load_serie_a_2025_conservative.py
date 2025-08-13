#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script conservador para carregar Série A Brasil 2025
- Rate limiting muito baixo (1 requisição a cada 2 segundos)
- Sem paralelismo para evitar travamentos
- Validação prévia de IDs
- Processamento sequencial
"""

import os
import json
import time
import psycopg2
from dotenv import load_dotenv
import httpx

load_dotenv()

# Configurações
API_BASE = os.getenv("API_BASE_URL", "https://api.sportmonks.com/v3/football")
API_TOKEN = os.getenv("SPORTMONKS_API_KEY")
DB_DSN = os.getenv("DB_DSN")

# IDs específicos
SERIE_A_LEAGUE_ID = 648
SERIE_A_2025_SEASON_ID = 25184

# Rate limiting conservador
REQUEST_DELAY = 2.0  # 2 segundos entre requisições
BATCH_SIZE = 5       # Processar apenas 5 por vez

def safe_api_request(client, url, params=None):
    """Requisição segura com delay obrigatório"""
    try:
        response = client.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print(f"         ⏳ Rate limit atingido - aguardando 10s...")
            time.sleep(10)
            return None
        else:
            print(f"         ❌ Erro {response.status_code}: {response.text[:100]}")
            return None
            
    except Exception as e:
        print(f"         ❌ Erro de conexão: {e}")
        return None

def fetch_schedule_fixtures():
    """Buscar fixture IDs do schedule"""
    print("📅 Buscando schedule da temporada...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/schedules/seasons/{SERIE_A_2025_SEASON_ID}"
        params = {"api_token": API_TOKEN}
        
        data = safe_api_request(client, url, params)
        if not data:
            return []
        
        fixture_ids = set()
        schedule_data = data.get("data", [])
        
        if isinstance(schedule_data, list):
            for schedule_item in schedule_data:
                rounds = schedule_item.get("rounds", [])
                for rnd in rounds:
                    for fixture in rnd.get("fixtures", []):
                        if "id" in fixture:
                            fixture_ids.add(int(fixture["id"]))
        
        return sorted(list(fixture_ids))

def validate_fixture_id(fixture_id):
    """Validar um fixture ID individual"""
    with httpx.Client() as client:
        url = f"{API_BASE}/fixtures/{fixture_id}"
        params = {"api_token": API_TOKEN}
        
        data = safe_api_request(client, url, params)
        if not data or not data.get("data"):
            return False
        
        fixture = data["data"]
        return (fixture.get("league_id") == SERIE_A_LEAGUE_ID and 
                fixture.get("season_id") == SERIE_A_2025_SEASON_ID)

def process_fixture(fixture_id):
    """Processar um fixture individual"""
    print(f"         ⚽ Processando fixture {fixture_id}...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/fixtures/{fixture_id}"
        params = {
            "api_token": API_TOKEN,
            "include": "participants;events.type;statistics;referee"
        }
        
        data = safe_api_request(client, url, params)
        if not data or not data.get("data"):
            print(f"         ❌ Falha ao obter dados do fixture {fixture_id}")
            return None
        
        fixture = data["data"]
        
        try:
            # Dados básicos
            fixture_row = (
                fixture["id"],
                fixture.get("league_id"),
                fixture.get("season_id"),
                fixture.get("starting_at"),
                fixture.get("state_id"),
                fixture.get("venue_id"),
                fixture.get("name", ""),
                json.dumps(fixture)
            )
            
            # Participantes
            participant_rows = []
            for participant in fixture.get("participants", []):
                participant_rows.append((
                    fixture["id"],
                    participant.get("id"),
                    participant.get("meta", {}).get("location"),
                    participant.get("name", "")
                ))
            
            # Eventos
            event_rows = []
            for event in fixture.get("events", []):
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
            
            # Estatísticas
            stat_rows = []
            for stat in fixture.get("statistics", []):
                stat_rows.append((
                    fixture["id"],
                    stat.get("type_id"),
                    stat.get("participant_id"),
                    stat.get("value")
                ))
            
            # Árbitros
            referee_rows = []
            for referee in fixture.get("referees", []):
                referee_rows.append((
                    fixture["id"],
                    referee.get("id"),
                    referee.get("type_id"),
                    json.dumps(referee)
                ))
            
            return {
                "fixture": fixture_row,
                "participants": participant_rows,
                "events": event_rows,
                "statistics": stat_rows,
                "referees": referee_rows
            }
            
        except Exception as e:
            print(f"         ❌ Erro ao processar fixture {fixture_id}: {e}")
            return None

def save_fixture_data(conn, fixture_data):
    """Salvar dados de um fixture no banco"""
    try:
        with conn.cursor() as cur:
            # Fixture
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
            """, fixture_data["fixture"])
            
            # Participantes
            for participant in fixture_data["participants"]:
                cur.execute("""
                    INSERT INTO fixture_participants (fixture_id, team_id, location, name)
                    VALUES (%s, %s, %s, %s)
                """, participant)
            
            # Eventos
            for event in fixture_data["events"]:
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
                """, event)
            
            # Estatísticas
            for stat in fixture_data["statistics"]:
                cur.execute("""
                    INSERT INTO fixture_statistics (fixture_id, type_id, participant_id, value)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (fixture_id, type_id, participant_id) DO UPDATE SET 
                        value = EXCLUDED.value
                """, stat)
            
            # Árbitros
            for referee in fixture_data["referees"]:
                cur.execute("""
                    INSERT INTO fixture_referees (fixture_id, referee_id, type_id, json_data)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (fixture_id, referee_id) DO UPDATE SET 
                        type_id = EXCLUDED.type_id,
                        json_data = EXCLUDED.json_data
                """, referee)
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"         ❌ Erro ao salvar fixture: {e}")
        conn.rollback()
        return False

def main():
    """Função principal"""
    print("🚀 CARREGAMENTO CONSERVADOR - SÉRIE A BRASIL 2025")
    print("=" * 60)
    print(f"⏱️  Delay entre requisições: {REQUEST_DELAY}s")
    print(f"📦 Tamanho do lote: {BATCH_SIZE}")
    print("=" * 60)
    
    if not API_TOKEN:
        print("❌ SPORTMONKS_API_KEY não configurada")
        return
    
    # 1. Buscar fixture IDs
    fixture_ids = fetch_schedule_fixtures()
    if not fixture_ids:
        print("❌ Nenhum fixture encontrado")
        return
    
    print(f"📊 Total de fixtures encontrados: {len(fixture_ids)}")
    
    # 2. Validar IDs em lotes pequenos
    print(f"\n🔍 Validando IDs em lotes de {BATCH_SIZE}...")
    valid_ids = []
    
    for i in range(0, len(fixture_ids), BATCH_SIZE):
        batch = fixture_ids[i:i + BATCH_SIZE]
        print(f"\n📦 Lote {i//BATCH_SIZE + 1}/{(len(fixture_ids) + BATCH_SIZE - 1)//BATCH_SIZE}")
        
        for fixture_id in batch:
            if validate_fixture_id(fixture_id):
                valid_ids.append(fixture_id)
                print(f"         ✅ Fixture {fixture_id} válido")
            else:
                print(f"         ❌ Fixture {fixture_id} inválido")
            
            time.sleep(REQUEST_DELAY)
    
    print(f"\n✅ Validação concluída: {len(valid_ids)} fixtures válidos")
    
    if not valid_ids:
        print("❌ Nenhum fixture válido encontrado")
        return
    
    # 3. Processar fixtures válidos
    print(f"\n💧 Processando {len(valid_ids)} fixtures válidos...")
    
    conn = psycopg2.connect(DB_DSN)
    processed_count = 0
    
    try:
        for i, fixture_id in enumerate(valid_ids, 1):
            print(f"\n📊 Progresso: {i}/{len(valid_ids)} ({i/len(valid_ids)*100:.1f}%)")
            
            fixture_data = process_fixture(fixture_id)
            if fixture_data:
                if save_fixture_data(conn, fixture_data):
                    processed_count += 1
                    print(f"         💾 Fixture {fixture_id} salvo com sucesso")
                else:
                    print(f"         ❌ Falha ao salvar fixture {fixture_id}")
            else:
                print(f"         ❌ Fixture {fixture_id} falhou no processamento")
            
            # Delay obrigatório entre fixtures
            if i < len(valid_ids):
                print(f"         ⏳ Aguardando {REQUEST_DELAY}s...")
                time.sleep(REQUEST_DELAY)
    
    finally:
        conn.close()
    
    print(f"\n🎉 CARREGAMENTO CONCLUÍDO!")
    print(f"   • Fixtures válidos: {len(valid_ids)}")
    print(f"   • Fixtures processados: {processed_count}")
    print(f"   • Taxa de sucesso: {processed_count/len(valid_ids)*100:.1f}%")

if __name__ == "__main__":
    main()
