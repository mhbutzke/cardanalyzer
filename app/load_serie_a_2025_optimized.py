#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script otimizado para carregar Série A Brasil 2025
- Validação de IDs antes do processamento
- Rate limiting inteligente com backoff
- Processamento em lotes menores
- Retry seletivo para fixtures válidos
"""

import os
import json
import time
import random
from datetime import datetime
import psycopg2
from psycopg2.extras import execute_values
import httpx
from dotenv import load_dotenv

load_dotenv()

# Configurações
API_BASE = os.getenv("API_BASE_URL", "https://api.sportmonks.com/v3/football")
API_TOKEN = os.getenv("SPORTMONKS_API_KEY")
DB_DSN = os.getenv("DB_DSN")

# IDs específicos para Série A 2025
SERIE_A_LEAGUE_ID = 648  # Série A Brasil
SERIE_A_2025_SEASON_ID = 25184  # Temporada 2025

# Configurações de processamento
MAX_WORKERS = 3  # Reduzido para evitar rate limit
BATCH_SIZE = 10  # Processar em lotes menores
MAX_RETRIES = 3
BASE_DELAY = 1.0

def get_with_backoff(client, url, params=None, max_retries=MAX_RETRIES):
    """Requisição com backoff exponencial inteligente"""
    delay = BASE_DELAY
    
    for attempt in range(max_retries):
        try:
            response = client.get(url, params=params, timeout=30)
            
            if response.status_code == 200:
                return response.json()
            
            # Rate limit - espera mais
            if response.status_code == 429:
                wait_time = delay * (2 ** attempt) + random.uniform(0, 1)
                print(f"         ⏳ Rate limit (429) - aguardando {wait_time:.1f}s")
                time.sleep(wait_time)
                delay = min(delay * 2, 30)
                continue
            
            # Erro 5xx - espera e tenta novamente
            if response.status_code >= 500:
                wait_time = delay * (1.5 ** attempt)
                print(f"         ⚠️  Erro {response.status_code} - aguardando {wait_time:.1f}s")
                time.sleep(wait_time)
                continue
            
            # Erro 4xx - não tentar novamente
            print(f"         ❌ Erro {response.status_code}: {response.text[:100]}")
            return None
            
        except Exception as e:
            if attempt == max_retries - 1:
                print(f"         ❌ Falha após {max_retries} tentativas: {e}")
                return None
            
            wait_time = delay * (1.5 ** attempt)
            print(f"         ⚠️  Erro de conexão - aguardando {wait_time:.1f}s")
            time.sleep(wait_time)
    
    return None

def validate_fixture_ids(fixture_ids):
    """Validar IDs de fixtures antes do processamento"""
    print(f"🔍 Validando {len(fixture_ids)} fixture IDs...")
    
    valid_ids = []
    invalid_ids = []
    
    with httpx.Client() as client:
        for i, fixture_id in enumerate(fixture_ids):
            if i % 20 == 0:
                print(f"         📊 Validando... {i}/{len(fixture_ids)}")
            
            url = f"{API_BASE}/fixtures/{fixture_id}"
            params = {"api_token": API_TOKEN}
            
            response = get_with_backoff(client, url, params)
            
            if response and response.get("data"):
                fixture_data = response["data"]
                # Verificar se é realmente da Série A 2025
                if (fixture_data.get("league_id") == SERIE_A_LEAGUE_ID and 
                    fixture_data.get("season_id") == SERIE_A_2025_SEASON_ID):
                    valid_ids.append(fixture_id)
                else:
                    invalid_ids.append(fixture_id)
                    print(f"         ⚠️  Fixture {fixture_id} não é da Série A 2025")
            else:
                invalid_ids.append(fixture_id)
                print(f"         ❌ Fixture {fixture_id} inválido ou não encontrado")
            
            # Delay entre validações
            time.sleep(0.5)
    
    print(f"✅ Validação concluída:")
    print(f"   • IDs válidos: {len(valid_ids)}")
    print(f"   • IDs inválidos: {len(invalid_ids)}")
    
    return valid_ids, invalid_ids

def fetch_schedule_fixtures(season_id):
    """Buscar todos os fixture IDs do schedule"""
    print(f"📅 Buscando schedule da temporada {season_id}...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/schedules/seasons/{season_id}"
        params = {"api_token": API_TOKEN}
        
        data = get_with_backoff(client, url, params)
        if not data:
            return []
        
        fixture_ids = set()
        
        # Processar rounds (liga pontos corridos)
        schedule_data = data.get("data", [])
        if isinstance(schedule_data, list):
            for schedule_item in schedule_data:
                rounds = schedule_item.get("rounds", [])
                for rnd in rounds:
                    for fixture in rnd.get("fixtures", []):
                        if "id" in fixture:
                            fixture_ids.add(int(fixture["id"]))
        else:
            # Fallback para estrutura diferente
            rounds = schedule_data.get("rounds", [])
            for rnd in rounds:
                for fixture in rnd.get("fixtures", []):
                    if "id" in fixture:
                        fixture_ids.add(int(fixture["id"]))
        
        return sorted(list(fixture_ids))

def process_fixture_data(fixture_data):
    """Processar dados de um fixture"""
    try:
        fixture = fixture_data.get("data", {})
        
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
        print(f"         ❌ Erro ao processar fixture {fixture.get('id', 'N/A')}: {e}")
        return None

def save_fixture_batch(conn, batch_data):
    """Salvar lote de dados no banco"""
    try:
        with conn.cursor() as cur:
            # Fixtures
            if batch_data["fixtures"]:
                for fixture in batch_data["fixtures"]:
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
                    """, fixture)
            
            # Participantes
            if batch_data["participants"]:
                for participant in batch_data["participants"]:
                    cur.execute("""
                        INSERT INTO fixture_participants (fixture_id, team_id, location, name)
                        VALUES (%s, %s, %s, %s)
                    """, participant)
            
            # Eventos
            if batch_data["events"]:
                for event in batch_data["events"]:
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
            if batch_data["statistics"]:
                for stat in batch_data["statistics"]:
                    cur.execute("""
                        INSERT INTO fixture_statistics (fixture_id, type_id, participant_id, value)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (fixture_id, type_id, participant_id) DO UPDATE SET 
                            value = EXCLUDED.value
                    """, stat)
            
            # Árbitros
            if batch_data["referees"]:
                for referee in batch_data["referees"]:
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
        print(f"         ❌ Erro ao salvar lote: {e}")
        conn.rollback()
        return False

def main():
    """Função principal"""
    print("🚀 CARREGAMENTO OTIMIZADO - SÉRIE A BRASIL 2025")
    print("=" * 60)
    
    if not API_TOKEN:
        print("❌ SPORTMONKS_API_KEY não configurada")
        return
    
    # 1. Buscar todos os fixture IDs
    fixture_ids = fetch_schedule_fixtures(SERIE_A_2025_SEASON_ID)
    if not fixture_ids:
        print("❌ Nenhum fixture encontrado")
        return
    
    print(f"📊 Total de fixtures encontrados: {len(fixture_ids)}")
    
    # 2. Validar IDs antes do processamento
    valid_ids, invalid_ids = validate_fixture_ids(fixture_ids)
    
    if not valid_ids:
        print("❌ Nenhum fixture válido encontrado")
        return
    
    # 3. Processar em lotes menores
    print(f"💧 Processando {len(valid_ids)} fixtures válidos em lotes de {BATCH_SIZE}...")
    
    conn = psycopg2.connect(DB_DSN)
    
    try:
        with httpx.Client() as client:
            for i in range(0, len(valid_ids), BATCH_SIZE):
                batch_ids = valid_ids[i:i + BATCH_SIZE]
                print(f"\n📦 Processando lote {i//BATCH_SIZE + 1}/{(len(valid_ids) + BATCH_SIZE - 1)//BATCH_SIZE}")
                print(f"   • Fixtures: {batch_ids[0]} a {batch_ids[-1]}")
                
                batch_data = {
                    "fixtures": [],
                    "participants": [],
                    "events": [],
                    "statistics": [],
                    "referees": []
                }
                
                # Processar cada fixture do lote
                for fixture_id in batch_ids:
                    url = f"{API_BASE}/fixtures/{fixture_id}"
                    params = {
                        "api_token": API_TOKEN,
                        "include": "participants;events.type;events.player;statistics;referee;scores"
                    }
                    
                    response = get_with_backoff(client, url, params)
                    if response and response.get("data"):
                        processed = process_fixture_data(response)
                        if processed:
                            batch_data["fixtures"].append(processed["fixture"])
                            batch_data["participants"].extend(processed["participants"])
                            batch_data["events"].extend(processed["events"])
                            batch_data["statistics"].extend(processed["statistics"])
                            batch_data["referees"].extend(processed["referees"])
                            print(f"         ✅ Fixture {fixture_id} processado")
                        else:
                            print(f"         ❌ Fixture {fixture_id} falhou no processamento")
                    else:
                        print(f"         ❌ Fixture {fixture_id} falhou na API")
                    
                    # Delay entre fixtures
                    time.sleep(0.3)
                
                # Salvar lote
                if any(batch_data.values()):
                    if save_fixture_batch(conn, batch_data):
                        print(f"         💾 Lote salvo com sucesso")
                    else:
                        print(f"         ❌ Falha ao salvar lote")
                
                # Delay entre lotes
                time.sleep(1.0)
    
    finally:
        conn.close()
    
    print(f"\n🎉 CARREGAMENTO CONCLUÍDO!")
    print(f"   • Fixtures válidos: {len(valid_ids)}")
    print(f"   • Fixtures inválidos: {len(invalid_ids)}")

if __name__ == "__main__":
    main()
