#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script que usa seasons com include=teams para extrair dados corretos da Série A 2025
- Endpoint mais eficiente: /seasons/{id}?include=teams
- Traz apenas times da temporada específica
- Menos rate limit, dados mais precisos
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
REQUEST_DELAY = 3.0  # 3 segundos entre requisições

def safe_api_request(client, url, params=None):
    """Requisição segura com delay"""
    try:
        response = client.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print(f"         ⏳ Rate limit - aguardando 15s...")
            time.sleep(15)
            return None
        else:
            print(f"         ❌ Erro {response.status_code}: {response.text[:100]}")
            return None
            
    except Exception as e:
        print(f"         ❌ Erro de conexão: {e}")
        return None

def extract_season_with_teams():
    """Extrair temporada com times incluídos"""
    print("📅 Extraindo temporada 2025 com times incluídos...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/seasons/{SERIE_A_2025_SEASON_ID}"
        params = {
            "api_token": API_TOKEN,
            "include": "teams"
        }
        
        data = safe_api_request(client, url, params)
        if data and data.get("data"):
            season = data["data"]
            print(f"   ✅ Temporada: {season.get('name')}")
            print(f"   🏆 Liga: {season.get('league_id')}")
            
            # Extrair times da temporada
            teams = []
            if "teams" in season:
                teams = season["teams"]
                print(f"   👥 {len(teams)} times encontrados na temporada")
                
                # Mostrar cada time
                for team in teams:
                    print(f"         🎯 {team.get('name')} - ID: {team.get('id')}")
            
            return season, teams
        else:
            print("   ❌ Falha ao obter temporada com times")
            return None, []

def extract_league_info():
    """Extrair informações da liga"""
    print("🏆 Extraindo informações da liga...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/leagues/{SERIE_A_LEAGUE_ID}"
        params = {"api_token": API_TOKEN}
        
        data = safe_api_request(client, url, params)
        if data and data.get("data"):
            league = data["data"]
            print(f"   ✅ Liga: {league.get('name')}")
            print(f"   📍 País: {league.get('country', {}).get('name')}")
            return league
        return None

def extract_schedule_structure():
    """Extrair estrutura do calendário"""
    print("📋 Extraindo estrutura do calendário...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/schedules/seasons/{SERIE_A_2025_SEASON_ID}"
        params = {"api_token": API_TOKEN}
        
        data = safe_api_request(client, url, params)
        if data and data.get("data"):
            schedule_data = data["data"]
            
            total_rounds = 0
            total_fixtures = 0
            
            if isinstance(schedule_data, list):
                for item in schedule_data:
                    rounds = item.get("rounds", [])
                    total_rounds += len(rounds)
                    for rnd in rounds:
                        fixtures = rnd.get("fixtures", [])
                        total_fixtures += len(fixtures)
            
            print(f"   ✅ {total_rounds} rodadas encontradas")
            print(f"   ⚽ {total_fixtures} jogos programados")
            
            return {
                "rounds": total_rounds,
                "fixtures": total_fixtures,
                "data": schedule_data
            }
        return None

def extract_venues_basic():
    """Extrair estádios básicos (se disponível)"""
    print("🏟️  Extraindo estádios básicos...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/venues"
        params = {
            "api_token": API_TOKEN,
            "per_page": 50  # Limitar para evitar rate limit
        }
        
        data = safe_api_request(client, url, params)
        if data and data.get("data"):
            venues = data["data"]
            print(f"   ✅ {len(venues)} estádios encontrados")
            return venues
        else:
            print("   ⚠️  Estádios não disponíveis")
            return []

def extract_referees_basic():
    """Extrair árbitros básicos (se disponível)"""
    print("👨‍⚖️  Extraindo árbitros básicos...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/referees"
        params = {
            "api_token": API_TOKEN,
            "per_page": 50  # Limitar para evitar rate limit
        }
        
        data = safe_api_request(client, url, params)
        if data and data.get("data"):
            referees = data["data"]
            print(f"   ✅ {len(referees)} árbitros encontrados")
            return referees
        else:
            print("   ⚠️  Árbitros não disponíveis")
            return []

def save_complete_data(conn, league_info, season_info, teams, schedule, venues, referees):
    """Salvar todos os dados no banco"""
    try:
        with conn.cursor() as cur:
            # Salvar liga
            if league_info:
                cur.execute("""
                    INSERT INTO leagues (id, name, country_id, json_data)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET 
                        name = EXCLUDED.name,
                        country_id = EXCLUDED.country_id,
                        json_data = EXCLUDED.json_data
                """, (
                    league_info["id"],
                    league_info.get("name"),
                    league_info.get("country", {}).get("id"),
                    json.dumps(league_info)
                ))
                print("         💾 Liga salva")
            
            # Salvar temporada
            if season_info:
                season_name = season_info.get("name", "")
                season_year = None
                if season_name and season_name.isdigit():
                    season_year = int(season_name)
                
                cur.execute("""
                    INSERT INTO seasons (id, name, league_id, year, json_data)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (id) DO UPDATE SET 
                        name = EXCLUDED.name,
                        league_id = EXCLUDED.league_id,
                        year = EXCLUDED.year,
                        json_data = EXCLUDED.json_data
                """, (
                    season_info["id"],
                    season_info.get("name"),
                    season_info.get("league_id"),
                    season_year,
                    json.dumps(season_info)
                ))
                print("         💾 Temporada salva")
            
            # Salvar times
            if teams:
                for team in teams:
                    cur.execute("""
                        INSERT INTO teams (id, name, country_id, json_data)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (id) DO UPDATE SET 
                            name = EXCLUDED.name,
                            country_id = EXCLUDED.country_id,
                            json_data = EXCLUDED.json_data
                    """, (
                        team["id"],
                        team.get("name"),
                        team.get("country_id"),
                        json.dumps(team)
                    ))
                print(f"         💾 {len(teams)} times salvos")
            
            # Salvar estádios (se existir tabela)
            if venues:
                try:
                    for venue in venues:
                        cur.execute("""
                            INSERT INTO venues (id, name, city, country_id, json_data)
                            VALUES (%s, %s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET 
                                name = EXCLUDED.name,
                                city = EXCLUDED.city,
                                country_id = EXCLUDED.country_id,
                                json_data = EXCLUDED.json_data
                        """, (
                            venue["id"],
                            venue.get("name"),
                            venue.get("city"),
                            venue.get("country_id"),
                            json.dumps(venue)
                        ))
                    print(f"         💾 {len(venues)} estádios salvos")
                except Exception as e:
                    print(f"         ⚠️  Estádios não salvos (tabela não existe): {e}")
            
            # Salvar árbitros (se existir tabela)
            if referees:
                try:
                    for referee in referees:
                        cur.execute("""
                            INSERT INTO referees (id, name, country_id, json_data)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (id) DO UPDATE SET 
                                name = EXCLUDED.name,
                                country_id = EXCLUDED.country_id,
                                json_data = EXCLUDED.json_data
                        """, (
                            referee["id"],
                            referee.get("name"),
                            referee.get("country_id"),
                            json.dumps(referee)
                        ))
                    print(f"         💾 {len(referees)} árbitros salvos")
                except Exception as e:
                    print(f"         ⚠️  Árbitros não salvos (tabela não existe): {e}")
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"         ❌ Erro ao salvar dados: {e}")
        conn.rollback()
        return False

def main():
    """Função principal"""
    print("🚀 EXTRAÇÃO COMPLETA - SÉRIE A BRASIL 2025")
    print("=" * 60)
    print("🎯 Usando seasons com include=teams")
    print("⏱️  Delay entre requisições: 3s")
    print("=" * 60)
    
    if not API_TOKEN:
        print("❌ SPORTMONKS_API_KEY não configurada")
        return
    
    # 1. Extrair temporada com times (endpoint principal)
    season_info, teams = extract_season_with_teams()
    time.sleep(REQUEST_DELAY)
    
    if not season_info or not teams:
        print("❌ Falha ao obter temporada com times")
        return
    
    # 2. Extrair dados complementares
    league_info = extract_league_info()
    time.sleep(REQUEST_DELAY)
    
    schedule = extract_schedule_structure()
    time.sleep(REQUEST_DELAY)
    
    venues = extract_venues_basic()
    time.sleep(REQUEST_DELAY)
    
    referees = extract_referees_basic()
    
    # 3. Salvar no banco
    print(f"\n💾 Salvando dados completos no banco...")
    
    conn = psycopg2.connect(DB_DSN)
    try:
        if save_complete_data(conn, league_info, season_info, teams, schedule, venues, referees):
            print("         ✅ Dados salvos com sucesso")
        else:
            print("         ❌ Falha ao salvar dados")
    finally:
        conn.close()
    
    # 4. Resumo
    print(f"\n📊 RESUMO DA EXTRAÇÃO:")
    print(f"   • Liga: {'✅' if league_info else '❌'}")
    print(f"   • Temporada: {'✅' if season_info else '❌'}")
    print(f"   • Times: {len(teams)}")
    print(f"   • Calendário: {'✅' if schedule else '❌'}")
    print(f"   • Estádios: {len(venues) if venues else 0}")
    print(f"   • Árbitros: {len(referees) if referees else 0}")
    
    print(f"\n🎯 PRÓXIMOS PASSOS:")
    print(f"   • Banco com estrutura completa")
    print(f"   • Aguardar rate limit resetar")
    print(f"   • Executar carregamento de fixtures")

if __name__ == "__main__":
    main()
