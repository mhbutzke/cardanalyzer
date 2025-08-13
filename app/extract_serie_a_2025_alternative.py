#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script alternativo para extrair dados da Série A 2025
- Usa endpoints menos intensivos
- Foca em dados básicos e agregados
- Evita rate limit dos fixtures individuais
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

# Rate limiting muito conservador
REQUEST_DELAY = 5.0  # 5 segundos entre requisições

def safe_api_request(client, url, params=None):
    """Requisição segura com delay longo"""
    try:
        response = client.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 429:
            print(f"         ⏳ Rate limit - aguardando 30s...")
            time.sleep(30)
            return None
        else:
            print(f"         ❌ Erro {response.status_code}: {response.text[:100]}")
            return None
            
    except Exception as e:
        print(f"         ❌ Erro de conexão: {e}")
        return None

def extract_league_info():
    """Extrair informações básicas da liga"""
    print("🏆 Extraindo informações da liga...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/leagues/{SERIE_A_LEAGUE_ID}"
        params = {"api_token": API_TOKEN}
        
        data = safe_api_request(client, url, params)
        if data and data.get("data"):
            league = data["data"]
            print(f"   ✅ Liga: {league.get('name')}")
            print(f"   📍 País: {league.get('country', {}).get('name')}")
            print(f"   🏟️  Tipo: {league.get('type')}")
            return league
        return None

def extract_season_info():
    """Extrair informações da temporada"""
    print("📅 Extraindo informações da temporada...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/seasons/{SERIE_A_2025_SEASON_ID}"
        params = {"api_token": API_TOKEN}
        
        data = safe_api_request(client, url, params)
        if data and data.get("data"):
            season = data["data"]
            print(f"   ✅ Temporada: {season.get('name')}")
            print(f"   📅 Início: {season.get('starting_at')}")
            print(f"   📅 Fim: {season.get('ending_at')}")
            return season
        return None

def extract_teams_basic():
    """Extrair times da liga (endpoint básico)"""
    print("👥 Extraindo times da liga...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/teams/countries/5"  # Brasil
        params = {"api_token": API_TOKEN}
        
        data = safe_api_request(client, url, params)
        if data and data.get("data"):
            teams = data["data"]
            print(f"   ✅ {len(teams)} times encontrados")
            
            # Filtrar apenas times da Série A (por nome)
            serie_a_teams = []
            serie_a_keywords = [
                "palmeiras", "flamengo", "são paulo", "santos", "corinthians", 
                "vasco", "fluminense", "botafogo", "grêmio", "internacional", 
                "atlético", "cruzeiro", "bragantino", "fortaleza", "bahia", 
                "vitória", "juventude", "criciúma", "atlético-go", "cuiabá"
            ]
            
            for team in teams:
                team_name = team.get("name", "").lower()
                if any(keyword in team_name for keyword in serie_a_keywords):
                    serie_a_teams.append(team)
                    print(f"         🎯 {team.get('name')} - ID: {team.get('id')}")
            
            print(f"   🎯 {len(serie_a_teams)} times da Série A identificados")
            
            # Verificar se encontramos todos os 20
            if len(serie_a_teams) < 20:
                print(f"   ⚠️  Faltam {20 - len(serie_a_teams)} times!")
                print(f"   🔍 Verificando times não identificados...")
                
                # Mostrar times não identificados
                for team in teams:
                    team_name = team.get("name", "")
                    if team not in serie_a_teams:
                        print(f"         ❓ {team_name} - ID: {team.get('id')}")
            
            return serie_a_teams
        return []

def extract_schedule_structure():
    """Extrair estrutura do calendário (sem detalhes)"""
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

def extract_standings_basic():
    """Extrair tabela básica (se disponível)"""
    print("📊 Extraindo tabela de classificação...")
    
    with httpx.Client() as client:
        url = f"{API_BASE}/standings/seasons/{SERIE_A_2025_SEASON_ID}"
        params = {"api_token": API_TOKEN}
        
        data = safe_api_request(client, url, params)
        if data and data.get("data"):
            standings = data["data"]
            print(f"   ✅ Tabela encontrada")
            
            # Contar times na tabela
            team_count = 0
            for standing in standings:
                if standing.get("type") == "league":
                    team_count = len(standing.get("standings", []))
                    break
            
            print(f"   👥 {team_count} times na tabela")
            return standings
        else:
            print(f"   ⚠️  Tabela não disponível ainda")
            return None

def extract_recent_results():
    """Extrair resultados recentes (endpoint menos intensivo)"""
    print("🏁 Extraindo resultados recentes...")
    
    with httpx.Client() as client:
        # Usar endpoint de fixtures recentes sem filtros complexos
        url = f"{API_BASE}/fixtures/latest"
        params = {
            "api_token": API_TOKEN,
            "per_page": 20  # Limitar para evitar rate limit
        }
        
        data = safe_api_request(client, url, params)
        if data and data.get("data"):
            fixtures = data["data"]
            print(f"   ✅ {len(fixtures)} jogos recentes encontrados")
            
            # Filtrar apenas da Série A 2025 (em código)
            serie_a_2025 = []
            for f in fixtures:
                if (f.get("league_id") == SERIE_A_LEAGUE_ID and 
                    f.get("season_id") == SERIE_A_2025_SEASON_ID):
                    serie_a_2025.append(f)
            
            print(f"   🎯 {len(serie_a_2025)} jogos da Série A 2025")
            return serie_a_2025
        return []

def save_basic_data(conn, league_info, season_info, teams, schedule, standings, recent_results):
    """Salvar dados básicos no banco"""
    try:
        with conn.cursor() as cur:
            # Salvar informações da liga
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
            
            # Salvar informações da temporada
            if season_info:
                # Extrair ano do nome da temporada
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
            
            # Salvar fixtures básicos (se houver)
            if recent_results:
                for fixture in recent_results:
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
                    """, (
                        fixture["id"],
                        fixture.get("league_id"),
                        fixture.get("season_id"),
                        fixture.get("starting_at"),
                        fixture.get("state_id"),
                        fixture.get("venue_id"),
                        fixture.get("name", ""),
                        json.dumps(fixture)
                    ))
                print(f"         💾 {len(recent_results)} fixtures básicos salvos")
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"         ❌ Erro ao salvar dados: {e}")
        conn.rollback()
        return False

def main():
    """Função principal"""
    print("🚀 EXTRAÇÃO ALTERNATIVA - SÉRIE A BRASIL 2025")
    print("=" * 60)
    print("🎯 Usando endpoints menos intensivos")
    print("⏱️  Delay entre requisições: 5s")
    print("=" * 60)
    
    if not API_TOKEN:
        print("❌ SPORTMONKS_API_KEY não configurada")
        return
    
    # 1. Extrair informações básicas
    league_info = extract_league_info()
    time.sleep(REQUEST_DELAY)
    
    season_info = extract_season_info()
    time.sleep(REQUEST_DELAY)
    
    teams = extract_teams_basic()
    time.sleep(REQUEST_DELAY)
    
    schedule = extract_schedule_structure()
    time.sleep(REQUEST_DELAY)
    
    standings = extract_standings_basic()
    time.sleep(REQUEST_DELAY)
    
    recent_results = extract_recent_results()
    
    # 2. Salvar no banco
    print(f"\n💾 Salvando dados no banco...")
    
    conn = psycopg2.connect(DB_DSN)
    try:
        if save_basic_data(conn, league_info, season_info, teams, schedule, standings, recent_results):
            print("         ✅ Dados salvos com sucesso")
        else:
            print("         ❌ Falha ao salvar dados")
    finally:
        conn.close()
    
    # 3. Resumo
    print(f"\n📊 RESUMO DA EXTRAÇÃO:")
    print(f"   • Liga: {'✅' if league_info else '❌'}")
    print(f"   • Temporada: {'✅' if season_info else '❌'}")
    print(f"   • Times: {len(teams) if teams else 0}")
    print(f"   • Calendário: {'✅' if schedule else '❌'}")
    print(f"   • Tabela: {'✅' if standings else '❌'}")
    print(f"   • Jogos recentes: {len(recent_results) if recent_results else 0}")
    
    print(f"\n🎯 PRÓXIMOS PASSOS:")
    print(f"   • Aguardar rate limit resetar")
    print(f"   • Executar carregamento completo em horário de baixo tráfego")
    print(f"   • Usar endpoints alternativos para dados específicos")

if __name__ == "__main__":
    main()
