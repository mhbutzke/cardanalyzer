#!/usr/bin/env python3
"""
CardAnalyzer - Carregar Times do Brasileirão
Script para carregar todos os times da Série A e seus IDs
"""
import os, sys, time, json
import httpx, psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

API = os.getenv("API_BASE_URL", "https://api.sportmonks.com/v3/football")
KEY = os.getenv("SPORTMONKS_API_KEY")
DSN = os.getenv("DB_DSN", "postgresql://card:card@localhost:5432/carddb")

# Configurações
LEAGUE_ID = 648  # Brasileirão Série A
SEASON_2025 = 25184

def http_get(client: httpx.Client, path: str, params: dict = None):
    """HTTP GET com retry"""
    assert KEY, "Defina SPORTMONKS_API_KEY no .env"
    url = API.rstrip("/") + "/" + path.lstrip("/")
    q = dict(params or {}); q.setdefault("api_token", KEY)
    
    for i in range(5):
        r = client.get(url, params=q)
        if r.status_code == 429:  # Rate limit
            print(f"   ⚠️  Rate limit atingido! Aguardando 60s...")
            time.sleep(60)
            continue
        elif r.status_code in (500, 502, 503, 504):
            time.sleep((2**i) + 0.1)
            continue
        r.raise_for_status()
        return r.json()
    
    r.raise_for_status()

def load_brasileirao_teams():
    """Carrega todos os times do Brasileirão"""
    print("🏆 Carregando times do Brasileirão Série A...")
    print("=" * 60)
    
    with httpx.Client(timeout=30.0) as client, psycopg2.connect(DSN) as conn:
        try:
            # Buscar times brasileiros (country_id = 5)
            params = {
                "include": "activeSeasons",
                "per_page": 200
            }
            
            data = http_get(client, "teams/countries/5", params)
            
            teams = data.get("data", [])
            print(f"📊 {len(teams)} times encontrados")
            
            # Salvar times no banco
            team_rows = []
            for team in teams:
                team_id = team["id"]
                team_name = team.get("name", "N/A")
                short_code = team.get("short_code", "N/A")
                
                # Verificar se está ativo na temporada 2025
                active_seasons = team.get("activeSeasons", {}).get("data", [])
                is_active_2025 = any(s.get("id") == SEASON_2025 for s in active_seasons)
                
                team_rows.append((
                    team_id,
                    team_name,
                    5,  # country_id = 5 (Brasil)
                    json.dumps(team)
                ))
                
                short_code = team.get("short_code", "N/A")
                status = "✅ 2025" if is_active_2025 else "❌ 2024"
                print(f"   {team_id:4d}: {team_name:<25} ({short_code}) {status}")
            
            # Upsert no banco
            if team_rows:
                with conn.cursor() as cur:
                    sql = """
                    INSERT INTO teams (id, name, country_id, json_data) 
                    VALUES %s 
                    ON CONFLICT (id) DO UPDATE SET 
                        name = EXCLUDED.name,
                        country_id = EXCLUDED.country_id,
                        json_data = EXCLUDED.json_data
                    """
                    execute_values(cur, sql, team_rows)
                
                conn.commit()
                print(f"\n💾 {len(team_rows)} times salvos no banco!")
            
            return teams
            
        except Exception as e:
            print(f"❌ Erro: {e}")
            return []

def show_team_ids():
    """Mostra os IDs dos times para uso no endpoint otimizado"""
    print("\n🔍 IDs dos times para endpoint otimizado:")
    print("=" * 60)
    
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT id, name, json_data->>'short_code' as short_code
                FROM teams 
                WHERE country_id = 5
                ORDER BY name
            """)
            
            teams = cur.fetchall()
            print(f"📊 {len(teams)} times ativos em 2025:")
            print()
            
            for team_id, name, short_code in teams:
                print(f"   {team_id:4d}: {name:<25} ({short_code})")
            
            print()
            print("🎯 Use esses IDs no endpoint:")
            print("   /fixtures/between/YYYY-MM-DD/YYYY-MM-DD/{team_id}")
            print()
            print("📝 Exemplo:")
            for team_id, name, short_code in teams[:3]:  # Primeiros 3 times
                print(f"   https://api.sportmonks.com/v3/football/fixtures/between/2025-01-01/2025-03-31/{team_id}")

if __name__ == "__main__":
    print("🚀 CardAnalyzer - Carregar Times do Brasileirão")
    print("=" * 60)
    
    # Carregar times
    teams = load_brasileirao_teams()
    
    if teams:
        # Mostrar IDs para uso
        show_team_ids()
        
        print(f"\n🎉 Processo concluído! {len(teams)} times carregados.")
    else:
        print("❌ Falha ao carregar times.")
        sys.exit(1)
