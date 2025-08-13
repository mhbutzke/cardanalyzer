#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Carregamento Completo de Ligas Principais
- Brasil: Série A, Série B, Copa do Brasil
- Argentina: Primeira Divisão
- Continentais: Libertadores, Sudamericana
- Temporadas: 2024 e 2025
"""

import os
import json
import time
import logging
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv
import httpx

load_dotenv()

# Configuração de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('load_complete_leagues.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class CompleteLeagueLoader:
    def __init__(self):
        self.api_base = os.getenv("API_BASE_URL", "https://api.sportmonks.com/v3/football")
        self.api_token = os.getenv("SPORTMONKS_API_KEY")
        self.db_dsn = os.getenv("DB_DSN")
        
        # Ligas para carregar
        self.leagues_to_load = {
            648: "Série A Brasil",
            651: "Série B Brasil", 
            654: "Copa do Brasil",
            636: "Primeira Divisão Argentina",
            1122: "Libertadores",
            1116: "Sudamericana"
        }
        
        # Temporadas para carregar
        self.seasons_to_load = [2024, 2025]
        
        # IDs de temporadas conhecidas (para fallback)
        self.known_season_ids = {
            # Série A Brasil
            648: {
                2024: [23265],  # Temporada 2024
                2025: [25037]   # Temporada 2025
            },
            # Série B Brasil  
            651: {
                2024: [25185],  # Temporada 2024
                2025: [25037]   # Temporada 2025
            }
        }
        
        # IDs de eventos importantes
        self.important_event_types = [14, 15, 16, 17, 19, 20, 21]  # Gols e cartões
        self.important_stat_types = [34, 52, 56]  # Corners, gols, faltas
    
    def get_with_backoff(self, url, params=None, max_retries=3):
        """Requisição com backoff exponencial"""
        params = params or {}
        params["api_token"] = self.api_token
        
        for attempt in range(max_retries):
            try:
                with httpx.Client() as client:
                    response = client.get(url, params=params, timeout=30)
                    
                    if response.status_code == 200:
                        return response.json()
                    elif response.status_code == 429:
                        wait_time = (2 ** attempt) * 60
                        logger.warning(f"Rate limit atingido. Aguardando {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"Erro HTTP {response.status_code}: {response.text[:200]}")
                        return None
                        
            except Exception as e:
                logger.error(f"Erro na requisição (tentativa {attempt + 1}): {e}")
                if attempt < max_retries - 1:
                    time.sleep(60)
                    continue
                return None
                
        return None
    
    def get_season_id(self, year, league_id):
        """Buscar ID da temporada por ano e liga"""
        try:
            # Usar endpoint que funciona: buscar todas as temporadas
            url = f"{self.api_base}/seasons"
            params = {
                "per_page": 200
            }
            
            data = self.get_with_backoff(url, params)
            if data and data.get("data"):
                for season in data["data"]:
                    season_name = str(season.get("name", ""))
                    season_league_id = season.get("league_id")
                    
                    # Verificar se é a liga e ano corretos
                    if season_league_id == league_id and str(year) in season_name:
                        logger.info(f"      ✅ Temporada {year} encontrada: {season_name} (ID: {season['id']})")
                        return season["id"]
            
            logger.warning(f"   ⚠️ Temporada {year} não encontrada para liga {league_id}")
            return None
            
        except Exception as e:
            logger.error(f"   ❌ Erro ao buscar temporada {year} para liga {league_id}: {e}")
            return None
    
    def load_fixtures_for_season(self, season_id, league_id, league_name, year):
        """Carregar todos os fixtures de uma temporada"""
        logger.info(f"   📅 Carregando temporada {year} da {league_name}...")
        
        try:
            # Buscar fixtures da temporada
            url = f"{self.api_base}/fixtures/season/{season_id}"
            params = {
                "per_page": 200,
                "include": "participants;events.type;statistics"
            }
            
            fixtures_loaded = 0
            page = 1
            
            while True:
                params["page"] = page
                logger.info(f"      📄 Processando página {page}...")
                
                data = self.get_with_backoff(url, params)
                if not data or not data.get("data"):
                    break
                
                fixtures = data["data"]
                if not fixtures:
                    break
                
                # Processar fixtures da página
                for fixture in fixtures:
                    if self.process_fixture(fixture, league_id, season_id):
                        fixtures_loaded += 1
                
                # Verificar se há mais páginas
                pagination = data.get("pagination", {})
                if not pagination.get("has_more", False):
                    break
                
                page += 1
                time.sleep(1)  # Delay entre páginas
            
            logger.info(f"      ✅ {fixtures_loaded} fixtures carregados da temporada {year}")
            return fixtures_loaded
            
        except Exception as e:
            logger.error(f"      ❌ Erro ao carregar temporada {year}: {e}")
            return 0
    
    def process_fixture(self, fixture, league_id, season_id):
        """Processar um fixture individual"""
        try:
            conn = psycopg2.connect(self.db_dsn)
            cur = conn.cursor()
            
            try:
                # 1. Inserir/atualizar fixture
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
                    league_id,
                    season_id,
                    fixture.get("starting_at"),
                    fixture.get("state_id"),
                    fixture.get("venue_id"),
                    fixture.get("name", ""),
                    json.dumps(fixture)
                ))
                
                # 2. Inserir/atualizar participantes
                for participant in fixture.get("participants", []):
                    cur.execute("""
                        INSERT INTO fixture_participants (fixture_id, team_id, location, name)
                        VALUES (%s, %s, %s, %s)
                        ON CONFLICT (fixture_id, team_id) DO UPDATE SET
                            location = EXCLUDED.location,
                            name = EXCLUDED.name
                    """, (
                        fixture["id"],
                        participant.get("id"),
                        participant.get("meta", {}).get("location"),
                        participant.get("name", "")
                    ))
                
                # 3. Inserir/atualizar eventos
                for event in fixture.get("events", []):
                    if event.get("type_id") in self.important_event_types:
                        cur.execute("""
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
                        """, (
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
                
                # 4. Inserir/atualizar estatísticas
                for stat in fixture.get("statistics", []):
                    if stat.get("type_id") in self.important_stat_types:
                        cur.execute("""
                            INSERT INTO fixture_statistics (fixture_id, type_id, participant_id, value)
                            VALUES (%s, %s, %s, %s)
                            ON CONFLICT (fixture_id, type_id, participant_id) DO UPDATE SET 
                                value = EXCLUDED.value
                        """, (
                            fixture["id"],
                            stat.get("type_id"),
                            stat.get("participant_id"),
                            stat.get("value")
                        ))
                
                conn.commit()
                return True
                
            except Exception as e:
                conn.rollback()
                logger.error(f"         ❌ Erro ao processar fixture {fixture.get('id')}: {e}")
                return False
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"         ❌ Erro geral ao processar fixture: {e}")
            return False
    
    def load_all_leagues(self):
        """Carregar todas as ligas selecionadas"""
        logger.info("🚀 INICIANDO CARREGAMENTO COMPLETO DAS LIGAS")
        logger.info("=" * 60)
        
        total_fixtures = 0
        start_time = time.time()
        
        try:
            for league_id, league_name in self.leagues_to_load.items():
                logger.info(f"🏆 CARREGANDO {league_name.upper()}")
                logger.info("-" * 40)
                
                league_fixtures = 0
                
                for year in self.seasons_to_load:
                    logger.info(f"   📅 Processando ano {year}...")
                    
                    # Buscar ID da temporada
                    season_id = self.get_season_id(year, league_id)
                    
                    # Fallback para temporadas conhecidas
                    if not season_id and league_id in self.known_season_ids:
                        if year in self.known_season_ids[league_id]:
                            season_id = self.known_season_ids[league_id][year][0]
                            logger.info(f"      🔄 Usando ID de temporada conhecido: {season_id}")
                    
                    if not season_id:
                        logger.warning(f"      ⚠️ Não foi possível encontrar temporada {year} para liga {league_id}")
                        continue
                    
                    # Carregar fixtures da temporada
                    fixtures_count = self.load_fixtures_for_season(season_id, league_id, league_name, year)
                    league_fixtures += fixtures_count
                    
                    time.sleep(2)  # Delay entre temporadas
                
                logger.info(f"   ✅ Total de {league_fixtures} fixtures carregados para {league_name}")
                total_fixtures += league_fixtures
                
                time.sleep(5)  # Delay entre ligas
            
            # Estatísticas finais
            elapsed_time = time.time() - start_time
            logger.info(f"\n🎉 CARREGAMENTO CONCLUÍDO!")
            logger.info(f"   ⏱️ Tempo total: {elapsed_time:.1f}s")
            logger.info(f"   📊 Total de fixtures: {total_fixtures}")
            logger.info(f"   🏆 Ligas processadas: {len(self.leagues_to_load)}")
            
            # Executar análise completa
            logger.info(f"\n🔄 Executando análise completa...")
            self.run_complete_analysis()
            
        except Exception as e:
            logger.error(f"❌ Erro no carregamento: {e}")
    
    def run_complete_analysis(self):
        """Executar análise completa após carregamento"""
        try:
            from complete_analysis import CompleteAnalysis
            analyzer = CompleteAnalysis()
            analyzer.run_complete_analysis()
        except Exception as e:
            logger.error(f"❌ Erro ao executar análise: {e}")

def main():
    """Função principal"""
    if not os.getenv("SPORTMONKS_API_KEY"):
        print("❌ SPORTMONKS_API_KEY não configurada")
        return
    
    loader = CompleteLeagueLoader()
    loader.load_all_leagues()

if __name__ == "__main__":
    main()
