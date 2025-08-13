#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sistema Automático Simplificado de Atualização
- Funciona com a estrutura atual do banco
- Atualizações incrementais inteligentes
- Logs detalhados
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
        logging.FileHandler('auto_update.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class SimpleAutoUpdate:
    def __init__(self):
        self.api_base = os.getenv("API_BASE_URL", "https://api.sportmonks.com/v3/football")
        self.api_token = os.getenv("SPORTMONKS_API_KEY")
        self.db_dsn = os.getenv("DB_DSN")
        
        # Ligas para monitorar
        self.leagues_to_monitor = {
            648: "Série A Brasil",
            649: "Série B Brasil"
        }
    
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
    
    def check_new_fixtures(self):
        """Verificar novos fixtures disponíveis"""
        logger.info("🔍 Verificando novos fixtures...")
        
        try:
            conn = psycopg2.connect(self.db_dsn)
            cur = conn.cursor()
            
            # Buscar último fixture por data
            cur.execute("SELECT MAX(starting_at) FROM fixtures")
            last_update = cur.fetchone()[0]
            
            if not last_update:
                last_update = datetime.now() - timedelta(days=7)
                logger.info("   📅 Usando data padrão (última semana)")
            else:
                logger.info(f"   📅 Último fixture: {last_update}")
                
                # Converter para datetime se necessário
                if isinstance(last_update, str):
                    last_update = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
            
            # Buscar fixtures recentes da API
            new_fixtures = []
            
            # Usar endpoint que funciona: buscar fixtures por data
            logger.info("   Verificando fixtures recentes...")
            
            # Buscar fixtures dos últimos 7 dias
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)
            
            url = f"{self.api_base}/fixtures/between/{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
            params = {
                "per_page": 50,  # Limitar para evitar rate limit
                "include": "participants;events.type;statistics"
            }
            
            data = self.get_with_backoff(url, params)
            if data and data.get("data"):
                fixtures = data["data"]
                
                for fixture in fixtures:
                    try:
                        # Verificar se é uma das ligas que monitoramos
                        if fixture.get("league_id") in self.leagues_to_monitor:
                            fixture_date = datetime.fromisoformat(fixture["starting_at"].replace("Z", "+00:00"))
                            
                            if fixture_date > last_update:
                                new_fixtures.append({
                                    "id": fixture["id"],
                                    "league_id": fixture["league_id"],
                                    "season_id": fixture["season_id"],
                                    "name": fixture["name"],
                                    "starting_at": fixture["starting_at"],
                                    "state_id": fixture["state_id"]
                                })
                    except Exception as e:
                        logger.warning(f"      ⚠️ Erro ao processar fixture {fixture.get('id')}: {e}")
                        continue
            
            time.sleep(2)  # Delay para evitar rate limit
            
            conn.close()
            
            logger.info(f"   ✅ {len(new_fixtures)} novos fixtures encontrados")
            return new_fixtures
            
        except Exception as e:
            logger.error(f"❌ Erro ao verificar novos fixtures: {e}")
            return []
    
    def update_fixture_data(self, fixture_id):
        """Atualizar dados de um fixture específico"""
        logger.info(f"   🔄 Atualizando fixture {fixture_id}...")
        
        try:
            # Buscar dados completos do fixture
            url = f"{self.api_base}/fixtures/{fixture_id}"
            params = {
                "include": "participants;events.type;statistics"
            }
            
            data = self.get_with_backoff(url, params)
            if not data or not data.get("data"):
                logger.warning(f"      ⚠️ Dados não encontrados para fixture {fixture_id}")
                return False
            
            fixture = data["data"]
            
            # Conectar ao banco
            conn = psycopg2.connect(self.db_dsn)
            cur = conn.cursor()
            
            try:
                # Atualizar fixture
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
                
                # Atualizar participantes
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
                
                # Atualizar eventos
                for event in fixture.get("events", []):
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
                
                # Atualizar estatísticas
                for stat in fixture.get("statistics", []):
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
                logger.info(f"      ✅ Fixture {fixture_id} atualizado com sucesso")
                return True
                
            except Exception as e:
                conn.rollback()
                logger.error(f"      ❌ Erro ao atualizar fixture {fixture_id}: {e}")
                return False
            finally:
                conn.close()
                
        except Exception as e:
            logger.error(f"      ❌ Erro geral ao atualizar fixture {fixture_id}: {e}")
            return False
    
    def refresh_analysis_tables(self):
        """Atualizar tabelas de análise"""
        logger.info("🔄 Atualizando tabelas de análise...")
        
        try:
            conn = psycopg2.connect(self.db_dsn)
            cur = conn.cursor()
            
            # Limpar tabelas existentes
            cur.execute("DELETE FROM card_analysis")
            cur.execute("DELETE FROM statistic_analysis")
            cur.execute("DELETE FROM referee_analysis")
            
            # Recriar análise de cartões
            cur.execute("""
                INSERT INTO card_analysis (fixture_id, team_id, period, card_type, action_type, count)
                SELECT 
                    f.id as fixture_id,
                    e.participant_id as team_id,
                    CASE 
                        WHEN e.minute <= 45 THEN 'HT'
                        ELSE 'FT'
                    END as period,
                    CASE 
                        WHEN e.type_id = 19 THEN 'YELLOW'
                        WHEN e.type_id = 20 THEN 'RED'
                        WHEN e.type_id = 21 THEN 'YELLOWRED'
                        ELSE 'UNKNOWN'
                    END as card_type,
                    'IT1' as action_type,
                    COUNT(*) as count
                FROM fixtures f
                JOIN events e ON f.id = e.fixture_id
                WHERE e.type_id IN (19, 20, 21)
                  AND e.rescinded = false
                  AND f.state_id = 5
                  AND e.participant_id IS NOT NULL
                GROUP BY f.id, e.participant_id, period, card_type
            """)
            
            # Recriar análise de estatísticas
            cur.execute("""
                INSERT INTO statistic_analysis (fixture_id, team_id, period, stat_type, action_type, count)
                SELECT 
                    f.id as fixture_id,
                    fs.participant_id as team_id,
                    'FT' as period,
                    CASE 
                        WHEN fs.type_id = 34 THEN 'CORNERS'
                        WHEN fs.type_id = 52 THEN 'GOALS'
                        WHEN fs.type_id = 56 THEN 'FOULS'
                        ELSE 'OTHER'
                    END as stat_type,
                    'IT1' as action_type,
                    fs.value as count
                FROM fixtures f
                JOIN fixture_statistics fs ON f.id = fs.fixture_id
                WHERE fs.type_id IN (34, 52, 56)
                  AND f.state_id = 5
                  AND fs.participant_id IS NOT NULL
                  AND fs.value IS NOT NULL
            """)
            
            # Recriar análise de árbitros
            cur.execute("""
                INSERT INTO referee_analysis (fixture_id, referee_id, period, total_cards, yellow_cards, red_cards, yellowred_cards)
                SELECT 
                    f.id as fixture_id,
                    1 as referee_id,
                    'FT' as period,
                    COUNT(CASE WHEN e.type_id IN (19, 20, 21) AND e.rescinded = false THEN 1 END) as total_cards,
                    COUNT(CASE WHEN e.type_id = 19 AND e.rescinded = false THEN 1 END) as yellow_cards,
                    COUNT(CASE WHEN e.type_id = 20 AND e.rescinded = false THEN 1 END) as red_cards,
                    COUNT(CASE WHEN e.type_id = 21 AND e.rescinded = false THEN 1 END) as yellowred_cards
                FROM fixtures f
                LEFT JOIN events e ON f.id = e.fixture_id AND e.type_id IN (19, 20, 21)
                WHERE f.state_id = 5
                GROUP BY f.id
                HAVING COUNT(CASE WHEN e.type_id IN (19, 20, 21) AND e.rescinded = false THEN 1 END) > 0
            """)
            
            conn.commit()
            logger.info("   ✅ Tabelas de análise atualizadas")
            
            # Verificar resultados
            cur.execute("SELECT COUNT(*) FROM card_analysis")
            card_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM statistic_analysis")
            stat_count = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM referee_analysis")
            ref_count = cur.fetchone()[0]
            
            logger.info(f"   📊 Resultados: Cartões: {card_count}, Estatísticas: {stat_count}, Árbitros: {ref_count}")
            
            conn.close()
            
        except Exception as e:
            logger.error(f"❌ Erro ao atualizar tabelas de análise: {e}")
    
    def run_update_cycle(self):
        """Executar um ciclo completo de atualização"""
        logger.info("🚀 INICIANDO CICLO DE ATUALIZAÇÃO AUTOMÁTICA")
        logger.info("=" * 60)
        
        start_time = time.time()
        
        try:
            # 1. Verificar novos fixtures
            new_fixtures = self.check_new_fixtures()
            
            if not new_fixtures:
                logger.info("   ℹ️ Nenhum novo fixture encontrado")
                return
            
            # 2. Atualizar fixtures
            updated_count = 0
            for fixture in new_fixtures:
                if self.update_fixture_data(fixture["id"]):
                    updated_count += 1
                time.sleep(1)  # Delay entre atualizações
            
            # 3. Atualizar tabelas de análise
            if updated_count > 0:
                self.refresh_analysis_tables()
            
            # 4. Estatísticas finais
            elapsed_time = time.time() - start_time
            logger.info(f"🎉 CICLO CONCLUÍDO!")
            logger.info(f"   ⏱️ Tempo total: {elapsed_time:.1f}s")
            logger.info(f"   📊 Fixtures novos: {len(new_fixtures)}")
            logger.info(f"   ✅ Fixtures atualizados: {updated_count}")
            
        except Exception as e:
            logger.error(f"❌ Erro no ciclo de atualização: {e}")

def main():
    """Função principal"""
    if not os.getenv("SPORTMONKS_API_KEY"):
        print("❌ SPORTMONKS_API_KEY não configurada")
        return
    
    system = SimpleAutoUpdate()
    system.run_update_cycle()

if __name__ == "__main__":
    main()
