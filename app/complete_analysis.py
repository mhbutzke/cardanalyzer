#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Sistema de Análise Completa
- Totais por temporada
- Pontuação e forma (V-E-D)
- Saldo de gols
- Cartões detalhados (HT/FT, IT1/IT2)
- Estatísticas por período
- Análise de árbitros
"""

import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

class CompleteAnalysis:
    def __init__(self):
        self.db_dsn = os.getenv("DB_DSN")
    
    def create_analysis_tables(self):
        """Criar tabelas de análise completa"""
        print("🏗️ CRIANDO TABELAS DE ANÁLISE COMPLETA")
        print("=" * 60)
        
        try:
            conn = psycopg2.connect(self.db_dsn)
            cur = conn.cursor()
            
            # 1. Tabela de análise por temporada
            cur.execute("""
                CREATE TABLE IF NOT EXISTS season_analysis (
                    id SERIAL PRIMARY KEY,
                    season_id BIGINT,
                    league_id BIGINT,
                    team_id BIGINT,
                    team_name TEXT,
                    games_played INTEGER DEFAULT 0,
                    wins INTEGER DEFAULT 0,
                    draws INTEGER DEFAULT 0,
                    losses INTEGER DEFAULT 0,
                    points INTEGER DEFAULT 0,
                    goals_for INTEGER DEFAULT 0,
                    goals_against INTEGER DEFAULT 0,
                    goal_difference INTEGER DEFAULT 0,
                    yellow_cards_ht_it1 INTEGER DEFAULT 0,
                    yellow_cards_ht_it2 INTEGER DEFAULT 0,
                    yellow_cards_ft_it1 INTEGER DEFAULT 0,
                    yellow_cards_ft_it2 INTEGER DEFAULT 0,
                    red_cards_ht_it1 INTEGER DEFAULT 0,
                    red_cards_ht_it2 INTEGER DEFAULT 0,
                    red_cards_ft_it1 INTEGER DEFAULT 0,
                    red_cards_ft_it2 INTEGER DEFAULT 0,
                    yellowred_cards_ht_it1 INTEGER DEFAULT 0,
                    yellowred_cards_ht_it2 INTEGER DEFAULT 0,
                    yellowred_cards_ft_it1 INTEGER DEFAULT 0,
                    yellowred_cards_ft_it2 INTEGER DEFAULT 0,
                    corners_ht_it1 INTEGER DEFAULT 0,
                    corners_ht_it2 INTEGER DEFAULT 0,
                    corners_ft_it1 INTEGER DEFAULT 0,
                    corners_ft_it2 INTEGER DEFAULT 0,
                    goals_ht_it1 INTEGER DEFAULT 0,
                    goals_ht_it2 INTEGER DEFAULT 0,
                    goals_ft_it1 INTEGER DEFAULT 0,
                    goals_ft_it2 INTEGER DEFAULT 0,
                    fouls_ht_it1 INTEGER DEFAULT 0,
                    fouls_ht_it2 INTEGER DEFAULT 0,
                    fouls_ft_it1 INTEGER DEFAULT 0,
                    fouls_ft_it2 INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 2. Tabela detalhada de cartões com minuto e jogador
            cur.execute("""
                CREATE TABLE IF NOT EXISTS card_details (
                    id SERIAL PRIMARY KEY,
                    fixture_id BIGINT,
                    season_id BIGINT,
                    league_id BIGINT,
                    team_id BIGINT,
                    team_name TEXT,
                    player_id BIGINT,
                    player_name TEXT,
                    card_type VARCHAR(20),
                    minute INTEGER,
                    minute_extra INTEGER,
                    period VARCHAR(10),
                    location VARCHAR(10),
                    fixture_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 3. Tabela de análise de árbitros por temporada
            cur.execute("""
                CREATE TABLE IF NOT EXISTS referee_season_analysis (
                    id SERIAL PRIMARY KEY,
                    season_id BIGINT,
                    league_id BIGINT,
                    referee_id BIGINT,
                    games_officiated INTEGER DEFAULT 0,
                    total_cards_ht INTEGER DEFAULT 0,
                    total_cards_ft INTEGER DEFAULT 0,
                    yellow_cards_ht INTEGER DEFAULT 0,
                    yellow_cards_ft INTEGER DEFAULT 0,
                    red_cards_ht INTEGER DEFAULT 0,
                    red_cards_ft INTEGER DEFAULT 0,
                    yellowred_cards_ht INTEGER DEFAULT 0,
                    yellowred_cards_ft INTEGER DEFAULT 0,
                    avg_cards_per_game NUMERIC(5,2) DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # 4. Índices para performance
            cur.execute("CREATE INDEX IF NOT EXISTS idx_season_analysis_season_team ON season_analysis(season_id, team_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_referee_season_analysis_season ON referee_season_analysis(season_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_card_details_fixture ON card_details(fixture_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_card_details_player ON card_details(player_id)")
            cur.execute("CREATE INDEX IF NOT EXISTS idx_card_details_team ON card_details(team_id)")
            
            conn.commit()
            print("   ✅ Tabelas de análise criadas com sucesso")
            
            conn.close()
            
        except Exception as e:
            print(f"   ❌ Erro ao criar tabelas: {e}")
    
    def populate_season_analysis(self):
        """Popular análise por temporada"""
        print("\n📊 POPULANDO ANÁLISE POR TEMPORADA")
        print("-" * 40)
        
        try:
            conn = psycopg2.connect(self.db_dsn)
            cur = conn.cursor()
            
            # Limpar dados existentes
            cur.execute("DELETE FROM season_analysis")
            
            # Buscar todas as temporadas
            cur.execute("SELECT DISTINCT season_id, league_id FROM fixtures ORDER BY season_id, league_id")
            seasons = cur.fetchall()
            
            for season_id, league_id in seasons:
                print(f"   📅 Processando temporada {season_id} (liga {league_id})...")
                
                # Buscar todos os times da temporada
                cur.execute("""
                    SELECT DISTINCT fp.team_id, fp.name
                    FROM fixtures f
                    JOIN fixture_participants fp ON f.id = fp.fixture_id
                    WHERE f.season_id = %s AND f.league_id = %s
                    ORDER BY fp.team_id
                """, (season_id, league_id))
                
                teams = cur.fetchall()
                
                for team_id, team_name in teams:
                    print(f"      🏆 Processando time: {team_name}")
                    
                    # Calcular estatísticas do time
                    self.calculate_team_stats(cur, season_id, league_id, team_id, team_name)
            
            conn.commit()
            print("   ✅ Análise por temporada populada com sucesso")
            
            conn.close()
            
        except Exception as e:
            print(f"   ❌ Erro ao popular análise: {e}")
    
    def populate_card_details(self):
        """Popular detalhes dos cartões com minuto e jogador"""
        print("\n🟨 POPULANDO DETALHES DOS CARTÕES")
        print("-" * 40)
        
        try:
            conn = psycopg2.connect(self.db_dsn)
            cur = conn.cursor()
            
            # Limpar dados existentes
            cur.execute("DELETE FROM card_details")
            
            # Buscar todos os cartões com detalhes
            cur.execute("""
                SELECT 
                    f.id as fixture_id,
                    f.season_id,
                    f.league_id,
                    f.name as fixture_name,
                    e.participant_id as team_id,
                    fp.name as team_name,
                    e.player_id,
                    e.minute,
                    e.minute_extra,
                    e.type_id,
                    fp.location,
                    CASE 
                        WHEN e.minute <= 45 THEN 'HT'
                        ELSE 'FT'
                    END as period
                FROM fixtures f
                JOIN events e ON f.id = e.fixture_id
                JOIN fixture_participants fp ON f.id = fp.fixture_id AND e.participant_id = fp.team_id
                WHERE e.type_id IN (19, 20, 21)  -- Cartões amarelos, vermelhos e amarelo-vermelho
                  AND e.rescinded = false
                  AND f.state_id = 5  -- Jogos finalizados
                ORDER BY f.season_id, f.league_id, f.id, e.minute
            """)
            
            cards = cur.fetchall()
            print(f"   📊 Processando {len(cards)} cartões...")
            
            for card in cards:
                fixture_id, season_id, league_id, fixture_name, team_id, team_name, player_id, minute, minute_extra, type_id, location, period = card
                
                # Determinar tipo de cartão
                card_type = {
                    19: 'YELLOW',
                    20: 'RED', 
                    21: 'YELLOWRED'
                }.get(type_id, 'UNKNOWN')
                
                # Buscar nome do jogador se disponível
                player_name = "Desconhecido"
                if player_id:
                    # Aqui você pode implementar uma busca pelo nome do jogador
                    # Por enquanto vamos usar o ID
                    player_name = f"Jogador {player_id}"
                
                # Inserir detalhes do cartão
                cur.execute("""
                    INSERT INTO card_details (
                        fixture_id, season_id, league_id, team_id, team_name,
                        player_id, player_name, card_type, minute, minute_extra,
                        period, location, fixture_name
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    fixture_id, season_id, league_id, team_id, team_name,
                    player_id, player_name, card_type, minute, minute_extra,
                    period, location, fixture_name
                ))
            
            conn.commit()
            print(f"   ✅ {len(cards)} cartões processados com sucesso")
            
            conn.close()
            
        except Exception as e:
            print(f"   ❌ Erro ao popular detalhes dos cartões: {e}")
    
    def calculate_team_stats(self, cur, season_id, league_id, team_id, team_name):
        """Calcular estatísticas de um time específico"""
        try:
            # 1. Jogos jogados
            cur.execute("""
                SELECT COUNT(*) FROM fixtures f
                JOIN fixture_participants fp ON f.id = fp.fixture_id
                WHERE f.season_id = %s AND f.league_id = %s AND fp.team_id = %s
            """, (season_id, league_id, team_id))
            games_played = cur.fetchone()[0]
            
            if games_played == 0:
                return
            
            # 2. Vitórias, empates e derrotas
            cur.execute("""
                SELECT 
                    COUNT(CASE WHEN fp.location = 'home' AND fs.value > fs2.value THEN 1 END) as wins_home,
                    COUNT(CASE WHEN fp.location = 'away' AND fs.value < fs2.value THEN 1 END) as wins_away,
                    COUNT(CASE WHEN fs.value = fs2.value THEN 1 END) as draws,
                    COUNT(CASE WHEN fp.location = 'home' AND fs.value < fs2.value THEN 1 END) as losses_home,
                    COUNT(CASE WHEN fp.location = 'away' AND fs.value > fs2.value THEN 1 END) as losses_away
                FROM fixtures f
                JOIN fixture_participants fp ON f.id = fp.fixture_id
                JOIN fixture_statistics fs ON f.id = fs.fixture_id AND fs.type_id = 52 AND fs.participant_id = fp.team_id
                JOIN fixture_participants fp2 ON f.id = fp2.fixture_id AND fp2.team_id != fp.team_id
                JOIN fixture_statistics fs2 ON f.id = fs2.fixture_id AND fs2.type_id = 52 AND fs2.participant_id = fp2.team_id
                WHERE f.season_id = %s AND f.league_id = %s AND fp.team_id = %s
            """, (season_id, league_id, team_id))
            
            result = cur.fetchone()
            wins = (result[0] or 0) + (result[1] or 0)
            draws = result[2] or 0
            losses = (result[3] or 0) + (result[4] or 0)
            points = (wins * 3) + draws
            
            # 3. Gols a favor e contra
            cur.execute("""
                SELECT 
                    COALESCE(SUM(fs.value), 0) as goals_for,
                    COALESCE(SUM(fs2.value), 0) as goals_against
                FROM fixtures f
                JOIN fixture_participants fp ON f.id = fp.fixture_id
                LEFT JOIN fixture_statistics fs ON f.id = fs.fixture_id AND fs.type_id = 52 AND fs.participant_id = fp.team_id
                LEFT JOIN fixture_participants fp2 ON f.id = fp2.fixture_id AND fp2.team_id != fp.team_id
                LEFT JOIN fixture_statistics fs2 ON f.id = fs2.fixture_id AND fs2.type_id = 52 AND fs2.participant_id = fp2.team_id
                WHERE f.season_id = %s AND f.league_id = %s AND fp.team_id = %s
            """, (season_id, league_id, team_id))
            
            result = cur.fetchone()
            goals_for = result[0] or 0
            goals_against = result[1] or 0
            goal_difference = goals_for - goals_against
            
            # 4. Cartões por período e tipo
            card_stats = self.calculate_card_stats(cur, season_id, league_id, team_id)
            
            # 5. Estatísticas por período
            stat_stats = self.calculate_stat_stats(cur, season_id, league_id, team_id)
            
            # 6. Inserir na tabela
            cur.execute("""
                INSERT INTO season_analysis (
                    season_id, league_id, team_id, team_name, games_played,
                    wins, draws, losses, points, goals_for, goals_against, goal_difference,
                    yellow_cards_ht_it1, yellow_cards_ht_it2, yellow_cards_ft_it1, yellow_cards_ft_it2,
                    red_cards_ht_it1, red_cards_ht_it2, red_cards_ft_it1, red_cards_ft_it2,
                    yellowred_cards_ht_it1, yellowred_cards_ht_it2, yellowred_cards_ft_it1, yellowred_cards_ft_it2,
                    corners_ht_it1, corners_ht_it2, corners_ft_it1, corners_ft_it2,
                    goals_ht_it1, goals_ht_it2, goals_ft_it1, goals_ft_it2,
                    fouls_ht_it1, fouls_ht_it2, fouls_ft_it1, fouls_ft_it2
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                )
            """, (
                season_id, league_id, team_id, team_name, games_played,
                wins, draws, losses, points, goals_for, goals_against, goal_difference,
                card_stats['yellow_ht_it1'], card_stats['yellow_ht_it2'], card_stats['yellow_ft_it1'], card_stats['yellow_ft_it2'],
                card_stats['red_ht_it1'], card_stats['red_ht_it2'], card_stats['red_ft_it1'], card_stats['red_ft_it2'],
                card_stats['yellowred_ht_it1'], card_stats['yellowred_ht_it2'], card_stats['yellowred_ft_it1'], card_stats['yellowred_ft_it2'],
                stat_stats['corners_ht_it1'], stat_stats['corners_ht_it2'], stat_stats['corners_ft_it1'], stat_stats['corners_ft_it2'],
                stat_stats['goals_ht_it1'], stat_stats['goals_ht_it2'], stat_stats['goals_ft_it1'], stat_stats['goals_ft_it2'],
                stat_stats['fouls_ht_it1'], stat_stats['fouls_ht_it2'], stat_stats['fouls_ft_it1'], stat_stats['fouls_ft_it2']
            ))
            
        except Exception as e:
            print(f"         ❌ Erro ao calcular stats do time {team_name}: {e}")
    
    def calculate_card_stats(self, cur, season_id, league_id, team_id):
        """Calcular estatísticas de cartões por período"""
        try:
            # Cartões amarelos
            cur.execute("""
                SELECT 
                    COUNT(CASE WHEN e.minute <= 45 AND fp.location = 'home' THEN 1 END) as yellow_ht_it1,
                    COUNT(CASE WHEN e.minute <= 45 AND fp.location = 'away' THEN 1 END) as yellow_ht_it2,
                    COUNT(CASE WHEN e.minute > 45 AND fp.location = 'home' THEN 1 END) as yellow_ft_it1,
                    COUNT(CASE WHEN e.minute > 45 AND fp.location = 'away' THEN 1 END) as yellow_ft_it2
                FROM fixtures f
                JOIN fixture_participants fp ON f.id = fp.fixture_id
                JOIN events e ON f.id = e.fixture_id AND e.participant_id = fp.team_id
                WHERE f.season_id = %s AND f.league_id = %s AND fp.team_id = %s 
                  AND e.type_id = 19 AND e.rescinded = false
            """, (season_id, league_id, team_id))
            
            yellow_result = cur.fetchone()
            
            # Cartões vermelhos
            cur.execute("""
                SELECT 
                    COUNT(CASE WHEN e.minute <= 45 AND fp.location = 'home' THEN 1 END) as red_ht_it1,
                    COUNT(CASE WHEN e.minute <= 45 AND fp.location = 'away' THEN 1 END) as red_ht_it2,
                    COUNT(CASE WHEN e.minute > 45 AND fp.location = 'home' THEN 1 END) as red_ft_it1,
                    COUNT(CASE WHEN e.minute > 45 AND fp.location = 'away' THEN 1 END) as red_ft_it2
                FROM fixtures f
                JOIN fixture_participants fp ON f.id = fp.fixture_id
                JOIN events e ON f.id = e.fixture_id AND e.participant_id = fp.team_id
                WHERE f.season_id = %s AND f.league_id = %s AND fp.team_id = %s 
                  AND e.type_id = 20 AND e.rescinded = false
            """, (season_id, league_id, team_id))
            
            red_result = cur.fetchone()
            
            # Cartões amarelo-vermelho
            cur.execute("""
                SELECT 
                    COUNT(CASE WHEN e.minute <= 45 AND fp.location = 'home' THEN 1 END) as yellowred_ht_it1,
                    COUNT(CASE WHEN e.minute <= 45 AND fp.location = 'away' THEN 1 END) as yellowred_ht_it2,
                    COUNT(CASE WHEN e.minute > 45 AND fp.location = 'home' THEN 1 END) as yellowred_ft_it1,
                    COUNT(CASE WHEN e.minute > 45 AND fp.location = 'away' THEN 1 END) as yellowred_ft_it2
                FROM fixtures f
                JOIN fixture_participants fp ON f.id = fp.fixture_id
                JOIN events e ON f.id = e.fixture_id AND e.participant_id = fp.team_id
                WHERE f.season_id = %s AND f.league_id = %s AND fp.team_id = %s 
                  AND e.type_id = 21 AND e.rescinded = false
            """, (season_id, league_id, team_id))
            
            yellowred_result = cur.fetchone()
            
            return {
                'yellow_ht_it1': yellow_result[0] or 0,
                'yellow_ht_it2': yellow_result[1] or 0,
                'yellow_ft_it1': yellow_result[2] or 0,
                'yellow_ft_it2': yellow_result[3] or 0,
                'red_ht_it1': red_result[0] or 0,
                'red_ht_it2': red_result[1] or 0,
                'red_ft_it1': red_result[2] or 0,
                'red_ft_it2': red_result[3] or 0,
                'yellowred_ht_it1': yellowred_result[0] or 0,
                'yellowred_ht_it2': yellowred_result[1] or 0,
                'yellowred_ft_it1': yellowred_result[2] or 0,
                'yellowred_ft_it2': yellowred_result[3] or 0
            }
            
        except Exception as e:
            print(f"         ⚠️ Erro ao calcular cartões: {e}")
            return {
                'yellow_ht_it1': 0, 'yellow_ht_it2': 0, 'yellow_ft_it1': 0, 'yellow_ft_it2': 0,
                'red_ht_it1': 0, 'red_ht_it2': 0, 'red_ft_it1': 0, 'red_ft_it2': 0,
                'yellowred_ht_it1': 0, 'yellowred_ht_it2': 0, 'yellowred_ft_it1': 0, 'yellowred_ft_it2': 0
            }
    
    def calculate_stat_stats(self, cur, season_id, league_id, team_id):
        """Calcular estatísticas por período"""
        try:
            # Corners, gols e faltas por período
            cur.execute("""
                SELECT 
                    COALESCE(SUM(CASE WHEN fs.type_id = 34 AND fp.location = 'home' THEN fs.value ELSE 0 END), 0) as corners_ht_it1,
                    COALESCE(SUM(CASE WHEN fs.type_id = 34 AND fp.location = 'away' THEN fs.value ELSE 0 END), 0) as corners_ht_it2,
                    COALESCE(SUM(CASE WHEN fs.type_id = 34 AND fp.location = 'home' THEN fs.value ELSE 0 END), 0) as corners_ft_it1,
                    COALESCE(SUM(CASE WHEN fs.type_id = 34 AND fp.location = 'away' THEN fs.value ELSE 0 END), 0) as corners_ft_it2,
                    COALESCE(SUM(CASE WHEN fs.type_id = 52 AND fp.location = 'home' THEN fs.value ELSE 0 END), 0) as goals_ht_it1,
                    COALESCE(SUM(CASE WHEN fs.type_id = 52 AND fp.location = 'away' THEN fs.value ELSE 0 END), 0) as goals_ht_it2,
                    COALESCE(SUM(CASE WHEN fs.type_id = 52 AND fp.location = 'home' THEN fs.value ELSE 0 END), 0) as goals_ft_it1,
                    COALESCE(SUM(CASE WHEN fs.type_id = 52 AND fp.location = 'away' THEN fs.value ELSE 0 END), 0) as goals_ft_it2,
                    COALESCE(SUM(CASE WHEN fs.type_id = 56 AND fp.location = 'home' THEN fs.value ELSE 0 END), 0) as fouls_ht_it1,
                    COALESCE(SUM(CASE WHEN fs.type_id = 56 AND fp.location = 'away' THEN fs.value ELSE 0 END), 0) as fouls_ht_it2,
                    COALESCE(SUM(CASE WHEN fs.type_id = 56 AND fp.location = 'home' THEN fs.value ELSE 0 END), 0) as fouls_ft_it1,
                    COALESCE(SUM(CASE WHEN fs.type_id = 56 AND fp.location = 'away' THEN fs.value ELSE 0 END), 0) as fouls_ft_it2
                FROM fixtures f
                JOIN fixture_participants fp ON f.id = fp.fixture_id
                LEFT JOIN fixture_statistics fs ON f.id = fs.fixture_id AND fs.participant_id = fp.team_id
                WHERE f.season_id = %s AND f.league_id = %s AND fp.team_id = %s
                  AND fs.type_id IN (34, 52, 56)
            """, (season_id, league_id, team_id))
            
            result = cur.fetchone()
            
            return {
                'corners_ht_it1': result[0] or 0,
                'corners_ht_it2': result[1] or 0,
                'corners_ft_it1': result[2] or 0,
                'corners_ft_it2': result[3] or 0,
                'goals_ht_it1': result[4] or 0,
                'goals_ht_it2': result[5] or 0,
                'goals_ft_it1': result[6] or 0,
                'goals_ft_it2': result[7] or 0,
                'fouls_ht_it1': result[8] or 0,
                'fouls_ht_it2': result[9] or 0,
                'fouls_ft_it1': result[10] or 0,
                'fouls_ft_it2': result[11] or 0
            }
            
        except Exception as e:
            print(f"         ⚠️ Erro ao calcular estatísticas: {e}")
            return {
                'corners_ht_it1': 0, 'corners_ht_it2': 0, 'corners_ft_it1': 0, 'corners_ft_it2': 0,
                'goals_ht_it1': 0, 'goals_ht_it2': 0, 'goals_ft_it1': 0, 'goals_ft_it2': 0,
                'fouls_ht_it1': 0, 'fouls_ht_it2': 0, 'fouls_ft_it1': 0, 'fouls_ft_it2': 0
            }
    
    def populate_referee_analysis(self):
        """Popular análise de árbitros por temporada"""
        print("\n👨‍⚖️ POPULANDO ANÁLISE DE ÁRBITROS")
        print("-" * 40)
        
        try:
            conn = psycopg2.connect(self.db_dsn)
            cur = conn.cursor()
            
            # Limpar dados existentes
            cur.execute("DELETE FROM referee_season_analysis")
            
            # Buscar todas as temporadas
            cur.execute("SELECT DISTINCT season_id, league_id FROM fixtures ORDER BY season_id, league_id")
            seasons = cur.fetchall()
            
            for season_id, league_id in seasons:
                print(f"   📅 Processando árbitros da temporada {season_id}...")
                
                # Calcular estatísticas dos árbitros
                cur.execute("""
                    SELECT 
                        f.season_id,
                        f.league_id,
                        1 as referee_id,
                        COUNT(DISTINCT f.id) as games_officiated,
                        COUNT(CASE WHEN e.minute <= 45 AND e.type_id IN (19, 20, 21) AND e.rescinded = false THEN 1 END) as total_cards_ht,
                        COUNT(CASE WHEN e.minute > 45 AND e.type_id IN (19, 20, 21) AND e.rescinded = false THEN 1 END) as total_cards_ft,
                        COUNT(CASE WHEN e.minute <= 45 AND e.type_id = 19 AND e.rescinded = false THEN 1 END) as yellow_cards_ht,
                        COUNT(CASE WHEN e.minute > 45 AND e.type_id = 19 AND e.rescinded = false THEN 1 END) as yellow_cards_ft,
                        COUNT(CASE WHEN e.minute <= 45 AND e.type_id = 20 AND e.rescinded = false THEN 1 END) as red_cards_ht,
                        COUNT(CASE WHEN e.minute > 45 AND e.type_id = 20 AND e.rescinded = false THEN 1 END) as red_cards_ft,
                        COUNT(CASE WHEN e.minute <= 45 AND e.type_id = 21 AND e.rescinded = false THEN 1 END) as yellowred_cards_ht,
                        COUNT(CASE WHEN e.minute > 45 AND e.type_id = 21 AND e.rescinded = false THEN 1 END) as yellowred_cards_ft
                    FROM fixtures f
                    LEFT JOIN events e ON f.id = e.fixture_id
                    WHERE f.season_id = %s AND f.league_id = %s
                    GROUP BY f.season_id, f.league_id
                """, (season_id, league_id))
                
                result = cur.fetchone()
                if result:
                    games_officiated = result[3]
                    if games_officiated > 0:
                        avg_cards_per_game = (result[4] + result[5]) / games_officiated
                        
                        cur.execute("""
                            INSERT INTO referee_season_analysis (
                                season_id, league_id, referee_id, games_officiated,
                                total_cards_ht, total_cards_ft, yellow_cards_ht, yellow_cards_ft,
                                red_cards_ht, red_cards_ft, yellowred_cards_ht, yellowred_cards_ft,
                                avg_cards_per_game
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        """, (
                            result[0], result[1], result[2], result[3], result[4], result[5],
                            result[6], result[7], result[8], result[9], result[10], result[11],
                            round(avg_cards_per_game, 2)
                        ))
            
            conn.commit()
            print("   ✅ Análise de árbitros populada com sucesso")
            
            conn.close()
            
        except Exception as e:
            print(f"   ❌ Erro ao popular análise de árbitros: {e}")
    
    def show_results(self):
        """Mostrar resultados da análise"""
        print("\n📊 RESULTADOS DA ANÁLISE COMPLETA")
        print("=" * 60)
        
        try:
            conn = psycopg2.connect(self.db_dsn)
            cur = conn.cursor()
            
            # 1. Ranking por temporada
            cur.execute("""
                SELECT season_id, team_name, games_played, wins, draws, losses, points, 
                       goals_for, goals_against, goal_difference
                FROM season_analysis 
                ORDER BY season_id, points DESC, goal_difference DESC
                LIMIT 20
            """)
            
            results = cur.fetchall()
            print("🏆 TOP 20 TIMES POR TEMPORADA:")
            print("-" * 40)
            
            current_season = None
            for row in results:
                if row[0] != current_season:
                    current_season = row[0]
                    print(f"\n📅 TEMPORADA {current_season}:")
                
                print(f"   {row[1]:<25} {row[2]:>2}J {row[3]:>2}V {row[4]:>2}E {row[5]:>2}D {row[6]:>3}Pts {row[7]:>2}GF {row[8]:>2}GS {row[9]:>+3}SG")
            
            # 2. Estatísticas de cartões
            print(f"\n🟨 ESTATÍSTICAS DE CARTÕES:")
            print("-" * 40)
            
            cur.execute("""
                SELECT 
                    SUM(yellow_cards_ht_it1 + yellow_cards_ht_it2 + yellow_cards_ft_it1 + yellow_cards_ft_it2) as total_yellow,
                    SUM(red_cards_ht_it1 + red_cards_ht_it2 + red_cards_ft_it1 + red_cards_ft_it2) as total_red,
                    SUM(yellowred_cards_ht_it1 + yellowred_cards_ht_it2 + yellowred_cards_ft_it1 + yellowred_cards_ft_it2) as total_yellowred
                FROM season_analysis
            """)
            
            card_stats = cur.fetchone()
            print(f"   🟨 Cartões amarelos: {card_stats[0] or 0}")
            print(f"   🔴 Cartões vermelhos: {card_stats[1] or 0}")
            print(f"   🟨🔴 Cartões amarelo-vermelho: {card_stats[2] or 0}")
            
            # 3. Estatísticas gerais
            print(f"\n📈 ESTATÍSTICAS GERAIS:")
            print("-" * 40)
            
            cur.execute("SELECT COUNT(*) FROM season_analysis")
            total_teams = cur.fetchone()[0]
            
            cur.execute("SELECT SUM(games_played) FROM season_analysis")
            total_games = cur.fetchone()[0]
            
            print(f"   🏆 Total de times analisados: {total_teams}")
            print(f"   ⚽ Total de jogos analisados: {total_games}")
            
            # 4. Detalhes dos cartões
            print(f"\n🟨 DETALHES DOS CARTÕES:")
            print("-" * 40)
            
            cur.execute("SELECT COUNT(*) FROM card_details")
            total_cards = cur.fetchone()[0]
            print(f"   📊 Total de cartões detalhados: {total_cards}")
            
            # Mostrar alguns exemplos de cartões
            cur.execute("""
                SELECT team_name, player_name, card_type, minute, minute_extra, period, location, fixture_name
                FROM card_details 
                ORDER BY minute, minute_extra
                LIMIT 10
            """)
            
            example_cards = cur.fetchall()
            print(f"   📋 Exemplos de cartões:")
            for card in example_cards:
                team, player, card_type, minute, extra, period, location, fixture = card
                extra_str = f"+{extra}" if extra else ""
                print(f"      • {minute}{extra_str}' - {player} ({team}) - {card_type} - {period} - {location}")
                print(f"        Jogo: {fixture}")
            
            conn.close()
            
        except Exception as e:
            print(f"❌ Erro ao mostrar resultados: {e}")
    
    def run_complete_analysis(self):
        """Executar análise completa"""
        print("🚀 INICIANDO ANÁLISE COMPLETA")
        print("=" * 60)
        
        try:
            # 1. Criar tabelas
            self.create_analysis_tables()
            
            # 2. Popular análise por temporada
            self.populate_season_analysis()
            
            # 3. Popular detalhes dos cartões
            self.populate_card_details()
            
            # 4. Popular análise de árbitros
            self.populate_referee_analysis()
            
            # 5. Mostrar resultados
            self.show_results()
            
            print(f"\n🎉 ANÁLISE COMPLETA CONCLUÍDA!")
            
        except Exception as e:
            print(f"❌ Erro na análise completa: {e}")

def main():
    """Função principal"""
    analyzer = CompleteAnalysis()
    analyzer.run_complete_analysis()

if __name__ == "__main__":
    main()
