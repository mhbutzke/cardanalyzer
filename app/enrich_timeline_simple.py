#!/usr/bin/env python3
"""
CardAnalyzer - Enriquecimento de Timeline (Versão Simplificada)
Script para enriquecer eventos com contexto: placar, jogadores em campo, períodos
"""

import os
import sys
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("DB_DSN", "postgresql://card:card@localhost:5432/carddb")

def get_minute_bucket(minute: int) -> str:
    """Converte minuto em bucket de período"""
    if minute <= 15:
        return "0-15"
    elif minute <= 30:
        return "16-30"
    elif minute <= 45:
        return "31-45"
    elif minute <= 60:
        return "46-60"
    elif minute <= 75:
        return "61-75"
    elif minute <= 90:
        return "76-90"
    else:
        return "90+"

def enrich_events_timeline():
    """Enriquece a timeline dos eventos com contexto"""
    print("🔄 Iniciando enriquecimento da timeline...")
    
    with psycopg2.connect(DSN) as conn:
        conn.autocommit = False
        
        try:
            # 1. Criar tabela para eventos enriquecidos
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS events_enriched (
                        id BIGINT PRIMARY KEY,
                        fixture_id BIGINT NOT NULL,
                        participant_id BIGINT,
                        player_id BIGINT,
                        related_player_id BIGINT,
                        type_id INT NOT NULL,
                        minute INT,
                        minute_extra INT,
                        period_id INT,
                        sort_order INT,
                        rescinded BOOLEAN,
                        attrs JSONB,
                        json_data JSONB,
                        -- Campos enriquecidos
                        score_home_at INT,
                        score_away_at INT,
                        manpower_home_after INT,
                        manpower_away_after INT,
                        minute_bucket TEXT,
                        context_summary TEXT
                    )
                """)
                
                # 2. Criar índices para performance
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS ix_events_enriched_fixture 
                    ON events_enriched(fixture_id)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS ix_events_enriched_minute 
                    ON events_enriched(minute)
                """)
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS ix_events_enriched_type 
                    ON events_enriched(type_id)
                """)
                
                print("✅ Tabela e índices criados")
            
            # 3. Processar cada fixture
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT f.id, f.name, f.season_id
                    FROM fixtures f
                    JOIN events e ON e.fixture_id = f.id
                    WHERE f.state_id IN (1, 2, 3, 4, 5, 10)
                    LIMIT 100
                """)
                
                fixtures = cur.fetchall()
                print(f"📊 Processando {len(fixtures)} fixtures (limitado para teste)...")
                
                for i, (fixture_id, fixture_name, season_id) in enumerate(fixtures, 1):
                    if i % 10 == 0:
                        print(f"   Processados: {i}/{len(fixtures)}")
                    
                    # Buscar todos os eventos da partida ordenados por tempo
                    cur.execute("""
                        SELECT e.id, e.fixture_id, e.participant_id, e.player_id, e.related_player_id,
                               e.type_id, e.minute, e.minute_extra, e.period_id, e.sort_order,
                               e.rescinded, e.attrs, e.json_data, fp.location, t.name as team_name
                        FROM events e
                        JOIN fixture_participants fp ON fp.id = e.participant_id
                        JOIN teams t ON t.id = fp.team_id
                        WHERE e.fixture_id = %s
                        ORDER BY e.minute, e.minute_extra, e.sort_order
                    """, (fixture_id,))
                    
                    events = cur.fetchall()
                    
                    if not events:
                        continue
                    
                    # Enriquecer cada evento
                    enriched_events = []
                    for event in events:
                        event_id = event[0]
                        minute = event[6] or 0
                        type_id = event[5]
                        location = event[13] or ''
                        team_name = event[14] or ''
                        
                        # Calcular placar no momento (simplificado)
                        score_home = 0
                        score_away = 0
                        manpower_home = 11
                        manpower_away = 11
                        
                        # Contar gols até este minuto
                        for other_event in events:
                            other_minute = other_event[6] or 0
                            other_type = other_event[5]
                            
                            if other_minute > minute:
                                continue
                                
                            if other_type in [14, 15, 16]:  # Gols
                                other_location = other_event[13] or ''
                                if other_location == 'home':
                                    score_home += 1
                                elif other_location == 'away':
                                    score_away += 1
                                # Own goal conta para o adversário
                                elif other_type == 15:
                                    if other_location == 'home':
                                        score_away += 1
                                    else:
                                        score_home += 1
                            
                            # Contar cartões vermelhos para manpower
                            if other_type in [20, 21]:  # Vermelhos
                                other_location = other_event[13] or ''
                                if other_location == 'home':
                                    manpower_home -= 1
                                elif other_location == 'away':
                                    manpower_away -= 1
                        
                        # Determinar bucket de minuto
                        minute_bucket = get_minute_bucket(minute)
                        
                        # Criar resumo de contexto
                        context_parts = []
                        if minute <= 45:
                            context_parts.append("1º tempo")
                        else:
                            context_parts.append("2º tempo")
                        
                        context_parts.append(f"Placar: {score_home}x{score_away}")
                        context_parts.append(f"Jogadores: {manpower_home}x{manpower_away}")
                        
                        if type_id in [19, 20, 21]:  # Cartões
                            card_type = "Amarelo" if type_id == 19 else "Vermelho Direto" if type_id == 20 else "Segundo Amarelo"
                            context_parts.append(f"{card_type} para {team_name}")
                        elif type_id in [14, 15, 16]:  # Gols
                            goal_type = "Gol" if type_id == 14 else "Gol Contra" if type_id == 15 else "Pênalti"
                            context_parts.append(f"{goal_type} de {team_name}")
                        
                        context_summary = " | ".join(context_parts)
                        
                        # Criar evento enriquecido
                        enriched_event = (
                            event_id,
                            fixture_id,
                            event[2],  # participant_id
                            event[3],  # player_id
                            event[4],  # related_player_id
                            type_id,
                            minute,
                            event[7],  # minute_extra
                            event[8],  # period_id
                            event[9],  # sort_order
                            event[10], # rescinded
                            None,  # attrs (simplificado)
                            None,  # json_data (simplificado)
                            score_home,
                            score_away,
                            manpower_home,
                            manpower_away,
                            minute_bucket,
                            context_summary
                        )
                        
                        enriched_events.append(enriched_event)
                    
                    # Inserir eventos enriquecidos
                    if enriched_events:
                        # Limpar eventos antigos desta partida
                        cur.execute("DELETE FROM events_enriched WHERE fixture_id = %s", (fixture_id,))
                        
                        # Inserir novos eventos enriquecidos
                        execute_values(cur, """
                            INSERT INTO events_enriched (
                                id, fixture_id, participant_id, player_id, related_player_id,
                                type_id, minute, minute_extra, period_id, sort_order,
                                rescinded, attrs, json_data, score_home_at, score_away_at,
                                manpower_home_after, manpower_away_after, minute_bucket, context_summary
                            ) VALUES %s
                        """, enriched_events)
                        
                        print(f"   ✅ {fixture_name}: {len(enriched_events)} eventos enriquecidos")
            
            conn.commit()
            print("🎉 Timeline enriquecida com sucesso!")
            
            # 4. Criar views para análise
            create_enriched_views(conn)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Erro: {e}")
            raise

def create_enriched_views(conn):
    """Cria views para análise dos eventos enriquecidos"""
    print("🔄 Criando views para análise...")
    
    with conn.cursor() as cur:
        # View 1: Timeline enriquecida completa
        cur.execute("""
            CREATE OR REPLACE VIEW v_timeline_enriquecida AS
            SELECT 
                ee.*,
                t.name as team_name,
                p.name as player_name,
                et.name as event_type_name,
                f.name as fixture_name,
                f.season_id,
                f.starting_at
            FROM events_enriched ee
            JOIN teams t ON t.id = ee.participant_id
            LEFT JOIN players p ON p.id = ee.player_id
            LEFT JOIN event_types et ON et.id = ee.type_id
            JOIN fixtures f ON f.id = ee.fixture_id
            ORDER BY ee.fixture_id, ee.minute, ee.minute_extra, ee.sort_order
        """)
        
        # View 2: Cartões com contexto
        cur.execute("""
            CREATE OR REPLACE VIEW v_cartoes_com_contexto AS
            SELECT 
                ee.fixture_id,
                f.name as fixture_name,
                f.season_id,
                t.name as team_name,
                p.name as player_name,
                ee.minute,
                ee.minute_bucket,
                ee.score_home_at,
                ee.score_away_at,
                ee.manpower_home_after,
                ee.manpower_away_after,
                CASE 
                    WHEN ee.type_id = 19 THEN 'Amarelo'
                    WHEN ee.type_id = 20 THEN 'Vermelho Direto'
                    WHEN ee.type_id = 21 THEN 'Segundo Amarelo'
                END as tipo_cartao,
                ee.context_summary
            FROM events_enriched ee
            JOIN fixtures f ON f.id = ee.fixture_id
            JOIN teams t ON t.id = ee.participant_id
            LEFT JOIN players p ON p.id = ee.player_id
            WHERE ee.type_id IN (19, 20, 21)
            AND COALESCE(ee.rescinded, false) = false
            ORDER BY ee.fixture_id, ee.minute
        """)
        
        # View 3: Gols com contexto
        cur.execute("""
            CREATE OR REPLACE VIEW v_gols_com_contexto AS
            SELECT 
                ee.fixture_id,
                f.name as fixture_name,
                f.season_id,
                t.name as team_name,
                p.name as player_name,
                ee.minute,
                ee.minute_bucket,
                ee.score_home_at,
                ee.score_away_at,
                ee.manpower_home_after,
                ee.manpower_away_after,
                CASE 
                    WHEN ee.type_id = 14 THEN 'Gol'
                    WHEN ee.type_id = 15 THEN 'Gol Contra'
                    WHEN ee.type_id = 16 THEN 'Pênalti'
                END as tipo_gol,
                ee.context_summary
            FROM events_enriched ee
            JOIN fixtures f ON f.id = ee.fixture_id
            JOIN teams t ON t.id = ee.participant_id
            LEFT JOIN players p ON p.id = ee.player_id
            WHERE ee.type_id IN (14, 15, 16)
            ORDER BY ee.fixture_id, ee.minute
        """)
        
        print("✅ Views criadas com sucesso!")

def main():
    """Função principal"""
    print("🚀 CardAnalyzer - Enriquecimento de Timeline (Versão Simplificada)")
    print("=" * 60)
    
    try:
        enrich_events_timeline()
        print("\n🎉 Timeline enriquecida com sucesso!")
        print("\n📊 Views criadas:")
        print("  - v_timeline_enriquecida")
        print("  - v_cartoes_com_contexto")
        print("  - v_gols_com_contexto")
        
    except Exception as e:
        print(f"\n❌ Erro: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
