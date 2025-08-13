#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para popular tabelas de análise com dados existentes
- Processa fixtures já carregados
- Calcula cartões por tempo (HT/FT)
- Calcula estatísticas por tempo
- Análise de árbitros
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def populate_card_analysis(conn):
    """Popular tabela card_analysis com cartões por tempo"""
    print("🟡 Populando análise de cartões...")
    
    try:
        with conn.cursor() as cur:
            # Limpar dados existentes
            cur.execute("DELETE FROM card_analysis")
            
            # Inserir análise de cartões por tempo
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
                    'IT1' as action_type, -- Cartões tomados pelo time
                    COUNT(*) as count
                FROM fixtures f
                JOIN events e ON f.id = e.fixture_id
                WHERE e.type_id IN (19, 20, 21)  -- Cartões
                  AND e.rescinded = false
                  AND f.state_id = 5  -- Jogos finalizados
                  AND e.participant_id IS NOT NULL
                GROUP BY f.id, e.participant_id, period, card_type
                
                UNION ALL
                
                SELECT 
                    f.id as fixture_id,
                    fp.team_id,
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
                    'IT2' as action_type, -- Cartões provocados pelo time
                    COUNT(*) as count
                FROM fixtures f
                JOIN fixture_participants fp ON f.id = fp.fixture_id
                JOIN events e ON f.id = e.fixture_id
                WHERE e.type_id IN (19, 20, 21)  -- Cartões
                  AND e.rescinded = false
                  AND f.state_id = 5  -- Jogos finalizados
                  AND e.participant_id != fp.team_id  -- Cartões do adversário
                GROUP BY f.id, fp.team_id, period, card_type
            """)
            
            print(f"   ✅ {cur.rowcount} registros de cartões inseridos")
            
    except Exception as e:
        print(f"   ❌ Erro ao popular cartões: {e}")
        conn.rollback()
        return False
    
    return True

def populate_statistic_analysis(conn):
    """Popular tabela statistic_analysis com estatísticas por tempo"""
    print("📊 Populando análise de estatísticas...")
    
    try:
        with conn.cursor() as cur:
            # Limpar dados existentes
            cur.execute("DELETE FROM statistic_analysis")
            
            # Inserir análise de estatísticas por tempo
            cur.execute("""
                INSERT INTO statistic_analysis (fixture_id, team_id, period, stat_type, action_type, count)
                SELECT 
                    f.id as fixture_id,
                    fs.participant_id as team_id,
                    'HT' as period,  -- 1º tempo (0-45 min)
                    CASE 
                        WHEN fs.type_id = 34 THEN 'CORNERS'
                        WHEN fs.type_id = 52 THEN 'GOALS'
                        WHEN fs.type_id = 56 THEN 'FOULS'
                        ELSE 'OTHER'
                    END as stat_type,
                    'IT1' as action_type, -- Estatísticas próprias
                    fs.value as count
                FROM fixtures f
                JOIN fixture_statistics fs ON f.id = fs.fixture_id
                WHERE fs.type_id IN (34, 52, 56)  -- Corners, Goals, Fouls
                  AND f.state_id = 5  -- Jogos finalizados
                  AND fs.participant_id IS NOT NULL
                  AND fs.value IS NOT NULL
                
                UNION ALL
                
                SELECT 
                    f.id as fixture_id,
                    fp.team_id,
                    'FT' as period,  -- 2º tempo (46-90 min)
                    CASE 
                        WHEN fs.type_id = 34 THEN 'CORNERS'
                        WHEN fs.type_id = 52 THEN 'GOALS'
                        WHEN fs.type_id = 56 THEN 'FOULS'
                        ELSE 'OTHER'
                    END as stat_type,
                    'IT1' as action_type, -- Estatísticas próprias
                    fs.value as count
                FROM fixtures f
                JOIN fixture_participants fp ON f.id = fp.fixture_id
                JOIN fixture_statistics fs ON f.id = fs.fixture_id
                WHERE fs.type_id IN (34, 52, 56)  -- Corners, Goals, Fouls
                  AND f.state_id = 5  -- Jogos finalizados
                  AND fs.participant_id = fp.team_id  -- Estatísticas do próprio time
                  AND fs.value IS NOT NULL
            """)
            
            print(f"   ✅ {cur.rowcount} registros de estatísticas inseridos")
            
    except Exception as e:
        print(f"   ❌ Erro ao popular estatísticas: {e}")
        conn.rollback()
        return False
    
    return True

def populate_referee_analysis(conn):
    """Popular tabela referee_analysis com análise de árbitros"""
    print("👨‍⚖️ Populando análise de árbitros...")
    
    try:
        with conn.cursor() as cur:
            # Limpar dados existentes
            cur.execute("DELETE FROM referee_analysis")
            
            # Inserir análise de árbitros por tempo
            cur.execute("""
                INSERT INTO referee_analysis (fixture_id, referee_id, period, total_cards, yellow_cards, red_cards, yellowred_cards)
                SELECT 
                    f.id as fixture_id,
                    fr.referee_id,
                    'HT' as period,  -- 1º tempo
                    COUNT(CASE WHEN e.type_id IN (19, 20, 21) AND e.minute <= 45 THEN 1 END) as total_cards,
                    COUNT(CASE WHEN e.type_id = 19 AND e.minute <= 45 THEN 1 END) as yellow_cards,
                    COUNT(CASE WHEN e.type_id = 20 AND e.minute <= 45 THEN 1 END) as red_cards,
                    COUNT(CASE WHEN e.type_id = 21 AND e.minute <= 45 THEN 1 END) as yellowred_cards
                FROM fixtures f
                JOIN fixture_referees fr ON f.id = fr.fixture_id
                LEFT JOIN events e ON f.id = e.fixture_id AND e.type_id IN (19, 20, 21) AND e.rescinded = false
                WHERE f.state_id = 5  -- Jogos finalizados
                GROUP BY f.id, fr.referee_id
                
                UNION ALL
                
                SELECT 
                    f.id as fixture_id,
                    fr.referee_id,
                    'FT' as period,  -- 2º tempo
                    COUNT(CASE WHEN e.type_id IN (19, 20, 21) AND e.minute > 45 THEN 1 END) as total_cards,
                    COUNT(CASE WHEN e.type_id = 19 AND e.minute > 45 THEN 1 END) as yellow_cards,
                    COUNT(CASE WHEN e.type_id = 20 AND e.minute > 45 THEN 1 END) as red_cards,
                    COUNT(CASE WHEN e.type_id = 21 AND e.minute > 45 THEN 1 END) as yellowred_cards
                FROM fixtures f
                JOIN fixture_referees fr ON f.id = fr.fixture_id
                LEFT JOIN events e ON f.id = e.fixture_id AND e.type_id IN (19, 20, 21) AND e.rescinded = false
                WHERE f.state_id = 5  -- Jogos finalizados
                GROUP BY f.id, fr.referee_id
            """)
            
            print(f"   ✅ {cur.rowcount} registros de árbitros inseridos")
            
    except Exception as e:
        print(f"   ❌ Erro ao popular árbitros: {e}")
        conn.rollback()
        return False
    
    return True

def main():
    """Função principal"""
    print("🚀 POPULANDO TABELAS DE ANÁLISE")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(os.getenv("DB_DSN"))
        
        # 1. Popular análise de cartões
        if populate_card_analysis(conn):
            print("   🟡 Análise de cartões: OK")
        else:
            print("   ❌ Análise de cartões: FALHOU")
            return
        
        # 2. Popular análise de estatísticas
        if populate_statistic_analysis(conn):
            print("   📊 Análise de estatísticas: OK")
        else:
            print("   ❌ Análise de estatísticas: FALHOU")
            return
        
        # 3. Popular análise de árbitros
        if populate_referee_analysis(conn):
            print("   👨‍⚖️ Análise de árbitros: OK")
        else:
            print("   ❌ Análise de árbitros: FALHOU")
            return
        
        # Commit final
        conn.commit()
        print(f"\n🎉 POPULAÇÃO CONCLUÍDA COM SUCESSO!")
        
        # Verificar resultados de forma simples
        print(f"\n📊 VERIFICANDO RESULTADOS:")
        print("=" * 60)
        
        with conn.cursor() as cur:
            # Verificar cartões
            cur.execute("SELECT COUNT(*) FROM card_analysis")
            result = cur.fetchone()
            if result:
                print(f"   • Cartões: {result[0]} registros")
            else:
                print(f"   • Cartões: 0 registros")
            
            # Verificar estatísticas
            cur.execute("SELECT COUNT(*) FROM statistic_analysis")
            result = cur.fetchone()
            if result:
                print(f"   • Estatísticas: {result[0]} registros")
            else:
                print(f"   • Estatísticas: 0 registros")
            
            # Verificar árbitros
            cur.execute("SELECT COUNT(*) FROM referee_analysis")
            result = cur.fetchone()
            if result:
                print(f"   • Árbitros: {result[0]} registros")
            else:
                print(f"   • Árbitros: 0 registros")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro geral: {e}")

if __name__ == "__main__":
    main()
