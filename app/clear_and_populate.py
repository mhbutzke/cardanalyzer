#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para limpar e popular tabelas de análise
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def clear_and_populate():
    """Limpar e popular tabelas de análise"""
    try:
        conn = psycopg2.connect(os.getenv("DB_DSN"))
        cur = conn.cursor()
        
        print("🧹 LIMPANDO E POPULANDO TABELAS DE ANÁLISE")
        print("=" * 60)
        
        # 1. Limpar todas as tabelas
        print("🧹 Limpando tabelas...")
        cur.execute("DELETE FROM card_analysis")
        cur.execute("DELETE FROM statistic_analysis")
        cur.execute("DELETE FROM referee_analysis")
        print("   ✅ Tabelas limpas")
        
        # 2. Popular card_analysis com dados agrupados
        print("🟡 Populando análise de cartões...")
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
        print(f"   ✅ {cur.rowcount} registros de cartões inseridos")
        
        # 3. Popular statistic_analysis
        print("📊 Populando análise de estatísticas...")
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
        print(f"   ✅ {cur.rowcount} registros de estatísticas inseridos")
        
        # 4. Popular referee_analysis
        print("👨‍⚖️ Populando análise de árbitros...")
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
        print(f"   ✅ {cur.rowcount} registros de árbitros inseridos")
        
        # Commit
        conn.commit()
        print(f"\n🎉 POPULAÇÃO CONCLUÍDA!")
        
        # Verificar resultados
        print(f"\n📊 RESULTADOS:")
        print("=" * 60)
        
        cur.execute("SELECT COUNT(*) FROM card_analysis")
        print(f"   • Cartões: {cur.fetchone()[0]} registros")
        
        cur.execute("SELECT COUNT(*) FROM statistic_analysis")
        print(f"   • Estatísticas: {cur.fetchone()[0]} registros")
        
        cur.execute("SELECT COUNT(*) FROM referee_analysis")
        print(f"   • Árbitros: {cur.fetchone()[0]} registros")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    clear_and_populate()
