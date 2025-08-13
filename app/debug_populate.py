#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script de debug para identificar problemas na população das tabelas
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def debug_data():
    """Debugar dados existentes"""
    try:
        conn = psycopg2.connect(os.getenv("DB_DSN"))
        cur = conn.cursor()
        
        print("🔍 DEBUG - VERIFICANDO DADOS EXISTENTES")
        print("=" * 60)
        
        # 1. Verificar fixtures
        cur.execute("SELECT COUNT(*) FROM fixtures")
        fixtures_count = cur.fetchone()[0]
        print(f"📊 Fixtures: {fixtures_count}")
        
        # 2. Verificar fixtures finalizados
        cur.execute("SELECT COUNT(*) FROM fixtures WHERE state_id = 5")
        finished_count = cur.fetchone()[0]
        print(f"🏁 Fixtures finalizados: {finished_count}")
        
        # 3. Verificar events de cartões
        cur.execute("SELECT COUNT(*) FROM events WHERE type_id IN (19, 20, 21)")
        cards_count = cur.fetchone()[0]
        print(f"🟡 Events de cartões: {cards_count}")
        
        # 4. Verificar events de cartões não rescindidos
        cur.execute("SELECT COUNT(*) FROM events WHERE type_id IN (19, 20, 21) AND rescinded = false")
        valid_cards_count = cur.fetchone()[0]
        print(f"✅ Cartões válidos: {valid_cards_count}")
        
        # 5. Verificar fixture_statistics
        cur.execute("SELECT COUNT(*) FROM fixture_statistics WHERE type_id IN (34, 52, 56)")
        stats_count = cur.fetchone()[0]
        print(f"📊 Estatísticas relevantes: {stats_count}")
        
        # 6. Verificar fixture_referees
        cur.execute("SELECT COUNT(*) FROM fixture_referees")
        refs_count = cur.fetchone()[0]
        print(f"👨‍⚖️ Árbitros: {refs_count}")
        
        # 7. Verificar fixture_participants
        cur.execute("SELECT COUNT(*) FROM fixture_participants")
        participants_count = cur.fetchone()[0]
        print(f"👥 Participantes: {participants_count}")
        
        # 8. Exemplo de fixture com cartões
        cur.execute("""
            SELECT f.id, f.name, COUNT(e.id) as cards
            FROM fixtures f
            JOIN events e ON f.id = e.fixture_id
            WHERE e.type_id IN (19, 20, 21) AND e.rescinded = false
            GROUP BY f.id, f.name
            LIMIT 3
        """)
        
        examples = cur.fetchall()
        print(f"\n🔍 EXEMPLOS DE FIXTURES COM CARTÕES:")
        for example in examples:
            print(f"   • {example[1]} (ID: {example[0]}) - {example[2]} cartões")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    debug_data()
