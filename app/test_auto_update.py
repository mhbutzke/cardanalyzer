#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Versão simplificada do sistema automático para teste
"""

import os
import psycopg2
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()

def test_auto_update():
    """Testar funcionalidades básicas do sistema automático"""
    print("🧪 TESTANDO SISTEMA AUTOMÁTICO")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(os.getenv("DB_DSN"))
        cur = conn.cursor()
        
        # 1. Testar busca de último fixture
        print("📊 Testando busca de último fixture...")
        cur.execute("SELECT MAX(starting_at) FROM fixtures")
        last_update = cur.fetchone()[0]
        
        if last_update:
            print(f"   ✅ Último fixture: {last_update}")
            
            # Converter para datetime se necessário
            if isinstance(last_update, str):
                last_update = datetime.fromisoformat(last_update.replace("Z", "+00:00"))
                print(f"   🔄 Convertido para: {last_update}")
        else:
            print("   ℹ️ Nenhum fixture encontrado")
            last_update = datetime.now() - timedelta(days=7)
            print(f"   📅 Usando data padrão: {last_update}")
        
        # 2. Testar busca de fixtures recentes
        print("\n🔍 Testando busca de fixtures recentes...")
        cur.execute("""
            SELECT id, name, starting_at, state_id 
            FROM fixtures 
            WHERE starting_at > %s 
            ORDER BY starting_at DESC 
            LIMIT 5
        """, (last_update,))
        
        recent_fixtures = cur.fetchall()
        print(f"   📊 Fixtures recentes encontrados: {len(recent_fixtures)}")
        
        for fixture in recent_fixtures:
            print(f"      • {fixture[1]} (ID: {fixture[0]}) - {fixture[2]}")
        
        # 3. Testar contagem de dados
        print("\n📈 Testando contagem de dados...")
        
        cur.execute("SELECT COUNT(*) FROM fixtures")
        total_fixtures = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM events")
        total_events = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM fixture_statistics")
        total_stats = cur.fetchone()[0]
        
        print(f"   📊 Total de fixtures: {total_fixtures}")
        print(f"   📊 Total de eventos: {total_events}")
        print(f"   📊 Total de estatísticas: {total_stats}")
        
        # 4. Testar estrutura das tabelas
        print("\n🏗️ Testando estrutura das tabelas...")
        
        tables_to_check = ['fixtures', 'events', 'fixture_statistics', 'card_analysis']
        
        for table in tables_to_check:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                count = cur.fetchone()[0]
                print(f"   ✅ {table}: {count} registros")
            except Exception as e:
                print(f"   ❌ {table}: Erro - {e}")
        
        conn.close()
        print(f"\n🎉 TESTE CONCLUÍDO COM SUCESSO!")
        
    except Exception as e:
        print(f"❌ Erro no teste: {e}")

if __name__ == "__main__":
    test_auto_update()
