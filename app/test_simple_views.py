#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para testar as views simples criadas
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def test_simple_views():
    """Testar views simples"""
    try:
        conn = psycopg2.connect(os.getenv("DB_DSN"))
        cur = conn.cursor()
        
        print("🧪 TESTANDO VIEWS SIMPLES")
        print("=" * 60)
        
        # 1. Testar v_cartoes_simples
        print("\n🟡 TESTANDO v_cartoes_simples:")
        cur.execute("SELECT * FROM v_cartoes_simples LIMIT 5")
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        
        print(f"   📋 Colunas: {', '.join(col_names)}")
        print(f"   📊 Registros: {len(rows)} encontrados")
        
        if rows:
            print("   🔍 Primeiros registros:")
            for row in rows:
                print(f"      • {row[1]} - Total: {row[2]}, Amarelos: {row[3]}, Vermelhos: {row[4]}, 2º Amarelo: {row[5]}")
        
        # 2. Testar v_estatisticas_simples
        print("\n📊 TESTANDO v_estatisticas_simples:")
        cur.execute("SELECT * FROM v_estatisticas_simples LIMIT 5")
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        
        print(f"   📋 Colunas: {', '.join(col_names)}")
        print(f"   📊 Registros: {len(rows)} encontrados")
        
        if rows:
            print("   🔍 Primeiros registros:")
            for row in rows:
                print(f"      • {row[1]} - Escanteios: {row[2]}, Gols: {row[3]}, Faltas: {row[4]}")
        
        # 3. Testar v_resumo_geral
        print("\n📋 TESTANDO v_resumo_geral:")
        cur.execute("SELECT * FROM v_resumo_geral")
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]
        
        print(f"   📋 Colunas: {', '.join(col_names)}")
        print(f"   📊 Registros: {len(rows)} encontrados")
        
        if rows:
            print("   🔍 Resumo:")
            for row in rows:
                print(f"      • {row[0]}: {row[1]} registros na tabela {row[2]}")
        
        conn.close()
        print(f"\n🎉 Teste das views simples concluído!")
        
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    test_simple_views()
