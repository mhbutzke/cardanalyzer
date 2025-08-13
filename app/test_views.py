#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def test_views():
    """Testar todas as views criadas"""
    try:
        conn = psycopg2.connect(os.getenv("DB_DSN"))
        cur = conn.cursor()
        
        print("🧪 TESTANDO TODAS AS VIEWS CRIADAS")
        print("=" * 60)
        
        # Lista de views para testar
        views_to_test = [
            "v_totais_por_temporada",
            "v_cartoes_detalhados", 
            "v_estatisticas_detalhadas",
            "v_arbitros_cartoes"
        ]
        
        for view_name in views_to_test:
            print(f"\n📊 Testando view: {view_name}")
            try:
                # Testar SELECT simples
                cur.execute(f"SELECT * FROM {view_name} LIMIT 3")
                rows = cur.fetchall()
                col_names = [desc[0] for desc in cur.description]
                
                print(f"   ✅ View funcionando")
                print(f"   📋 Colunas: {', '.join(col_names)}")
                print(f"   📊 Registros: {len(rows)} encontrados")
                
                if rows:
                    print(f"   🔍 Primeiro registro:")
                    for i, col_name in enumerate(col_names):
                        value = rows[0][i]
                        if isinstance(value, str) and len(value) > 50:
                            value = value[:50] + "..."
                        print(f"      {col_name}: {value}")
                
            except Exception as e:
                print(f"   ❌ Erro na view {view_name}: {e}")
        
        # Verificar tabelas de análise
        print(f"\n📋 VERIFICANDO TABELAS DE ANÁLISE:")
        print("=" * 60)
        
        analysis_tables = [
            "card_analysis",
            "statistic_analysis", 
            "referee_analysis"
        ]
        
        for table_name in analysis_tables:
            try:
                cur.execute(f"SELECT COUNT(*) FROM {table_name}")
                count = cur.fetchone()[0]
                print(f"   📊 {table_name}: {count} registros")
            except Exception as e:
                print(f"   ❌ Erro na tabela {table_name}: {e}")
        
        conn.close()
        print(f"\n🎉 Teste concluído!")
        
    except Exception as e:
        print(f"❌ Erro geral: {e}")

if __name__ == "__main__":
    test_views()
