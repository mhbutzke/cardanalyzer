#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Verificar disponibilidade de dados no banco
- Analisar estrutura das tabelas
- Verificar dados disponíveis
- Identificar o que pode ser extraído
"""

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_data_availability():
    """Verificar quais dados temos disponíveis"""
    print("🔍 VERIFICANDO DISPONIBILIDADE DE DADOS")
    print("=" * 60)
    
    try:
        conn = psycopg2.connect(os.getenv("DB_DSN"))
        cur = conn.cursor()
        
        # 1. Verificar estrutura das tabelas principais
        print("🏗️ ESTRUTURA DAS TABELAS:")
        print("-" * 40)
        
        tables_to_check = [
            'fixtures', 'events', 'fixture_statistics', 
            'fixture_participants', 'card_analysis', 
            'statistic_analysis', 'referee_analysis'
        ]
        
        for table in tables_to_check:
            try:
                cur.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}' ORDER BY ordinal_position")
                columns = cur.fetchall()
                print(f"\n📋 {table.upper()}:")
                for col in columns:
                    print(f"   • {col[0]} ({col[1]})")
            except Exception as e:
                print(f"   ❌ Erro ao verificar {table}: {e}")
        
        # 2. Verificar dados disponíveis
        print(f"\n📊 DADOS DISPONÍVEIS:")
        print("-" * 40)
        
        # Contagem geral
        cur.execute("SELECT COUNT(*) FROM fixtures")
        total_fixtures = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM events")
        total_events = cur.fetchone()[0]
        
        cur.execute("SELECT COUNT(*) FROM fixture_statistics")
        total_stats = cur.fetchone()[0]
        
        print(f"   📈 Total de fixtures: {total_fixtures}")
        print(f"   📈 Total de eventos: {total_events}")
        print(f"   📈 Total de estatísticas: {total_stats}")
        
        # 3. Verificar temporadas disponíveis
        print(f"\n📅 TEMPORADAS DISPONÍVEIS:")
        print("-" * 40)
        
        cur.execute("SELECT DISTINCT season_id FROM fixtures ORDER BY season_id")
        seasons = cur.fetchall()
        for season in seasons:
            print(f"   • Temporada: {season[0]}")
        
        # 4. Verificar ligas disponíveis
        print(f"\n🏆 LIGAS DISPONÍVEIS:")
        print("-" * 40)
        
        cur.execute("SELECT DISTINCT league_id FROM fixtures ORDER BY league_id")
        leagues = cur.fetchall()
        for league in leagues:
            print(f"   • Liga ID: {league[0]}")
        
        # 5. Verificar tipos de eventos disponíveis
        print(f"\n🎯 TIPOS DE EVENTOS DISPONÍVEIS:")
        print("-" * 40)
        
        cur.execute("SELECT DISTINCT type_id, COUNT(*) FROM events GROUP BY type_id ORDER BY type_id")
        event_types = cur.fetchall()
        for event_type in event_types:
            print(f"   • Tipo {event_type[0]}: {event_type[1]} eventos")
        
        # 6. Verificar tipos de estatísticas disponíveis
        print(f"\n📊 TIPOS DE ESTATÍSTICAS DISPONÍVEIS:")
        print("-" * 40)
        
        cur.execute("SELECT DISTINCT type_id, COUNT(*) FROM fixture_statistics GROUP BY type_id ORDER BY type_id")
        stat_types = cur.fetchall()
        for stat_type in stat_types:
            print(f"   • Tipo {stat_type[0]}: {stat_type[1]} registros")
        
        # 7. Verificar dados de análise existentes
        print(f"\n🔍 DADOS DE ANÁLISE EXISTENTES:")
        print("-" * 40)
        
        # Cartões
        cur.execute("SELECT COUNT(*) FROM card_analysis")
        card_count = cur.fetchone()[0]
        print(f"   • Análise de cartões: {card_count} registros")
        
        # Estatísticas
        cur.execute("SELECT COUNT(*) FROM statistic_analysis")
        stat_count = cur.fetchone()[0]
        print(f"   • Análise de estatísticas: {stat_count} registros")
        
        # Árbitros
        cur.execute("SELECT COUNT(*) FROM referee_analysis")
        ref_count = cur.fetchone()[0]
        print(f"   • Análise de árbitros: {ref_count} registros")
        
        # 8. Verificar dados específicos solicitados
        print(f"\n🎯 DADOS SOLICITADOS - VERIFICAÇÃO:")
        print("-" * 40)
        
        # Jogos por temporada
        print("   📊 Jogos por temporada:")
        cur.execute("""
            SELECT season_id, COUNT(*) as jogos 
            FROM fixtures 
            GROUP BY season_id 
            ORDER BY season_id
        """)
        jogos_por_temporada = cur.fetchall()
        for row in jogos_por_temporada:
            print(f"      • Temporada {row[0]}: {row[1]} jogos")
        
        # Cartões por período (HT/FT)
        print("   🟨 Cartões por período:")
        cur.execute("""
            SELECT period, card_type, SUM(count) as total
            FROM card_analysis 
            GROUP BY period, card_type 
            ORDER BY period, card_type
        """)
        cartoes_por_periodo = cur.fetchall()
        for row in cartoes_por_periodo:
            print(f"      • {row[0]} {row[1]}: {row[2]} cartões")
        
        # Estatísticas por tipo
        print("   📈 Estatísticas por tipo:")
        cur.execute("""
            SELECT stat_type, SUM(count) as total
            FROM statistic_analysis 
            GROUP BY stat_type 
            ORDER BY stat_type
        """)
        stats_por_tipo = cur.fetchall()
        for row in stats_por_tipo:
            print(f"      • {row[0]}: {row[1]} registros")
        
        conn.close()
        
        print(f"\n🎉 VERIFICAÇÃO CONCLUÍDA!")
        
    except Exception as e:
        print(f"❌ Erro na verificação: {e}")

if __name__ == "__main__":
    check_data_availability()
