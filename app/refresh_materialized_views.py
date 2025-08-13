#!/usr/bin/env python3
"""
CardAnalyzer - Refresh de Materialized Views
Script para atualizar as Materialized Views com opções de refresh concorrente
"""

import os
import sys
import time
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("DB_DSN", "postgresql://card:card@localhost:5432/carddb")

# Lista das Materialized Views
MATERIALIZED_VIEWS = [
    "mv_cards_by_team_season",
    "mv_cards_by_referee_season", 
    "mv_stats_by_team_season"
]

def refresh_materialized_view(conn, view_name: str, concurrent: bool = False):
    """Refresh de uma Materialized View específica"""
    try:
        with conn.cursor() as cur:
            if concurrent:
                # Refresh concorrente (não bloqueia leituras)
                sql = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}"
                print(f"🔄 Refresh CONCURRENT de {view_name}...")
            else:
                # Refresh completo (mais rápido, mas bloqueia leituras)
                sql = f"REFRESH MATERIALIZED VIEW {view_name}"
                print(f"🔄 Refresh COMPLETO de {view_name}...")
            
            start_time = time.time()
            cur.execute(sql)
            conn.commit()
            elapsed = time.time() - start_time
            
            print(f"✅ {view_name} atualizada em {elapsed:.2f}s")
            return True
            
    except Exception as e:
        print(f"❌ Erro ao atualizar {view_name}: {e}")
        conn.rollback()
        return False

def refresh_all_views(concurrent: bool = False):
    """Refresh de todas as Materialized Views"""
    print(f"🚀 Iniciando refresh de {len(MATERIALIZED_VIEWS)} Materialized Views...")
    print(f"📊 Modo: {'CONCORRENTE' if concurrent else 'COMPLETO'}")
    print("=" * 60)
    
    with psycopg2.connect(DSN) as conn:
        conn.autocommit = False
        
        success_count = 0
        total_start = time.time()
        
        for view in MATERIALIZED_VIEWS:
            if refresh_materialized_view(conn, view, concurrent):
                success_count += 1
            else:
                print(f"⚠️ Falha ao atualizar {view}")
        
        total_elapsed = time.time() - total_start
        
        print("=" * 60)
        print(f"📊 RESUMO: {success_count}/{len(MATERIALIZED_VIEWS)} views atualizadas")
        print(f"⏱️ Tempo total: {total_elapsed:.2f}s")
        
        if success_count == len(MATERIALIZED_VIEWS):
            print("🎉 Todas as Materialized Views foram atualizadas com sucesso!")
            return True
        else:
            print("⚠️ Algumas views falharam na atualização")
            return False

def refresh_specific_view(view_name: str, concurrent: bool = False):
    """Refresh de uma Materialized View específica"""
    if view_name not in MATERIALIZED_VIEWS:
        print(f"❌ View '{view_name}' não encontrada")
        print(f"📋 Views disponíveis: {', '.join(MATERIALIZED_VIEWS)}")
        return False
    
    print(f"🎯 Refresh da view específica: {view_name}")
    
    with psycopg2.connect(DSN) as conn:
        conn.autocommit = False
        return refresh_materialized_view(conn, view_name, concurrent)

def show_materialized_views_status():
    """Mostra o status das Materialized Views"""
    print("📊 STATUS DAS MATERIALIZED VIEWS:")
    print("=" * 60)
    
    with psycopg2.connect(DSN) as conn:
        with conn.cursor() as cur:
            # Verificar se as views existem
            cur.execute("""
                SELECT schemaname, matviewname, matviewowner, definition
                FROM pg_matviews 
                WHERE schemaname = 'public'
                ORDER BY matviewname
            """)
            
            views = cur.fetchall()
            
            if not views:
                print("❌ Nenhuma Materialized View encontrada")
                return
            
            for view in views:
                schema, name, owner, definition = view
                print(f"📋 {name}")
                print(f"   👤 Owner: {owner}")
                print(f"   📝 Schema: {schema}")
                print()
            
            # Verificar tamanho das views
            print("📏 TAMANHO DAS VIEWS:")
            print("-" * 40)
            
            for view_name in MATERIALIZED_VIEWS:
                cur.execute(f"""
                    SELECT pg_size_pretty(pg_total_relation_size('{view_name}'))
                """)
                size = cur.fetchone()[0]
                print(f"📊 {view_name}: {size}")

def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Refresh de Materialized Views")
    parser.add_argument("--concurrent", "-c", action="store_true", 
                       help="Refresh concorrente (não bloqueia leituras)")
    parser.add_argument("--view", "-v", type=str, 
                       help="Refresh de uma view específica")
    parser.add_argument("--status", "-s", action="store_true", 
                       help="Mostrar status das Materialized Views")
    parser.add_argument("--all", "-a", action="store_true", 
                       help="Refresh de todas as views (padrão)")
    
    args = parser.parse_args()
    
    if args.status:
        show_materialized_views_status()
        return
    
    if args.view:
        refresh_specific_view(args.view, args.concurrent)
    elif args.all or not args.view:
        refresh_all_views(args.concurrent)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

