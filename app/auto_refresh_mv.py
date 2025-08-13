#!/usr/bin/env python3
"""
CardAnalyzer - Auto Refresh de Materialized Views
Script para automatizar o refresh das Materialized Views com diferentes estratégias
"""

import os
import sys
import time
import schedule
import psycopg2
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

DSN = os.getenv("DB_DSN", "postgresql://card:card@localhost:5432/carddb")

# Lista das Materialized Views
MATERIALIZED_VIEWS = [
    "mv_cards_by_team_season",
    "mv_cards_by_referee_season", 
    "mv_stats_by_team_season"
]

def log_message(message: str, level: str = "INFO"):
    """Log de mensagens com timestamp"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {message}")

def check_if_refresh_needed() -> bool:
    """Verifica se é necessário fazer refresh baseado na última atualização"""
    try:
        with psycopg2.connect(DSN) as conn:
            with conn.cursor() as cur:
                # Verificar quando foi o último jogo carregado
                cur.execute("""
                    SELECT MAX(starting_at) as ultimo_jogo 
                    FROM fixtures 
                    WHERE state_id IN (1, 2, 3, 4, 5, 10)
                """)
                ultimo_jogo = cur.fetchone()[0]
                
                if not ultimo_jogo:
                    log_message("Nenhum jogo encontrado no banco", "WARN")
                    return False
                
                # Verificar quando foi o último refresh
                cur.execute("""
                    SELECT MAX(reltuples) as ultimo_refresh
                    FROM pg_class 
                    WHERE relname = 'mv_cards_by_team_season'
                """)
                ultimo_refresh = cur.fetchone()[0]
                
                # Se o último jogo foi hoje, fazer refresh
                hoje = datetime.now().date()
                if ultimo_jogo.date() == hoje:
                    log_message(f"Último jogo foi hoje ({ultimo_jogo.date()}), refresh necessário", "INFO")
                    return True
                
                log_message(f"Último jogo foi em {ultimo_jogo.date()}, refresh não necessário", "INFO")
                return False
                
    except Exception as e:
        log_message(f"Erro ao verificar necessidade de refresh: {e}", "ERROR")
        return False

def refresh_materialized_view(conn, view_name: str, concurrent: bool = False) -> bool:
    """Refresh de uma Materialized View específica"""
    try:
        with conn.cursor() as cur:
            if concurrent:
                sql = f"REFRESH MATERIALIZED VIEW CONCURRENTLY {view_name}"
                log_message(f"Refresh CONCURRENT de {view_name}...", "INFO")
            else:
                sql = f"REFRESH MATERIALIZED VIEW {view_name}"
                log_message(f"Refresh COMPLETO de {view_name}...", "INFO")
            
            start_time = time.time()
            cur.execute(sql)
            conn.commit()
            elapsed = time.time() - start_time
            
            log_message(f"{view_name} atualizada em {elapsed:.2f}s", "SUCCESS")
            return True
            
    except Exception as e:
        log_message(f"Erro ao atualizar {view_name}: {e}", "ERROR")
        conn.rollback()
        return False

def refresh_all_views(concurrent: bool = False) -> bool:
    """Refresh de todas as Materialized Views"""
    log_message(f"Iniciando refresh de {len(MATERIALIZED_VIEWS)} Materialized Views...", "INFO")
    log_message(f"Modo: {'CONCORRENTE' if concurrent else 'COMPLETO'}", "INFO")
    
    with psycopg2.connect(DSN) as conn:
        conn.autocommit = False
        
        success_count = 0
        total_start = time.time()
        
        for view in MATERIALIZED_VIEWS:
            if refresh_materialized_view(conn, view, concurrent):
                success_count += 1
            else:
                log_message(f"Falha ao atualizar {view}", "WARN")
        
        total_elapsed = time.time() - total_start
        
        log_message(f"RESUMO: {success_count}/{len(MATERIALIZED_VIEWS)} views atualizadas", "INFO")
        log_message(f"Tempo total: {total_elapsed:.2f}s", "INFO")
        
        if success_count == len(MATERIALIZED_VIEWS):
            log_message("Todas as Materialized Views foram atualizadas com sucesso!", "SUCCESS")
            return True
        else:
            log_message("Algumas views falharam na atualização", "WARN")
            return False

def smart_refresh():
    """Refresh inteligente baseado na necessidade"""
    log_message("Iniciando verificação inteligente de refresh...", "INFO")
    
    if check_if_refresh_needed():
        log_message("Refresh necessário detectado, iniciando...", "INFO")
        return refresh_all_views(concurrent=True)  # Usar concurrent para não bloquear
    else:
        log_message("Refresh não necessário neste momento", "INFO")
        return True

def schedule_refresh():
    """Agenda refresh automático"""
    log_message("Configurando agendamento automático de refresh...", "INFO")
    
    # Refresh diário às 02:00 (horário de baixo tráfego)
    schedule.every().day.at("02:00").do(refresh_all_views, concurrent=True)
    
    # Refresh a cada 6 horas durante o dia
    schedule.every(6).hours.do(smart_refresh)
    
    # Refresh semanal completo aos domingos às 03:00
    schedule.every().sunday.at("03:00").do(refresh_all_views, concurrent=False)
    
    log_message("Agendamento configurado:", "INFO")
    log_message("  - Diário: 02:00 (concurrent)", "INFO")
    log_message("  - A cada 6h: verificação inteligente", "INFO")
    log_message("  - Semanal: domingo 03:00 (completo)", "INFO")
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(60)  # Verificar a cada minuto
    except KeyboardInterrupt:
        log_message("Agendamento interrompido pelo usuário", "INFO")

def main():
    """Função principal"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Auto Refresh de Materialized Views")
    parser.add_argument("--once", action="store_true", 
                       help="Executar refresh uma vez e sair")
    parser.add_argument("--smart", action="store_true", 
                       help="Refresh inteligente (verifica necessidade)")
    parser.add_argument("--schedule", action="store_true", 
                       help="Iniciar agendamento automático")
    parser.add_argument("--concurrent", "-c", action="store_true", 
                       help="Usar refresh concorrente")
    
    args = parser.parse_args()
    
    if args.once:
        log_message("Executando refresh único...", "INFO")
        refresh_all_views(args.concurrent)
    elif args.smart:
        log_message("Executando refresh inteligente...", "INFO")
        smart_refresh()
    elif args.schedule:
        log_message("Iniciando agendamento automático...", "INFO")
        schedule_refresh()
    else:
        parser.print_help()

if __name__ == "__main__":
    main()

