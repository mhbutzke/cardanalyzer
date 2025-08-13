#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para configurar o cron automaticamente
"""

import os
import subprocess
from datetime import datetime

def setup_cron():
    """Configurar cron jobs automaticamente"""
    print("🔧 CONFIGURANDO CRON AUTOMÁTICO")
    print("=" * 60)
    
    # Obter diretório atual
    current_dir = os.getcwd()
    python_path = subprocess.check_output(["which", "python3"]).decode().strip()
    
    # Jobs do cron
    cron_jobs = [
        # Atualização automática a cada 2 horas
        f"0 */2 * * * cd {current_dir} && {python_path} app/auto_update_system.py once >> logs/cron_update.log 2>&1",
        
        # Atualização das tabelas de análise diariamente às 6h
        f"0 6 * * * cd {current_dir} && {python_path} app/clear_and_populate.py >> logs/cron_analysis.log 2>&1",
        
        # Geração do dashboard diariamente às 7h
        f"0 7 * * * cd {current_dir} && {python_path} app/dashboard.py >> logs/cron_dashboard.log 2>&1",
        
        # Backup do banco diariamente às 2h
        f"0 2 * * * cd {current_dir} && ./docker-init.sh backup >> logs/cron_backup.log 2>&1",
        
        # Limpeza de logs semanalmente (domingo às 3h)
        f"0 3 * * 0 cd {current_dir} && find logs/ -name '*.log' -mtime +7 -delete >> logs/cron_cleanup.log 2>&1"
    ]
    
    try:
        # Criar diretório de logs se não existir
        os.makedirs("logs", exist_ok=True)
        
        # Listar cron atual
        print("📋 Cron atual:")
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if result.returncode == 0:
            print(result.stdout)
        else:
            print("   Nenhum cron configurado")
        
        print("\n🔧 Adicionando novos jobs...")
        
        # Adicionar novos jobs
        for job in cron_jobs:
            print(f"   ✅ {job}")
        
        # Salvar no crontab
        all_jobs = []
        
        # Adicionar jobs existentes (se houver)
        if result.returncode == 0:
            all_jobs.extend(result.stdout.strip().split('\n'))
        
        # Adicionar novos jobs
        all_jobs.extend(cron_jobs)
        
        # Filtrar linhas vazias
        all_jobs = [job for job in all_jobs if job.strip()]
        
        # Salvar no crontab
        cron_content = '\n'.join(all_jobs) + '\n'
        
        with open('/tmp/new_crontab', 'w') as f:
            f.write(cron_content)
        
        subprocess.run(["crontab", "/tmp/new_crontab"], check=True)
        os.remove('/tmp/new_crontab')
        
        print(f"\n🎉 CRON CONFIGURADO COM SUCESSO!")
        print(f"📁 Logs serão salvos em: {os.path.join(current_dir, 'logs')}")
        
        # Mostrar cron configurado
        print(f"\n📋 Cron configurado:")
        subprocess.run(["crontab", "-l"])
        
    except Exception as e:
        print(f"❌ Erro ao configurar cron: {e}")
        print(f"💡 Configure manualmente adicionando estas linhas ao crontab:")
        for job in cron_jobs:
            print(f"   {job}")

def show_cron_status():
    """Mostrar status do cron"""
    print("📊 STATUS DO CRON")
    print("=" * 60)
    
    try:
        # Verificar se cron está rodando
        result = subprocess.run(["pgrep", "cron"], capture_output=True)
        if result.returncode == 0:
            print("✅ Serviço cron está rodando")
        else:
            print("❌ Serviço cron não está rodando")
        
        # Mostrar cron configurado
        print("\n📋 Jobs configurados:")
        result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
        if result.returncode == 0:
            print(result.stdout)
        else:
            print("   Nenhum cron configurado")
            
    except Exception as e:
        print(f"❌ Erro ao verificar status: {e}")

def main():
    """Função principal"""
    print("🚀 CONFIGURADOR DE CRON - CARDANALYZER")
    print("=" * 60)
    
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "status":
        show_cron_status()
    else:
        setup_cron()

if __name__ == "__main__":
    main()
