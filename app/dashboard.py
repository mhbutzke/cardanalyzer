#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Dashboard HTML para visualizar análises de cartões e estatísticas
"""

import os
import psycopg2
from dotenv import load_dotenv
import json
from datetime import datetime

load_dotenv()

def get_dashboard_data():
    """Buscar dados para o dashboard"""
    try:
        conn = psycopg2.connect(os.getenv("DB_DSN"))
        cur = conn.cursor()
        
        data = {}
        
        # 1. Resumo geral
        cur.execute("SELECT * FROM v_resumo_geral")
        data['resumo'] = [dict(zip(['categoria', 'total', 'tabela'], row)) for row in cur.fetchall()]
        
        # 2. Top 10 times por gols
        cur.execute("SELECT team_name, gols, escanteios, faltas FROM v_estatisticas_simples ORDER BY gols DESC LIMIT 10")
        data['top_gols'] = [dict(zip(['time', 'gols', 'escanteios', 'faltas'], row)) for row in cur.fetchall()]
        
        # 3. Top 10 times por cartões
        cur.execute("SELECT team_name, total_cartoes, amarelos, vermelhos, segundo_amarelo FROM v_cartoes_simples ORDER BY total_cartoes DESC LIMIT 10")
        data['top_cartoes'] = [dict(zip(['time', 'total', 'amarelos', 'vermelhos', 'segundo_amarelo'], row)) for row in cur.fetchall()]
        
        # 4. Estatísticas por categoria
        cur.execute("""
            SELECT 
                stat_type,
                COUNT(*) as total_registros,
                AVG(count) as media,
                MAX(count) as maximo
            FROM statistic_analysis 
            GROUP BY stat_type
        """)
        data['stats_categoria'] = [dict(zip(['tipo', 'total', 'media', 'maximo'], row)) for row in cur.fetchall()]
        
        # 5. Cartões por período
        cur.execute("""
            SELECT 
                period,
                card_type,
                COUNT(*) as total
            FROM card_analysis 
            GROUP BY period, card_type
            ORDER BY period, card_type
        """)
        data['cartoes_periodo'] = [dict(zip(['periodo', 'tipo', 'total'], row)) for row in cur.fetchall()]
        
        conn.close()
        return data
        
    except Exception as e:
        print(f"❌ Erro ao buscar dados: {e}")
        return {}

def generate_html_dashboard(data):
    """Gerar dashboard HTML"""
    
    html = f"""
<!DOCTYPE html>
<html lang="pt-BR">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>CardAnalyzer - Dashboard</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }}
        
        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}
        
        .header {{
            text-align: center;
            color: white;
            margin-bottom: 30px;
        }}
        
        .header h1 {{
            font-size: 2.5rem;
            margin-bottom: 10px;
            text-shadow: 2px 2px 4px rgba(0,0,0,0.3);
        }}
        
        .header p {{
            font-size: 1.1rem;
            opacity: 0.9;
        }}
        
        .stats-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }}
        
        .card {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            transition: transform 0.3s ease;
        }}
        
        .card:hover {{
            transform: translateY(-5px);
        }}
        
        .card h3 {{
            color: #333;
            margin-bottom: 20px;
            font-size: 1.3rem;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }}
        
        .stat-item {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 10px 0;
            border-bottom: 1px solid #eee;
        }}
        
        .stat-item:last-child {{
            border-bottom: none;
        }}
        
        .stat-label {{
            font-weight: 500;
            color: #666;
        }}
        
        .stat-value {{
            font-weight: bold;
            color: #667eea;
            font-size: 1.1rem;
        }}
        
        .table-container {{
            background: white;
            border-radius: 15px;
            padding: 25px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.1);
            margin-bottom: 20px;
        }}
        
        .table-container h3 {{
            color: #333;
            margin-bottom: 20px;
            font-size: 1.3rem;
            border-bottom: 2px solid #667eea;
            padding-bottom: 10px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
        }}
        
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #eee;
        }}
        
        th {{
            background-color: #f8f9fa;
            font-weight: 600;
            color: #333;
        }}
        
        tr:hover {{
            background-color: #f8f9fa;
        }}
        
        .highlight {{
            background-color: #e3f2fd;
            font-weight: 600;
        }}
        
        .footer {{
            text-align: center;
            color: white;
            margin-top: 30px;
            opacity: 0.8;
        }}
        
        @media (max-width: 768px) {{
            .stats-grid {{
                grid-template-columns: 1fr;
            }}
            
            .header h1 {{
                font-size: 2rem;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>⚽ CardAnalyzer</h1>
            <p>Dashboard de Análise de Cartões e Estatísticas</p>
            <p><small>Atualizado em: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}</small></p>
        </div>
        
        <!-- Resumo Geral -->
        <div class="stats-grid">
"""
    
    # Adicionar cards de resumo
    for item in data.get('resumo', []):
        html += f"""
            <div class="card">
                <h3>📊 {item['categoria']}</h3>
                <div class="stat-item">
                    <span class="stat-label">Total de Registros:</span>
                    <span class="stat-value">{item['total']}</span>
                </div>
                <div class="stat-item">
                    <span class="stat-label">Tabela:</span>
                    <span class="stat-value">{item['tabela']}</span>
                </div>
            </div>
        """
    
    html += """
        </div>
        
        <!-- Top Times por Gols -->
        <div class="table-container">
            <h3>🏆 Top 10 Times por Gols</h3>
            <table>
                <thead>
                    <tr>
                        <th>Posição</th>
                        <th>Time</th>
                        <th>Gols</th>
                        <th>Escanteios</th>
                        <th>Faltas</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for i, time in enumerate(data.get('top_gols', []), 1):
        highlight_class = "highlight" if i <= 3 else ""
        html += f"""
                    <tr class="{highlight_class}">
                        <td>{i}º</td>
                        <td>{time['time']}</td>
                        <td><strong>{time['gols']}</strong></td>
                        <td>{time['escanteios']}</td>
                        <td>{time['faltas']}</td>
                    </tr>
        """
    
    html += """
                </tbody>
            </table>
        </div>
        
        <!-- Top Times por Cartões -->
        <div class="table-container">
            <h3>🟡 Top Times por Cartões</h3>
            <table>
                <thead>
                    <tr>
                        <th>Posição</th>
                        <th>Time</th>
                        <th>Total</th>
                        <th>Amarelos</th>
                        <th>Vermelhos</th>
                        <th>2º Amarelo</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for i, time in enumerate(data.get('top_cartoes', []), 1):
        if time['total'] > 0:  # Só mostrar times com cartões
            highlight_class = "highlight" if i <= 3 else ""
            html += f"""
                    <tr class="{highlight_class}">
                        <td>{i}º</td>
                        <td>{time['time']}</td>
                        <td><strong>{time['total']}</strong></td>
                        <td>{time['amarelos']}</td>
                        <td>{time['vermelhos']}</td>
                        <td>{time['segundo_amarelo']}</td>
                    </tr>
            """
    
    html += """
                </tbody>
            </table>
        </div>
        
        <!-- Estatísticas por Categoria -->
        <div class="table-container">
            <h3>📈 Estatísticas por Categoria</h3>
            <table>
                <thead>
                    <tr>
                        <th>Categoria</th>
                        <th>Total de Registros</th>
                        <th>Média</th>
                        <th>Máximo</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for stat in data.get('stats_categoria', []):
        html += f"""
                    <tr>
                        <td><strong>{stat['tipo']}</strong></td>
                        <td>{stat['total']}</td>
                        <td>{stat['media']:.1f}</td>
                        <td>{stat['maximo']}</td>
                    </tr>
        """
    
    html += """
                </tbody>
            </table>
        </div>
        
        <!-- Cartões por Período -->
        <div class="table-container">
            <h3>⏰ Cartões por Período</h3>
            <table>
                <thead>
                    <tr>
                        <th>Período</th>
                        <th>Tipo de Cartão</th>
                        <th>Total</th>
                    </tr>
                </thead>
                <tbody>
    """
    
    for cartao in data.get('cartoes_periodo', []):
        periodo_nome = "1º Tempo" if cartao['periodo'] == 'HT' else "2º Tempo"
        html += f"""
                    <tr>
                        <td><strong>{periodo_nome}</strong></td>
                        <td>{cartao['tipo']}</td>
                        <td>{cartao['total']}</td>
                    </tr>
        """
    
    html += """
                </tbody>
            </table>
        </div>
        
        <div class="footer">
            <p>CardAnalyzer - Sistema de Análise de Futebol</p>
            <p>Desenvolvido com ❤️ para análise de dados esportivos</p>
        </div>
    </div>
</body>
</html>
    """
    
    return html

def main():
    """Função principal"""
    print("🚀 GERANDO DASHBOARD HTML")
    print("=" * 60)
    
    # 1. Buscar dados
    print("📊 Buscando dados do banco...")
    data = get_dashboard_data()
    
    if not data:
        print("❌ Falha ao buscar dados")
        return
    
    print("   ✅ Dados carregados com sucesso")
    
    # 2. Gerar HTML
    print("🖥️ Gerando dashboard HTML...")
    html_content = generate_html_dashboard(data)
    
    # 3. Salvar arquivo
    output_file = "dashboard.html"
    with open(output_file, "w", encoding="utf-8") as f:
        f.write(html_content)
    
    print(f"   ✅ Dashboard salvo em: {output_file}")
    print(f"\n🎉 DASHBOARD GERADO COM SUCESSO!")
    print(f"📁 Arquivo: {os.path.abspath(output_file)}")
    print(f"🌐 Abra no navegador para visualizar")

if __name__ == "__main__":
    main()
