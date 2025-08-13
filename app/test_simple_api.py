#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script simples para testar a API Sportmonks
- Sem processamento complexo
- Apenas validação básica
"""

import os
import httpx
from dotenv import load_dotenv

load_dotenv()

def test_api_connection():
    """Testar conexão básica com a API"""
    print("🧪 TESTE SIMPLES - API SPORTMONKS")
    print("=" * 40)
    
    API_TOKEN = os.getenv("SPORTMONKS_API_KEY")
    if not API_TOKEN:
        print("❌ SPORTMONKS_API_KEY não configurada")
        return
    
    print(f"✅ API Token configurado: {API_TOKEN[:10]}...")
    
    # Teste 1: Endpoint básico
    print("\n📡 Teste 1: Endpoint básico")
    try:
        with httpx.Client() as client:
            url = "https://api.sportmonks.com/v3/football/leagues/648"
            params = {"api_token": API_TOKEN}
            
            response = client.get(url, params=params, timeout=10)
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                league_name = data.get("data", {}).get("name", "N/A")
                print(f"   ✅ Liga: {league_name}")
            else:
                print(f"   ❌ Erro: {response.text[:100]}")
                
    except Exception as e:
        print(f"   ❌ Falha: {e}")
    
    # Teste 2: Schedule simples
    print("\n📅 Teste 2: Schedule da temporada")
    try:
        with httpx.Client() as client:
            url = "https://api.sportmonks.com/v3/football/schedules/seasons/25184"
            params = {"api_token": API_TOKEN}
            
            response = client.get(url, params=params, timeout=10)
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                fixture_count = 0
                
                # Contar fixtures
                schedule_data = data.get("data", [])
                if isinstance(schedule_data, list):
                    for item in schedule_data:
                        rounds = item.get("rounds", [])
                        for rnd in rounds:
                            fixtures = rnd.get("fixtures", [])
                            fixture_count += len(fixtures)
                
                print(f"   ✅ Fixtures encontrados: {fixture_count}")
            else:
                print(f"   ❌ Erro: {response.text[:100]}")
                
    except Exception as e:
        print(f"   ❌ Falha: {e}")
    
    # Teste 3: Fixture individual
    print("\n⚽ Teste 3: Fixture individual")
    try:
        with httpx.Client() as client:
            url = "https://api.sportmonks.com/v3/football/fixtures/18791001"
            params = {"api_token": API_TOKEN}
            
            response = client.get(url, params=params, timeout=10)
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                fixture_name = data.get("data", {}).get("name", "N/A")
                print(f"   ✅ Fixture: {fixture_name}")
            else:
                print(f"   ❌ Erro: {response.text[:100]}")
                
    except Exception as e:
        print(f"   ❌ Falha: {e}")
    
    print("\n🎯 Teste concluído!")

if __name__ == "__main__":
    test_api_connection()
