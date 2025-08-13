#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Script para debugar a resposta do endpoint seasons com include=teams
"""

import os
import json
import httpx
from dotenv import load_dotenv

load_dotenv()

def debug_season_teams():
    """Debugar endpoint seasons com include=teams"""
    print("🔍 DEBUG - Endpoint seasons com include=teams")
    print("=" * 50)
    
    API_TOKEN = os.getenv("SPORTMONKS_API_KEY")
    if not API_TOKEN:
        print("❌ SPORTMONKS_API_KEY não configurada")
        return
    
    with httpx.Client() as client:
        url = "https://api.sportmonks.com/v3/football/seasons/25184"
        params = {
            "api_token": API_TOKEN,
            "include": "teams"
        }
        
        print(f"📡 URL: {url}")
        print(f"🔑 Params: {params}")
        print()
        
        try:
            response = client.get(url, params=params, timeout=30)
            print(f"📊 Status: {response.status_code}")
            print(f"📄 Headers: {dict(response.headers)}")
            print()
            
            if response.status_code == 200:
                data = response.json()
                
                print("📋 ESTRUTURA DA RESPOSTA:")
                print("=" * 50)
                
                # Verificar seções principais
                print(f"✅ data: {'✅' if 'data' in data else '❌'}")
                if 'data' in data:
                    season = data['data']
                    print(f"   • id: {season.get('id')}")
                    print(f"   • name: {season.get('name')}")
                    print(f"   • league_id: {season.get('league_id')}")
                
                print(f"✅ included: {'✅' if 'included' in data else '❌'}")
                if 'included' in data:
                    included = data['included']
                    print(f"   • Chaves: {list(included.keys())}")
                    
                    if 'teams' in included:
                        teams = included['teams']
                        print(f"   • teams: {len(teams)} times encontrados")
                        for team in teams[:5]:  # Mostrar primeiros 5
                            print(f"     - {team.get('name')} (ID: {team.get('id')})")
                    else:
                        print("   • ❌ 'teams' não encontrado em included")
                else:
                    print("   • ❌ 'included' não encontrado na resposta")
                
                print()
                print("📄 RESPOSTA COMPLETA (primeiros 1000 chars):")
                print("=" * 50)
                print(json.dumps(data, indent=2, ensure_ascii=False)[:1000])
                
            else:
                print(f"❌ Erro: {response.text}")
                
        except Exception as e:
            print(f"❌ Exceção: {e}")

if __name__ == "__main__":
    debug_season_teams()
