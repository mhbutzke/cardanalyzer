#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_fixtures_structure():
    """Verificar estrutura real da tabela fixtures"""
    try:
        conn = psycopg2.connect(os.getenv("DB_DSN"))
        cur = conn.cursor()
        
        # Verificar estrutura da tabela
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'fixtures' 
            ORDER BY ordinal_position
        """)
        
        columns = cur.fetchall()
        print("📋 Estrutura da tabela fixtures:")
        print("=" * 60)
        for col in columns:
            nullable = "NULL" if col[2] == "YES" else "NOT NULL"
            default = f" DEFAULT {col[3]}" if col[3] else ""
            print(f"  {col[0]:<20} {col[1]:<15} {nullable}{default}")
        
        # Verificar dados de exemplo
        cur.execute("SELECT * FROM fixtures LIMIT 1")
        row = cur.fetchone()
        if row:
            col_names = [desc[0] for desc in cur.description]
            print(f"\n📋 Exemplo de fixture:")
            for i, col_name in enumerate(col_names):
                value = row[i]
                if isinstance(value, str) and len(value) > 100:
                    value = value[:100] + "..."
                print(f"   {col_name}: {value}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    check_fixtures_structure()
