#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def check_teams_structure():
    """Verificar estrutura da tabela teams"""
    try:
        conn = psycopg2.connect(os.getenv("DB_DSN"))
        cur = conn.cursor()
        
        # Verificar estrutura da tabela
        cur.execute("""
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns 
            WHERE table_name = 'teams' 
            ORDER BY ordinal_position
        """)
        
        columns = cur.fetchall()
        print("📋 Estrutura da tabela teams:")
        print("=" * 60)
        for col in columns:
            nullable = "NULL" if col[2] == "YES" else "NOT NULL"
            default = f" DEFAULT {col[3]}" if col[3] else ""
            print(f"  {col[0]:<20} {col[1]:<15} {nullable}{default}")
        
        # Verificar constraints
        cur.execute("""
            SELECT constraint_name, constraint_type
            FROM information_schema.table_constraints 
            WHERE table_name = 'teams'
        """)
        
        constraints = cur.fetchall()
        print("\n🔒 Constraints:")
        print("=" * 60)
        for const in constraints:
            print(f"  {const[0]:<30} {const[1]}")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Erro: {e}")

if __name__ == "__main__":
    check_teams_structure()
