#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time
from dotenv import load_dotenv

load_dotenv()

from backfill_leagues_by_dates import backfill_league_year, LEAGUES

if __name__ == "__main__":
    targets = [648, 651]  # Série A, Série B
    years = [2024, 2025]

    for league_id in targets:
        for year in years:
            backfill_league_year(league_id, LEAGUES[league_id], year)
            time.sleep(1)

    try:
        from complete_analysis import CompleteAnalysis
        analyzer = CompleteAnalysis()
        analyzer.run_complete_analysis()
    except Exception as e:
        print(f"Erro ao executar análise completa: {e}")
