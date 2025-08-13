# Cheatsheet — Brasileirão Série A 2025
- league_id: 648 (Brasil Série A)
- season_id: 25184 (2025)
- Datas: 2025-03-29 a 2025-12-21
- Endpoints base:
  - /v3/football/leagues/countries/5
  - /v3/football/seasons/search/2025?filters=seasonLeagues:{LEAGUE_ID}
  - /v3/football/fixtures/between/{start}/{end}
  - /v3/football/fixtures/latest  →  /v3/football/fixtures/multi/{ids}
- Includes & filtros: ver `.cursorrules`.
- Objetivo: BD com eventos atômicos + stats (corners/fouls/goals), views prontas e rotina diária

