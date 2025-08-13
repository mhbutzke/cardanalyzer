# CardAnalyzer - Prompts Prontos

## 🎯 **Prompts para o Cursor**

### **1. Validação rápida de dados**
```sql
-- Executar no banco para validar dados 2025
\i sql/validation.sql
```

**O que faz:** Conta jogos 2025 e compara cards/goals entre views de eventos e estatísticas.

### **2. Resolver IDs por API**
```bash
# Executar para consultar API e validar IDs
python app/resolve_ids.py
```

**O que faz:** Consulta `leagues/countries/5` + `seasons/search/2025` e imprime `LEAGUE_ID`, `SEASON_ID`.

### **3. Materialized Views**
```bash
# Criar materialized views
psql -U card -d carddb -f sql/materialized.sql

# Refresh das views
python app/refresh_gold.py --all
python app/refresh_gold.py --status
```

**O que faz:** Cria `mv_cards_by_team_season` e `mv_cards_by_referee_season` com refresh automático.

### **4. Contexto do evento**
```bash
# Enriquecer timeline de uma partida específica
python app/enrich_timeline.py --fixture 12345

# Enriquecer todas as partidas da temporada
python app/enrich_timeline.py --all
```

**O que faz:** Calcula `score_home_at`, `score_away_at`, `manpower_after`, `minute_bucket` por evento.

### **5. Dockerizar**
```bash
# Construir e executar
docker-compose up -d

# Ver logs
docker-compose logs -f app

# Executar comandos específicos
docker-compose exec app python app/manage.py seed
docker-compose exec app python app/refresh_gold.py --all
```

**O que faz:** Cria containers para Postgres + app + cron com volume de dados e atualizações automáticas.

## 🚀 **Comandos Rápidos**

```bash
# Setup inicial
python app/manage.py initdb

# Carregar dados da temporada
python app/manage.py seed

# Atualização diária
python app/manage.py update-daily --days-back 3

# Refresh das views
python app/refresh_gold.py --all

# Enriquecer timeline
python app/enrich_timeline.py --all
```

## 📊 **Views Disponíveis**

- `v_events_cards` - Cartões por evento
- `v_events_goals` - Gols por evento  
- `v_team_stats_pivot` - Estatísticas por time/jogo
- `mv_cards_by_team_season` - Cartões por time/temporada (materializada)
- `mv_cards_by_referee_season` - Cartões por árbitro/temporada (materializada)

## 🔧 **Configuração**

1. **Editar `.env`** com sua `SPORTMONKS_API_KEY`
2. **Executar `initdb`** para criar schema
3. **Executar `seed`** para carregar dados
4. **Configurar cron** para atualizações automáticas

## 📈 **Análises Disponíveis**

- **Disciplina das equipes:** Cartões por time/temporada
- **Critério dos árbitros:** Cartões por árbitro/temporada
- **Timeline enriquecida:** Score e manpower em cada evento
- **Validação de dados:** Comparação eventos vs estatísticas
- **Atualizações automáticas:** Cron diário para dados recentes

