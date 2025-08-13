# 🐳 CardAnalyzer - Docker Setup

## 📋 **Visão Geral**

Este documento descreve como usar o Docker para executar o CardAnalyzer, uma aplicação de análise de dados de futebol que integra com a API Sportmonks.

## 🚀 **Pré-requisitos**

- Docker Desktop instalado e rodando
- docker-compose instalado
- Arquivo `.env` configurado com sua `SPORTMONKS_API_KEY`

## 🏗️ **Estrutura do Projeto**

```
CardAnalyzer/
├── docker/
│   ├── postgres/
│   │   └── postgresql.conf
│   └── cron/
│       └── crontab
├── app/
│   ├── manage.py
│   ├── enrich_timeline_simple.py
│   ├── refresh_materialized_views.py
│   └── auto_refresh_mv.py
├── sql/
│   ├── schema.sql
│   ├── views_completas.sql
│   ├── views_temporais.sql
│   ├── materialized_views.sql
│   └── views_timeline_avancadas_corrigidas.sql
├── Dockerfile
├── docker-compose.yml
├── docker-init.sh
└── .dockerignore
```

## 🐳 **Serviços Disponíveis**

### **1. PostgreSQL (postgres)**
- **Porta:** 5432
- **Usuário:** card
- **Senha:** card
- **Banco:** carddb
- **Configuração:** Otimizada para desenvolvimento

### **2. Aplicação (app)**
- **Container:** cardanalyzer_app
- **Funcionalidades:** Scripts de gerenciamento e análise
- **Dependências:** PostgreSQL

### **3. Cron (cron)**
- **Container:** cardanalyzer_cron
- **Funcionalidades:** Agendamento automático de tarefas
- **Tarefas:** Refresh de views, atualizações, backups

### **4. Adminer (adminer)**
- **Porta:** 8080
- **Funcionalidades:** Interface web para PostgreSQL
- **Acesso:** http://localhost:8080

## 🚀 **Início Rápido**

### **1. Configurar Variáveis de Ambiente**

```bash
# Copiar template
cp .env.example .env

# Editar .env e configurar sua API key
SPORTMONKS_API_KEY=SUA_CHAVE_AQUI
```

### **2. Iniciar Apenas o Banco**

```bash
./docker-init.sh start postgres
```

### **3. Iniciar Aplicação + Banco**

```bash
./docker-init.sh start app
```

### **4. Iniciar Todos os Serviços**

```bash
./docker-init.sh start all
```

## 📊 **Comandos Disponíveis**

### **Gerenciamento de Serviços**

```bash
# Iniciar serviços
./docker-init.sh start [perfil]

# Parar todos os serviços
./docker-init.sh stop

# Ver status dos serviços
./docker-init.sh status

# Ver logs
./docker-init.sh logs [serviço]
```

### **Execução de Comandos**

```bash
# Shell interativo na aplicação
./docker-init.sh exec

# Executar comando específico
./docker-init.sh exec 'python app/manage.py help'
./docker-init.sh exec 'python app/enrich_timeline_simple.py'
```

### **Backup e Restauração**

```bash
# Fazer backup do banco
./docker-init.sh backup

# Restaurar banco de backup
./docker-init.sh restore backup_20250101_120000.sql
```

## 🔧 **Perfis de Serviços**

### **postgres**
- Apenas o banco de dados
- Útil para desenvolvimento local

### **app**
- Banco + aplicação
- Para execução manual de scripts

### **cron**
- Banco + agendamento automático
- Para produção com automação

### **adminer**
- Banco + interface web
- Para administração visual

### **all**
- Todos os serviços
- Stack completo para produção

## 📅 **Tarefas Agendadas (Cron)**

### **Diárias**
- **02:00** - Refresh das Materialized Views (concurrent)
- **03:00** - Atualização diária de dados (últimos 3 dias)

### **Semanais (Domingo)**
- **04:00** - Refresh completo das Materialized Views
- **05:00** - Enriquecimento completo da timeline

### **Manutenção**
- **A cada 6h** - Verificação inteligente de refresh
- **01:00** - Limpeza de logs antigos (30 dias)
- **06:00** - Backup automático do banco

## 🐛 **Troubleshooting**

### **Problemas Comuns**

#### **1. Porta 5432 já em uso**
```bash
# Parar PostgreSQL local
sudo service postgresql stop

# Ou usar porta diferente no docker-compose.yml
ports:
  - "5433:5432"
```

#### **2. Erro de permissão no .env**
```bash
# Verificar permissões
ls -la .env

# Corrigir se necessário
chmod 600 .env
```

#### **3. Container não inicia**
```bash
# Ver logs detalhados
docker-compose logs [serviço]

# Reconstruir container
docker-compose build --no-cache [serviço]
```

### **Logs e Debugging**

```bash
# Ver logs em tempo real
./docker-init.sh logs

# Ver logs de serviço específico
./docker-init.sh logs postgres

# Acessar container para debug
docker-compose exec postgres bash
docker-compose exec app bash
```

## 🔒 **Segurança**

### **Configurações de Segurança**
- Usuário não-root nos containers
- Rede isolada (172.20.0.0/16)
- Volumes persistentes para dados
- Health checks para monitoramento

### **Variáveis Sensíveis**
- `SPORTMONKS_API_KEY` deve estar no `.env`
- Arquivo `.env` não é incluído no Docker build
- Senhas do banco são definidas no docker-compose.yml

## 📈 **Performance**

### **Otimizações do PostgreSQL**
- `shared_buffers`: 256MB
- `effective_cache_size`: 1GB
- `work_mem`: 4MB
- `maintenance_work_mem`: 64MB
- Autovacuum habilitado

### **Monitoramento**
- Health checks automáticos
- Logs estruturados
- Métricas de performance
- Backup automático

## 🚀 **Próximos Passos**

### **Desenvolvimento**
1. Testar com dados reais
2. Ajustar configurações de performance
3. Implementar monitoramento avançado

### **Produção**
1. Configurar volumes externos
2. Implementar backup remoto
3. Configurar monitoramento e alertas
4. Implementar CI/CD

## 📞 **Suporte**

Para dúvidas ou problemas:
1. Verificar logs com `./docker-init.sh logs`
2. Consultar este README
3. Verificar configurações no docker-compose.yml
4. Testar comandos individualmente

---

**🎉 Docker configurado com sucesso! Agora você pode executar o CardAnalyzer em qualquer ambiente!**

