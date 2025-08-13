#!/bin/bash
# =====================================================
# Docker Init Script - CardAnalyzer
# Script de inicialização e gerenciamento
# =====================================================

set -e

# Cores para output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Função para imprimir mensagens coloridas
print_message() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_header() {
    echo -e "${BLUE}================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}================================================${NC}"
}

# Função para verificar se Docker está rodando
check_docker() {
    if ! docker info > /dev/null 2>&1; then
        print_error "Docker não está rodando. Inicie o Docker e tente novamente."
        exit 1
    fi
    print_message "Docker está rodando"
}

# Função para verificar se docker-compose está disponível
check_docker_compose() {
    if ! command -v docker-compose &> /dev/null; then
        print_error "docker-compose não está instalado. Instale e tente novamente."
        exit 1
    fi
    print_message "docker-compose está disponível"
}

# Função para criar diretórios necessários
create_directories() {
    print_message "Criando diretórios necessários..."
    mkdir -p logs
    mkdir -p docker/postgres
    mkdir -p docker/cron
    print_message "Diretórios criados"
}

# Função para verificar arquivo .env
check_env_file() {
    if [ ! -f .env ]; then
        print_warning "Arquivo .env não encontrado. Criando template..."
        cat > .env << EOF
# =====================================================
# CardAnalyzer - Variáveis de Ambiente
# =====================================================

# API Sportmonks
SPORTMONKS_API_KEY=SEU_TOKEN_AQUI

# Banco de Dados
DB_DSN=postgresql://card:card@localhost:5432/carddb

# API Base URL
API_BASE_URL=https://api.sportmonks.com/v3/football

# Timezone
TZ=America/Sao_Paulo
EOF
        print_warning "Arquivo .env criado. Configure sua API key e execute novamente."
        exit 1
    fi
    
    # Verificar se API key está configurada
    if grep -q "SEU_TOKEN_AQUI" .env; then
        print_error "Configure sua SPORTMONKS_API_KEY no arquivo .env"
        exit 1
    fi
    
    print_message "Arquivo .env configurado corretamente"
}

# Função para construir e iniciar serviços
start_services() {
    local profile=$1
    
    print_message "Iniciando serviços com perfil: $profile"
    
    case $profile in
        "postgres")
            docker-compose up -d postgres
            ;;
        "app")
            docker-compose --profile app up -d
            ;;
        "cron")
            docker-compose --profile cron up -d
            ;;
        "adminer")
            docker-compose --profile adminer up -d
            ;;
        "all")
            docker-compose --profile app --profile cron --profile adminer up -d
            ;;
        *)
            print_error "Perfil inválido: $profile"
            print_message "Perfis disponíveis: postgres, app, cron, adminer, all"
            exit 1
            ;;
    esac
    
    print_message "Serviços iniciados com sucesso!"
}

# Função para parar serviços
stop_services() {
    print_message "Parando todos os serviços..."
    docker-compose down
    print_message "Serviços parados"
}

# Função para mostrar status dos serviços
show_status() {
    print_message "Status dos serviços:"
    docker-compose ps
}

# Função para mostrar logs
show_logs() {
    local service=$1
    
    if [ -z "$service" ]; then
        print_message "Mostrando logs de todos os serviços..."
        docker-compose logs -f
    else
        print_message "Mostrando logs do serviço: $service"
        docker-compose logs -f "$service"
    fi
}

# Função para executar comandos na aplicação
exec_app() {
    local command=$1
    
    if [ -z "$command" ]; then
        print_message "Executando shell interativo na aplicação..."
        docker-compose exec app bash
    else
        print_message "Executando comando: $command"
        docker-compose exec app $command
    fi
}

# Função para backup do banco
backup_database() {
    local backup_file="backup_$(date +%Y%m%d_%H%M%S).sql"
    
    print_message "Fazendo backup do banco de dados..."
    
    if docker-compose ps postgres | grep -q "Up"; then
        docker-compose exec -T postgres pg_dump -U card carddb > "$backup_file"
        print_message "Backup salvo em: $backup_file"
    else
        print_error "Serviço Postgres não está rodando"
        exit 1
    fi
}

# Função para restaurar banco
restore_database() {
    local backup_file=$1
    
    if [ -z "$backup_file" ]; then
        print_error "Especifique o arquivo de backup"
        exit 1
    fi
    
    if [ ! -f "$backup_file" ]; then
        print_error "Arquivo de backup não encontrado: $backup_file"
        exit 1
    fi
    
    print_message "Restaurando banco de dados de: $backup_file"
    
    if docker-compose ps postgres | grep -q "Up"; then
        docker-compose exec -T postgres psql -U card -d carddb < "$backup_file"
        print_message "Banco restaurado com sucesso"
    else
        print_error "Serviço Postgres não está rodando"
        exit 1
    fi
}

# Função para mostrar ajuda
show_help() {
    print_header "CardAnalyzer - Docker Management Script"
    echo
    echo "Uso: $0 [COMANDO] [OPÇÕES]"
    echo
    echo "Comandos:"
    echo "  start [PERFIL]     Inicia serviços (perfil: postgres, app, cron, adminer, all)"
    echo "  stop               Para todos os serviços"
    echo "  status             Mostra status dos serviços"
    echo "  logs [SERVIÇO]     Mostra logs (opcional: nome do serviço)"
    echo "  exec [COMANDO]     Executa comando na aplicação (opcional: comando)"
    echo "  backup             Faz backup do banco de dados"
    echo "  restore ARQUIVO    Restaura banco de dados do arquivo"
    echo "  help               Mostra esta ajuda"
    echo
    echo "Exemplos:"
    echo "  $0 start postgres          # Inicia apenas o banco"
    echo "  $0 start app               # Inicia banco + aplicação"
    echo "  $0 start all               # Inicia todos os serviços"
    echo "  $0 exec 'python app/manage.py help'  # Executa comando específico"
    echo "  $0 logs postgres           # Mostra logs do banco"
    echo
}

# Função principal
main() {
    local command=$1
    local option=$2
    
    print_header "CardAnalyzer - Docker Management"
    
    # Verificações iniciais
    check_docker
    check_docker_compose
    create_directories
    check_env_file
    
    case $command in
        "start")
            start_services "$option"
            ;;
        "stop")
            stop_services
            ;;
        "status")
            show_status
            ;;
        "logs")
            show_logs "$option"
            ;;
        "exec")
            exec_app "$option"
            ;;
        "backup")
            backup_database
            ;;
        "restore")
            restore_database "$option"
            ;;
        "help"|"--help"|"-h"|"")
            show_help
            ;;
        *)
            print_error "Comando inválido: $command"
            show_help
            exit 1
            ;;
    esac
}

# Executar função principal
main "$@"
