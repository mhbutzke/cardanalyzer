# =====================================================
# Dockerfile - CardAnalyzer
# Aplicação de análise de dados de futebol
# =====================================================

FROM python:3.11-slim

# Definir variáveis de ambiente
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1
ENV DEBIAN_FRONTEND=noninteractive

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    postgresql-client \
    curl \
    cron \
    && rm -rf /var/lib/apt/lists/*

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements primeiro para aproveitar cache do Docker
COPY requirements.txt .

# Instalar dependências Python
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código da aplicação
COPY . .

# Tornar scripts executáveis
RUN chmod +x app/*.py

# Criar diretório para logs
RUN mkdir -p /app/logs

# Criar usuário não-root
RUN useradd -m -u 1000 carduser && \
    chown -R carduser:carduser /app

# Mudar para usuário não-root
USER carduser

# Expor porta (se necessário para API futura)
EXPOSE 8000

# Comando padrão - manter container rodando
CMD ["tail", "-f", "/dev/null"]
