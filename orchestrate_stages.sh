#!/usr/bin/env bash
set -euo pipefail

# Rodar no container em nohup cada etapa e aguardar fins por arquivo de lock simples
cd /app
mkdir -p logs

run_stage() {
  local script=$1
  local tag=$2
  echo "[ORCH] iniciando ${tag}" | tee -a logs/orchestrator.log
  nohup python ${script} > logs/${tag}.log 2>&1 &
  local pid=$!
  echo "[ORCH] pid ${pid} para ${tag}" | tee -a logs/orchestrator.log
  # aguarda término
  wait ${pid}
  echo "[ORCH] concluído ${tag}" | tee -a logs/orchestrator.log
}

run_stage run_stage1.py stage1 || true
run_stage run_stage2.py stage2 || true
run_stage run_stage3.py stage3 || true

python - <<'PY'
from complete_analysis import CompleteAnalysis
try:
    analyzer = CompleteAnalysis()
    analyzer.run_complete_analysis()
except Exception as e:
    print(f"[ORCH] erro analise final: {e}")
PY

echo "[ORCH] fim" | tee -a logs/orchestrator.log
