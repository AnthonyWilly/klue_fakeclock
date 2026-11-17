#!/bin/bash

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
REPORT_HELPER="$SCRIPT_DIR/src/run_report.py"

# Função para exibir a ajuda
usage() {
    echo "Uso: $0 [--dev | --sim] [--use-cluster CONTEXT] [--data-path PATH] [--nodepool-path PATH] [--use-karpenter] [--use-hpa] [--skip-tracer] [--static-infra] [--static-workload] [--speed-factor F] [--kwok-workers-config PATH] [--output-dir PATH] [-h | --help]"
    echo ""
    echo "Opções:"
    echo "  --dev                 Criar ambiente de desenvolvimento"
    echo "  --sim                 Criar ambiente de emulação"
    echo "  --use-cluster CONTEXT Usar um cluster existente (passe o nome do contexto)"
    echo "  --data-path PATH      Especificar o caminho do trace a ser usado na emulação"
    echo "  --nodepool-path PATH  Especificar o caminho do nodepool a ser usado na emulação"
    echo "  --use-karpenter       Ativar o Karpenter para gerenciamento de nós"
    echo "  --use-hpa           Ativar uso de HPA na emulação"
    echo "  --skip-tracer         Pular a execução do tracer"
    echo "  --static-infra        Usar infraestrutura estática na emulação"
    echo "  --static-workload     Usar workload estático na emulação"
    echo "  --output-dir PATH     Diretório exclusivo para CSVs, log e run.json"
    echo "  --kwok-workers-config PATH Overrides opcionais de paralelismo KWOK"
    echo "  --speed-factor F      Fator finito >= 1 (padrão 1; aceleração sem HPA/Karpenter)"
    echo "  -h, --help            Exibir esta mensagem de ajuda"
    exit 1
}

# Função para configurar um cluster existente
use_existing_cluster() {
    local context=$1
    echo "Usando o cluster existente: $context"
    kubectl config use-context "$context"
}

# Parsing das flags
ENVIRONMENT=""
CLUSTER_ACTION=""
CLUSTER_CONTEXT=""
TRACE_PATH=""
NODEPOOL_PATH=""
TRACER="no-skip"
KARPENTER="karpenter-off"
INFRASTRUCTURE="dynamic"
WORKLOAD="dynamic"
HPA="hpa-off"
SPEED_FACTOR="1"
KWOK_WORKERS_CONFIG=""
OUTPUT_DIR=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --output-dir)
            if [[ $# -lt 2 || $2 == --* ]]; then
                echo "Erro: --output-dir requer um caminho."
                exit 1
            fi
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --kwok-workers-config)
            if [[ $# -lt 2 || $2 == --* ]]; then
                echo "Erro: --kwok-workers-config requer um caminho."
                exit 1
            fi
            KWOK_WORKERS_CONFIG="$2"
            shift 2
            ;;
        --speed-factor)
            if [[ $# -lt 2 || $2 == --* ]]; then
                echo "Erro: --speed-factor requer um valor."
                exit 1
            fi
            SPEED_FACTOR="$2"
            shift 2
            ;;
        --dev)
            ENVIRONMENT="development"
            shift
            ;;
        --sim)
            ENVIRONMENT="emulation"
            shift
            ;;
        --use-cluster)
            CLUSTER_ACTION="use"
            CLUSTER_CONTEXT="$2"
            shift 2
            ;;
        --data-path)
            TRACE_PATH="$2"
            shift 2
            ;;
        --nodepool-path)
            NODEPOOL_PATH="$2"
            shift 2
            ;;
        --use-karpenter)
            KARPENTER="karpenter-on"
            shift
            ;;
        --use-hpa)
            HPA="hpa-on"
            shift
            ;;
        --skip-tracer)
            TRACER="skip-tracer"
            shift
            ;;
        --static-infra)
            INFRASTRUCTURE="static"
            shift
            ;;
        --static-workload)
            WORKLOAD="static"
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Opção inválida: $1"
            usage
            ;;
    esac
done

# Establish one output identity before validation/setup. Python owns it until
# returning to EXIT; shell never writes while the coordinator is running.
output_args=()
[[ -n $OUTPUT_DIR ]] && output_args=(--output-dir "$OUTPUT_DIR")
OUTPUT_DIR=$(python3 "$REPORT_HELPER" init "${output_args[@]}" --mode "$ENVIRONMENT" \
    --trace-path "$TRACE_PATH" \
    --speed-factor="$SPEED_FACTOR" --infrastructure "$INFRASTRUCTURE" --workload "$WORKLOAD" \
    --karpenter "$KARPENTER" --hpa "$HPA") || exit $?
export KLUE_RUN_ID
KLUE_RUN_ID=$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["run_id"])' "$OUTPUT_DIR/run.json") || exit $?
finish_report() {
    local code=$?
    trap - EXIT INT TERM
    python3 "$REPORT_HELPER" finish --output-dir "$OUTPUT_DIR" --exit-code "$code"
    exit "$code"
}
trap finish_report EXIT
ACTIVE_PID=""
handle_signal() {
    local signal=$1 code=$2
    trap '' INT TERM
    if [[ -n $ACTIVE_PID ]]; then
        kill -s "$signal" "$ACTIVE_PID" 2>/dev/null || true
        wait "$ACTIVE_PID" || true
    fi
    exit "$code"
}
run_step() {
    "$@" <&0 &
    ACTIVE_PID=$!
    wait "$ACTIVE_PID"
    local code=$?
    ACTIVE_PID=""
    return "$code"
}
trap 'handle_signal INT 130' INT
trap 'handle_signal TERM 143' TERM
LOG_FILE="$OUTPUT_DIR/emulation.log"
exec > >(tee -a "$LOG_FILE") 2>&1

# Validação das flags
if [[ -z $ENVIRONMENT ]]; then
    echo "Erro: É necessário especificar --dev ou --sim."
    usage
fi

if [[ $CLUSTER_ACTION == "use" && -z $CLUSTER_CONTEXT ]]; then
    echo "Erro: O nome do contexto é necessário ao usar --use-cluster."
    usage
fi

if [[ $ENVIRONMENT == "emulation" && -z $TRACE_PATH ]]; then
    echo "Erro: É necessário especificar --data-path ao usar --sim."
    usage
fi

if [[ $ENVIRONMENT == "emulation" && $KARPENTER == "karpenter-on" && -z $NODEPOOL_PATH ]]; then
    echo "Erro: É necessário especificar --nodepool-path ao usar --sim com --use-karpenter."
    usage
fi

# Dependency-free Python preflight runs before setup or cluster access.
speed_args=(--speed-factor="$SPEED_FACTOR")
[[ $KARPENTER == "karpenter-on" ]] && speed_args+=(--karpenter)
[[ $HPA == "hpa-on" ]] && speed_args+=(--hpa)
[[ $ENVIRONMENT == "development" ]] && speed_args+=(--development)
python3 "$(dirname "${BASH_SOURCE[0]}")/src/experiment_clock.py" "${speed_args[@]}" || exit $?

workers_args=()
if [[ -n $KWOK_WORKERS_CONFIG ]]; then
    # Keep the path valid after setup changes directory.
    KWOK_WORKERS_CONFIG="$(realpath -- "$KWOK_WORKERS_CONFIG")" || exit $?
    python3 "$(dirname "${BASH_SOURCE[0]}")/src/kwok_config.py" validate --workers-config "$KWOK_WORKERS_CONFIG" || exit $?
    workers_args=(--kwok-workers-config "$KWOK_WORKERS_CONFIG")
fi
export KLUE_SPEED_FACTOR="$SPEED_FACTOR"
export KLUE_KWOK_WORKERS_CONFIG="$KWOK_WORKERS_CONFIG"

python3 "$REPORT_HELPER" stage --output-dir "$OUTPUT_DIR" --stage setup --workers-config "$KWOK_WORKERS_CONFIG" || exit $?
if [[ $CLUSTER_ACTION == "use" ]]; then
    use_existing_cluster "$CLUSTER_CONTEXT" || exit $?
fi

# Configuração do ambiente
if [[ $ENVIRONMENT == "development" ]]; then
    echo "Configurando ambiente de desenvolvimento..."
    cd "$SCRIPT_DIR/kwok-karpenter-install" || exit $?
    run_step ./setup.sh "$KARPENTER" || exit $?
elif [[ $ENVIRONMENT == "emulation" ]]; then
    echo "Configurando ambiente de emulação..."
    cd "$SCRIPT_DIR/kwok-karpenter-install" || exit $?
    run_step ./setup.sh "$KARPENTER" || exit $?
    cd ..
    run_step python3 -u src/main.py "$TRACE_PATH" "$NODEPOOL_PATH" "$KARPENTER" "$TRACER" "$INFRASTRUCTURE" "$WORKLOAD" "$HPA" "${workers_args[@]}" --speed-factor="$SPEED_FACTOR" --output-dir "$OUTPUT_DIR"
fi
