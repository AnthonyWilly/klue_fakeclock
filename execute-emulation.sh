#!/bin/bash

LOG_FILE="/tmp/emulation.log"

# Clear log at start
: > "$LOG_FILE"

# Redirect ALL output (stdout + stderr) to the log file
exec > >(tee -a "$LOG_FILE") 2>&1

# Função para exibir a ajuda
usage() {
    echo "Uso: $0 [--dev | --sim] [--use-cluster CONTEXT] [--data-path PATH] [--nodepool-path PATH] [--use-karpenter] [--use-hpa] [--skip-tracer] [--static-infra] [--static-workload] [--speed-factor F] [--kwok-workers-config PATH] [-h | --help]"
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

while [[ $# -gt 0 ]]; do
    case $1 in
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

# Configuração do ambiente
if [[ $ENVIRONMENT == "development" ]]; then
    echo "Configurando ambiente de desenvolvimento..."
    cd kwok-karpenter-install
    ./setup.sh "$KARPENTER" || exit $?
elif [[ $ENVIRONMENT == "emulation" ]]; then
    echo "Configurando ambiente de emulação..."
    cd kwok-karpenter-install
    ./setup.sh "$KARPENTER" || exit $?
    cd ..
    python3 -u src/main.py "$TRACE_PATH" "$NODEPOOL_PATH" "$KARPENTER" "$TRACER" "$INFRASTRUCTURE" "$WORKLOAD" "$HPA" "${workers_args[@]}" --speed-factor="$SPEED_FACTOR"
fi
