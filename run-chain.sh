#!/bin/bash
# Roda uma sequência de emulações (Klue original e/ou fork) na mesma VM,
# uma de cada vez, recriando o cluster antes de cada uma.
#
# Uso:
#   ./run-chain.sh <trace> <run> [<run> ...]
#     <trace>  caminho do trace (relativo à pasta atual ou absoluto)
#     <run>    orig        -> Klue original em 1x
#              fork:<F>    -> fork com --speed-factor F
#
# Exemplo (rodar dentro do tmux):
#   ./run-chain.sh data/prod-dj-iostore-eks-use1c-m4t_17junho_1hora/ orig fork:2 fork:5
#
# Saída: ~/run-logs/<trace>_<data>/
#   <n>-<run>-cluster.log   criação do cluster
#   <n>-<run>.log           emulação (com `real` no fim)
#   summary.tsv             tempos em segundos de cada etapa

FORK_DIR=${FORK_DIR:-$(cd "$(dirname "$0")" && pwd)}
ORIG_DIR=${ORIG_DIR:-$(dirname "$FORK_DIR")/klue}

if [[ $# -lt 2 ]]; then
    sed -n '2,19p' "$0"
    exit 1
fi

TRACE=$1; shift
if [[ ! -d $TRACE ]]; then
    echo "Trace não encontrado: $TRACE"
    exit 1
fi
# Caminho absoluto e sem / no fim, como o execute-emulation.sh espera.
TRACE=$(cd "$TRACE" && pwd)

LOG_DIR="$HOME/run-logs/$(basename "${TRACE%/}")_$(date +%Y-%m-%d-%H%M%S)"
mkdir -p "$LOG_DIR"
SUMMARY="$LOG_DIR/summary.tsv"
printf 'n\trun\tinicio\tcluster_s\temulacao_s\texit\n' > "$SUMMARY"
echo "Trace: $TRACE"
echo "Logs:  $LOG_DIR"

# Encerra sobras de execuções anteriores que seguram terminais e portas
# (ex.: o port-forward do Prometheus que o teardown do original não mata).
cleanup_leftovers() {
    pkill -f 'kubectl port-forward' 2>/dev/null || true
}

# Encerra o tail que espelha o log na tela, inclusive se o script sair no meio.
TAIL_PID=
stop_tail() {
    [[ -n $TAIL_PID ]] && kill "$TAIL_PID" 2>/dev/null
    TAIL_PID=
}
trap stop_tail EXIT

n=0
for spec in "$@"; do
    n=$((n + 1))
    case $spec in
        orig)   repo=$ORIG_DIR; args=() ;;
        fork:*) repo=$FORK_DIR; args=(--speed-factor "${spec#fork:}") ;;
        *)      echo "Run inválido: $spec (use orig ou fork:<F>)"; exit 1 ;;
    esac
    name="$n-${spec/:/-}"
    echo
    echo "=== [$name] $(date '+%d/%m %H:%M:%S') em $repo ==="

    cd "$repo" || exit 1
    cleanup_leftovers
    printf '2\n' | ./delete-cluster.sh > "$LOG_DIR/$name-delete.log" 2>&1

    t0=$(date +%s)
    { time (printf '2\n' | ./create-cluster.sh); } > "$LOG_DIR/$name-cluster.log" 2>&1
    create_code=$?
    t1=$(date +%s)
    if [[ $create_code -ne 0 ]]; then
        echo "Criação do cluster falhou (veja $name-cluster.log). Parando a cadeia."
        printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$n" "$spec" "$(date -d @"$t0" '+%F %T')" $((t1 - t0)) "" "create:$create_code" >> "$SUMMARY"
        exit 1
    fi

    # A saída vai para o log e um tail a repete na tela. Sem pipe para o tee:
    # processos filhos que sobrevivem não travam a cadeia.
    log="$LOG_DIR/$name.log"
    : > "$log"
    tail -n +1 -f "$log" &
    TAIL_PID=$!
    { time ./execute-emulation.sh --sim --use-cluster minikube --data-path "$TRACE" "${args[@]}"; } \
        >> "$log" 2>&1
    code=$?
    t2=$(date +%s)
    sleep 1
    stop_tail
    cleanup_leftovers

    printf '%s\t%s\t%s\t%s\t%s\t%s\n' "$n" "$spec" "$(date -d @"$t0" '+%F %T')" $((t1 - t0)) $((t2 - t1)) "$code" >> "$SUMMARY"
    echo "    cluster: $(( (t1 - t0) / 60 ))m$(( (t1 - t0) % 60 ))s  emulação: $(( (t2 - t1) / 60 ))m$(( (t2 - t1) % 60 ))s  exit: $code"
done

echo
echo "Fim: $(date '+%d/%m %H:%M:%S')"
column -t -s $'\t' "$SUMMARY"
