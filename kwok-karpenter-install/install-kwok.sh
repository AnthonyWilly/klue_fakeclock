SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
if [[ -n ${KLUE_KWOK_WORKERS_CONFIG:-} ]]; then
    python3 "$SCRIPT_DIR/../src/kwok_config.py" validate --workers-config "$KLUE_KWOK_WORKERS_CONFIG" || exit $?
fi

# KWOK repository
KWOK_REPO=kubernetes-sigs/kwok
# Get latest
KWOK_LATEST_RELEASE=$(curl "https://api.github.com/repos/${KWOK_REPO}/releases/latest" | jq -r '.tag_name')

kubectl apply -f "https://github.com/${KWOK_REPO}/releases/download/${KWOK_LATEST_RELEASE}/kwok.yaml"

kubectl apply -f "https://github.com/${KWOK_REPO}/releases/download/${KWOK_LATEST_RELEASE}/stage-fast.yaml"
kubectl apply -f "https://github.com/${KWOK_REPO}/releases/download/${KWOK_LATEST_RELEASE}/metrics-usage.yaml"

# Setup Metrics Server and Usage Metric

kubectl apply -f configuration-files/usage-metrics.yaml

minikube addons enable metrics-server

kubectl delete clusterresourceusage usage-from-annotation --ignore-not-found=true

# Preserve the release configuration and override only requested worker counts.
if [[ -n ${KLUE_KWOK_WORKERS_CONFIG:-} ]]; then
    KWOK_CONFIG=$(mktemp) || exit $?
    kubectl -n kube-system get configmap kwok -o json > "$KWOK_CONFIG" || exit $?
    python3 "$SCRIPT_DIR/../src/kwok_config.py" merge --workers-config "$KLUE_KWOK_WORKERS_CONFIG" < "$KWOK_CONFIG" > "$KWOK_CONFIG.patched" || exit $?
    kubectl apply -f "$KWOK_CONFIG.patched" || exit $?
    rm -f "$KWOK_CONFIG" "$KWOK_CONFIG.patched"
    # The release mounts this config with subPath; restart to load changes.
    kubectl -n kube-system rollout restart deployment/kwok-controller || exit $?
    kubectl -n kube-system rollout status deployment/kwok-controller --timeout=180s || exit $?
fi
