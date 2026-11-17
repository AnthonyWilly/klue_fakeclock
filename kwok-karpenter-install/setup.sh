#!/bin/bash
set -e -o pipefail

# Validate before any setup command, including direct invocation.
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
python3 "$SCRIPT_DIR/../src/experiment_clock.py" --speed-factor="${KLUE_SPEED_FACTOR:-1}" || exit $?
if [[ -n ${KLUE_KWOK_WORKERS_CONFIG:-} ]]; then
    python3 "$SCRIPT_DIR/../src/kwok_config.py" validate --workers-config "$KLUE_KWOK_WORKERS_CONFIG" || exit $?
fi

go install github.com/google/ko@latest
export PATH=$PATH:~/go/bin

source ~/.bashrc || true
# source ~/.zshrc

if [ $# -lt 1 ]; then
	echo "Uso: $0 <karpenter-on/karpenter-off>"
	exit 1
fi

KARPENTER="$1"

# Setup Prometheus and Grafana

kubectl create namespace monitoring --dry-run=client -o yaml | kubectl apply -f -

kubectl apply --server-side -f kube-prometheus/manifests/setup
kubectl wait \
	--for condition=Established \
	--all CustomResourceDefinition \
	--namespace=monitoring
# Repair last-applied metadata left by older setups that applied the live
# ServiceMonitor. Otherwise the base manifest tries to clear resourceVersion.
KSM_MANIFEST="$SCRIPT_DIR/kube-prometheus/manifests/kubeStateMetrics-serviceMonitor.yaml"
KSM_EXISTING=$(kubectl -n monitoring get servicemonitor kube-state-metrics --ignore-not-found -o name)
if [[ -n $KSM_EXISTING ]]; then
    kubectl apply set-last-applied -f "$KSM_MANIFEST" --create-annotation=true
fi
kubectl apply -f kube-prometheus/manifests/
# Release defaults throttle the main metrics proxy on large traces. Reapply
# our CPU overlay after the manifests so subsequent setup runs retain it.
kubectl -n monitoring patch deployment kube-state-metrics --type=strategic \
    --patch-file "$SCRIPT_DIR/configuration-files/kube-state-metrics-resources.yaml"
# Main metrics endpoint only; preserve the installed monitor's other fields.
KSM_OVERLAY=$(mktemp) || exit $?
if ! kubectl -n monitoring get servicemonitor kube-state-metrics -o json > "$KSM_OVERLAY"; then
    rm -f "$KSM_OVERLAY"
    exit 1
fi
python3 "$SCRIPT_DIR/../src/kwok_config.py" ksm --speed-factor="${KLUE_SPEED_FACTOR:-1}" < "$KSM_OVERLAY" > "$KSM_OVERLAY.patched" || exit $?
kubectl -n monitoring patch servicemonitor kube-state-metrics --type=merge \
    --patch-file "$KSM_OVERLAY.patched" || exit $?
rm -f "$KSM_OVERLAY" "$KSM_OVERLAY.patched"

docker login

if [ "$KARPENTER" = "karpenter-on" ]; then
	cd karpenter-code

	make toolchain
	make build
	make install-kwok
	make apply
	make gen_instance_types

	cd ..

	kubectl apply -f configuration-files/karpenter-servicemonitor.yml
fi

./install-kwok.sh || exit $?

# The operator creates this pod asynchronously, especially on a fresh cluster.
# A get before creation exits under set -e; wait for creation and then readiness.
echo "Waiting for Prometheus to be created and ready..."
kubectl -n monitoring wait --for=create pod/prometheus-k8s-0 --timeout=300s
kubectl -n monitoring wait --for=condition=Ready pod/prometheus-k8s-0 --timeout=300s

echo "Prometheus is ready."

# -----------------------------
# Grafana configuration
# -----------------------------

echo "▶ Detecting Minikube IP"
MINIKUBE_IP=$(minikube ip)

if [ -z "$MINIKUBE_IP" ]; then
  echo "❌ Failed to get Minikube IP"
  exit 1
fi

export MINIKUBE_IP
echo "▶ Minikube IP: $MINIKUBE_IP"

echo "▶ Applying Grafana ConfigMap"
envsubst < configuration-files/grafana/grafana-configmap.yml | kubectl apply -f -

echo "▶ Applying Grafana NodePort Service"
kubectl apply -f configuration-files/grafana/grafana-service-nodeport.yml

echo "▶ Restarting Grafana"
kubectl -n monitoring rollout restart deployment grafana

kubectl -n monitoring wait \
  --for=condition=available \
  deployment/grafana \
  --timeout=180s

echo ""
echo "✅ Grafana is ready!"
echo "👉 Open: http://${MINIKUBE_IP}:32000"

# -----------------------------
# VPA configuration
# -----------------------------

chmod +x setup-vpa.sh

./setup-vpa.sh # to use vpa, you need to apply an .yaml manifest with VPA configuration, e.g. vpa.yaml

kubectl rollout status deployment/vpa-admission-controller -n kube-system
kubectl rollout status deployment/vpa-recommender -n kube-system
kubectl rollout status deployment/vpa-updater -n kube-system
