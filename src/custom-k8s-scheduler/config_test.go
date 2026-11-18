package main

import (
	"os"
	"path/filepath"
	"testing"

	"k8s.io/client-go/tools/clientcmd"
)

// All config paths point into TempDir; no test uses the user's kubeconfig or API.
func TestLoadKubernetesConfig(t *testing.T) {
	const cluster = `apiVersion: v1
kind: Config
clusters:
- name: test-cluster
  cluster:
    server: https://127.0.0.1:18443
`
	const context = `current-context: test-context
contexts:
- name: test-context
  context:
    cluster: test-cluster
`
	for _, mode := range []string{"default", "environment", "merged-environment", "invalid-context"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("KUBECONFIG", "")
			t.Setenv("KUBERNETES_SERVICE_HOST", "")
			t.Setenv("KUBERNETES_SERVICE_PORT", "")
			original := clientcmd.RecommendedHomeFile
			clientcmd.RecommendedHomeFile = filepath.Join(t.TempDir(), "config")
			t.Cleanup(func() { clientcmd.RecommendedHomeFile = original })
			write := func(path, body string) {
				t.Helper()
				if err := os.WriteFile(path, []byte(body), 0600); err != nil {
					t.Fatal(err)
				}
			}
			switch mode {
			case "default":
				write(clientcmd.RecommendedHomeFile, cluster+context)
			case "environment":
				path := filepath.Join(t.TempDir(), "explicit-config")
				write(path, cluster+context)
				t.Setenv("KUBECONFIG", path)
			case "merged-environment":
				first, second := filepath.Join(t.TempDir(), "cluster"), filepath.Join(t.TempDir(), "context")
				write(first, cluster)
				write(second, "apiVersion: v1\nkind: Config\n"+context)
				t.Setenv("KUBECONFIG", first+string(os.PathListSeparator)+second)
			case "invalid-context":
				write(clientcmd.RecommendedHomeFile, context)
			}
			config, err := loadKubernetesConfig()
			if mode == "invalid-context" {
				if err == nil {
					t.Fatal("expected an error for a context without its cluster")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if config.Host != "https://127.0.0.1:18443" {
				t.Fatalf("unexpected API server %q", config.Host)
			}
		})
	}
}
