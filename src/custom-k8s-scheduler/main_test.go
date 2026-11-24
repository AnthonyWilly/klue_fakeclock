package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	typed "k8s.io/client-go/kubernetes/typed/core/v1"
)

func TestCSV(t *testing.T) {
	for _, tc := range []struct {
		body  string
		valid bool
		count int
	}{
		{"pod,namespace,node\np,a,n\np,b,m\n", true, 2},
		{"pod,namespace,node\n\"p\",a,n\np,a,n\n", true, 1},
		{"pod,namespace,node\n", true, 0},
		{"", false, 0}, {"p,a,n\n", false, 0},
		{"\"pod,namespace,node\"\n\"p,a,n\"\n", false, 0},
		{"pod,namespace,node\np,a,n\np,a,m\n", false, 0},
		{"pod,namespace,node\np,a\n", false, 0},
		{"pod,namespace,node\np,,n\n", false, 0},
		{"pod,namespace,node\n\"p,a,n\n", false, 0},
	} {
		t.Run(fmt.Sprintf("%q", tc.body), func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "map.csv")
			if err := os.WriteFile(path, []byte(tc.body), 0600); err != nil {
				t.Fatal(err)
			}
			m, err := loadPodNodeMapping(path)
			if (err == nil) != tc.valid {
				t.Fatalf("map=%v error=%v", m, err)
			}
			if tc.valid && len(m) != tc.count {
				t.Fatal(m)
			}
		})
	}
}

type bindingClient struct {
	kubernetes.Interface
	bind func(context.Context, *corev1.Binding) error
}
type bindingCore struct {
	typed.CoreV1Interface
	bind func(context.Context, *corev1.Binding) error
}
type bindingPods struct {
	typed.PodInterface
	bind func(context.Context, *corev1.Binding) error
}

func (c bindingClient) CoreV1() typed.CoreV1Interface {
	return bindingCore{c.Interface.CoreV1(), c.bind}
}
func (c bindingCore) Pods(ns string) typed.PodInterface {
	return bindingPods{c.CoreV1Interface.Pods(ns), c.bind}
}
func (c bindingPods) Bind(ctx context.Context, b *corev1.Binding, _ metav1.CreateOptions) error {
	return c.bind(ctx, b)
}
func pod(ns, name, node string) *corev1.Pod {
	return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name}, Spec: corev1.PodSpec{NodeName: node}}
}
func placement(pods ...*corev1.Pod) map[string]PodNodeMapping {
	m := map[string]PodNodeMapping{}
	for _, p := range pods {
		m[p.Namespace+"/"+p.Name] = PodNodeMapping{Namespace: p.Namespace, NodeName: "node-" + p.Namespace}
	}
	return m
}
func observe(client *fake.Clientset, b *corev1.Binding) error {
	resource := schema.GroupVersionResource{Version: "v1", Resource: "pods"}
	obj, err := client.Tracker().Get(resource, b.Namespace, b.Name)
	if err != nil {
		return err
	}
	p := obj.(*corev1.Pod).DeepCopy()
	p.Spec.NodeName = b.Target.Name
	return client.Tracker().Update(resource, p, b.Namespace)
}
func runTest(t *testing.T, client kubernetes.Interface, m map[string]PodNodeMapping, workers int) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	return runScheduler(ctx, client, m, workers)
}
func TestAlreadyScheduledAndWrongDestination(t *testing.T) {
	for _, node := range []string{"node-a", "wrong"} {
		t.Run(node, func(t *testing.T) {
			p := pod("a", "p", node)
			c := fake.NewSimpleClientset(p)
			var calls atomic.Int32
			client := bindingClient{c, func(context.Context, *corev1.Binding) error { calls.Add(1); return nil }}
			err := runTest(t, client, placement(p), 2)
			if (err == nil) != (node == "node-a") {
				t.Fatal(err)
			}
			if calls.Load() != 0 {
				t.Fatal("bound scheduled pod")
			}
			if err != nil && !strings.Contains(err.Error(), "expected node node-a, observed wrong") {
				t.Fatal(err)
			}
		})
	}
}
func TestBoundedWorkersAndEquivalentPlacement(t *testing.T) {
	for _, workers := range []int{1, 2, 8} {
		t.Run(fmt.Sprint(workers), func(t *testing.T) {
			objects := []runtime.Object{}
			m := map[string]PodNodeMapping{}
			for i := 0; i < 150; i++ {
				for _, ns := range []string{"a", "b"} {
					p := pod(ns, fmt.Sprintf("p-%d", i), "")
					objects = append(objects, p)
					m[ns+"/"+p.Name] = PodNodeMapping{Namespace: ns, NodeName: "node-" + ns}
				}
			}
			c := fake.NewSimpleClientset(objects...)
			var active, peak, calls atomic.Int32
			client := bindingClient{c, func(ctx context.Context, b *corev1.Binding) error {
				n := active.Add(1)
				defer active.Add(-1)
				calls.Add(1)
				for {
					old := peak.Load()
					if n <= old || peak.CompareAndSwap(old, n) {
						break
					}
				}
				select {
				case <-time.After(time.Millisecond):
				case <-ctx.Done():
					return ctx.Err()
				}
				if b.Target.Name != "node-"+b.Namespace {
					return fmt.Errorf("wrong target")
				}
				return observe(c, b)
			}}
			if err := runTest(t, client, m, workers); err != nil {
				t.Fatal(err)
			}
			if peak.Load() > int32(workers) || peak.Load() == 0 || (workers > 1 && peak.Load() < 2) || calls.Load() != 300 {
				t.Fatalf("peak=%d calls=%d", peak.Load(), calls.Load())
			}
			for key, want := range m {
				parts := strings.Split(key, "/")
				p, err := c.CoreV1().Pods(parts[0]).Get(context.Background(), parts[1], metav1.GetOptions{})
				if err != nil || p.Spec.NodeName != want.NodeName {
					t.Fatalf("%s: %v %v", key, p, err)
				}
			}
		})
	}
}
func TestRetryAndPermanentFailure(t *testing.T) {
	for _, mode := range []string{"transient", "permanent", "exhausted"} {
		t.Run(mode, func(t *testing.T) {
			p := pod("a", "p", "")
			c := fake.NewSimpleClientset(p)
			var calls atomic.Int32
			client := bindingClient{c, func(_ context.Context, b *corev1.Binding) error {
				n := calls.Add(1)
				if mode == "permanent" {
					return apierrors.NewForbidden(schema.GroupResource{Resource: "pods"}, b.Name, errors.New("denied"))
				}
				if n == 1 || mode == "exhausted" {
					return apierrors.NewServiceUnavailable("retry")
				}
				return observe(c, b)
			}}
			err := runTest(t, client, placement(p), 2)
			if mode == "transient" {
				if err != nil || calls.Load() != 2 {
					t.Fatalf("%v calls=%d", err, calls.Load())
				}
			} else {
				if err == nil || !strings.Contains(err.Error(), "bind pod a/p to node-a") {
					t.Fatal(err)
				}
				expected := int32(1)
				if mode == "exhausted" {
					expected = 6
				}
				if calls.Load() != expected {
					t.Fatal(calls.Load())
				}
			}
		})
	}
}
func TestTimeoutMissingOrUnconfirmed(t *testing.T) {
	for _, present := range []bool{false, true} {
		t.Run(fmt.Sprint(present), func(t *testing.T) {
			p := pod("a", "p", "")
			objects := []runtime.Object{}
			if present {
				objects = append(objects, p)
			}
			c := fake.NewSimpleClientset(objects...)
			var calls atomic.Int32
			client := bindingClient{c, func(context.Context, *corev1.Binding) error { calls.Add(1); return nil }}
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			err := runScheduler(ctx, client, placement(p), 2)
			if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "a/p->node-a") {
				t.Fatal(err)
			}
			expected := int32(0)
			if present {
				expected = 1
			}
			if calls.Load() != expected {
				t.Fatal(calls.Load())
			}
		})
	}
}
func TestCancellationJoinsWorkers(t *testing.T) {
	p := pod("a", "p", "")
	c := fake.NewSimpleClientset(p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{})
	exited := make(chan struct{})
	client := bindingClient{c, func(ctx context.Context, _ *corev1.Binding) error {
		close(entered)
		<-ctx.Done()
		close(exited)
		return ctx.Err()
	}}
	done := make(chan error, 1)
	go func() { done <- runScheduler(ctx, client, placement(p), 2) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("bind never started")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("workers did not stop")
	}
	select {
	case <-exited:
	default:
		t.Fatal("bind outlived scheduler")
	}
}
func TestCancellationDuringBackoff(t *testing.T) {
	p := pod("a", "p", "")
	c := fake.NewSimpleClientset(p)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	entered := make(chan struct{}, 1)
	client := bindingClient{c, func(context.Context, *corev1.Binding) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		return apierrors.NewServiceUnavailable("retry")
	}}
	done := make(chan error, 1)
	go func() { done <- runScheduler(ctx, client, placement(p), 1) }()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("no bind")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("backoff ignored cancellation")
	}
}

type stalledClient struct{ kubernetes.Interface }
type stalledCore struct{ typed.CoreV1Interface }
type stalledPods struct{ typed.PodInterface }

func (c stalledClient) CoreV1() typed.CoreV1Interface { return stalledCore{c.Interface.CoreV1()} }
func (c stalledCore) Pods(ns string) typed.PodInterface {
	return stalledPods{c.CoreV1Interface.Pods(ns)}
}
func (c stalledPods) List(ctx context.Context, _ metav1.ListOptions) (*corev1.PodList, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}
func TestCancellationDuringInitialList(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		done <- runScheduler(ctx, stalledClient{fake.NewSimpleClientset()}, placement(pod("a", "p", "")), 1)
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("list outlived deadline")
	}
}
func TestInvalidWorkersAndEmptyMapping(t *testing.T) {
	for _, workers := range []int{0, 9} {
		if err := runScheduler(context.Background(), nil, nil, workers); err == nil {
			t.Fatal("accepted invalid worker count")
		}
	}
	if err := runScheduler(context.Background(), nil, nil, 1); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runScheduler(ctx, nil, nil, 1); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestBindResponseDoesNotHideWrongObservedDestination(t *testing.T) {
	p := pod("a", "p", "")
	c := fake.NewSimpleClientset(p)
	client := bindingClient{c, func(_ context.Context, b *corev1.Binding) error {
		copy := b.DeepCopy()
		copy.Target.Name = "wrong"
		return observe(c, copy)
	}}
	err := runTest(t, client, placement(p), 2)
	if err == nil || !strings.Contains(err.Error(), "expected node node-a, observed wrong") {
		t.Fatal(err)
	}
}

func TestSchedulingTimeoutScalesWithPods(t *testing.T) {
	for pods, expected := range map[int]time.Duration{
		0:    600 * time.Second,
		2195: 608750 * time.Millisecond,
		3713: 988250 * time.Millisecond,
	} {
		if got := schedulingTimeout(pods); got != expected {
			t.Fatalf("%d pods: got %s, want %s", pods, got, expected)
		}
	}
	// The scaled deadline covers the mapping even at the old 5 QPS client limit.
	if schedulingTimeout(3713) < time.Duration(3713/5)*time.Second {
		t.Fatal("deadline below the default client-go throughput")
	}
}

func TestParseOptions(t *testing.T) {
	o, err := parseOptions(nil)
	if err != nil || o.workers != 1 || o.timeout != 0 || o.qps != 50 || o.burst != 100 {
		t.Fatalf("defaults: %v %+v", err, o)
	}
	o, err = parseOptions([]string{"--workers", "2", "--timeout", "90s", "--qps", "20", "--burst", "40"})
	if err != nil || o.workers != 2 || o.timeout != 90*time.Second || o.qps != 20 || o.burst != 40 {
		t.Fatalf("explicit: %v %+v", err, o)
	}
	for _, args := range [][]string{{"--workers", "0"}, {"--timeout", "-1s"}, {"--qps", "0"}, {"--burst", "0"}} {
		if _, err := parseOptions(args); err == nil {
			t.Fatalf("%v accepted", args)
		}
	}
}

func TestDeadlineRefusalReportsProgress(t *testing.T) {
	done, refused := pod("a", "p", ""), pod("b", "q", "")
	c := fake.NewSimpleClientset(done, refused)
	var refusals atomic.Int32
	client := bindingClient{c, func(_ context.Context, b *corev1.Binding) error {
		if b.Namespace == "b" {
			refusals.Add(1)
			return fmt.Errorf("client rate limiter Wait returned an error: %w", errors.New("rate: Wait(n=1) would exceed context deadline"))
		}
		return observe(c, b)
	}}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	err := runScheduler(ctx, client, placement(done, refused), 2)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "1/2 pods confirmed") || !strings.Contains(err.Error(), "b/q->node-b") {
		t.Fatal(err)
	}
	if refusals.Load() != 1 {
		t.Fatal(refusals.Load())
	}
}

func TestDeadlineErrorBoundsPendingList(t *testing.T) {
	pods := []*corev1.Pod{}
	for i := 0; i < maxListedPending+5; i++ {
		pods = append(pods, pod(fmt.Sprintf("ns%02d", i), "p", ""))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	err := runScheduler(ctx, fake.NewSimpleClientset(), placement(pods...), 1)
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "0/15 pods confirmed") || !strings.Contains(err.Error(), "(+5 more)") || strings.Contains(err.Error(), "ns14/p") {
		t.Fatal(err)
	}
}
