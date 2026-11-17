package main

import (
	"context"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/workqueue"
)

type PodNodeMapping struct {
	NodeName  string
	Namespace string
}

func loadPodNodeMapping(path string) (map[string]PodNodeMapping, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	reader := csv.NewReader(file)
	header, err := reader.Read()
	if err != nil {
		return nil, fmt.Errorf("mapping header: %w", err)
	}
	if len(header) != 3 || header[0] != "pod" || header[1] != "namespace" || header[2] != "node" {
		return nil, fmt.Errorf("expected CSV header pod,namespace,node, got %q", header)
	}
	result := make(map[string]PodNodeMapping)
	for line := 2; ; line++ {
		row, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("mapping row %d: %w", line, err)
		}
		for _, field := range row {
			if strings.TrimSpace(field) == "" || strings.TrimSpace(field) != field {
				return nil, fmt.Errorf("mapping row %d: empty or padded field", line)
			}
		}
		if len(validation.IsDNS1123Subdomain(row[0])) != 0 || len(validation.IsDNS1123Label(row[1])) != 0 || len(validation.IsDNS1123Subdomain(row[2])) != 0 {
			return nil, fmt.Errorf("mapping row %d: invalid pod, namespace or node name", line)
		}
		key := row[1] + "/" + row[0]
		mapping := PodNodeMapping{NodeName: row[2], Namespace: row[1]}
		if old, exists := result[key]; exists && old != mapping {
			return nil, fmt.Errorf("conflicting mapping for %s: %s vs %s", key, old.NodeName, mapping.NodeName)
		}
		result[key] = mapping
	}
	return result, nil
}

// runScheduler confirms the entire placement through a synchronized list/watch. The
// unbounded workqueue deduplicates queued and in-flight keys; callbacks never
// block on binds or on workers re-enqueuing retries.
func runScheduler(ctx context.Context, client kubernetes.Interface, mapping map[string]PodNodeMapping, workers int) error {
	if workers < 1 || workers > 8 {
		return fmt.Errorf("workers must be between 1 and 8")
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if len(mapping) == 0 {
		return nil
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Generated informers in this client-go version use context.TODO for
	// list/watch requests. Bind both requests to our real global deadline so
	// cancellation also bounds shutdown during an unresponsive initial list.
	informer := cache.NewSharedIndexInformer(&cache.ListWatch{
		ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
			return client.CoreV1().Pods(metav1.NamespaceAll).List(ctx, options)
		},
		WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
			return client.CoreV1().Pods(metav1.NamespaceAll).Watch(ctx, options)
		},
	}, &corev1.Pod{}, 0, cache.Indexers{})
	queue := workqueue.NewTypedRateLimitingQueue(workqueue.NewTypedItemExponentialFailureRateLimiter[string](100*time.Millisecond, 2*time.Second))
	defer queue.ShutDown()
	enqueue := func(obj interface{}) {
		if tombstone, ok := obj.(cache.DeletedFinalStateUnknown); ok {
			obj = tombstone.Obj
		}
		pod, ok := obj.(*corev1.Pod)
		if !ok {
			return
		}
		key := pod.Namespace + "/" + pod.Name
		if _, exists := mapping[key]; exists {
			queue.Add(key)
		}
	}
	_, err := informer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: enqueue, UpdateFunc: func(_, obj interface{}) { enqueue(obj) }, DeleteFunc: enqueue,
	})
	if err != nil {
		return err
	}
	informerDone := make(chan struct{})
	go func() { defer close(informerDone); informer.Run(ctx.Done()) }()
	defer func() { cancel(); <-informerDone }()
	if !cache.WaitForCacheSync(ctx.Done(), informer.HasSynced) {
		return fmt.Errorf("sync pod cache: %w", ctx.Err())
	}

	var lock sync.Mutex
	confirmed := make(map[string]bool)
	bound := make(map[string]types.UID)
	result := make(chan error, 1)
	finish := func(err error) {
		select {
		case result <- err:
		default:
		}
	}
	process := func(key string) {
		expected := mapping[key]
		obj, exists, err := informer.GetStore().GetByKey(key)
		if err != nil {
			finish(fmt.Errorf("read %s: %w", key, err))
			return
		}
		lock.Lock()
		delete(confirmed, key)
		lock.Unlock()
		if !exists {
			return
		} // A missing pod remains pending until the real deadline.
		pod := obj.(*corev1.Pod)
		if pod.Spec.NodeName != "" {
			if pod.Spec.NodeName != expected.NodeName {
				finish(fmt.Errorf("pod %s: expected node %s, observed %s", key, expected.NodeName, pod.Spec.NodeName))
				return
			}
			lock.Lock()
			confirmed[key] = true
			complete := len(confirmed) == len(mapping)
			lock.Unlock()
			if complete {
				finish(nil)
			}
			queue.Forget(key)
			return
		}
		// A successful bind is not confirmation. Wait for the observed spec update.
		lock.Lock()
		boundUID, alreadyBound := bound[key]
		alreadyBound = alreadyBound && boundUID == pod.UID
		lock.Unlock()
		if alreadyBound {
			return
		}
		binding := &corev1.Binding{
			ObjectMeta: metav1.ObjectMeta{Name: pod.Name, Namespace: pod.Namespace, UID: pod.UID},
			Target:     corev1.ObjectReference{Kind: "Node", Name: expected.NodeName},
		}
		err = client.CoreV1().Pods(pod.Namespace).Bind(ctx, binding, metav1.CreateOptions{})
		if err == nil {
			lock.Lock()
			bound[key] = pod.UID
			lock.Unlock()
			queue.Forget(key)
		} else if ctx.Err() == nil {
			transient := apierrors.IsTimeout(err) || apierrors.IsServerTimeout(err) || apierrors.IsTooManyRequests(err) || apierrors.IsServiceUnavailable(err) || apierrors.IsInternalError(err) || apierrors.IsConflict(err) || apierrors.IsNotFound(err)
			if transient && queue.NumRequeues(key) < 5 {
				queue.AddRateLimited(key)
			} else {
				finish(fmt.Errorf("bind pod %s to %s after %d attempts: %w", key, expected.NodeName, queue.NumRequeues(key)+1, err))
			}
		}
	}
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				key, shutdown := queue.Get()
				if shutdown {
					return
				}
				if ctx.Err() == nil {
					process(key)
				}
				queue.Done(key)
			}
		}()
	}
	// Also covers mappings not delivered as initial Add events (missing pods).
	for key := range mapping {
		queue.Add(key)
	}
waiting:
	for {
		select {
		case err = <-result:
			if err != nil {
				break waiting
			}
			// A final cache snapshot prevents an old confirmation from hiding a
			// deletion or changed destination queued behind another worker.
			observed := make(map[string]string)
			for _, obj := range informer.GetStore().List() {
				pod := obj.(*corev1.Pod)
				observed[pod.Namespace+"/"+pod.Name] = pod.Spec.NodeName
			}
			complete := true
			for key, expected := range mapping {
				node, exists := observed[key]
				if exists && node != "" && node != expected.NodeName {
					err = fmt.Errorf("pod %s: expected node %s, observed %s", key, expected.NodeName, node)
					break waiting
				}
				if !exists || node == "" {
					complete = false
					lock.Lock()
					delete(confirmed, key)
					lock.Unlock()
					queue.Add(key)
				}
			}
			if complete {
				break waiting
			}
		case <-ctx.Done():
			lock.Lock()
			pending := []string{}
			for key, expected := range mapping {
				if !confirmed[key] {
					pending = append(pending, key+"->"+expected.NodeName)
				}
			}
			lock.Unlock()
			sort.Strings(pending)
			err = fmt.Errorf("initial scheduling incomplete (%d/%d), pending %v: %w", len(mapping)-len(pending), len(mapping), pending, ctx.Err())
			break waiting
		}
	}
	// Cancellation takes precedence over a concurrent success event.
	if err == nil && ctx.Err() != nil {
		err = ctx.Err()
	}

	cancel()
	queue.ShutDown()
	wg.Wait()
	return err
}

func run() error {
	path := flag.String("mapping", "./pods_and_nodes_map.csv", "initial placement CSV")
	workers := flag.Int("workers", 1, "maximum concurrent binds (1-8)")
	timeout := flag.Duration("timeout", 600*time.Second, "global real scheduling deadline")
	flag.Parse()
	if *workers < 1 || *workers > 8 || *timeout <= 0 {
		return fmt.Errorf("invalid workers or timeout")
	}
	mapping, err := loadPodNodeMapping(*path)
	if err != nil {
		return err
	}
	config, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return err
	}
	client, err := kubernetes.NewForConfig(config)
	if err != nil {
		return err
	}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(signals, *timeout)
	defer cancel()
	return runScheduler(ctx, client, mapping, *workers)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	fmt.Println("All initial pod destinations confirmed")
}
