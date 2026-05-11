package loki

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"go.starlark.net/starlark"
	"go.starlark.net/syntax"
	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

const (
	defaultHookTimeout = 100 * time.Millisecond
	podCacheTTL        = 30 * time.Second
)

// errSkip is returned by execute when the event should be acknowledged
// without being sent — either because the hook script returned None, or
// because the pod was not found and SkipOnPodNotFound is set.
var errSkip = errors.New("hook: skip event")

// errPodNotFound is a sentinel returned by enrichPod when the pod does not
// exist in the Kubernetes API (404). Distinct from transient errors so callers
// can react differently.
var errPodNotFound = errors.New("pod not found")

// hookExecutor runs a Starlark script to potentially modify the Loki config
// per event. The script file is loaded and compiled once at construction;
// the compiled function is reused (it is safe to share across goroutines).
type hookExecutor struct {
	fn         *starlark.Function
	hook       ConfigHook
	logger     *slog.Logger
	kubeClient kubernetes.Interface
	podCache   *podCache
}

// podCache is a short-lived cache for pod lookups to avoid hammering the
// Kubernetes API for repeated events on the same pod.
type podCache struct {
	mu      sync.Mutex
	entries map[string]podCacheEntry
}

type podCacheEntry struct {
	pod       *corev1.Pod
	expiresAt time.Time
}

func newPodCache() *podCache {
	return &podCache{entries: make(map[string]podCacheEntry)}
}

func (c *podCache) get(namespace, name string) (*corev1.Pod, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.entries[namespace+"/"+name]
	if !ok || time.Now().After(entry.expiresAt) {
		return nil, false
	}
	return entry.pod, true
}

func (c *podCache) set(namespace, name string, pod *corev1.Pod) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[namespace+"/"+name] = podCacheEntry{
		pod:       pod,
		expiresAt: time.Now().Add(podCacheTTL),
	}
}

// newHookExecutor loads and compiles the Starlark script. The script must
// define a top-level function named "transform".
func newHookExecutor(hook ConfigHook, logger *slog.Logger, kubeClient kubernetes.Interface) (*hookExecutor, error) {
	thread := &starlark.Thread{Name: "kigeon-hook-init"}
	globals, err := starlark.ExecFileOptions(&syntax.FileOptions{}, thread, hook.Script, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("loading starlark hook %q: %w", hook.Script, err)
	}

	val, ok := globals["transform"]
	if !ok {
		return nil, fmt.Errorf("starlark hook %q must define a \"transform\" function", hook.Script)
	}
	fn, ok := val.(*starlark.Function)
	if !ok {
		return nil, fmt.Errorf("starlark hook %q: \"transform\" must be a function, got %s", hook.Script, val.Type())
	}

	return &hookExecutor{
		fn:         fn,
		hook:       hook,
		logger:     logger,
		kubeClient: kubeClient,
		podCache:   newPodCache(),
	}, nil
}

// execute calls the Starlark transform function with the current config and
// event, returning the (potentially modified) config. The base config's Hook
// field is always preserved in the result (scripts cannot change hook settings).
func (h *hookExecutor) execute(ctx context.Context, cfg Config, event *corev1.Event) (Config, error) {
	timeout := h.hook.Timeout
	if timeout == 0 {
		timeout = defaultHookTimeout
	}

	configDict, err := toStarlarkDict(cfg)
	if err != nil {
		return Config{}, fmt.Errorf("converting config to starlark: %w", err)
	}

	eventDict, err := toStarlarkDict(event)
	if err != nil {
		return Config{}, fmt.Errorf("converting event to starlark: %w", err)
	}

	if h.hook.EnrichPod && event.InvolvedObject.Kind == "Pod" {
		if err := h.enrichPod(ctx, eventDict, event.InvolvedObject.Namespace, event.InvolvedObject.Name); err != nil {
			if errors.Is(err, errPodNotFound) && h.hook.SkipOnPodNotFound {
				return Config{}, errSkip
			}
			h.logger.Debug("failed to enrich event with pod definition",
				slog.String("pod", event.InvolvedObject.Namespace+"/"+event.InvolvedObject.Name),
				slog.String("error", err.Error()),
			)
		}
	}

	thread := &starlark.Thread{
		Name: "kigeon-hook",
		Print: func(_ *starlark.Thread, msg string) {
			h.logger.Debug("starlark hook print", slog.String("message", msg))
		},
	}

	// Enforce timeout via cooperative cancellation.
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	go func() {
		<-ctx.Done()
		thread.Cancel("hook timed out")
	}()

	result, err := starlark.Call(thread, h.fn, starlark.Tuple{configDict, eventDict}, nil)
	if err != nil {
		return Config{}, fmt.Errorf("starlark hook execution failed: %w", err)
	}

	if result == starlark.None {
		return Config{}, errSkip
	}

	resultDict, ok := result.(*starlark.Dict)
	if !ok {
		return Config{}, fmt.Errorf("starlark hook must return a dict, got %s", result.Type())
	}

	resultCfg, err := fromStarlarkDict[Config](resultDict)
	if err != nil {
		return Config{}, fmt.Errorf("converting starlark result to config: %w", err)
	}

	// Scripts cannot alter hook settings.
	resultCfg.Hook = cfg.Hook

	return resultCfg, nil
}

// enrichPod fetches the full pod definition (using the cache) and sets it as
// the "pod" key on the event dict. Errors are non-fatal; callers log and
// proceed without the pod definition.
func (h *hookExecutor) enrichPod(ctx context.Context, eventDict *starlark.Dict, namespace, name string) error {
	pod, ok := h.podCache.get(namespace, name)
	if !ok {
		var err error
		pod, err = h.kubeClient.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			if k8serrors.IsNotFound(err) {
				return errPodNotFound
			}
			return fmt.Errorf("getting pod %s/%s: %w", namespace, name, err)
		}
		h.podCache.set(namespace, name, pod)
	}

	podDict, err := toStarlarkDict(pod)
	if err != nil {
		return fmt.Errorf("converting pod to starlark: %w", err)
	}
	return eventDict.SetKey(starlark.String("pod"), podDict)
}

// toStarlarkDict serializes v to JSON, then deserializes to a *starlark.Dict.
func toStarlarkDict(v any) (*starlark.Dict, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("json marshal: %w", err)
	}

	var m map[string]any
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, fmt.Errorf("json unmarshal to map: %w", err)
	}

	return goMapToStarlarkDict(m)
}

// fromStarlarkDict converts a *starlark.Dict back to a Go type via JSON.
func fromStarlarkDict[T any](d *starlark.Dict) (T, error) {
	var zero T

	m, err := starlarkDictToGoMap(d)
	if err != nil {
		return zero, err
	}

	data, err := json.Marshal(m)
	if err != nil {
		return zero, fmt.Errorf("json marshal: %w", err)
	}

	var result T
	if err := json.Unmarshal(data, &result); err != nil {
		return zero, fmt.Errorf("json unmarshal: %w", err)
	}

	return result, nil
}

func goMapToStarlarkDict(m map[string]any) (*starlark.Dict, error) {
	d := starlark.NewDict(len(m))
	for k, v := range m {
		sv, err := goValueToStarlark(v)
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", k, err)
		}
		if err := d.SetKey(starlark.String(k), sv); err != nil {
			return nil, err
		}
	}
	return d, nil
}

func goValueToStarlark(v any) (starlark.Value, error) {
	switch v := v.(type) {
	case nil:
		return starlark.None, nil
	case bool:
		return starlark.Bool(v), nil
	case float64:
		return starlark.Float(v), nil
	case string:
		return starlark.String(v), nil
	case map[string]any:
		return goMapToStarlarkDict(v)
	case []any:
		elems := make([]starlark.Value, len(v))
		for i, e := range v {
			var err error
			elems[i], err = goValueToStarlark(e)
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
		}
		return starlark.NewList(elems), nil
	default:
		return nil, fmt.Errorf("unsupported type %T", v)
	}
}

func starlarkDictToGoMap(d *starlark.Dict) (map[string]any, error) {
	m := make(map[string]any, d.Len())
	for _, item := range d.Items() {
		key, ok := starlark.AsString(item[0])
		if !ok {
			return nil, fmt.Errorf("non-string dict key: %s", item[0].Type())
		}
		val, err := starlarkValueToGo(item[1])
		if err != nil {
			return nil, fmt.Errorf("key %q: %w", key, err)
		}
		m[key] = val
	}
	return m, nil
}

func starlarkValueToGo(v starlark.Value) (any, error) {
	switch v := v.(type) {
	case starlark.NoneType:
		return nil, nil
	case starlark.Bool:
		return bool(v), nil
	case starlark.Int:
		i, ok := v.Int64()
		if !ok {
			return nil, fmt.Errorf("int value overflows int64")
		}
		return i, nil
	case starlark.Float:
		return float64(v), nil
	case starlark.String:
		return string(v), nil
	case *starlark.Dict:
		return starlarkDictToGoMap(v)
	case *starlark.List:
		elems := make([]any, v.Len())
		for i := range elems {
			var err error
			elems[i], err = starlarkValueToGo(v.Index(i))
			if err != nil {
				return nil, fmt.Errorf("[%d]: %w", i, err)
			}
		}
		return elems, nil
	default:
		return nil, fmt.Errorf("unsupported starlark type %s", v.Type())
	}
}
