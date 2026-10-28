# kigeon

<img src="logo.png" width="200" />

kigeon (like the Pigeon, but with 'k') is a Kubernetes event exporter which
allows to send these events to a long term storage.

Kubernetes events are short-lived by default (1 hour TTL). kigeon watches for
events cluster-wide, persists them in an internal queue, and forwards them to
configurable backends. It is designed to survive restarts without re-sending
events that have already been delivered.

## Features

- **Persistent queue** — events are buffered in an embedded NATS JetStream
  store so no events are lost on restart
- **Deduplication** — events are deduplicated both in-flight and across
  restarts, so each event is sent exactly once
- **Namespace filtering** — senders can be scoped to namespaces matching a
  label selector, which is kept up-to-date dynamically
- **Per-event hooks** — a [Starlark](https://github.com/bazelbuild/starlark)
  script can modify the sender config on a per-event basis (e.g. routing to
  different tenants, adding labels)
- **Multiple senders** — multiple sender instances can run in parallel, each
  with their own filter and configuration

## Supported backends

| Type | Description |
|------|-------------|
| `loki` | [Grafana Loki](https://grafana.com/oss/loki/) |

## Installation

### Helm

```bash
helm repo add kigeon https://ninech.github.io/kigeon
helm repo update

# Example installation with a Loki sender and persistent storage:
helm install kigeon kigeon/kigeon \
  --namespace kigeon --create-namespace \
  --set persistence.enabled=true \
  --set config.eventsenders[0].name=loki \
  --set config.eventsenders[0].type=loki \
  --set config.eventsenders[0].config.url=http://loki:3100
```

For a full list of configuration options see [`deploy/helm/kigeon/values.yaml`](deploy/helm/kigeon/values.yaml).

### Container image

Container images are published to `ghcr.io/ninech/kigeon` for each release.
See the [releases page](https://github.com/ninech/kigeon/releases) for available versions.

```bash
docker pull ghcr.io/ninech/kigeon:0.0.4
```

## Configuration

kigeon is configured via a YAML file passed with `--config`.

```yaml
global:
  # Directory where kigeon stores its queue data.
  # Default: /var/lib/kigeon/data
  dataDir: /var/lib/kigeon/data

  # Log level: debug, info, warn, error. Default: info
  logLevel: info

  # Log format: json, text. Default: json
  logFormat: json

queue:
  # How long undelivered events are kept in the queue. Default: 24h
  eventsMaxAge: 24h

  # Maximum queue size on disk. The data directory needs to provide at least
  # this amount plus 32Mi for the deduplication store. Default: 500Mi
  eventsMaxBytes: 500Mi

  # How long processed event UIDs are remembered (prevents re-sending after
  # restart). Default: 1h
  kubernetesEventsMaxLifetime: 1h

pusher:
  # Timeout for pushing a single event into the queue. Default: 5s
  pushTimeout: 5s

pods:
  # Emit synthetic "OOMKilled" events for containers which were killed due to
  # running out of memory. Default: false
  oomKilledEvents: false

  # How long deleted pods remain available for pod enrichment. Default: 5m
  deletedPodRetention: 5m

eventsenders:
  - name: my-loki
    type: loki

    # Optional: only forward events from namespaces matching these labels.
    filter:
      namespaceSelector:
        matchLabels:
          environment: production
      # Also forward events for cluster-scoped objects (Nodes, etc.).
      includeNonNamespaced: false
      # How often to force-refresh the namespace list. Default: 0 (disabled)
      hardRefreshInterval: 4h

    # Sender-specific configuration (see below).
    config:
      url: http://loki:3100
      tenantID: my-tenant
      streamLabels:
        app: kigeon
        cluster: my-cluster
```

## Loki sender

```yaml
config:
  # Required. Base URL of the Loki instance (e.g. http://loki:3100, without path).
  url: http://loki:3100

  # X-Scope-OrgID header for multi-tenant Loki.
  tenantID: my-tenant

  # Static labels added to every log stream.
  streamLabels:
    app: kigeon

  # Basic authentication. Values can be set directly or via environment
  # variable references (direct values take precedence).
  basicAuth:
    username: user
    password: secret
    # Alternative: read from environment variables at startup.
    usernameEnvVar: LOKI_USER
    passwordEnvVar: LOKI_PASS

  # TLS configuration for the Loki HTTP client.
  tlsConfig:
    # Disable TLS certificate verification (e.g. for self-signed certs).
    insecureSkipVerify: false

  # Optional Starlark hook — see "Hooks" section below.
  hook:
    script: /etc/kigeon/hooks/my-hook.star
    timeout: 100ms       # Max execution time per event. Default: 100ms
    onError: use-default # use-default | skip | fail
    enrichPod: false     # Fetch full Pod definition and expose it as "pod"
    skipOnPodNotFound: true  # Skip event if the involved Pod no longer exists
```

## Hooks

Hooks allow a [Starlark](https://github.com/bazelbuild/starlark) script to
dynamically modify the Loki sender config on a per-event basis — for example
to route events to different tenants, add stream labels, or change credentials.

The script must define a `transform(config, event)` function that receives the
current config and the Kubernetes event as dicts, and returns either the
(potentially modified) config dict or `None` to skip the event entirely.

Returning `None` acknowledges the event without sending it — useful for
filtering out events that do not match your criteria:

```python
def transform(config, event):
    # Skip non-Pod events
    if (event.get("involvedObject") or {}).get("kind") != "Pod":
        return None

    ns = (event.get("metadata") or {}).get("namespace", "")
    if ns == "production":
        config["tenantID"] = "prod"
    return config
```

### Hook config

| Field | Description | Default |
|-------|-------------|---------|
| `script` | Path to the `.star` script file | required |
| `timeout` | Max execution time per event | `100ms` |
| `onError` | Behaviour on script error: `use-default`, `skip`, `fail` | `use-default` |
| `enrichPod` | Fetch the full Pod definition and expose it as `pod` on the event dict | `false` |
| `skipOnPodNotFound` | Skip the event (without calling the hook) if the involved Pod no longer exists. Only meaningful with `enrichPod: true`. Transient API errors still fall through to the hook. | `false` |

### `onError` behaviour

| Value | Description |
|-------|-------------|
| `use-default` | Use the base config and send the event normally |
| `skip` | Acknowledge the event without sending it |
| `fail` | Return an error so the event is redelivered via NATS |

### Pod enrichment

When `enrichPod: true` is set and the involved object is a Pod, kigeon makes the
full Pod definition available as `pod` on the event dict. Pods are served from
a cluster-wide pod informer, which is only started if a sender enables pod
enrichment or `pods.oomKilledEvents` is set.

Deleted pods remain available for `pods.deletedPodRetention`, as events are
often processed after the pod they refer to was deleted (e.g. short-lived job
pods or when catching up after a restart). Pods which the informer did not
observe yet are fetched from the Kubernetes API.

Always guard against the pod being unavailable:

```python
def transform(config, event):
    pod = event.get("pod") or {}
    labels = pod.get("metadata", {}).get("labels") or {}
    ...
```

### Examples

See [`examples/hooks/`](examples/hooks/) for ready-to-use scripts:

| Script | Description |
|--------|-------------|
| [`route-by-namespace.star`](examples/hooks/route-by-namespace.star) | Route events to different Loki tenants based on namespace |
| [`add-pod-labels-as-stream-labels.star`](examples/hooks/add-pod-labels-as-stream-labels.star) | Forward selected pod labels as Loki stream labels |
| [`route-oom-events.star`](examples/hooks/route-oom-events.star) | Send OOMKilling events to a dedicated Loki tenant |

## OOM killed containers

Kubernetes does not emit an event when a container gets killed due to running
out of memory. The kernel OOM kill is only reported as an `OOMKilling` event on
the Node, which can not be attributed to a pod or namespace.

With `pods.oomKilledEvents: true`, kigeon watches the container statuses of all
pods and emits a synthetic Warning event with reason `OOMKilled` for the pod
(source component `kigeon`), for example:

```
Container app was killed because it ran out of memory (memory limit 512Mi)
```

These events are queued like any other event, so namespace filters, hooks and
pod enrichment apply to them as well. OOM kills which happened more than
`queue.kubernetesEventsMaxLifetime` ago (e.g. while kigeon was not running)
are not reported, as they might have been sent already.

## Running

```bash
kigeon --config /etc/kigeon/config.yaml

# Use a local kubeconfig instead of in-cluster credentials
kigeon --config /etc/kigeon/config.yaml --kubeconfig ~/.kube/config

# Override log level
kigeon --config /etc/kigeon/config.yaml --log-level debug
```

## How it works

```
Kubernetes API
     │  (informer)
     ▼
EventPusher       ← watches all events cluster-wide
     │  (deduplicate + publish)
     ▼
EventQueue        ← embedded NATS JetStream (persistent)
     │  (fan-out)
     ├──▶ Sender 1 (loki) ──▶ Loki instance A
     └──▶ Sender 2 (loki) ──▶ Loki instance B
```

Each sender subscribes independently to the queue. An event is removed from
the queue once all senders acknowledged it, so the queue only grows while a
sender can not deliver events (bounded by `eventsMaxAge` and
`eventsMaxBytes`). Subscriptions of senders which were removed from the
configuration are deleted on startup. Namespace filters and hooks
are applied per sender, so different senders can have completely different
routing logic from the same event stream.
