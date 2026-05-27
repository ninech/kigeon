# kigeon Helm Chart

Deploys [kigeon](https://github.com/ninech/kigeon), a tool for sending Kubernetes events to configurable receivers.

## Installation

```bash
helm repo add kigeon https://ninech.github.io/kigeon
helm repo update
helm install kigeon kigeon/kigeon
```

## Configuration

All kigeon configuration lives under the `config` key and is rendered verbatim into a ConfigMap. See the [kigeon documentation](https://github.com/ninech/kigeon) for all available options.

### Example: send events to Loki

```yaml
config:
  eventsenders:
    - name: loki
      type: loki
      config:
        url: http://loki.monitoring.svc:3100
```

### Persistence

By default, the NATS data directory uses an `emptyDir`, meaning deduplication state is lost on pod restart and kigeon may re-send events that were already forwarded. Enable persistence for production use:

```yaml
persistence:
  enabled: true
  size: 1Gi
```

### Passing secrets via environment variables

Use `extraEnv` to inject secrets (e.g. credentials for a sender):

```yaml
extraEnv:
  - name: LOKI_PASSWORD
    valueFrom:
      secretKeyRef:
        name: loki-credentials
        key: password
```

## Values

| Key | Default | Description |
|-----|---------|-------------|
| `image.repository` | `ghcr.io/ninech/kigeon` | Image repository |
| `image.tag` | `""` | Image tag (defaults to `.Chart.AppVersion`) |
| `image.pullPolicy` | `IfNotPresent` | Image pull policy |
| `serviceAccount.create` | `true` | Create a ServiceAccount |
| `persistence.enabled` | `false` | Enable persistent storage for NATS data |
| `persistence.size` | `1Gi` | PVC size |
| `persistence.storageClass` | `""` | StorageClass name (uses cluster default when empty) |
| `persistence.existingClaim` | `""` | Use an existing PVC instead of creating one |
| `extraEnv` | `[]` | Extra environment variables injected into the container |
| `resources` | `{}` | Container resource requests and limits |
| `nodeSelector` | `{}` | Node selector |
| `tolerations` | `[]` | Tolerations |
| `affinity` | `{}` | Affinity rules |
