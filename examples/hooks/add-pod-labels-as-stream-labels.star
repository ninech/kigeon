# Add selected pod labels as Loki stream labels.
# Useful for filtering logs by team, environment, or other pod-level metadata.
#
# Requires enrichPod: true in the hook config.
#
# Config:
#   hook:
#     script: examples/hooks/add-pod-labels-as-stream-labels.star
#     enrichPod: true
#     onError: use-default

# Pod labels to forward as stream labels.
FORWARDED_LABELS = ["team", "app", "environment"]

def transform(config, event):
    pod_labels = (event.get("pod") or {}).get("metadata", {}).get("labels") or {}

    stream_labels = config.get("streamLabels") or {}
    for key in FORWARDED_LABELS:
        if key in pod_labels:
            stream_labels[key] = pod_labels[key]
    config["streamLabels"] = stream_labels

    return config
