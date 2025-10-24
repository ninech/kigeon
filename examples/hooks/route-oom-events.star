# Send OOMKilled events to a dedicated Loki tenant for easier alerting.
# All other events are forwarded unchanged.
#
# Config:
#   hook:
#     script: examples/hooks/route-oom-events.star
#     onError: use-default

OOM_TENANT = "oom-alerts"

def transform(config, event):
    if event.get("reason") == "OOMKilling":
        config["tenantID"] = OOM_TENANT
    return config
