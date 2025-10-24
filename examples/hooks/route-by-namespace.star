# Route events to different Loki tenants based on the event's namespace.
#
# Config:
#   hook:
#     script: examples/hooks/route-by-namespace.star
#     onError: use-default

TENANT_MAP = {
    "production": "prod",
    "staging": "staging",
}

def transform(config, event):
    ns = (event.get("metadata") or {}).get("namespace", "")
    tenant = TENANT_MAP.get(ns)
    if tenant:
        config["tenantID"] = tenant
    return config
