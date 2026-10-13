# analytics-flow-engine

Generate swagger docs:

    swag init -g api.go -o docs -dir pkg/api --parseDependency --ot json

## Operator resources

Every operator container gets limits of 500m CPU and 512Mi memory and requests of 25m CPU and 128Mi memory. `OPERATOR_RESOURCES` (`operator_resources` in the config file) overrides them per image. It is a JSON object: the key is the image repository without tag or digest, the value holds any of `memory_limit`, `memory_request`, `cpu_limit` and `cpu_request` as Kubernetes quantities. A field left out keeps its default, and an image that is not listed keeps all of them.

    OPERATOR_RESOURCES='{"ghcr.io/senergy-platform/consumption-forecast-operator":{"memory_limit":"2Gi","memory_request":"1Gi"}}'

The service refuses to start if a quantity does not parse, a request is above its limit (defaults included, so `memory_request` of `1Gi` alone is rejected), or a key carries a tag or digest. A registry port in the key (`registry:5000/team/operator`) is fine.

## Operator timescale DSN

Cloud operators with a trusted image read history through a timescale DSN they get as the env var `TS_CONN`, by `secretKeyRef` to a Secret in the pipelines namespace. The engine only names the Secret and never holds the DSN, so it shows neither in the operator Deployments nor in the operator config (`CONFIG`); a `ts_conn` in a flow's node config is dropped. Fog operators get no DSN.

Any user can register an operator with an image of their own, so only images whose reference starts with a trusted prefix get `TS_CONN`; every other operator runs without it and has no database access. The prefix is matched against the image reference exactly as the operator stores it, without normalisation: a trusted image has to be written fully qualified (`ghcr.io/senergy-platform/adder:1`, not `senergy-platform/adder:1`). Each operator start logs at info level whether the operator got `TS_CONN` (`operator timescale access`, with operator id and image).

- `OPERATOR_TIMESCALE_SECRET_NAME` (`operator_timescale_secret_name`): name of the Secret. Required, the service refuses to start without it.
- `OPERATOR_TIMESCALE_SECRET_KEY` (`operator_timescale_secret_key`): key of the DSN in the Secret, default `TS_CONN`.
- `OPERATOR_TIMESCALE_TRUSTED_IMAGE_PREFIXES` (`operator_timescale_trusted_image_prefixes`): JSON array of image reference prefixes whose operators get `TS_CONN`, default `["ghcr.io/senergy-platform/"]`. Each prefix has to end in `/`, so `ghcr.io/senergy-platform/` does not also trust `ghcr.io/senergy-platform-x/`; the service refuses to start otherwise. `[]` gives no operator the DSN.

The Secret is provisioned with the deployment, not by the engine. The reference is not optional: if the Secret or key is missing, the operator pods fail to start and their events name it.
