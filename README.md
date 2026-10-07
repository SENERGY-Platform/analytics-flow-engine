# analytics-flow-engine

Generate swagger docs:

    swag init -g api.go -o docs -dir pkg/api --parseDependency --ot json

## Operator resources

Every operator container gets limits of 500m CPU and 512Mi memory and requests of 100m CPU and 128Mi memory. `OPERATOR_RESOURCES` (`operator_resources` in the config file) overrides them per image. It is a JSON object: the key is the image repository without tag or digest, the value holds any of `memory_limit`, `memory_request`, `cpu_limit` and `cpu_request` as Kubernetes quantities. A field left out keeps its default, and an image that is not listed keeps all of them.

    OPERATOR_RESOURCES='{"ghcr.io/senergy-platform/consumption-forecast-operator":{"memory_limit":"2Gi","memory_request":"1Gi"}}'

The service refuses to start if a quantity does not parse, a request is above its limit (defaults included, so `memory_request` of `1Gi` alone is rejected), or a key carries a tag or digest. A registry port in the key (`registry:5000/team/operator`) is fine.
