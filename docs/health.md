# Health

Oxia uses a new convention supported by Kubernetes v1.24 which is the
[GRPCAction](https://kubernetes.io/docs/reference/generated/kubernetes-api/v1.24/#grpcaction-v1-core) in probes. This
means that Oxia includes the [health-checking](https://github.com/grpc/grpc/blob/master/doc/health-checking.md) gRPC
service. An implementation for this service is provided by the main
[gRPC library](https://pkg.go.dev/google.golang.org/grpc/health). This service is exposed on the same port as the
internal Oxia services.

The health gRPC service takes a service name and returns a status of `SERVING` or `NOT_SERVING`. To get the overall
health of the server, clients should pass an empty string as the service name. Initially Oxia will simply expose the
service without any customization of the status response. As long as the go application is healthy enough to respond
to a request it will return a `SERVING` response. This may change in the future.

### Example Kubernetes container configuration

```yaml
container:
  livenessProbe:
    grpc: 
      port: {{ internalPort }}
      service: ""
  readinessProbe:
    grpc: 
      port: {{ internalPort }}
      service: ""
```
