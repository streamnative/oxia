
# Deploying in Kubernetes with Oxia operator 


## Monitoring Oxia

Oxia support monitoring through exposing a `ServiceMonitor` profile. If you have already a Prometheus deployment 
int your Kubernetes cluster, you won't need any extra steps. It would directly start collecting monitoring data
from Oxia clusters.

The Helm chart for the controller and the Oxia operator both use a `monitoringEnabled` flag to decide whether to 
install the service monitor. If you don't have Prometheus installed and don't want to install it, you can set
`monitoringEnabled: false` to skip this part.

Grafana's dashboards are available at [../deploy/dashboards](). These can just be imported in your existing Grafana
instance.

### Deploying Prometheus Stack

If you don't have already a Prometheus deployment, you can easily add it with Helm.

Prepare:

```shell
$ kubectl create namespace monitoring

$ helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
$ helm repo update
```

#### Deploying the stock Prometheus & Grafana

This installs the stock Prometheus & Grafana, without the Oxia dashboards, which you will be able to manually 
import later.


```shell
$ helm install monitoring prometheus-community/kube-prometheus-stack \
          --namespace monitoring \
          --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false
```

#### Deploying Prometheus & Grafana with Oxia dashboards

To deploy the stack with Oxia's Grafana dashboards pre-configured:

> ***Note***: The unpack method has to be used as the grafana chart utilises Helm's `.Files.Get` to load custom dashboards
which cannot reach files outside the chart directory.

```shell
$ helm pull prometheus-community/kube-prometheus-stack
$ tar -xf kube-prometheus-stack-*.tgz
$ cp deploy/dashboards/*.json kube-prometheus-stack/charts/grafana/dashboards

$ helm upgrade --install monitoring kube-prometheus-stack \
  --namespace monitoring \
  --values deploy/dashboards/values-kube-prometheus-stack.yaml

$ rm -rf kube-prometheus-stack*
```

> ***Note***:  The default login credentials for grafana are admin/prom-operator.

## Deploying the Oxia Operator controller

The Oxia Controller is the operator instance and, once deployed, it will be responsible for creating Oxia clusters
based on the CRDs (Custom Resource Definitions).

To deploy the controller with Helm: 

```shell
$ kubectl create namespace oxia

$ helm upgrade --install oxia-controller \
  --namespace oxia \
  --set monitoringEnabled=true \
  https://github.com/streamnative/oxia/tree/main/deploy/charts/oxia-controller
```


## Deploying an Oxia cluster with the CRD

Now that the Oxia controller is running, we can create K8S resources of type `OxiaCluster`, by submitting a CRD.

An example of CRD can be found at [example-oxia-cluster.yaml](../deploy/examples/example-oxia-cluster.yaml).

The full specification of the CRD is at [oxiaclusters.yaml](../deploy/crds/oxiaclusters.yaml).

A minimal example of CRD is: 

```yaml
apiVersion: oxia.streamnative.io/v1alpha1
kind: OxiaCluster
metadata:
  namespace: oxia
  name: my-oxia-cluster
spec:
  namespaces:
    - name: default
      initialShardCount: 3
      replicationFactor: 3

  coordinator:
    cpu: 100m
    memory: 128Mi
  server:
    replicas: 3
    cpu: 1
    memory: 1Gi
    storage: 8Gi
  image:
    repository: streamnative/oxia
    tag: main
    pullPolicy: Always
  monitoringEnabled: true
  pprofEnabled: false
```

You can save it into a file `oxia-cluster.yaml` and then deploy to Kubernetes:

```shell
$ kubectl apply -f oxia-cluster.yaml
```