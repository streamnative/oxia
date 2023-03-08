
# Installing an Oxia Cluster and/or Operator

## Install Prometheus Stack

Prepare:

```shell
kubectl create namespace monitoring

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
```

Install an empty prometheus stack:

```shell
helm install monitoring prometheus-community/kube-prometheus-stack \
  --namespace monitoring \
  --set prometheus.prometheusSpec.serviceMonitorSelectorNilUsesHelmValues=false
```

To deploy the stack with grafana dashboards pre-configured:

:notebook: The unpack method has to be used as the grafana chart utilises Helm's `.Files.Get` to load custom dashboards
which cannot reach files outside the chart directory.

```shell
helm pull prometheus-community/kube-prometheus-stack
tar -xf kube-prometheus-stack-*.tgz
cp deploy/dashboards/*.json kube-prometheus-stack/charts/grafana/dashboards

helm upgrade --install monitoring kube-prometheus-stack \
  --namespace monitoring \
  --values deploy/dashboards/values-kube-prometheus-stack.yaml

rm -rf kube-prometheus-stack*
```

:notebook: The default login credentials for grafana are admin/prom-operator.

## Install Oxia Operator

### Install oxiaclusters CRD

```shell
kubectl apply -f deploy/crds/oxiaclusters.yaml
```

### Prepare namespace

```shell
kubectl create namespace oxia
```

### Install Oxia Controller

```shell
helm upgrade --install oxia-controller \
  --namespace oxia \
  --set monitoringEnabled=true \
  deploy/charts/oxia-controller
```

## Create an unmanaged Oxia Cluster

```shell
helm upgrade --install oxia \
  --namespace oxia \
  --set monitoringEnabled=true \
  deploy/charts/oxia-cluster
```

## Run perf

```shell
kubectl run perf --image=streamnative/oxia:main --command -- tail -f /dev/null
kubectl exec -ti perf -- bash

  oxia perf --service-address=oxia:6648
```
