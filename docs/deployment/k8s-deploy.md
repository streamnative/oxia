# Deploying in Kubernetes with Oxia cluster Helm chart

## Deploying the Oxia cluster

To deploy the Oxia cluster with Helm:

```shell
$ kubectl create namespace oxia

$ git clone https://github.com/streamnative/oxia.git

$ cd oxia

$ helm upgrade --install oxia \
  --namespace oxia \
  --set image.repository=streamnative/oxia \
  --set image.tag=main \
  --set image.pullPolicy=IfNotPresent \
  deploy/charts/oxia-cluster
```

## Monitoring Oxia

Oxia support monitoring through exposing a `ServiceMonitor` profile. If you have already a Prometheus deployment 
int your Kubernetes cluster, you won't need any extra steps. It would directly start collecting monitoring data
from Oxia clusters.

The Helm Chart for the Oxia cluster uses a `monitoringEnabled` flag to decide whether to 
install the service monitor. If you don't have Prometheus installed and don't want to install it, you can set
`monitoringEnabled: false` to skip this part.

Grafana's dashboards are available at [deploy/dashboards](/deploy/dashboards).
These can just be imported in your existing Grafana instance.

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
