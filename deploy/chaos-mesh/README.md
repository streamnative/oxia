# Run chaos-mesh in KinD

## Create KinD Kubernetes cluster

```shell
kind create cluster
```

## Install chaos-mesh

```shell
kubectl create namespace chaos-mesh

helm repo add chaos-mesh https://charts.chaos-mesh.org
helm upgrade --install chaos-mesh \
  --namespace chaos-mesh \
  --version 2.5.1 \
  chaos-mesh/chaos-mesh
```

## Load Oxia image

```shell
make docker
kind load docker-image oxia:latest
```

## Create Oxia cluster

```shell
kubectl create namespace oxia

helm upgrade --install oxia \
  --namespace oxia \
  --set image.repository=oxia \
  --set image.tag=latest \
  --set image.pullPolicy=Never \
  deploy/charts/oxia-cluster
```

## Run perf

```shell
kubectl run perf \
  --namespace oxia \
  --image oxia:latest \
  --image-pull-policy Never \
  --command -- oxia perf --service-address=oxia:6648
```

## Run chaos experiment

```shell
kubectl apply --namespace oxia -f deploy/chaos-mesh/chaos-mesh.yaml 
```
