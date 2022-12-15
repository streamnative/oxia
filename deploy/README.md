
# Set up a test environment in AWS

## Create an EKS cluster

```shell
eksctl create cluster \
  --name oxia-test \
  --region us-west-2 \
  --version 1.24 \
  --node-type m5a.xlarge \
  --nodes 3 \
  --nodes-min 3 \
  --nodes-max 5
```

:notebook: This also creates a kubeconfig and sets it as the default.

## Install Prometheus Stack

```shell
kubectl create namespace monitoring

helm repo add prometheus-community https://prometheus-community.github.io/helm-charts
helm repo update
helm install monitoring prometheus-community/kube-prometheus-stack --namespace monitoring
```

## Publish Oxia Docker Image

```shell
aws ecr create-repository --repository-name oxia --region us-west-2

AWS_ACCOUNT=598203581484
REGISTRY=$AWS_ACCOUNT.dkr.ecr.us-west-2.amazonaws.com
NAME=oxia
TAG=latest
IMAGE=$NAME:$TAG
REPOSITORY=$REGISTRY/$NAME

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin $REGISTRY

docker build --platform linux/x86_64 -t $NAME:$TAG .
docker tag $NAME:$TAG $REPOSITORY:$TAG
docker push $REPOSITORY:$TAG
```

## Install Oxia Operator

### Install oxiaclusters CRD

```shell
kubectl apply -f deploy/crds/oxiaclusters.yaml
```

### Prepare namespace

```shell
kubectl create namespace oxia

kubectl create secret docker-registry oxia \
  --docker-server=$REGISTRY \
  --docker-username=AWS \
  --docker-password=$(aws ecr get-login-password --region us-west-2) \
  --namespace oxia
  
kubectl apply -f deploy/storage/storageclass.yaml
```

:notebook: The above secret is valid for 12 hours.

### Install Oxia Controller

```shell
helm upgrade --install oxia-controller \
  --namespace oxia \
  --set image.repository=$REPOSITORY \
  --set image.tag=$TAG \
  --set image.pullPolicy=Always \
  --set imagePullSecrets=oxia \
  --set serviceMonitor=true \
  deploy/charts/oxia-controller
```
