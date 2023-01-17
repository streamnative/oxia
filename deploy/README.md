
# Set up a test environment in AWS

## Prerequisites

AWS credentials, config and permissions

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

## Configure AWS EBS CSI Driver

```shell
eksctl utils associate-iam-oidc-provider \
  --region=us-west-2 \
  --cluster=oxia-test \
  --approve

eksctl create iamserviceaccount \
  --name ebs-csi-controller-sa \
  --namespace kube-system \
  --cluster oxia-test \
  --attach-policy-arn arn:aws:iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy \
  --approve \
  --role-only \
  --role-name AmazonEKS_EBS_CSI_DriverRole \
  --region us-west-2

eksctl create addon \
  --name aws-ebs-csi-driver \
  --cluster oxia-test \
  --service-account-role-arn arn:aws:iam::598203581484:role/AmazonEKS_EBS_CSI_DriverRole \
  --force \
  --region us-west-2
```

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

If you need to use a custom image in ECR then you'll need to configure appropriately:

#### Set some basic variables

```shell
AWS_ACCOUNT=598203581484
REGISTRY=$AWS_ACCOUNT.dkr.ecr.us-west-2.amazonaws.com
NAME=oxia
TAG=latest
IMAGE=$NAME:$TAG
REPOSITORY=$REGISTRY/$NAME
```

#### Publish Oxia Docker Image

:notebook: Only applicable if using ECR

```shell
aws ecr create-repository --repository-name oxia --region us-west-2

aws ecr get-login-password --region us-west-2 | docker login --username AWS --password-stdin $REGISTRY

docker build --platform linux/x86_64 -t $NAME:$TAG .
docker tag $NAME:$TAG $REPOSITORY:$TAG
docker push $REPOSITORY:$TAG
```

#### Create Docker registry secret

```shell
kubectl create secret docker-registry oxia \
  --docker-server=$REGISTRY \
  --docker-username=AWS \
  --docker-password=$(aws ecr get-login-password --region us-west-2) \
  --namespace oxia
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

## Create an unmanaged Oxia Cluster

```shell
helm upgrade --install oxia-cluster \
  --namespace oxia \
  --set image.repository=$REPOSITORY \
  --set image.tag=$TAG \
  --set image.pullPolicy=Always \
  --set imagePullSecrets=oxia \
  --set storageClassName=gp2 \
  --set serviceMonitor=true \
  deploy/charts/oxia-cluster
```
