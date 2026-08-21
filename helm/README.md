# Helm

This project is deployed to kubernetes using kustomize (`kubectl apply -k ...`).
However, it also takes advantage of a number of helm charts.
To do this we render the helm charts into `.yaml` files in the [`kube/` directory](../kube).
Those files are rendered by the [`render-helm.sh` script](./render-helm.sh) which is controlled 
by the various `*-values.yaml` files present in this directory.

## How to use
In order to make a change to one of the helm chart deployments, make your change to the 
respective `*-values.yaml` file.
Run `make render-helm` to render the files in the [`kube/` directory](../kube).
If you run `git status` you should ten be able to see which files changed.

## Deploying a new helm chart
To do this you should extend the [`render-helm.sh` script](./render-helm.sh).
