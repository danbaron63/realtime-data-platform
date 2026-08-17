data-generator.tar: images/data-generator images/data-generator/generator
	docker build -t data-generator:latest images/data-generator
	docker save -o data-generator.tar data-generator:latest

provisioner.tar: images/provisioner images/provisioner/pinot_config
	docker build -t provisioner:latest images/provisioner
	docker save -o provisioner.tar provisioner:latest

spark-jobs.tar: images/spark-jobs
	docker build -t spark-jobs:latest images/spark-jobs
	docker save -o spark-jobs.tar spark-jobs:latest

dbt.tar: images/dbt images/dbt images/dbt
	docker build -t dbt:latest images/dbt
	docker save -o dbt.tar dbt:latest

pinot-load.tar: images/pinot-load
	docker build -t pinot-load:latest images/pinot-load
	docker save -o pinot-load.tar pinot-load:latest

feature-store.tar: images/feature_store images/feature_store/feature_config
	docker build -t feature-store:latest images/feature_store
	docker save -o feature-store.tar feature-store:latest

simulation.tar: images/simulation
	docker build -t simulation:latest images/simulation
	docker save -o simulation.tar simulation:latest

.PHONY: apply
apply: data-generator.tar provisioner.tar spark-jobs.tar dbt.tar pinot-load.tar feature-store.tar simulation.tar
	eval $$(minikube docker-env) \
		&& docker load --input provisioner.tar \
		&& docker load --input spark-jobs.tar \
		&& docker load --input data-generator.tar \
		&& docker load --input pinot-load.tar \
		&& docker load --input feature-store.tar \
		&& docker load --input simulation.tar \
		&& docker load --input dbt.tar
	kubectl apply -k kube --server-side --force-conflicts

.PHONY: render-helm
render-helm:
	./helm/render-helm.sh
