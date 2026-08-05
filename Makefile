data-generator.tar: data-generator/generator data-generator/requirements.txt data-generator/Dockerfile data-generator/main.py
	cd data-generator && \
	docker build -t data-generator:latest .
	docker save -o data-generator.tar data-generator:latest

provisioner.tar: provisioner
	docker build -f provisioner/Dockerfile -t provisioner:latest provisioner
	docker save -o provisioner.tar provisioner:latest

spark-jobs.tar: spark-jobs
	docker build -t spark-jobs:latest spark-jobs
	docker save -o spark-jobs.tar spark-jobs:latest

.PHONY: apply
apply: data-generator.tar provisioner.tar spark-jobs.tar
	eval $$(minikube docker-env) \
		&& docker load --input provisioner.tar \
		&& docker load --input spark-jobs.tar \
		&& docker load --input data-generator.tar
	kubectl apply -k kube --server-side --force-conflicts
