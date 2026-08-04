data-generator.tar: data-generator/generator data-generator/requirements.txt data-generator/Dockerfile data-generator/main.py
	cd data-generator && \
	docker build -t data-generator:latest .
	docker save -o data-generator.tar data-generator:latest

.PHONY: apply
apply: data-generator.tar
	eval $$(minikube docker-env) \
		&& docker load --input data-generator.tar
	kubectl apply -k kube --server-side --force-conflicts
