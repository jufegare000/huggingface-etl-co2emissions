.PHONY: init-dev plan-dev deploy-dev

init-dev:
	cd iac/environments/dev && terraform init

validate-dev: init-dev
	cd iac/environments/dev && terraform validate

plan-dev: validate-dev
	cd iac/environments/dev && terraform plan -out=tfplan

deploy-dev: plan-dev
	cd iac/environments/dev && terraform apply tfplan