.PHONY: init-dev plan-dev deploy-dev

init-dev:
	cd iac/environments/dev && terraform init

plan-dev: init-dev
	cd iac/environments/dev && terraform plan

deploy-dev: init-dev
	cd iac/environments/dev && terraform apply