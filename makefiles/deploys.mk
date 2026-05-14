.PHONY: init-dev validate-dev plan-dev deploy-dev destroy-dev

include .env
export

init-dev:
	cd iac/environments/dev && terraform init

validate-dev: init-dev
	cd iac/environments/dev && TF_VAR_hf_token="$$HF_TOKEN" terraform validate

plan-dev: validate-dev
	cd iac/environments/dev && TF_VAR_hf_token="$$HF_TOKEN" terraform plan -out=tfplan

deploy-dev: plan-dev
	cd iac/environments/dev && TF_VAR_hf_token="$$HF_TOKEN" terraform apply tfplan

destroy-dev:
	cd iac/environments/dev && TF_VAR_hf_token="$$HF_TOKEN" terraform destroy