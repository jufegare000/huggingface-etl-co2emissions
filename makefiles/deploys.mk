.PHONY: init-dev validate-dev plan-dev deploy-dev destroy-dev deploy-lambda-dev
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

deploy-lambda-dev:
	cd iac/environments/dev && TF_VAR_hf_token="$$HF_TOKEN" terraform plan \
		-target=module.data_prep_lambda \
		-out=tfplan_lambda && \
	TF_VAR_hf_token="$$HF_TOKEN" terraform apply tfplan_lambda

deploy-security-dev:
	cd iac/environments/dev && TF_VAR_hf_token="$$HF_TOKEN" terraform plan \
		-target=module.security_policies \
		-out=tfplan_security && \
	TF_VAR_hf_token="$$HF_TOKEN" terraform apply tfplan_security