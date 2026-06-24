.PHONY: init-dev validate-dev plan-dev deploy-dev destroy-dev deploy-lambda-dev build-glue-libs deploy-glue-dev deploy-security-dev deploy-security-base-dev

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

deploy-security-base-dev:
	cd iac/environments/dev && TF_VAR_hf_token="$$HF_TOKEN" terraform plan \
		-target=module.security_base \
		-out=tfplan_security && \
	TF_VAR_hf_token="$$HF_TOKEN" terraform apply tfplan_security

build-glue-libs:
	rm -f dist/glue_src.zip
	mkdir -p dist
	cd src && zip -r ../dist/glue_src.zip . \
		--exclude "*__pycache__*" \
		--exclude "*.pyc" \
		--exclude "*.egg-info*"

deploy-glue-dev: build-glue-libs validate-dev
	cd iac/environments/dev && TF_VAR_hf_token="$$HF_TOKEN" terraform plan \
		-target=aws_s3_object.glue_src_zip \
		-target=aws_s3_object.glue_discovery_script \
		-target=module.glue_discovery_job \
		-out=tfplan && \
	TF_VAR_hf_token="$$HF_TOKEN" terraform apply tfplan