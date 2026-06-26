.PHONY: init-dev validate-dev plan-dev deploy-dev destroy-dev deploy-lambda-dev build-glue-libs deploy-glue-dev deploy-raw-ingestion-dev deploy-security-dev deploy-security-base-dev update-hf-token update-hf-token-sm

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

update-hf-token-sm:
	@test -n "$(HF_TOKEN)" || (echo "HF_TOKEN not set in .env" && exit 1)
	cd iac/environments/dev && terraform init && \
	TF_VAR_hf_token="$(HF_TOKEN)" terraform apply \
		-target=module.hf_secrets.aws_secretsmanager_secret_version.this \
		-auto-approve
	@echo "HF_TOKEN updated in Secrets Manager via Terraform"

deploy-glue-dev: build-glue-libs validate-dev
	cd iac/environments/dev && TF_VAR_hf_token="$$HF_TOKEN" terraform plan \
		-target=aws_s3_object.glue_src_zip \
		-target=aws_s3_object.glue_discovery_script \
		-target=module.glue_discovery_job \
		-target=aws_s3_object.glue_script \
		-target=module.glue_ingestion_job \
		-out=tfplan && \
	TF_VAR_hf_token="$$HF_TOKEN" terraform apply tfplan

deploy-raw-ingestion-dev: build-glue-libs validate-dev
	cd iac/environments/dev && TF_VAR_hf_token="$$HF_TOKEN" terraform plan \
		-target=aws_s3_object.glue_src_zip \
		-target=aws_s3_object.glue_script \
		-target=module.glue_ingestion_job \
		-out=tfplan_raw_ingestion && \
	TF_VAR_hf_token="$$HF_TOKEN" terraform apply tfplan_raw_ingestion