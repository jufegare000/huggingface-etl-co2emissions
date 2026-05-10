# Makefile

.PHONY: test init-all

MODULES := $(wildcard iac/modules/*)

test:
	@echo "Initiating unit testing for the infrastructure"
	@for mod in $(MODULES); do \
		if [ -d "$$mod/tests" ]; then \
			echo "========================================"; \
			echo "Testing module: $$mod"; \
			echo "========================================"; \
			(cd $$mod && terraform init -backend=false > /dev/null && terraform test) || exit 1; \
		fi \
	done
	@echo "All tests passed sucessfully"
