.PHONY: test

MODULES := $(wildcard iac/modules/*)

test:
	@echo "Unit testing initializing for IAC"
	@for mod in $(MODULES); do \
		if [ -d "$$mod/tests" ]; then \
			(cd $$mod && terraform init -backend=false > /dev/null && terraform test) || exit 1; \
		fi \
	done