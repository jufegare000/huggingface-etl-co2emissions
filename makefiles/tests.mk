.PHONY: test

IAC_ROOT := iac
N_PROCS  := $(shell nproc 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)

ifdef mod
    TARGET_MODULES := $(IAC_ROOT)/modules/$(mod)
else
    TARGET_MODULES := $(wildcard $(IAC_ROOT)/modules/*)
endif

test:
	@echo "Preparing tests : $(if $(mod),$(mod), all modules)..."
	@printf "%s\n" $(TARGET_MODULES) | xargs -I {} -P $(N_PROCS) /bin/bash -c \
		'if [ -d "{}/tests" ]; then \
			echo "Testing: {}"; \
			(cd {} && terraform init -backend=false > /dev/null && terraform test) || exit 1; \
		else \
			if [ "$(mod)" != "" ]; then echo "Error: Module {} does not have tests directory"; exit 1; fi \
		fi'