# Small Makefile to wire env + fixtures for DeHashed domain searches
# Usage examples:
#   make fixtures-run DOMAIN=example.com FIXTURES=./fixtures OUT=out/example.csv VERBOSE=1
#   make live-run     DOMAIN=example.com OUT=out/example.csv PREFER_DOMAIN=example.com VERBOSE=1
#   make clean OUT=out/example.csv
#
# Variables (override on the CLI):
#   DOMAIN          - domain to search (required)
#   OUT             - output CSV path (required)
#   FIXTURES        - directory with *.json fixture files (for fixtures-run)
#   PREFER_DOMAIN   - prioritize this domain in the excerpt (optional)
#   COLS            - CSV columns (optional; defaults inside the script)
#   LIMIT           - excerpt limit (default: 10; used by python helper via script)
#   TIMEOUT         - curl timeout per request (default: 30)
#   SLEEP           - base sleep between pages/retries (default: 1)
#   MAX_PAGES       - safety guard for pagination (default: large)
#   VERBOSE         - 1 to enable verbose logging
#
# Files:
#   dehashed_domain_search.sh   - main script (auto-sources .env if present)
#   dehashed_excerpt.py         - robust excerpt/summary helper
#   .env                        - set DEHASHED_USER / DEHASHED_KEY (see .env.example)

SHELL := /bin/bash
SCRIPT ?= ./dehashed_domain_search.sh

# Propagate VERBOSE into -v flag
ifeq ($(VERBOSE),1)
  VFLAG := -v
else
  VFLAG :=
endif

# Optional flags
ifdef COLS
  CFLAG := -C "$(COLS)"
else
  CFLAG :=
endif

ifdef PREFER_DOMAIN
  PDFLAG := --prefer-domain "$(PREFER_DOMAIN)"
else
  PDFLAG :=
endif

ifdef TIMEOUT
  TOFLAG := --timeout $(TIMEOUT)
else
  TOFLAG :=
endif

ifdef SLEEP
  SLFLAG := --sleep $(SLEEP)
else
  SLFLAG :=
endif

ifdef MAX_PAGES
  MPFLAG := --max-pages $(MAX_PAGES)
else
  MPFLAG :=
endif

.PHONY: help env-check fixtures-run live-run clean show-vars

help:
	@echo "Targets:"
	@echo "  make fixtures-run DOMAIN=example.com FIXTURES=./fixtures OUT=out/example.csv [PREFER_DOMAIN=example.com] [VERBOSE=1]"
	@echo "  make live-run     DOMAIN=example.com OUT=out/example.csv [PREFER_DOMAIN=example.com] [VERBOSE=1]"
	@echo "  make env-check    # verifies DEHASHED_USER/DEHASHED_KEY from environment or .env"
	@echo "  make clean OUT=out/example.csv"
	@echo ""
	@$(MAKE) show-vars --no-print-directory

show-vars:
	@echo "DOMAIN=$(DOMAIN)"
	@echo "OUT=$(OUT)"
	@echo "FIXTURES=$(FIXTURES)"
	@echo "PREFER_DOMAIN=$(PREFER_DOMAIN)"
	@echo "COLS=$(COLS)"
	@echo "TIMEOUT=$(TIMEOUT) SLEEP=$(SLEEP) MAX_PAGES=$(MAX_PAGES) VERBOSE=$(VERBOSE)"

env-check:
	@set -euo pipefail; \
	if [[ -f ".env" ]]; then source ./.env; fi; \
	: $${DEHASHED_USER:?DEHASHED_USER not set (set in environment or in .env)}; \
	: $${DEHASHED_KEY:?DEHASHED_KEY not set (set in environment or in .env)}; \
	echo "OK: credentials present."

fixtures-run:
	@set -euo pipefail; \
	[[ -n "$(DOMAIN)" ]] || { echo "ERROR: set DOMAIN=..."; exit 2; }; \
	[[ -n "$(OUT)" ]]    || { echo "ERROR: set OUT=..."; exit 2; }; \
	[[ -n "$(FIXTURES)" ]] || { echo "ERROR: set FIXTURES=..."; exit 2; }; \
	mkdir -p "$$(dirname "$(OUT)")"; \
	$(SCRIPT) -d "$(DOMAIN)" -c "$(OUT)" $(CFLAG) --dry-run -F "$(FIXTURES)" $(PDFLAG) $(TOFLAG) $(SLFLAG) $(MPFLAG) $(VFLAG)

live-run: env-check
	@set -euo pipefail; \
	[[ -n "$(DOMAIN)" ]] || { echo "ERROR: set DOMAIN=..."; exit 2; }; \
	[[ -n "$(OUT)" ]]    || { echo "ERROR: set OUT=..."; exit 2; }; \
	mkdir -p "$$(dirname "$(OUT)")"; \
	$(SCRIPT) -d "$(DOMAIN)" -c "$(OUT)" $(CFLAG) $(PDFLAG) $(TOFLAG) $(SLFLAG) $(MPFLAG) $(VFLAG)

clean:
	@set -euo pipefail; \
	if [[ -n "$(OUT)" ]]; then rm -f "$(OUT)"; echo "Removed $(OUT)"; else echo "Set OUT=... to clean"; fi


# Generate sample fixtures quickly (no API needed)
# Example:
#   make sample-fixtures DOMAIN=example.com OUT=./fixtures PAGES=3 PER_PAGE=300 SEED=42 PREFER=0.8
# Then run:
#   make fixtures-run DOMAIN=example.com FIXTURES=./fixtures OUT=out/example.csv VERBOSE=1 PREFER_DOMAIN=example.com
sample-fixtures:
	@set -euo pipefail; \
	OUT_DIR="$(if [[ -n "$(OUT)" ]]; then echo "$(OUT)"; else echo "./fixtures"; fi)"; \
	mkdir -p "$$OUT_DIR"; \
	python3 ./generate_fixtures.py --domain "$(DOMAIN)" --out "$$OUT_DIR" --pages "$(if [[ -n "$(PAGES)" ]]; then echo "$(PAGES)"; else echo 2; fi)" --per-page "$(if [[ -n "$(PER_PAGE)" ]]; then echo "$(PER_PAGE)"; else echo 200; fi)" --seed "$(if [[ -n "$(SEED)" ]]; then echo "$(SEED)"; else echo 1234; fi)" --prefer-corporate "$(if [[ -n "$(PREFER)" ]]; then echo "$(PREFER)"; else echo 0.7; fi)"
