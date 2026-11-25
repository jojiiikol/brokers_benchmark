PYTHON ?= python3

.PHONY: smoke
smoke:
	$(PYTHON) test/smoke.py
