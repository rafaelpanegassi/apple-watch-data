.PHONY: infra-up infra-down infra-logs run-all run-bronze run-silver run-gold clean

PYTHON = .venv/bin/python

infra-up:
	docker compose up -d

infra-down:
	docker compose down -v

infra-logs:
	docker compose logs -f

run-all:
	$(PYTHON) main.py --stage all

run-bronze:
	$(PYTHON) main.py --stage bronze

run-silver:
	$(PYTHON) main.py --stage silver

run-gold:
	$(PYTHON) main.py --stage gold
clean:
	rm -rf data/tmp_* src/__pycache__ src/*/__pycache__ src/*/*/__pycache__
