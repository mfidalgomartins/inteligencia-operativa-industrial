.PHONY: install lock generate-synthetic run test test-quick test-full pack all

install:
	python -m venv .venv
	./.venv/bin/python -m pip install -r requirements.txt

lock:
	./.venv/bin/python -m pip freeze > requirements.lock.txt

generate-synthetic:
	./.venv/bin/python scripts/generate_synthetic_data.py

run:
	./.venv/bin/python -m src.run_pipeline

test:
	./.venv/bin/python -m pytest -q

test-quick:
	./.venv/bin/python -m pytest -m quick -q

test-full:
	./.venv/bin/python -m pytest -m full -q

pack:
	./.venv/bin/python -m src.repro_packaging

all: run test
