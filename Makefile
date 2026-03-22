SHELL=/bin/bash

activate:
	source .venv/bin/activate

venv:
	uv venv --python 3.12

install: activate
	uv run maturin develop

install-release: activate
	uv run maturin develop --release

pre-commit:
	cargo +nightly fmt --all && cargo clippy --all-features
	uv run pre-commit run --all-files

test:
	uv run pytest tests

docstr-coverage:
	uv run docstr-coverage ./**/*.py --fail-under 20 --verbose=2 --skip-file-doc --skip-init

run: install
	uv run run.py

run-release: install-release
	uv run run.py

run-local-gh-action-python-checks:
	act workflow_run -j python-checks --use-new-action-cache

run-local-gh-action-rust-checks:
	act workflow_run -j rust-checks --use-new-action-cache

run-local-gh-action-ci:
	act workflow_run -j ci.yml --use-new-action-cache