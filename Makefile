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

run-local-gh-action-style-checks:
	act --use-new-action-cache -j style-checks

run-local-gh-action-tests:
	echo '{"workflow_run":{"conclusion":"success","name":"Style checks"}}' > /tmp/workflow_run_event.json
	act workflow_run -j test -e /tmp/workflow_run_event.json --use-new-action-cache

run-local-gh-action-complexity-checks:
	echo '{"workflow_run":{"conclusion":"success","name":"Run Tests"}}' > /tmp/workflow_run_event.json
	act workflow_run -j complexity-checks -e /tmp/workflow_run_event.json --use-new-action-cache

run-local-gh-action-ci:
	echo '{"workflow_run":{"conclusion":"success","name":"Complexity checks"}}' > /tmp/workflow_run_event.json
	act workflow_run -j ci -e /tmp/workflow_run_event.json --use-new-action-cache