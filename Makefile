.PHONY: help db test chaos chaos-quick clean

help:
	@echo "make db          start Postgres (docker compose)"
	@echo "make test        run the test suite"
	@echo "make chaos       full campaign: 100 runs, 60 kills"
	@echo "make chaos-quick 20 runs, 10 kills"
	@echo "make clean       stop Postgres and remove its volume"

db:
	docker compose up -d
	@until docker compose exec -T postgres pg_isready -U anchor >/dev/null 2>&1; do sleep 1; done
	@echo "postgres ready on :55432"

test:
	pytest -q

chaos:
	python -m chaos.harness --runs 100 --kills 60 --json chaos-report.json

chaos-quick:
	python -m chaos.harness --runs 20 --kills 10 --json chaos-report.json

clean:
	docker compose down -v
