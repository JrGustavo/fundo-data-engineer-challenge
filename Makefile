RUN  = docker compose exec -T pipeline python -m
DAYS ?= 1

.PHONY: up down reset ping seed mutate measure

up:            ## Start SQL Server + pipeline container (waits until SQL Server is healthy)
	docker compose up -d --build --wait

ping: up        ## Check connectivity to source and warehouse
	$(RUN) pipeline.ping

seed: up        ## (Re)create the source database with deterministic synthetic data
	$(RUN) pipeline.seed

mutate: up      ## Simulate DAYS business days of changes in the source (default 1): make mutate DAYS=3
	@for i in $$(seq $(DAYS)); do $(RUN) pipeline.mutate || exit 1; done

measure: up     ## Snapshot the source, simulate one day, report real change rates and transfer cost
	$(RUN) pipeline.profile baseline
	$(RUN) pipeline.mutate
	$(RUN) pipeline.profile report

down:          ## Stop containers, keep data
	docker compose down

reset:         ## Stop containers and delete ALL local data (source volume + warehouse file)
	docker compose down -v
	rm -f data/*.duckdb data/*.wal data/*.json
