RUN  = docker compose exec -T pipeline python -m
DAYS ?= 1

.PHONY: up down reset ping seed mutate measure load status crash-demo check break repair break-demo

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

load: up        ## Load the warehouse: initial load the first time, incremental afterwards
	$(RUN) pipeline.load

status: up      ## Show watermarks and the last table loads
	$(RUN) pipeline.load status

crash-demo: up  ## Simulate a day, crash while loading Transactions, show state, then recover
	$(RUN) pipeline.mutate
	@printf "\n>>> Loading with a simulated crash in Transactions (this failure is expected)\n"
	-docker compose exec -T -e CRASH_AT=Transactions pipeline python -m pipeline.load
	@printf "\n>>> State after the crash: Transactions watermark did not move\n"
	$(RUN) pipeline.load status
	@printf "\n>>> Re-running the load: it resumes from the old Transactions watermark\n"
	$(RUN) pipeline.load

check: up       ## Is the warehouse complete and correct right now? Exit code 1 on a real problem
	$(RUN) pipeline.check

break: up       ## Damage the warehouse on purpose (one missing, one extra, some mismatched rows)
	$(RUN) pipeline.break_it

repair: up      ## Re-copy only the rows the last check flagged (watermarks untouched)
	$(RUN) pipeline.check repair

break-demo: up  ## Full demo: check passes, break it, check fails, repair, check passes again
	@printf "\n>>> 1. Check a healthy warehouse\n"
	$(RUN) pipeline.check
	@printf "\n>>> 2. Break it on purpose\n"
	$(RUN) pipeline.break_it
	@printf "\n>>> 3. Check again (this failure is expected)\n"
	-$(RUN) pipeline.check
	@printf "\n>>> 4. Repair only the flagged rows\n"
	$(RUN) pipeline.check repair
	@printf "\n>>> 5. Check once more\n"
	$(RUN) pipeline.check

down:          ## Stop containers, keep data
	docker compose down

reset:         ## Stop containers and delete ALL local data (source volume + warehouse file)
	docker compose down -v
	rm -f data/*.duckdb data/*.wal data/*.json
