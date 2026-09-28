RUN = docker compose exec -T pipeline python -m

.PHONY: up down reset ping

up:            ## Start SQL Server + pipeline container (waits until SQL Server is healthy)
	docker compose up -d --build --wait

ping: up        ## Check connectivity to source and warehouse
	$(RUN) pipeline.ping

down:          ## Stop containers, keep data
	docker compose down

reset:         ## Stop containers and delete ALL local data (source volume + warehouse file)
	docker compose down -v
	rm -f data/*.duckdb data/*.wal
