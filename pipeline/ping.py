"""Step 1 smoke test: can we reach both ends of the pipeline?"""
from pipeline import config


def main() -> None:
    # `master` always exists; the FundoOps database is created in the seed step.
    with config.source(database="master") as conn:
        cur = conn.cursor()
        cur.execute("SELECT @@VERSION")
        version = cur.fetchone()[0].splitlines()[0]
    print(f"source    OK  {version}")

    wh = config.warehouse()
    duck_version = wh.execute("SELECT version()").fetchone()[0]
    wh.close()
    print(f"warehouse OK  DuckDB {duck_version}")


if __name__ == "__main__":
    main()
