# DuckDB

- <https://duckdb.org/docs/stable/>

## Docker UI

- <https://duckdb.org/docs/stable/operations_manual/duckdb_docker>

## CLI quickstart (selecting from `trend_docs`)

### Install the DuckDB CLI (Linux / macOS)

Download the official static binary from the DuckDB releases page:

```bash
DUCKDB_VERSION=1.1.3
curl -L "https://github.com/duckdb/duckdb/releases/download/v${DUCKDB_VERSION}/duckdb_cli-linux-amd64.zip" -o duckdb_cli.zip
unzip -j duckdb_cli.zip "duckdb" -d /tmp
chmod +x /tmp/duckdb
sudo mv /tmp/duckdb /usr/local/bin/duckdb
rm duckdb_cli.zip
duckdb --version
```

Alternatively, run the CLI through Docker:

```bash
docker run --rm -it -v "$(pwd):/workspace" -w /workspace duckdb/duckdb duckdb
# .open /workspace/trend_docs.duckdb
"SELECT * FROM trend_docs LIMIT 20;"
```

### Query the local database

```bash
duckdb ./trend_docs.duckdb -c "SELECT * FROM trend_docs LIMIT 20;"
```

You can drop into an interactive shell instead:

```bash
duckdb ./trend_docs.duckdb
-- now inside the prompt
SELECT COUNT(*) FROM trend_docs;
```

If your config points at a different file, substitute the path or pass `DUCK_DB_FILE=<path>` before running the command.

## Running queries in DuckDB UI

1. Launch the UI (e.g., `make duckdb-ui`) and wait for it to open `http://localhost:4213` in your browser. The target uses the repo's `./trend_docs.duckdb` file by default, so you'll immediately hit the same `trend_docs` table the miner/CLI writes to.
2. Click **New Notebook** (or open an existing one). Each cell runs standard SQL against the same database file the CLI would use.
3. In a SQL cell, type:

    ```sql
    SELECT * FROM trend_docs LIMIT 20;
    SELECT noun_stems, verb_stems, categories, pub_date, language FROM trend_docs LIMIT 10;
    ```

4. Press `Shift+Enter` (or hit the ▶ Run button). The results grid appears below the cell; you can sort, filter, or export directly from the UI.

Because the UI connects to the on-disk DuckDB instance started by the CLI/Makefile, any inserts from the miner show up immediately. To change the database path, run `make duckdb-ui DUCK_DB_FILE=/path/to/other.duckdb` before visiting the UI.
