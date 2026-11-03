# GenericTransfer + AdbcHook: is "never hits Arrow / adbc_ingest" correct?

Date: 2026-11-03. Sources checked: apache-arrow provider 0.1.1 (`hooks/adbc.py`, identical on the fork branch and `upstream/main`), common.sql `generic_transfer.py`, postgres provider `hooks/postgres.py`, ADBC PostgreSQL driver source (`c/driver/postgresql/bind_stream.h`, `statement.cc` on arrow-adbc main), ADBC dbapi `executemany`/`adbc_ingest` docs, the poster's repo `aoelvp94/airflow-adbc-vs-odbc-bench`.

## Verdict on the assumption

Half right.

| Claim | Status | Evidence |
| --- | --- | --- |
| GenericTransfer always ends in `destination_hook.insert_rows` | Correct | `generic_transfer.py:157`; every branch (paginated, deferrable, plain) calls `_insert_rows` |
| ... so you never hit Arrow | Wrong | `AdbcHook.insert_rows` already builds a `pyarrow.RecordBatch` per chunk (`adbc.py:235`) and hands it to `cursor.executemany` (`adbc.py:248`); the data path is Arrow |
| ... and never hit `adbc_ingest` | Correct | no caller anywhere in `providers/apache/arrow` or `common/sql` |

So the hook is Arrow-native but not bulk-ingest-native. The slowness is not "ADBC overhead": it is the PostgreSQL driver's bind path.

## Why stock ADBC is 2x slower than psycopg2 here

- ADBC `executemany` with a RecordBatch executes the prepared INSERT **once per row**: dbapi docstring ("it will be executed once per row"), and the driver loops `PQexecPrepared` per row in `bind_stream.h:285`.
- `PostgresHook.insert_rows(fast_executemany=True)` uses `psycopg2.extras.execute_values`, one multi-row INSERT per page (`postgres.py:761`); psycopg3 pipelines. Both beat per-row round trips.
- This matches your Aug 2025 result (dags `9e497fe27`: specialised PostgresHook `insert_rows` on `execute_batch` "way faster" than the ADBC path) and the poster's table: libpq 14.6 s, ADBC 31.2 s, ODBC 65.2 s for 1M rows.
- `adbc_ingest` on the PostgreSQL driver is `COPY <table> (<cols>) FROM STDIN WITH (FORMAT binary)` (`statement.cc:760-764`). That is the 9x. His own `SQLBulkLoadOperator` row (COPY via psycopg2, 1.6 s) is faster still, so the win is COPY, not Arrow.

Your point on the driver stands: for Python row tuples (GenericTransfer's data shape) the ADBC PostgreSQL driver has no advantage over psycopg2 unless the hook switches to ingest. ADBC pays off when both ends stay Arrow (its reads use COPY too).

## Where the fix belongs: inside `AdbcHook.insert_rows`

Agree with you, not with the three-PR cut. `AdbcArrowTransfer` is the shape spec 13 refuses: a transfer operator per destination technology. It is the same discussion as the HTTP/MSGraph to S3/ADLS one: `GenericTransfer` stays one operator that knows nothing about its hooks, and the capability (bulk ingest here, upload there) lives in the hook, so every operator and every `@task` on that hook gets it, and it can go upstream as a hook change.

- GenericTransfer's contract is "destination hook, `insert_rows`". The hook knows its driver; the operator must not. A flag on GenericTransfer or an Arrow operator in `apache.arrow` leaks the driver into the operator. His subclass shows the cost already: it reimplements `execute`, drops `page_size`, `deferrable` and `rows_processor`, and concatenates every page in memory before one ingest. Next comes an `AdbcArrowTransfer` per source type, then one per pair.
- `insert_rows` already has everything ingest needs: the table schema from `adbc_get_table_schema`, the `target_fields` subset (COPY takes its column list from the batch schema, `statement.cc:762`), the per-`commit_every` RecordBatch, and `Dialect.extract_schema_from_table` for `db_schema_name`.
- Sketch: `cursor.adbc_ingest(table_name, batch, mode="append", db_schema_name=schema_name)` per chunk, then `conn.commit()`; keep the current bind path for `replace=True` (ON CONFLICT cannot ride on COPY) and as fallback when a driver reports ingest unsupported. One PR in the apache-arrow provider, no common.sql change.
- Remaining cost after that: `_to_record_batch` converts rows cell by cell in Python (`adbc.py:235-239`). Expect somewhere between his 4 s and 15 s, not 4 s. Measure before claiming a number.

## Points to check before answering him

- His docstring says "`AdbcHook.insert_rows` cannot bind PostgreSQL NUMERIC". The driver's type table lists no decimal128 mapping for bind or for bulk ingestion, yet his ingest with `decimal128(10,2)` passed his md5 verify. Separate bug report if bind really fails; not a GenericTransfer matter.
- "Arrow source" (PR 3) means changing what `get_records` returns, a common.sql contract change. Park it until the ingest change is measured.
- Nothing upstream yet on this (searched issues and PRs for `adbc_ingest`, "ADBC ingest", "GenericTransfer arrow" on 2026-11-03). Open ADBC items are unrelated: #74454 (your URI fix), #75988, #76273/#76290 (autocommit-only drivers).

## Suggested answer shape (if you reply)

Issue: "AdbcHook.insert_rows should load through adbc_ingest (COPY) instead of per-row bind", one PR on the apache-arrow provider, no flag or operator on GenericTransfer, Arrow source as a later separate discussion with numbers. Credit his benchmark; correct "never hit Arrow" to "never hit bulk ingest".

## Choosing the path: reuse `fast_executemany`, default `True` on AdbcHook

Recommendation: reuse the existing flag, no new one.

- **It already means this.** `fast_executemany` is "take the driver's bulk fast path" everywhere it exists: pyodbc's `cursor.fast_executemany` on OdbcHook/MsSqlHook, `execute_values` on PostgresHook (`postgres.py:694-713`). `adbc_ingest` is that path for ADBC. A second word (`ingest=`, `mode=`) for the same idea is the vocabulary split we avoid, and would be ADBC-only.
- **No operator change.** GenericTransfer and SQLInsertRowsOperator pass `insert_args` straight into `insert_rows`, so `insert_args={"fast_executemany": False}` gives the old INSERT path on demand.
- **Repurposing breaks nobody.** Today the flag is a no-op on AdbcHook: it sets `cursor.fast_executemany` on a Cursor that has no such attribute, inside `suppress(AttributeError)` (`adbc.py:260-266`).
- **A new flag only wins if a "fast bind" exists** that is distinct from ingest. For ADBC it does not: bind is per-row `PQexecPrepared`, nothing in between it and COPY.

Default `True` in the override is right, because the bind path is never the sane default. Overriding a keyword default in the subclass is fine: same name, same type, explicit callers get what they pass. PostgresHook keeps `False` because its default path is already batched; ADBC's is not. Guards that keep the flag a hint rather than a contract, like PostgresHook's psycopg3 warning:

| Situation | Behaviour |
| --- | --- |
| `fast_executemany=True`, `replace=False` | `cursor.adbc_ingest(table_name, batch, mode="append", db_schema_name=schema)` per `commit_every` chunk, then commit |
| `replace=True` | COPY cannot do ON CONFLICT: log once, use the bind path |
| driver raises `NotSupportedError` on ingest | log once, bind path |
| `fast_executemany=False` | current bind path, unchanged |

Points for the PR:

- **NUMERIC.** `_to_record_batch` builds the batch from `adbc_get_table_schema`, and the PostgreSQL driver returns NUMERIC as `utf8` by default (its docs: "cannot be losslessly converted"). Bind then declares a TEXT parameter against a NUMERIC column, which is the "cannot bind NUMERIC" he hit. Ingest in append mode resolves the target types from the existing table (`statement.cc:497-503`) and the driver has NUMERIC writers for decimal input (`copy/writer.h:322`), so the test table needs a NUMERIC column to see whether `utf8` is converted or the hook must build `decimal128` from the table's precision and scale. ADBC 1.13 (release 25) also added a type-resolver option worth checking for reads.
- **Semantics change.** COPY fires row triggers like INSERT, does not fire rules, and a bad row fails the chunk rather than the row. Changelog entry as a significant change; the release manager picks the bump.
- **Measure** before claiming a number: the Python `_to_record_batch` conversion stays in the path.

## The change as written (worktree `~/github/dabla/airflow-adbc-ingest`, branch `fix/adbc-insert-rows-ingest` off upstream/main, unpushed)

Diff: `adbc-insert-rows-ingest.diff` next to this file. Slack text: `slack.md`. Checks: ruff format + check clean, mypy clean on `adbc.py` (remaining errors are pre-existing in other files), the 19 hook tests pass against the modified module (run from the fork checkout with `PYTHONPATH=<worktree>/providers/apache/arrow/src`, the ADBC manager 1.13.0 + SQLite driver were installed into the fork venv for that).

Principles applied:

- Reusing `fast_executemany` instead of a new parameter: one vocabulary per feature (python skill [13](~/.claude/skills/python/specs/13-design-patterns.md); https://wiki.c2.com/?OnceAndOnlyOnce).
- `replace=True` routes to the bind path with an info line, nothing swallowed: "Errors should never pass silently" ([04](~/.claude/skills/python/specs/04-contracts-and-exceptions.md); PEP 20).
- Default `True` in the override keeps the parent's call shape and types, only the default differs: Liskov ([08](~/.claude/skills/python/specs/08-solid.md); Liskov and Wing 1994).
- No automatic fallback for drivers without ingest: every shipped driver has it, and the escape hatch is `fast_executemany=False`: YAGNI ([05](~/.claude/skills/python/specs/05-design-heuristics.md); Fowler, *Yagni*).
- Two `partial`s instead of a flag checked inside the loop: the choice is made once, the loop stays one line ([05](~/.claude/skills/python/specs/05-design-heuristics.md); `functools.partial`).
- Left alone, to propose separately: `executemany` is accepted and ignored by AdbcHook (pre-existing), and the hook sends no OpenLineage like PostgresHook does.

## Your point on 2026-11-03: AdbcHook to AdbcHook should not convert at all

Correct on all three counts, with two refinements.

**The round trip exists today, on both sides.**

| Side | Where | What |
| --- | --- | --- |
| source | `fetch_all_handler` (`adbc.py:40-49`), used by `get_records` | `cursor.fetch_arrow_table()` then `to_pylist()` per column and `zip` into tuples |
| destination | `_to_record_batch` (`adbc.py:234`) in `insert_rows` | per cell list comprehension back into a RecordBatch per `commit_every` chunk |

Measured here (fork venv, pyarrow 25, 1M rows x 4 columns: int64, string, int32, timestamp): Arrow to rows 1.47 s, rows to batches 0.36 s, 1.84 s in total. That is the size of his whole ingest run (3.4 to 4.0 s), so after the COPY change the conversion is the remaining gap, as predicted.

**A `@task` with two AdbcHooks has the same round trip** as long as it stays on the hook API: `get_records` and `insert_rows` are the only read and write methods the hook offers, and `get_df` materialises pandas or polars. Avoiding it today means `get_conn()` plus `cursor.fetch_record_batch()` and `cursor.adbc_ingest()`, which is driver API in the DAG, the thing spec 13 refuses. So the missing piece is a hook-level contract, not an operator.

**Refinement 1: the contract is half extension, half new.**

- Destination: `insert_rows` can accept Arrow input next to row tuples (`RecordBatch`, `Table`, `RecordBatchReader`) and skip `_to_record_batch` for it. Same method, wider parameter type, Liskov holds; a reader streams batch by batch with a commit per batch, and `adbc_ingest` takes the reader as is. No new name.
- Source: `get_records` returns `list[tuple]` and callers index into it, so it cannot return Arrow. A new read is unavoidable. Two shapes fit the existing vocabulary: `get_df(df_type="arrow")` returning a `Table` (materialised, like pandas and polars), or a streaming read that returns a `RecordBatchReader`. Streaming is what makes the memory story work, so the second is the one to propose; the name is to pick against upstream analogues (`fetch_record_batch` is the ADBC dbapi name).
- `rows_processor` has to be defined for batches too (it receives a RecordBatch) or declared incompatible with the Arrow path.

**Refinement 2: GenericTransfer's awareness can stay at one decision.** It asks the source hook for batches when both hooks implement the protocol (a `typing.Protocol` in common.sql, checked with `isinstance` at runtime), otherwise `get_records`. Everything else (paging, preoperator, commits) stays as it is. That is a common.sql change plus the arrow provider implementing the protocol; PostgresHook on psycopg cannot implement it, DuckDB, Snowflake and BigQuery hooks could later.

**Order.** Ingest first: it is one provider PR, independent, and it also speeds up every non-ADBC source loading into ADBC. The Arrow contract second: bigger, touches common.sql, and its payoff is the 1.84 s per million rows measured above. Calling the contract "the first issue" is right by value, not by sequence.

## Your point: DuckDB speaks Arrow underneath, so the contract is wider than AdbcHook

Correct, and DuckDB is the stronger case. Upstream `DuckDBHook` (duckdb provider 0.2.1) is a plain `DbApiHook`: it overrides neither `get_records` nor `insert_rows`, so a transfer into DuckDB goes through the DB-API cursor's `executemany`, one row at a time. The DuckDB Python client itself is Arrow-native in both directions and zero-copy. Measured here, duckdb 2.0.0 in the fork venv, 1M rows x 3 columns (int64, varchar, int32):

| Path | Time |
| --- | --- |
| read, DB-API `fetchall` | 0.41 s |
| read, `to_arrow_table()` / `fetch_record_batch()` (1.5.x names `arrow()` / `fetch_record_batch()`) | 0.02 s |
| write, DB-API `executemany` (what `DuckDBHook.insert_rows` does today) | 71.8 s for 100k rows, so about 12 min for 1M |
| write, `register(name, table)` + `INSERT INTO t SELECT * FROM name` | 0.03 s |
| write, `from_arrow(table).insert_into("t")` | 0.01 s |

So the same protocol fits four hooks today and more later:

| Hook | Arrow read | Arrow write |
| --- | --- | --- |
| AdbcHook (apache.arrow) | `cursor.fetch_record_batch()` reader | `cursor.adbc_ingest(reader)` |
| DuckDBHook (duckdb), AwsDuckDBHook and the Azure variant inherit | `conn.sql(sql).fetch_record_batch()` | `conn.from_arrow(batch).insert_into(table)` |
| SnowflakeHook | `cursor.fetch_arrow_batches()` | `write_pandas` only; Arrow write via stage |
| DatabricksSqlHook | `cursor.fetchall_arrow()` | none native |
| BigQueryHook | `to_arrow()` via Storage API | `load_table_from_file` with Parquet |
| PostgresHook (psycopg) | none | none, stays on `get_records` / `insert_rows` |

This settles where the contract lives: in common.sql on `DbApiHook`, as the pandas and polars `get_df` already are, with a default that raises or falls back to rows, and each provider implementing what its client offers. `GenericTransfer` takes the Arrow path when both ends implement it, otherwise rows. The ADBC ingest change stays the independent first PR; the DuckDB `insert_rows` is a second independent fix of the same kind (override `insert_rows` to `from_arrow().insert_into`), even before the contract exists.

## Your proposal: an Arrow interface implemented by DuckDBHook and AdbcHook

Yes, and it should be a `typing.Protocol`, in common.sql, split in two.

**Two protocols, not one.** Snowflake, Databricks SQL and BigQuery can read Arrow but have no native Arrow write, and a hook might only write. A source needs the read half, a destination the write half, so `GenericTransfer` checks each end for the half it needs. Interface segregation ([08](~/.claude/skills/python/specs/08-solid.md) I; Martin, *The Principles of OOD*).

```python
# airflow/providers/common/sql/hooks/arrow.py  (pyarrow only under TYPE_CHECKING: common.sql does not depend on it)
@runtime_checkable
class RecordBatchReadable(Protocol):
    def get_record_batches(self, sql: str, parameters=None, *, batch_size: int = 10_000) -> RecordBatchReader: ...

@runtime_checkable
class RecordBatchWritable(Protocol):
    def insert_record_batches(self, table: str, batches: Iterable[RecordBatch], target_fields=None, *, commit_every: int = 1000) -> None: ...
```

Names are placeholders for the design thread; the rule is one term per concept across the hooks, the protocols and `GenericTransfer` ([13](~/.claude/skills/python/specs/13-design-patterns.md)), and no `Interface`, `Mixin` or `ABC` in the identifier ([13](~/.claude/skills/python/specs/13-design-patterns.md)).

**Protocol rather than an ABC, for a reason that matters across providers.** A Protocol is structural ([PEP 544](https://peps.python.org/pep-0544/)): AdbcHook and DuckDBHook implement the two methods and import nothing from common.sql for it; `GenericTransfer`, which lives in common.sql next to the protocols, does `isinstance(self.source_hook, RecordBatchReadable)` with `@runtime_checkable`. That removes the version-skew problem an ABC brings: an ABC in common.sql that the hooks must inherit forces every implementing provider onto the common.sql release that ships it, through `common.compat` shims or floor bumps the release manager owns. It also follows "inherit to specialise, never to borrow" ([06](~/.claude/skills/python/specs/06-inheritance-and-composition.md)): the hooks borrow nothing. Cost to state: `runtime_checkable` checks that the methods exist, not their signatures ([PEP 544](https://peps.python.org/pep-0544/#runtime-checkable-decorator-and-narrowing-types-by-isinstance)); mypy checks the signatures where the hook is annotated against the protocol in tests.

**This revises my earlier "widen `insert_rows`".** A widened `insert_rows` is not detectable (every DbApiHook has one) and gives one method two contracts. A separate `insert_record_batches` is detectable, keeps each method's contract simple, and `insert_rows` on AdbcHook becomes "build the batches from the rows, then `insert_record_batches`", so ingest is written once (DRY, [05](~/.claude/skills/python/specs/05-design-heuristics.md)). The ingest PR can already be shaped that way without the protocol existing: the method name is the only thing to agree on.

**Implementations.**

| | `get_record_batches` | `insert_record_batches` |
| --- | --- | --- |
| AdbcHook | `cursor.execute(sql, parameters)` then `cursor.fetch_record_batch()` | `cursor.adbc_ingest(table, reader_or_batches, mode="append", db_schema_name=...)`, commit per `commit_every` |
| DuckDBHook | `conn.sql(sql).fetch_record_batch(batch_size)` (2.0: `to_arrow_reader`) | `conn.from_arrow(batch).insert_into(table)` per batch, or `register` + `INSERT INTO ... SELECT` for a column subset |
| AwsDuckDBHook, Azure variant | inherited | inherited |
| Snowflake, Databricks SQL, BigQuery | later, from their Arrow fetches | not implemented |
| PostgresHook, others | not implemented: `GenericTransfer` stays on rows | |

**GenericTransfer.** One decision in `_transfer`: both ends implement their half → `insert_record_batches(table, get_record_batches(sql))`, otherwise `get_records` + `insert_rows` as today. The paged variant builds one reader per page. `rows_processor` receives `RecordBatch`es on the columnar path, or the operator raises at parse time when a `rows_processor` is set together with two Arrow hooks; pick one and document it. No flag to force rows until someone needs it ([05](~/.claude/skills/python/specs/05-design-heuristics.md), YAGNI); `insert_args` already reaches the write method.

**Precedent in DbApiHook.** `supports_autocommit` and `supports_executemany` are capability flags on the base class; this is the same idea expressed as a type, which also carries the method shapes. Mentioning it in the thread helps common.sql maintainers place it.
