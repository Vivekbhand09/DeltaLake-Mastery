# 🎯 Delta Lake 4.0 — Deep Dive Guide (Senior Data Engineer )
![Delta Lake](https://img.shields.io/badge/Delta-Lake%204.0-00ADD8?style=for-the-badge&logo=databricks&logoColor=white)
![Transaction Log](https://img.shields.io/badge/Transaction-Log%20Internals-FF3621?style=for-the-badge)
![ACID](https://img.shields.io/badge/ACID-Transactions-blue?style=for-the-badge)
![Schema Evolution](https://img.shields.io/badge/Schema-Evolution%20%26%20Enforcement-brightgreen?style=for-the-badge)
![MERGE](https://img.shields.io/badge/DML-MERGE%20%26%20Upserts-orange?style=for-the-badge)
![Deletion Vectors](https://img.shields.io/badge/Deletion-Vectors-9cf?style=for-the-badge)
![CDC](https://img.shields.io/badge/Change%20Data-Feed%20(CDC)-success?style=for-the-badge)
![Time Travel](https://img.shields.io/badge/Time%20Travel-Restore%20%26%20Vacuum-yellow?style=for-the-badge)
![UniForm](https://img.shields.io/badge/UniForm-Iceberg%20Interop-lightgrey?style=for-the-badge)
![Optimization](https://img.shields.io/badge/Z--Order-Liquid%20Clustering-red?style=for-the-badge)
![SQL](https://img.shields.io/badge/SQL-Editor%20%26%20Queries-4479A1?style=for-the-badge&logo=postgresql&logoColor=white)

> **9 notebooks. 9 core pillars of Delta Lake. One person who went from "what's a transaction log?" to running merges, time travel, CDC, Uniform, and Z-Ordering like second nature.**

This repository is a complete, practical walkthrough of **Delta Lake 4.0** on Databricks — not theory slides, but real tables created, real data mutated, real logs inspected byte-by-byte. Anyone browsing this repo should walk away knowing exactly one thing: **the author didn't just watch a Delta Lake course — they built it, broke it, and rebuilt it with their own hands.**

---

## 🧭 What's Inside

| # | Notebook | Core Theme |
|---|----------|-------------|
| 1 | `1_detalake` | Creating Delta Tables (SQL + Python API) & Generated Columns |
| 2 | `2_deltalake` | The Delta Transaction Log — peeking under the hood |
| 3 | `3_deltalake` | Schema Enforcement, Evolution & Overwrite |
| 4 | `4_deltalake` | DML, Upserts & the `MERGE` command |
| 5 | `5_deltalake` | Delta Log-Level Schema Changes (Column Mapping & Renames) |
| 6 | `6_deltalake` | Table Utility Commands — Time Travel, Restore, Clone, Vacuum |
| 7 | `7_deltalake` | Change Data Feed (CDC) |
| 8 | `8_deltalake` | UniForm — Delta ↔ Iceberg Interoperability |
| 9 | `9_deltalake` | Table Optimization — Compaction, Z-Order & Liquid Clustering |

---

## 📘 Notebook 1 — Creating Delta Tables & Generated Columns

**What this notebook proves:** the author knows there's more than one way to create a Delta Table, and picked the right tool for the right job.

- Built a table two different ways — the **SQL DDL route** (`CREATE TABLE ... id INT NOT NULL, salary INT UNIQUE`) and the **Delta Python API route** using `DeltaTable.createIfNotExists()` — showing comfort in both SQL-first and programmatic-first workflows.
- Went a level deeper into **Generated Columns**, a feature many practitioners skip entirely:
  - **Identity Columns** — auto-incrementing surrogate keys via `IdentityGenerator()`, so `id_col` fills itself in without any manual sequence logic.
  - **Computed Columns** — a column that derives itself from another, e.g. `salaryAfterTax` computed live as `CAST((salary * 0.7) AS BIGINT)` — meaning tax logic lives *inside the table definition*, not scattered across downstream ETL jobs.
- Rounded it off with basic `INSERT` + `SELECT` to confirm the generated logic actually fires correctly.

**Why it matters:** Generated columns are an intermediate-to-advanced Delta feature. Most tutorials stop at "create a table" — this notebook pushes straight into automated key generation and derived-column logic on day one.

---

## 📗 Notebook 2 — The Delta Transaction Log (Demystified)

**What this notebook proves:** the author refuses to treat Delta Lake as a black box.

- Wrote a small DataFrame to a Delta path using `.write.format("delta").mode("append")`.
- Then did the thing most people never bother to do: **opened the `_delta_log/00000000000000000001.json` file directly** and read it back into a DataFrame with `spark.read.format("json")`.

**Why it matters:** This is the single most important mental model in all of Delta Lake — *a Delta table is just Parquet files plus a JSON transaction log describing which files are "active."* By manually reading the log file instead of trusting it blindly, the author demonstrates real understanding of **ACID transactions, versioning, and how Delta achieves reliability on top of plain object storage** — not just "it works because Databricks said so."

---

## 📙 Notebook 3 — Schema Enforcement vs. Schema Evolution vs. Schema Overwrite

**What this notebook proves:** the author can tell you the difference between three concepts people constantly confuse — because they *triggered all three on purpose.*

- **Schema Enforcement:** wrote a DataFrame containing an *extra, unexpected column* (`sus_col`) straight into an existing Delta path to intentionally observe Delta's protective schema-mismatch behavior.
- **Schema Evolution:** immediately fixed it the correct way, using `.option("mergeSchema", True)` to let the new column merge safely into the table schema.
- **Schema Overwrite:** went further and completely restructured the table — renaming columns like `id → cust_id`, `salary → income` — using `.mode("overwrite").option("overwriteSchema", True)`, which replaces the schema entirely rather than merging it.
- Also compared reading the **same data two ways**: via the registered **table name** vs. directly via the **Delta Lake path** (`delta.\`/Volumes/...\``) — proving fluency with both the Unity Catalog and raw-path access patterns.

**Why it matters:** These three behaviors (enforce / evolve / overwrite) are the #1 source of production incidents in real Delta pipelines. Deliberately reproducing each one is exactly how a real engineer builds intuition instead of memorizing a definition.

---

## 📕 Notebook 4 — DML, Upserts & the `MERGE` Statement

**What this notebook proves:** the author can go beyond simple appends and handle real-world **update-or-insert (upsert)** logic — the backbone of every incremental data pipeline.

- Ran a plain `UPDATE ... SET income = 1000 WHERE cust_id = 5` directly against a Delta path, showing that Delta tables support standard SQL DML, not just append-only writes.
- Then tackled the real challenge — **UPSERT logic** — by:
  1. Preparing a new batch of incoming data (some existing customers, some brand new).
  2. Using `DeltaTable.forPath()` to grab a handle on the target table.
  3. Executing a full **`MERGE`**: 
     ```python
     dlt_obj.alias("trg").merge(df.alias("src"), "trg.cust_id = src.cust_id") \
         .whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
     ```
- This single command replaces what would otherwise be a messy multi-step "compare, update, insert" pipeline — matched rows are updated, unmatched rows are inserted, in one atomic transaction.

**Why it matters:** `MERGE INTO` is *the* signature capability that separates a data lake from a Delta Lake. Mastering this pattern means being able to build real SCD (slowly changing dimension) pipelines and CDC ingestion — not just batch dumps.

---

## 📔 Notebook 5 — Delta Log-Level Schema Changes (Column Mapping)

**What this notebook proves:** the author understands schema changes that go deeper than just "add a column" — changes that require touching Delta's **protocol version** itself.

- Took an existing table and enabled **Column Mapping mode** by explicitly bumping the protocol:
  ```sql
  ALTER TABLE ...
  SET TBLPROPERTIES (
    'delta.minReaderVersion' = '2',
    'delta.minWriterVersion' = '5',
    'delta.columnMapping.mode' = 'name'
  );
  ```
- Only *after* enabling column mapping did the author perform a **column rename** (`name → customer_name`) — something that is impossible/unsafe on a standard Delta table without this step.
- Verified the change by going straight back into the `_delta_log` JSON to see exactly how the rename is recorded at the metadata level — closing the loop back to Notebook 2's log-reading skill.

**Why it matters:** This is genuinely advanced territory. Most Delta users don't even know `minReaderVersion`/`minWriterVersion`/column mapping exist. Understanding *why* a rename needs a protocol bump — and confirming it by reading the raw log — is protocol-level mastery, not surface-level usage.

---

## 📒 Notebook 6 — Table Utility Commands (Time Travel, Restore, Clone, Vacuum)

**What this notebook proves:** the author has full command of Delta's "operations toolkit" — the commands that make Delta Lake production-safe.

- `DESCRIBE DETAIL` / `DESCRIBE EXTENDED` — inspecting table metadata, location, size, and properties.
- `DESCRIBE HISTORY` — pulling the complete audit trail of every operation ever performed on a table.
- **Time Travel** — querying a table exactly as it looked at a past moment: `SELECT * FROM delta.\`...\` TIMESTAMP AS OF '2025-07-05T01:12:36.000+00:00'`.
- **`RESTORE ... TO VERSION AS OF 3`** — actually rolling a table back to a previous version, not just viewing it.
- `SHOW TBLPROPERTIES` — auditing table-level configuration.
- **`VACUUM ... RETAIN 0 HOURS`** — purging stale, un-referenced data files to reclaim storage (and understanding the retention-window trade-off that comes with going down to 0 hours).
- **`SHALLOW CLONE`** — creating a zero-copy clone of a table for safe experimentation without duplicating the underlying data.

**Why it matters:** This notebook is basically "Delta Lake for Production Engineers." Time travel, restore, and vacuum are the exact tools used for disaster recovery, auditing, and cost control in real enterprise lakehouses.

---

## 📓 Notebook 7 — Change Data Feed (CDC)

**What this notebook proves:** the author can track *row-level* change history — not just table-level snapshots.

- Enabled CDC on an existing table: `SET TBLPROPERTIES (delta.enableChangeDataFeed = true)`.
- Generated a realistic mix of changes: an `INSERT`, an `UPDATE`, and a `DELETE`.
- Used `DESCRIBE HISTORY` to identify version numbers, then queried the captured change stream directly with the **`table_changes()`** function:
  ```sql
  SELECT * FROM table_changes('deltalakeansh.default.clonetbl', 1, 3)
  ```
- This returns every insert/update/delete between versions 1 and 3, complete with `_change_type`, `_commit_version`, and `_commit_timestamp` metadata columns.

**Why it matters:** Change Data Feed is what powers real downstream CDC pipelines — syncing changes to a data warehouse, feeding a streaming job, or building audit logs — without needing full-table re-scans. This is a genuinely advanced, high-value Delta feature.

---

## 📖 Notebook 8 — UniForm (Universal Format): Delta ↔ Iceberg

**What this notebook proves:** the author is current on the *newest* frontier of the lakehouse world — table format interoperability.

- Created a Delta table with UniForm explicitly enabled:
  ```sql
  CREATE TABLE ... USING DELTA TBLPROPERTIES(
    'delta.enableIcebergCompatV2' = 'true',
    'delta.universalFormat.enabledFormats' = 'iceberg'
  );
  ```
- Inserted data into it to trigger UniForm's automatic generation of Iceberg-compatible metadata alongside the native Delta log.

**Why it matters:** UniForm solves one of the biggest political/technical debates in the data engineering world right now: "Delta vs. Iceberg." Instead of picking a side, UniForm lets a single physical table be read natively by both ecosystems. Knowing how to enable this puts the author ahead of the curve on cutting-edge lakehouse architecture — this isn't a "textbook" feature, it's an actively-evolving one.

---

## 📚 Notebook 9 — Table Optimization (Compaction, Z-Order, Liquid Clustering)

**What this notebook proves:** the author knows that a working Delta pipeline and a *fast* Delta pipeline are two different achievements — and knows how to close that gap.

- Read data from a table, wrote it back out with repeated appends to deliberately create the classic **"small files problem."**
- Ran plain **`OPTIMIZE`** to compact those small files into fewer, larger, query-efficient files.
- Went further with **`OPTIMIZE ... ZORDER BY (cust_id)`** — physically co-locating related data on disk so that filters on `cust_id` skip far more data at query time.
- Finished with the newest evolution of this idea: **`ALTER TABLE ... CLUSTER BY AUTO`**, enabling **Liquid Clustering** — Delta 4.0's self-tuning replacement for manual Z-Ordering, which continuously re-optimizes layout without needing manual `OPTIMIZE` scheduling.
- Verified every step against `DESCRIBE HISTORY` to see the optimize operations logged as first-class transactions.

**Why it matters:** This notebook shows the full performance-tuning arc — from understanding *why* small files hurt performance, to manual compaction, to manual data clustering, all the way to the modern **auto-tuning** approach. That progression (old technique → new technique, with reasoning for both) is exactly what separates someone who "used Delta Lake" from someone who **understands Delta Lake's internals well enough to make it fast.**

---

## 🏆 The Bottom Line

Across these nine notebooks, this person has hands-on, tested experience across the **entire Delta Lake surface area**:

✅ Table creation (SQL + Python API, generated & computed columns)
✅ Transaction log internals (reading raw `_delta_log` JSON)
✅ Schema enforcement, evolution, and overwrite semantics
✅ DML, upserts, and atomic `MERGE` operations
✅ Protocol-level schema changes (column mapping, safe renames)
✅ Time travel, restore, cloning, and vacuum for production safety
✅ Change Data Feed for row-level CDC pipelines
✅ UniForm for Delta/Iceberg interoperability
✅ Performance tuning via `OPTIMIZE`, Z-Ordering, and Liquid Clustering

This isn't "watched a video" knowledge. Every single concept above was **executed, inspected, and verified** by hand. Anyone reviewing this repository should walk away with one clear conclusion:

> **This person has mastered Delta Lake 4.0 — from the transaction log up to production-grade optimization.** 🚀
