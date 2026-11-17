# GDS Arrow POC: build and query

All commands run from the worktree root (`~/work/relate-gds-poc`). Each
`relate query` session loads the whole graph first (about 20 s for LDBC SF1),
so batch several statements into one session.

## 1. Build the CLI with the GDS codec

```bash
cd ~/work/relate-gds-poc
cargo build --release -p relate-cli --features gds-codec
```

## 2. Query the SF1 GDS export

```bash
D=target/gds-export/ldbc-sf1

# Interactive shell (graph loads once, then type queries)
target/release/relate query --read-only -d $D

# One statement
target/release/relate query --read-only -d $D "MATCH (p:Person) RETURN count(p)"

# Several statements, one load (';'-separated, from a file or stdin)
target/release/relate query --read-only -d $D < my-queries.cypher
```

- `--read-only` is required: a writable open of a GDS export is refused.
- Output: add `--json`, `--csv`, or `--jsonl --timings` (piped output
  defaults to JSON Lines).
- Telemetry: the CLI sends anonymous usage metrics by default; disable with
  `target/release/relate telemetry off`.

### Frozen topology

By default the export's adjacency lists are read in place by the engine: no
relationship records are stored and no adjacency is rebuilt. Set
`RELATE_GDS_TOPOLOGY=materialized` to load one record per relationship
instead (the earlier behaviour, kept for comparison):

```bash
RELATE_GDS_TOPOLOGY=materialized target/release/relate query --read-only -d $D
```

Benchmark both (12 queries, 5 runs each; results land in `target/bench/`):

```bash
python3 scripts/gds-poc/bench.py $D id frozen
RELATE_GDS_TOPOLOGY=materialized python3 scripts/gds-poc/bench.py $D id materialized
```

## 3. Writable copy

Clone the export to Parquet, then open the clone normally:

```bash
target/release/relate clone --to-snapshot-format parquet $D target/bench/gds-as-parquet
target/release/relate query -d target/bench/gds-as-parquet
```

## 4. Compare with Relate's own SF1 store

The store loaded from the LDBC CSVs, with string properties and indexes:

```bash
target/release/relate query --read-only -d target/bench/ldbc-sf1.rl
```

## Schema differences

| | GDS export | Relate store |
|---|---|---|
| LDBC id property | `id` | `ID` |
| String properties (names, titles, content, ...) | absent | present |
| Node dates (`creationDate`, `birthday`) | epoch millis, integer | string |
| Relationship properties (`creationDate`, `joinDate`, `classYear`, `workFrom`) | float | string / integer |
| `originalId` (dense GDS id) | on every node | absent |
| Indexes | none (read-only open cannot create them) | `ID` on every label + lookup indexes |

Labels and relationship types are identical in both: `Person`, `Comment`,
`Post`, `Forum`, `Tag`, `Tagclass`, `Place`, `Organisation`; `knows`,
`likeComment`, `likePost`, `replyOfPost`, `replyOfComment`,
`commentHasCreator`, `postHasCreator`, `hasMember`, `hasModerator`,
`containerOf`, `hasInterest`, `commentHasTag`, `postHasTag`, `forumHasTag`,
`hasType`, `isSubclassOf`, `commentIsLocatedIn`, `postIsLocatedIn`,
`personIsLocatedIn`, `organisationIsLocatedIn`, `isPartOf`, `studyAt`,
`workAt`.

Example against the GDS export:

```cypher
MATCH (p:Person {id: 933})-[:knows]-()-[:knows]-(fof) WHERE fof <> p
RETURN count(DISTINCT fof);
```

## 5. Tests

```bash
cargo test -p relate-storage --features gds-codec --lib codec::gds
cargo test -p relate --features gds-codec --test gds_codec
```

## 6. Regenerate the GDS export from Java

The throwaway exporter test is saved at
`target/gds-export/LdbcExportTest.java`. It reads the LDBC CSVs from
`target/query-latency/data/social_network-sf1-CsvBasic-StringDateFormatter`
(fetch them with
`cargo run --release -p relate-query-latency --bin query-latency -- fetch --sf 1`)
and writes `target/gds-export/ldbc-sf1/graph.arrow`; both paths are
hard-coded in the test.

```bash
cp target/gds-export/LdbcExportTest.java \
  ~/work/graph-analytics/private/arrow/io/src/test/java/com/neo4j/gds/arrow/io/
# add to ~/work/graph-analytics/private/arrow/io/build.gradle, in dependencies:
#   testImplementation project(':gds-values')
cd ~/work/graph-analytics/graph-data-science
JAVA_HOME=~/.sdkman/candidates/java/21.0.5-tem ./gradlew :arrow-graphstore-io:test \
  --tests 'com.neo4j.gds.arrow.io.LdbcExportTest' --offline
```

Revert both graph-analytics changes afterwards.

## 7. Rebuild Relate's own SF1 store

About 20 minutes and 8 GiB:

```bash
cargo build --release -p relate-query-latency --bin query-latency
target/release/query-latency run --sf 1 --store target/bench/ldbc-sf1.rl --qids 30 --offline
```
