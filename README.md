# Dynamic Key Grouping (DKG)

A load-balancing stream grouping algorithm for Apache Storm that solves the **data skew problem** in stateful stream processing. DKG dynamically assigns hot keys to multiple worker bolts instead of pinning each key to a single worker, trading a small amount of aggregation overhead for significantly better load distribution.

---

## The Problem

In stream processing, **key-based grouping** routes all tuples with the same key to the same worker bolt. This guarantees correctness for stateful operations (e.g., count by key) but breaks down when the key distribution is skewed — a handful of "hot" keys dominate traffic and overwhelm the workers assigned to them, while other workers sit idle.

**Shuffle grouping** avoids skew by distributing tuples round-robin, but loses the key-locality property required for per-key aggregation.

DKG sits between these two extremes: it routes hot keys to multiple workers (accepting a merge step) while routing cold keys to a single worker (avoiding unnecessary overhead).

---

## Architecture

### Storm Topology Pipeline

```
Kafka Topic
    │
    ▼
KafkaSpout (parallelism: N)
    │  shuffle grouping
    ▼
SplitterBolt (parallelism: 10×N)
    │  parse message → emit (key, count, timestamp)
    │  [custom stream grouping: SHUFFLE | KEY | PARTIAL_KEY | DYNAMIC_KEY]
    ▼
WorkerBolt (parallelism: W)          DistributionObserverBolt (parallelism: 1)
    │  time-windowed per-key           │  tracks key→worker mapping
    │  aggregation + emit              │  computes stdDev, throughput, distCost
    │  (workerId, key, count)          ▼
    │  fields grouping by key     Kafka (distribution topic)
    ▼
AggregatorBolt (parallelism: 10)
    │  final merge across workers
    ▼
KafkaOutputBolt → Kafka (aggregator topic)
```

### Component Responsibilities

| Component | Role |
|---|---|
| `KafkaSpout` | Reads raw messages from a Kafka topic |
| `SplitterBolt` | Parses dataset-specific message formats into `(key, count, timestamp)` tuples |
| `WorkerBolt` | Performs windowed per-key aggregation; emits window results on tick; simulates configurable processing cost |
| `AggregatorBolt` | Final aggregation layer receiving from all worker bolts via fields grouping |
| `DistributionObserverBolt` | Observes the stream from worker bolts; measures load distribution metrics |
| `KafkaOutputBolt` | Writes aggregated results and distribution metrics back to Kafka |

---

## Stream Grouping Strategies

Four grouping strategies are implemented and compared:

### 1. Shuffle Grouping (`SHUFFLE`)
Round-robin across all worker bolts. Perfect load balance, but keys land on arbitrary workers so cross-worker aggregation is required.

### 2. Key Grouping (`KEY`)
MurmurHash-based deterministic routing: `worker = hash(key) % numWorkers`. All tuples for a given key go to the same worker. Zero aggregation overhead, but hot keys create severe imbalance.

### 3. Partial Key Grouping (`PARTIAL_KEY`)
Implemented after the algorithm by [Gianmarco De Francisci Morales](https://github.com/gdfm/partial-key-grouping/). Each key has two candidate workers (computed via two independent hash functions). The tuple is sent to whichever candidate is less loaded. Provides good balance with moderate aggregation overhead (each key uses at most 2 workers).

### 4. Dynamic Key Grouping (`DYNAMIC_KEY`) — novel contribution
The core algorithm of this project. Each key starts with `numberOfInitialTasks = 2` candidate workers and adaptively expands or contracts that pool based on observed load.

---

## The DKG Algorithm

### Key Idea

Every key has a **home index** derived from its hash. Its candidate worker pool is a contiguous ring slice starting at that index. The size of this slice grows when the key is hot and shrinks when load drops.

```
hash(key) → homeIndex → [homeIndex, homeIndex+1, ..., homeIndex+poolSize-1] (mod numWorkers)
```

On each tuple, DKG picks the **least-loaded worker** from the key's current candidate pool.

### Load Measurement

```
currentLoad(task) = 100 × tasksStats[task] / totalItems
```

Thresholds are set at topology startup:
```
idealLoad      = 100 / numberOfWorkers
loadToScaleUp  = idealLoad + sqrt(idealLoad)
loadToScaleDown = idealLoad - sqrt(idealLoad)
```

### Scale-Up Decision

A key's pool size is increased (up to `numWorkers`) if all three conditions are met:

1. **Warm-up passed** — the topology has been running for at least `warmUpDuration` ms (default 15 s), avoiding premature scaling on startup bursts.
2. **Load exceeds threshold** — the least-loaded candidate in the current pool exceeds `loadToScaleUp`.
3. **Key is in old space** — the key has been consistently observed long enough to be promoted to old-generation (see KeySpace below). Prevents ephemeral spikes from over-provisioning.

The candidate added is the next worker in the ring. It is only accepted if its current load is less than the minimum load of existing candidates.

### Scale-Down Decision

A key's pool size is reduced by one if `workerCount > 2` and at least two current candidates have load below `loadToScaleDown`.

Scaling checks are rate-limited per key by `checkInterval` (default 60 s) to avoid oscillation.

### KeySpace — Generational Key Lifecycle

Inspired by JVM generational garbage collection, keys progress through three spaces based on frequency:

```
babySpace (50% of capacity)
    │  promoted every 15 s
    ▼
teenageSpace (40% of capacity)
    │  promoted every 4 cycles (60 s)
    ▼
oldSpace (10% of capacity)
```

`KeySpaceManager` runs a background thread that every 15 seconds:
1. Sorts keys by frequency descending.
2. Promotes the top-frequency keys from `baby → teenage`.
3. Every 4th cycle also promotes `teenage → old`.

Only keys in `oldSpace` are eligible for scale-up. This ensures that only consistently high-frequency keys expand their worker pool, filtering out temporary traffic spikes.

`KeySpace.gc()` evicts keys from old/teenage space that haven't been seen in the last hour, reclaiming capacity.

---

## Datasets

DKG is evaluated against several real-world and synthetic datasets:

| SourceType | Description |
|---|---|
| `COUNTRY_SKEW_R{0..100}` | Country name stream with configurable skew ratio (R0 = uniform, R100 = fully skewed to one key) |
| `COUNTRY_HALF_SKEW_R80` | Half-skewed country distribution at 80% skew |
| `TWITTER_TICKER` | Stock ticker symbols from Twitter stream |
| `TWITTER_ELECTION` | Election-related tweets, keyed by candidate or hashtag |
| `WIKIPEDIA_PAGEVIEWS` | Wikipedia page view counts |
| `WIKIPEDIA_PAGEVIEWS_BY_LANG` | Wikipedia page views grouped by language |
| `WIKIPEDIA_CLICKSTREAM` | Wikipedia clickstream navigation data |

Kafka producers exist for all datasets in both Java (`src/main/java/.../data/producer/`) and Python (`src/main/python/.../dkg/`).

---

## Observability Metrics

The `DistributionObserverBolt` computes and logs the following metrics on each tick window:

| Metric | Description |
|---|---|
| `STD_DEV` | Standard deviation of load (% of total tuples) across all worker bolts — the primary fairness metric |
| `DIST_COST` | `totalWorkersUsed / distinctKeys` — measures how many workers are used per key on average (overhead of splitting) |
| `THROUGHPUT_RATIO` | Records processed per second |
| `TOTAL_COUNT` | Total tuples processed since topology start |
| `DURATION` | Elapsed time since first tuple |

Log prefix `#DIST` contains all distribution metrics. `#HOTTEST KEYS` lists the current occupants of old space sorted by frequency.

---

## Configuration

The application is launched via:

```bash
java -jar dkg-wd-<version>.jar <stormMode> <sourceType> <groupingType> <sourceName> <numSpouts> <numWorkers> <speed>
```

| Argument | Values | Example |
|---|---|---|
| `stormMode` | `LOCAL`, `CLUSTER` | `LOCAL` |
| `sourceType` | See SourceType enum | `COUNTRY_SKEW_R80` |
| `groupingType` | `SHUFFLE`, `KEY`, `PARTIAL_KEY`, `DYNAMIC_KEY` | `DYNAMIC_KEY` |
| `sourceName` | Kafka topic name | `country-skew-80` |
| `numSpouts` | Integer | `5` |
| `numWorkers` | Integer | `10` |
| `speed` | `x1`, `x2`, ... (processing speed multiplier) | `x1` |

Example:
```bash
java -jar dkg-wd-1.3.3.jar LOCAL COUNTRY_SKEW_R80 DYNAMIC_KEY country-skew-80 5 10 x1
```

### Key Configuration Parameters (set in `Application.java`)

| Parameter | Default | Description |
|---|---|---|
| `processDuration` | 1 ms | Simulated per-tuple processing time |
| `terminationDuration` | 10 days | Max topology runtime |
| `timeIntervalOfWorkerBolts` | 15 s | Window tick for WorkerBolt |
| `timeIntervalOfAggregatorBolts` | 60 s | Window tick for AggregatorBolt/DistributionObserver |
| `distinctKeyCount` | 100 | Hint to KeySpace for sizing its generational spaces |

---

## Building

```bash
mvn package -DskipTests
```

Produces `target/dkg-wd-<version>.jar` — a fat JAR with all dependencies.

**Requirements:**
- Java 8
- Maven 3.x
- Apache Storm 0.9.7 cluster (or local mode)
- Apache Kafka 0.10.2.x
- ZooKeeper (used by Storm and Kafka)

---

## Project Structure

```
src/
├── main/
│   ├── java/com/orhundalabasmaz/storm/
│   │   ├── Application.java                    # entrypoint & CLI arg parsing
│   │   ├── config/                             # Configuration & builder
│   │   ├── common/                             # Shared enums, factories, base types
│   │   ├── data/
│   │   │   ├── producer/                       # Kafka producers per dataset
│   │   │   └── message/                        # Dataset-specific message DTOs
│   │   ├── loadbalancer/
│   │   │   ├── LoadBalancerTopology.java       # Storm topology wiring
│   │   │   ├── bolts/                          # SplitterBolt, WorkerBolt, AggregatorBolt, observers
│   │   │   ├── grouping/                       # Shuffle, Key, PartialKey, DynamicKey groupings
│   │   │   │   └── dkg/                        # DKG core: KeySpace, KeyItem, KeySpaceManager
│   │   │   └── aggregator/                     # KeyAggregator, DistributionAggregator
│   │   ├── model/                              # Output message models
│   │   ├── serializer/                         # JSON encoder/decoder for Kafka
│   │   └── utils/                             # DKGUtils (hashing, sleep, stats)
│   └── python/com.orhundalabasmaz.storm/dkg/  # Python Kafka producers
└── test/
    └── java/com/orhundalabasmaz/storm/        # JUnit + Cucumber tests
```

---

## How It Compares

| Strategy | Load Balance | Aggregation Overhead | Handles Hot Keys |
|---|---|---|---|
| Shuffle | Optimal | High (all keys split) | Yes |
| Key | Poor (skewed) | None | No |
| Partial Key | Good | Low (2 workers/key) | Partially |
| **Dynamic Key** | **Good** | **Adaptive (2..N workers/key)** | **Yes** |

DKG's key advantage over Partial Key Grouping is that it adapts to the actual observed frequency of each key. Cold keys use 2 workers (same as PKG), while hot keys that survive generational promotion can use more, bounded by the total worker count.

---

## Author

**Orhun Dalabasmaz** — [odalabasmaz@gmail.com](mailto:odalabasmaz@gmail.com)
