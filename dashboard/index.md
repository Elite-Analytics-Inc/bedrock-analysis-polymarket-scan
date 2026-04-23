---
title: Polymarket Sequential Scan Benchmark
---

# Polymarket Sequential Scan Benchmark

Full `SELECT * FROM bedrock.predictions.polymarket_orderbook` over Arrow Flight
(gRPC, port 7778). DuckDB threads auto-tuned to the sidecar's CPU limit.
Each point on the timeline is a 1,000,000-row checkpoint.

```sql summary
SELECT * FROM summary
```

```sql timeline
SELECT
  rows,
  rows / 1e6                                    AS millions,
  elapsed_s,
  mrows_per_s,
  ROUND(mrows_per_s * 1.0, 2)                   AS mrows_per_s_round
FROM scan_timeline
ORDER BY rows
```

```sql trailing
SELECT
  rows,
  ROUND(elapsed_s, 1)                            AS elapsed_s,
  ROUND(mrows_per_s, 2)                          AS mrows_per_s
FROM scan_timeline
ORDER BY rows DESC
LIMIT 12
```

{% big_value data="$summary" value="total_rows"        title="Rows scanned"        fmt="num0" /%}
{% big_value data="$summary" value="total_elapsed_s"   title="Elapsed (s)"         fmt="num1" /%}
{% big_value data="$summary" value="avg_mrows_per_s"   title="Avg throughput"      fmt="num2" suffix=" M rows/s" /%}

## Scan time vs rows

How long the scan has taken at each 1M-row checkpoint. A straight line means
constant throughput; an upward bend means throughput is degrading; a downward
bend means it's accelerating (warming caches).

{% line_chart data="$timeline"
              x="millions"
              y=["elapsed_s"]
              title="Cumulative elapsed time"
              yAxisTitle="Seconds"
              colors=["#3b82f6"] /%}

## Throughput vs rows

Cumulative throughput (M rows/s) at each checkpoint. Flat means R2/disk is the
bottleneck; declining means CPU/decompression saturating; step changes hint at
per-file boundaries.

{% line_chart data="$timeline"
              x="millions"
              y=["mrows_per_s"]
              title="Cumulative throughput"
              yAxisTitle="M rows / second"
              colors=["#22c55e"] /%}

## Last 12 checkpoints

{% data_table data="$trailing" rows=12 rowShading=true %}
{% column id="rows"        title="Rows"               fmt="num0" /%}
{% column id="elapsed_s"   title="Elapsed (s)"        fmt="num1" /%}
{% column id="mrows_per_s" title="Throughput"         contentType="colorscale" scaleColor=["#fecaca","#22c55e"] fmt="num2" /%}
{% /data_table %}
