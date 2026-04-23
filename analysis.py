"""
Polymarket sequential-scan benchmark.

Streams `SELECT * FROM bedrock.predictions.polymarket_orderbook` end-to-end via
Arrow Flight (gRPC, port 7778) to measure the Iceberg → DuckDB → Flight → SDK
throughput curve. Logs cumulative rows + elapsed every PARAM_LOG_EVERY rows
(default 1,000,000) so the live log shows progress and the final dashboard
plots scan time as a function of rows scanned.

Why raw Flight instead of job.fetch():
  job.fetch() calls reader.read_all() — blocks until the entire result is
  materialized in DuckDB. For an unbounded SELECT * over a 100M+ row table
  that would OOM the analysis container and produce no progress logs. We
  iterate read_chunk() instead so memory stays flat and we emit progress live.

DuckDB threading is auto-tuned by the sidecar at startup from its cgroup CPU
limit (main.rs:57-89 → threads_per_connection = max(cores, 2)). With the
4-CPU sidecar configured by bedrock.json's resources block, DuckDB will use
4 threads automatically — no SET threads needed here.
"""

import json
import os
import sys
import time

import pyarrow as pa
from pyarrow import flight

# bedrock_sdk is COPYed to /bedrock_sdk/ by the Dockerfile; not on sys.path by default.
sys.path.insert(0, "/")
from bedrock_sdk import BedrockJob


def main() -> None:
    job = BedrockJob()
    log_every = int(os.environ.get("PARAM_LOG_EVERY", "1000000"))

    # Flight endpoint = sidecar HTTP port + 1 (mirrors SDK _fetch_flight pattern).
    qe_host = job.qe_url.replace("http://", "").replace("https://", "")
    host, http_port = qe_host.rsplit(":", 1)
    flight_url = f"grpc://{host}:{int(http_port) + 1}"

    job.progress(0, f"Connecting to Arrow Flight at {flight_url}")
    client = flight.FlightClient(flight_url)

    # Ticket payload is JSON {"sql": "..."}, exactly like SDK _fetch_flight does.
    sql = "SELECT * FROM bedrock.predictions.polymarket_orderbook"
    ticket = flight.Ticket(json.dumps({"sql": sql}).encode("utf-8"))
    headers = [(b"authorization", f"Bearer {job.job_token}".encode())]
    options = flight.FlightCallOptions(headers=headers)

    job.progress(1, f"Starting unbounded scan: {sql}")
    reader = client.do_get(ticket, options)

    start = time.monotonic()
    rows = 0
    next_milestone = log_every
    samples: list[dict] = []  # one row per checkpoint → scan_timeline.parquet

    while True:
        try:
            batch = reader.read_chunk().data
        except StopIteration:
            break

        rows += batch.num_rows

        # Multiple milestones can be crossed by a single big batch — drain them.
        while rows >= next_milestone:
            elapsed = time.monotonic() - start
            mrows_per_s = (rows / 1e6) / elapsed if elapsed > 0 else 0.0
            samples.append({
                "rows": rows,
                "elapsed_s": round(elapsed, 3),
                "mrows_per_s": round(mrows_per_s, 3),
            })
            # progress() pct caps at 99 since we don't know total rows up-front.
            # The live log message carries the true progress detail.
            job.progress(
                min(99, int(rows / 1_000_000_000 * 100)),
                f"{rows / 1e6:.1f}M rows in {elapsed:.1f}s ({mrows_per_s:.2f} M rows/s)",
            )
            next_milestone += log_every

    elapsed = time.monotonic() - start
    final_mrows_per_s = (rows / 1e6) / elapsed if elapsed > 0 else 0.0

    # Make sure the FINAL row is always in the timeline, even if it didn't land
    # on a clean log_every boundary.
    if not samples or samples[-1]["rows"] != rows:
        samples.append({
            "rows": rows,
            "elapsed_s": round(elapsed, 3),
            "mrows_per_s": round(final_mrows_per_s, 3),
        })

    # Write the timeline as parquet so the dashboard can chart it.
    conn = job.connect()
    arrow_samples = pa.Table.from_pylist(samples)
    conn.register("samples", arrow_samples)
    job.write_parquet("scan_timeline", "SELECT * FROM samples ORDER BY rows")

    # Also write a one-row summary parquet so the dashboard has clean KPI inputs.
    conn.execute(
        "CREATE TABLE summary AS "
        "SELECT MAX(rows) AS total_rows, "
        "       MAX(elapsed_s) AS total_elapsed_s, "
        f"      ({rows} / {elapsed if elapsed > 0 else 1.0} / 1e6) AS avg_mrows_per_s "
        "FROM samples"
    )
    job.write_parquet("summary", "SELECT * FROM summary")

    job.update_progress(
        "running_analysis",
        progress_pct=99,
        progress_message=f"Scan complete — {rows:,} rows in {elapsed:.1f}s",
        lineage={
            "inputs": ["bedrock.predictions.polymarket_orderbook"],
            "outputs": [
                f"analytics/bedrock/{job.job_id}/data/scan_timeline.parquet",
                f"analytics/bedrock/{job.job_id}/data/summary.parquet",
            ],
        },
    )

    # Upload dashboard/ (markdown + _manifest.json) to R2 so the in-browser
    # Bedrock Dash renderer at /analysis_runs/<id>/dashboard/index.html can
    # load it. Without this call, the renderer 404s on _manifest.json.
    job.write_dashboard_dir()

    job.conclusion(
        f"Scanned {rows:,} rows in {elapsed:.1f}s — "
        f"avg throughput {final_mrows_per_s:.2f} M rows/s via Arrow Flight."
    )
    job.complete()


if __name__ == "__main__":
    main()
