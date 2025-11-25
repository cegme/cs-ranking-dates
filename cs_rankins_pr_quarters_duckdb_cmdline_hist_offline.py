#!/usr/bin/env python3
"""
Fetch, cache (DuckDB), and plot merged PRs over time
with quarterly markers and a daily histogram.

Left Y-axis: cumulative merged PR count
Right Y-axis: merged PRs per day (binned by date)

Run:
  pip install requests duckdb matplotlib loguru

Examples:
  # Normal incremental mode (hits GitHub, uses cache)
  python csrankings_pr_quarters_duckdb.py \
      --db csrankings.duckdb \
      --owner emeryberger \
      --repo CSrankings

  # Full refresh (refetch everything, clears cache)
  python csrankings_pr_quarters_duckdb.py \
      --db csrankings.duckdb \
      --owner emeryberger \
      --repo CSrankings \
      --full-refresh true

  # Offline mode (NO GitHub calls, only use existing DuckDB data)
  python csrankings_pr_quarters_duckdb.py \
      --db csrankings.duckdb \
      --owner emeryberger \
      --repo CSrankings \
      --offline true
"""

import os
import argparse
import datetime as dt
import requests
import duckdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from collections import Counter
from loguru import logger


# ---------------------------------------------------------
# DB Helpers
# ---------------------------------------------------------

def get_connection(db_path: str):
    logger.info(f"Opening DuckDB database: {db_path}")
    con = duckdb.connect(db_path)
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS pulls (
            number       INTEGER PRIMARY KEY,
            merged_at    TIMESTAMP,
            created_at   TIMESTAMP,
            closed_at    TIMESTAMP,
            state        VARCHAR,
            merged       BOOLEAN
        )
        """
    )
    return con


def get_max_cached_number(con):
    result = con.execute("SELECT MAX(number) FROM pulls").fetchone()
    return result[0] if result and result[0] is not None else None


def insert_prs(con, prs):
    if not prs:
        return

    logger.debug(f"Inserting {len(prs)} PR rows into DuckDB…")

    con.executemany(
        """
        INSERT OR REPLACE INTO pulls
        (number, merged_at, created_at, closed_at, state, merged)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        [
            (
                p["number"],
                p["merged_at"],
                p["created_at"],
                p["closed_at"],
                p["state"],
                p["merged"],
            )
            for p in prs
        ],
    )


# ---------------------------------------------------------
# API + Caching
# ---------------------------------------------------------

def fetch_and_cache_prs(db_path, owner, repo, full_refresh=False):
    """Fetch closed PRs from GitHub, caching them in DuckDB."""

    api_url = f"https://api.github.com/repos/{owner}/{repo}/pulls"

    logger.info(f"Repository: {owner}/{repo}")
    logger.info(f"GitHub API: {api_url}")

    con = get_connection(db_path)

    max_cached_number = None if full_refresh else get_max_cached_number(con)

    if full_refresh:
        logger.warning("Full refresh requested: clearing existing PR cache.")
        con.execute("DELETE FROM pulls")

    logger.info(f"Max cached PR number: {max_cached_number}")

    session = requests.Session()
    token = os.getenv("GITHUB_TOKEN")
    if token:
        session.headers["Authorization"] = f"Bearer {token}"

    page = 1
    per_page = 100
    total_new = 0

    while True:
        params = {
            "state": "closed",
            "per_page": per_page,
            "page": page,
            "sort": "created",
            "direction": "desc",
        }

        logger.info(f"Fetching PR page {page}…")
        resp = session.get(api_url, params=params)
        resp.raise_for_status()

        prs = resp.json()
        if not prs:
            logger.info("No more PRs returned.")
            break

        batch = []
        stop_due_to_cache = False

        for pr in prs:
            number = pr["number"]

            # Stop if hitting cached region
            if max_cached_number is not None and number <= max_cached_number:
                logger.info("Reached previously cached PR number; stopping incrementally.")
                stop_due_to_cache = True
                break

            # Normalize timestamps
            def parse_ts(ts):
                if ts is None:
                    return None
                return dt.datetime.fromisoformat(ts.replace("Z", "+00:00")).replace(tzinfo=None)

            merged_at = parse_ts(pr.get("merged_at"))
            created_at = parse_ts(pr.get("created_at"))
            closed_at = parse_ts(pr.get("closed_at"))

            batch.append(
                {
                    "number": number,
                    "merged_at": merged_at,
                    "created_at": created_at,
                    "closed_at": closed_at,
                    "state": pr.get("state"),
                    "merged": merged_at is not None,
                }
            )

        insert_prs(con, batch)
        total_new += len(batch)

        if stop_due_to_cache:
            break

        link = resp.headers.get("Link", "")
        if 'rel="next"' not in link:
            logger.info("No next page in Link header.")
            break

        page += 1

    con.close()
    logger.success(f"Fetch complete. New rows inserted this run: {total_new}")


# ---------------------------------------------------------
# Query + Plotting
# ---------------------------------------------------------

def load_merged_dates_from_db(db_path):
    con = get_connection(db_path)
    rows = con.execute(
        """
        SELECT merged_at
        FROM pulls
        WHERE merged = TRUE AND merged_at IS NOT NULL
        ORDER BY merged_at
        """
    ).fetchall()
    con.close()
    return [r[0] for r in rows]


def quarter_markers(start, end):
    markers = []
    year = start.year
    quarter_months = [1, 4, 7, 10]

    while dt.datetime(year, 1, 1) <= end:
        for m in quarter_months:
            q = dt.datetime(year, m, 1)
            if start <= q <= end:
                markers.append(q)
        year += 1

    return markers


def plot_merged_with_quarters(merged_dates, owner, repo):
    """
    Left axis: cumulative merged PRs.
    Right axis: binned daily counts of merged PRs.
    """
    if not merged_dates:
        logger.warning("No merged PRs found; nothing to plot.")
        return

    # --- Cumulative line (left axis) ---
    xs = merged_dates
    ys = list(range(1, len(xs) + 1))

    # Make plot twice as wide (24 instead of 12)
    fig, ax = plt.subplots(figsize=(24, 6))
    ax.plot(xs, ys, linewidth=1.5)

    ax.set_title(
        f"Merged PRs Over Time for {owner}/{repo}"
    )
    ax.set_xlabel("Date")
    ax.set_ylabel("Cumulative merged PR count")

    # --- Quarter markers ---
    qmarks = quarter_markers(xs[0], xs[-1])
    ymin, ymax = ax.get_ylim()

    for q in qmarks:
        ax.axvline(q, linestyle="--", alpha=0.25)
        label = f"Q{((q.month - 1) // 3) + 1} {q.year}"
        ax.text(
            q, ymax, label,
            rotation=90,
            ha="right",
            va="top",
            fontsize=8,
        )

    # --- Daily histogram on right axis ---
    # Count merges per calendar day
    dates_only = [d.date() for d in merged_dates]
    counts = Counter(dates_only)
    unique_dates = sorted(counts.keys())
    counts_per_day = [counts[d] for d in unique_dates]

    # Convert back to datetime for plotting
    hist_x = [dt.datetime.combine(d, dt.time()) for d in unique_dates]

    ax2 = ax.twinx()
    one_day = 1.0  # Matplotlib date units
    ax2.bar(hist_x, counts_per_day, width=one_day, alpha=0.3, color="orange", label="Merged PRs per day")
    ax2.set_ylabel("Merged PRs per day")

    # Make sure both axes share the same x-limits
    ax.set_xlim(xs[0], xs[-1])

    # Nicer date formatting on x-axis
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m-%d"))
    fig.autofmt_xdate()

    fig.tight_layout()
    plt.show()


# ---------------------------------------------------------
# CLI
# ---------------------------------------------------------

def parse_args():
    parser = argparse.ArgumentParser(
        description="Analyze and plot merged PRs for a GitHub repo using DuckDB cache."
    )

    parser.add_argument("--db", required=True, help="Path to DuckDB database file.")
    parser.add_argument("--owner", required=True, help="GitHub repository owner.")
    parser.add_argument("--repo", required=True, help="GitHub repository name.")
    parser.add_argument(
        "--full-refresh",
        default="false",
        choices=["true", "false"],
        help="If true, ignore cache and refetch everything (slow).",
    )
    parser.add_argument(
        "--offline",
        default="false",
        choices=["true", "false"],
        help="If true, do NOT hit GitHub; only use existing DuckDB data.",
    )

    return parser.parse_args()


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():
    args = parse_args()

    full_refresh = args.full_refresh.lower() == "true"
    offline = args.offline.lower() == "true"

    if offline and full_refresh:
        logger.warning(
            "--offline true and --full-refresh true were both specified. "
            "Offline mode wins; full refresh is ignored."
        )

    # Only fetch from GitHub if not offline
    if not offline:
        fetch_and_cache_prs(
            db_path=args.db,
            owner=args.owner,
            repo=args.repo,
            full_refresh=full_refresh,
        )
    else:
        logger.info("Offline mode: skipping all GitHub API calls.")

    merged_dates = load_merged_dates_from_db(args.db)
    logger.info(f"Loaded {len(merged_dates)} merged PRs from DB.")

    if not merged_dates:
        logger.error("No merged PR data found in DuckDB. Nothing to plot.")
        return

    plot_merged_with_quarters(merged_dates, args.owner, args.repo)


if __name__ == "__main__":
    main()
