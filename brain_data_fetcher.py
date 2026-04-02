"""
WorldQuant BRAIN Data Field Fetcher

Authenticates with the BRAIN platform, paginates through all available
data fields, and saves the result to a JSON file.  The submitter can then
read that file instead of re-fetching on every run.

Usage:
    python brain_data_fetcher.py --credentials credentials.json
    python brain_data_fetcher.py --credentials credentials.json \\
        --region USA --universe TOP3000 --output data_fields.json
"""

import argparse
import json
import logging
import sys
import time

from brain_submitter import (
    BRAIN_BASE,
    build_client,
    _get_session,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def fetch_all_fields(
    credentials,
    instrument_type: str,
    region: str,
    universe: str,
    delay: int,
    category: str,
) -> list:
    client = build_client(credentials)
    fields = []
    offset, limit = 0, 20

    log.info(
        "Fetching data fields (instrumentType=%s, region=%s, universe=%s, delay=%s) ...",
        instrument_type, region, universe, delay,
    )

    while True:
        params = {
            "instrumentType": instrument_type,
            "region":         region,
            "universe":       universe,
            "delay":          delay,
            "language":       "FASTEXPR",
            "limit":          limit,
            "offset":         offset,
        }
        if category:
            params["category"] = category

        r = _get_session(client).get(f"{BRAIN_BASE}/data-fields", params=params)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 60))
            log.warning("Rate limited. Waiting %ds ...", wait)
            time.sleep(wait)
            continue
        if r.status_code == 400:
            log.error("400 Bad Request. Response: %s", r.text)
        r.raise_for_status()

        page = r.json()
        results = page.get("results", [])
        fields.extend(results)
        log.info("  fetched %d fields so far (offset=%d) ...", len(fields), offset)
        if len(results) < limit:
            break
        offset += limit
        time.sleep(0.3)

    log.info("Total data fields fetched: %d", len(fields))
    return fields


def main() -> None:
    p = argparse.ArgumentParser(
        description="Fetch and cache WorldQuant BRAIN data fields to a JSON file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--credentials",     default=None,
                   help="Path to credentials JSON {username, password}")
    p.add_argument("--region",          default="USA")
    p.add_argument("--universe",        default="TOP3000")
    p.add_argument("--instrument-type", default="EQUITY")
    p.add_argument("--delay",           default=1, type=int)
    p.add_argument("--category",        default="",
                   help="Optional category filter (e.g. 'fundamental')")
    p.add_argument("--output",          default="data_fields.json",
                   help="Output JSON file path")
    args = p.parse_args()

    fields = fetch_all_fields(
        credentials=args.credentials,
        instrument_type=args.instrument_type,
        region=args.region,
        universe=args.universe,
        delay=args.delay,
        category=args.category,
    )

    with open(args.output, "w") as f:
        json.dump(fields, f, indent=2)

    log.info("Saved %d data fields to %s", len(fields), args.output)


if __name__ == "__main__":
    main()
