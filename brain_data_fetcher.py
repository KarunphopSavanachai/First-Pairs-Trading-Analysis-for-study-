"""
WorldQuant BRAIN Data Field Fetcher

Authenticates with the BRAIN platform, paginates through all available
data fields (optionally grouped by category), and saves the result to a
JSON file.  The submitter can then read that file instead of re-fetching
on every run.

Usage:
    # Fetch all fields grouped into 7 categories (default)
    python brain_data_fetcher.py --credentials credentials.json

    # Fetch only one category
    python brain_data_fetcher.py --credentials credentials.json --category fundamental

    # Custom output path / universe
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

# ── Category definitions ───────────────────────────────────────────────────────
# Maps friendly display names to the API query values accepted by BRAIN.
# If the API uses different values for your region/universe, adjust these.
CATEGORIES = {
    "analyst":      "analyst",
    "fundamental":  "fundamental",
    "model":        "model",
    "news":         "news",
    "option":       "option",
    "price_volume": "price_volume",
    "social_media": "social_media",
}


def _paginate(client, base_params: dict) -> list:
    """Paginate through /data-fields with base_params, return all results."""
    fields = []
    offset, limit = 0, 20
    params = {**base_params, "limit": limit}
    while True:
        params["offset"] = offset
        r = _get_session(client).get(f"{BRAIN_BASE}/data-fields", params=params)
        if r.status_code == 429:
            wait = int(r.headers.get("Retry-After", 60))
            cat = base_params.get("category", "all")
            log.warning("Rate limited (category=%s). Waiting %ds ...", cat, wait)
            time.sleep(wait)
            continue
        if r.status_code == 400:
            cat = base_params.get("category", "")
            log.warning(
                "400 Bad Request%s. Response: %s",
                f" for category={cat!r}" if cat else "",
                r.text,
            )
            return []
        r.raise_for_status()
        page = r.json()
        results = page.get("results", [])
        fields.extend(results)
        log.info(
            "  category=%-15s  fetched %d so far (offset=%d)",
            base_params.get("category", "all"), len(fields), offset,
        )
        if len(results) < limit:
            break
        offset += limit
        time.sleep(0.3)
    return fields


def fetch_all_fields(
    credentials,
    instrument_type: str,
    region: str,
    universe: str,
    delay: int,
    category: str,
    all_categories: bool = False,
):
    """Fetch data fields from BRAIN.

    Parameters
    ----------
    all_categories : bool
        When True, fetch fields separately for each category in CATEGORIES
        and return a dict keyed by category name.
        When False (or when category is given), return a flat list.
    """
    client = build_client(credentials)

    base_params = {
        "instrumentType": instrument_type,
        "region":         region,
        "universe":       universe,
        "delay":          delay,
        "language":       "FASTEXPR",
    }

    if all_categories:
        log.info(
            "Fetching fields for all %d categories "
            "(instrumentType=%s, region=%s, universe=%s, delay=%s) ...",
            len(CATEGORIES), instrument_type, region, universe, delay,
        )
        result = {}
        for cat_name, cat_key in CATEGORIES.items():
            cat_params = {**base_params, "category": cat_key}
            cat_fields = _paginate(client, cat_params)
            result[cat_name] = cat_fields
            if not cat_fields:
                log.warning(
                    "  category=%r returned 0 fields. "
                    "The API value %r may need adjustment.",
                    cat_name, cat_key,
                )
            else:
                log.info("  category=%-15s  %d fields", cat_name, len(cat_fields))

        total = sum(len(v) for v in result.values())
        log.info("Total data fields across all categories: %d", total)
        return result

    else:
        log.info(
            "Fetching data fields (instrumentType=%s, region=%s, "
            "universe=%s, delay=%s%s) ...",
            instrument_type, region, universe, delay,
            f", category={category!r}" if category else "",
        )
        params = {**base_params}
        if category:
            params["category"] = category
        fields = _paginate(client, params)
        log.info("Total data fields fetched: %d", len(fields))
        return fields


def main() -> None:
    p = argparse.ArgumentParser(
        description="Fetch and cache WorldQuant BRAIN data fields to a JSON file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--credentials",     default="credentials.json",
                   help="Path to credentials JSON {username, password}")
    p.add_argument("--region",          default="USA")
    p.add_argument("--universe",        default="TOP3000")
    p.add_argument("--instrument-type", default="EQUITY")
    p.add_argument("--delay",           default=1, type=int)
    p.add_argument(
        "--category", default="",
        help=(
            "Fetch only a single category (e.g. 'fundamental'). "
            "When omitted, all 7 categories are fetched and saved as a "
            "structured dict."
        ),
    )
    p.add_argument("--output", default="data_fields.json",
                   help="Output JSON file path")
    args = p.parse_args()

    # Fetch all categories when no specific category is requested
    all_categories = not args.category

    fields = fetch_all_fields(
        credentials=args.credentials,
        instrument_type=args.instrument_type,
        region=args.region,
        universe=args.universe,
        delay=args.delay,
        category=args.category,
        all_categories=all_categories,
    )

    with open(args.output, "w") as f:
        json.dump(fields, f, indent=2)

    if isinstance(fields, dict):
        total = sum(len(v) for v in fields.values())
        log.info(
            "Saved %d fields across %d categories to %s",
            total, len(fields), args.output,
        )
    else:
        log.info("Saved %d data fields to %s", len(fields), args.output)


if __name__ == "__main__":
    main()
