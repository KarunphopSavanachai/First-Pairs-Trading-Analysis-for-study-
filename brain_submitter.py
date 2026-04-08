"""
WorldQuant BRAIN Automatic Alpha Submitter

Swaps {DATA} placeholders in operator templates across all available
data fields, submits each alpha, polls for results, then exports
passing alphas to Excel.

Requires:
    pip install autobrain-sim openpyxl requests

Usage:
    python brain_submitter.py --template "ts_rank({DATA}, 10)" \\
        --sharpe 1.2 --fitness 1.0 \\
        --universe TOP3000 --region USA \\
        --neutralization SUBINDUSTRY

    # Verify credentials before a full run:
    python brain_submitter.py --test-auth --credentials credentials.json

    # Resume a crashed run:
    python brain_submitter.py --template "ts_rank({DATA}, 10)" --resume
"""

import argparse
import csv
import hashlib
import itertools
import json
import logging
import random
import re
import sqlite3
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Dict, List, Optional, Tuple

from brain_client import BrainClient

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("brain_submitter.log"),
    ],
)
log = logging.getLogger(__name__)

BRAIN_BASE = "https://api.worldquantbrain.com"

# ═══════════════════════════════════════════════════════════════════════════════
# 1. CREDENTIALS
# ═══════════════════════════════════════════════════════════════════════════════

def load_credentials(path: str) -> Dict[str, str]:
    """Load username/password from a JSON credentials file."""
    with open(path) as f:
        creds = json.load(f)
    if "username" not in creds or "password" not in creds:
        raise ValueError(
            f"'{path}' must contain both 'username' and 'password' keys"
        )
    return creds

# ═══════════════════════════════════════════════════════════════════════════════
# 3. AUTH HELPER
# ═══════════════════════════════════════════════════════════════════════════════

def build_client(credentials: Optional[str]) -> BrainClient:
    """Create and authenticate a BrainClient.

    If credentials path is given, reads {"username", "password"} from the JSON
    file and passes them directly.  Otherwise autobrain-sim's own priority chain
    applies: ~/.brain_credentials → interactive prompt.
    """
    if credentials:
        creds  = load_credentials(credentials)
        client = BrainClient(email=creds["username"], password=creds["password"])
    else:
        client = BrainClient()  # reads ~/.brain_credentials or prompts
    client.authenticate()
    log.info("Authentication successful.")
    return client

def _get_session(client: BrainClient):
    """Return the underlying requests.Session from BrainClient.

    Different versions of autobrain-sim expose it under different names.
    """
    for attr in ("session", "_session", "requests_session"):
        s = getattr(client, attr, None)
        if s is not None:
            return s
    raise AttributeError(
        "Cannot find requests.Session on BrainClient. "
        "Try: pip install --upgrade autobrain-sim"
    )

def simulate_with_retry(client, expr: str, settings: dict, max_retries: int = 6):
    """Call client.simulate() with automatic retry on HTTP 429."""
    import requests as _requests
    for attempt in range(max_retries + 1):
        try:
            return client.simulate(expr, settings=settings)
        except _requests.HTTPError as e:
            if e.response is not None and e.response.status_code == 429:
                wait = int(e.response.headers.get("Retry-After", 60))
                log.warning(
                    "Rate limited on submission (attempt %d/%d). Waiting %ds ...",
                    attempt + 1, max_retries, wait,
                )
                time.sleep(wait)
                continue
            raise
        except Exception as e:
            if "429" in str(e) or "Too Many Requests" in str(e):
                backoff = min(60 * (2 ** attempt), 600)
                log.warning(
                    "Rate limited (429) on submission (attempt %d/%d). Waiting %ds ...",
                    attempt + 1, max_retries, backoff,
                )
                time.sleep(backoff)
                continue
            raise
    raise RuntimeError(f"Max retries ({max_retries}) exceeded on simulate()")

# ═══════════════════════════════════════════════════════════════════════════════
# 4. SIMULATION POLLER  (polls progress_url stored in the state DB)
# ═══════════════════════════════════════════════════════════════════════════════

def poll_simulation(
    client: BrainClient,
    progress_url: str,
    poll_interval: int = 15,
    timeout: int = 600,
) -> Optional[Dict]:
    """Poll a simulation progress URL until COMPLETE or timeout.

    Uses only the documented client.session attribute.
    Returns the result dict on success, None on error/timeout.
    Raises ValueError if progress_url is empty.
    """
    if not progress_url:
        raise ValueError("progress_url is empty — cannot poll simulation")

    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = _get_session(client).get(progress_url)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                log.warning("Rate limited while polling. Waiting %ds ...", wait)
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
        except Exception as e:
            log.warning("Poll error (%s): %s", progress_url.split("/")[-1], e)
            time.sleep(poll_interval)
            continue

        status = data.get("status", "")
        if status == "COMPLETE":
            return data
        if status in ("ERROR", "CANCELLED"):
            log.warning(
                "Simulation %s ended with status=%s",
                progress_url.split("/")[-1], status,
            )
            return None
        time.sleep(poll_interval)

    log.warning("Simulation timed out: %s", progress_url.split("/")[-1])
    return None

# ═══════════════════════════════════════════════════════════════════════════════
# 5. STATE DATABASE
# ═══════════════════════════════════════════════════════════════════════════════

class StateDB:
    """SQLite-backed state store — enables crash-safe resumable runs.

    sim_id stores the simulation progress_url returned by autobrain-sim.
    alpha_id stores the final alpha ID once the simulation completes.
    """

    DDL = """
    CREATE TABLE IF NOT EXISTS runs (
        expr_hash    TEXT PRIMARY KEY,
        expression   TEXT NOT NULL,
        field_id     TEXT NOT NULL,
        status       TEXT NOT NULL DEFAULT 'pending',
        template_idx INTEGER NOT NULL DEFAULT 0,
        sim_id       TEXT,
        alpha_id     TEXT,
        sharpe       REAL,
        fitness      REAL,
        turnover     REAL,
        returns      REAL,
        drawdown     REAL,
        passed       INTEGER DEFAULT 0,
        submitted_at TEXT,
        finished_at  TEXT,
        error        TEXT
    );
    """

    def __init__(self, db_path: str = "brain_runs.db"):
        self.conn: Optional[sqlite3.Connection] = sqlite3.connect(
            db_path, check_same_thread=False
        )
        self.conn.row_factory = sqlite3.Row
        self.conn.execute(self.DDL)
        self.conn.commit()
        # Migrate existing DBs that predate the template_idx column
        try:
            self.conn.execute(
                "ALTER TABLE runs ADD COLUMN template_idx INTEGER NOT NULL DEFAULT 0"
            )
            self.conn.commit()
        except sqlite3.OperationalError:
            pass  # column already exists

    def close(self) -> None:
        """Explicitly close the database connection."""
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def __del__(self) -> None:
        self.close()

    # ── Write methods ─────────────────────────────────────────────────────────

    def upsert_pending(self, expr: str, field_id: str) -> None:
        h = _sha(expr)
        self.conn.execute(
            """INSERT OR IGNORE INTO runs (expr_hash, expression, field_id, status)
               VALUES (?, ?, ?, 'pending')""",
            (h, expr, field_id),
        )
        self.conn.commit()

    def bulk_upsert_pending(self, rows: List[Tuple[str, str, int]]) -> None:
        """Insert many (expr, field_id, template_idx) triples in one transaction."""
        self.conn.executemany(
            """INSERT OR IGNORE INTO runs
                   (expr_hash, expression, field_id, status, template_idx)
               VALUES (?, ?, ?, 'pending', ?)""",
            [(_sha(expr), expr, field_id, tidx) for expr, field_id, tidx in rows],
        )
        self.conn.commit()

    def clear(self) -> None:
        """Delete all rows — wipes stale state before a fresh (non-resume) run."""
        self.conn.execute("DELETE FROM runs")
        self.conn.commit()

    def mark_submitted(self, expr: str, progress_url: str) -> None:
        self.conn.execute(
            """UPDATE runs SET status='submitted', sim_id=?, submitted_at=?
               WHERE expr_hash=?""",
            (progress_url, _now(), _sha(expr)),
        )
        self.conn.commit()

    def mark_done(
        self,
        expr: str,
        metrics: Dict,
        passed: bool,
        alpha_id: str = "",
    ) -> None:
        self.conn.execute(
            """UPDATE runs
               SET status='done', sharpe=?, fitness=?, turnover=?,
                   returns=?, drawdown=?, passed=?, alpha_id=?, finished_at=?
               WHERE expr_hash=?""",
            (
                metrics.get("sharpe"),
                metrics.get("fitness"),
                metrics.get("turnover"),
                metrics.get("returns"),
                metrics.get("drawdown"),
                1 if passed else 0,
                alpha_id,
                _now(),
                _sha(expr),
            ),
        )
        self.conn.commit()

    def mark_failed(self, expr: str, error: str) -> None:
        self.conn.execute(
            """UPDATE runs SET status='failed', error=?, finished_at=?
               WHERE expr_hash=?""",
            (error, _now(), _sha(expr)),
        )
        self.conn.commit()

    # ── Read methods ──────────────────────────────────────────────────────────

    def pending(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM runs WHERE status='pending' ORDER BY rowid"
        ).fetchall()

    def submitted(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM runs WHERE status='submitted'"
        ).fetchall()

    def all_done(self) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM runs WHERE status='done' ORDER BY sharpe DESC"
        ).fetchall()

    def pending_for_template(self, template_idx: int) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM runs WHERE status='pending' AND template_idx=? ORDER BY rowid",
            (template_idx,),
        ).fetchall()

    def submitted_for_template(self, template_idx: int) -> List[sqlite3.Row]:
        return self.conn.execute(
            "SELECT * FROM runs WHERE status='submitted' AND template_idx=? ORDER BY rowid",
            (template_idx,),
        ).fetchall()

# ═══════════════════════════════════════════════════════════════════════════════
# 5. TEMPLATE ENGINE
# ═══════════════════════════════════════════════════════════════════════════════

class TemplateEngine:
    """Generates alpha expressions by substituting field IDs for placeholders.

    Supported placeholder styles
    ----------------------------
    Single   : {DATA}            All occurrences replaced with the same field.
                                 e.g. "ts_rank({DATA},10) / ts_mean({DATA},5)"
    Numbered : {DATA1},{DATA2},… Each replaced with a different field ID.
                                 All combinations are generated automatically.
                                 e.g. "rank({DATA1},10) + rank({DATA2},5)"
    """

    _SINGLE   = "{DATA}"
    _NUMBERED = re.compile(r'\{DATA\d+\}')

    def __init__(self, template: str):
        numbered = sorted(set(self._NUMBERED.findall(template)))
        if numbered:
            self.placeholders = numbered          # e.g. ['{DATA1}', '{DATA2}']
            self.mode         = "multi"
        elif self._SINGLE in template:
            self.placeholders = [self._SINGLE]
            self.mode         = "single"
        else:
            # No placeholder — the expression is fully hardcoded; submit as-is.
            self.placeholders = []
            self.mode         = "static"
        self.template = template

    def generate(self, *field_ids: str) -> str:
        """Replace each placeholder with the corresponding field ID."""
        expr = self.template
        for ph, fid in zip(self.placeholders, field_ids):
            expr = expr.replace(ph, fid)
        return expr

    def all_combinations(
        self, field_lists
    ) -> List[Tuple[str, List[str]]]:
        """Return (expression, [field_ids]) for every combination.

        Parameters
        ----------
        field_lists : List[List[str]] or List[str]
            Per-placeholder field ID lists.  ``field_lists[i]`` provides the
            candidates for ``self.placeholders[i]``.  Pass a flat ``List[str]``
            for backward compatibility — it will be used for all placeholders.

        Modes
        -----
        static           →  1  pair  (the template verbatim, no substitution)
        single / multi   →  cartesian product across per-placeholder lists
        """
        if self.mode == "static":
            return [(self.template, [])]

        # Backward compat: flat list → use for every placeholder
        if field_lists and not isinstance(field_lists[0], list):
            field_lists = [field_lists] * len(self.placeholders)

        results: List[Tuple[str, List[str]]] = []
        for combo in itertools.product(*field_lists):
            results.append((self.generate(*combo), list(combo)))
        return results

    def validate(self, expr: str) -> tuple:
        """Basic syntax validation before sending to BRAIN."""
        if not expr.strip():
            return False, "Empty expression"
        if expr.count("(") != expr.count(")"):
            return False, "Unbalanced parentheses"
        if "//" in expr:
            return False, "Double slash detected"
        return True, ""

# ═══════════════════════════════════════════════════════════════════════════════
# 6. RESULT PARSER
# ═══════════════════════════════════════════════════════════════════════════════

def parse_metrics(raw: Dict) -> Dict:
    """Extract numeric metrics from a BRAIN simulation result.

    autobrain-sim nests performance stats under raw["is"].
    Falls back to top-level keys for forward/backward compatibility.
    """
    is_ = raw.get("is", {})

    def _f(*keys: str) -> Optional[float]:
        for k in keys:
            v = is_.get(k)
            if v is None:
                v = raw.get(k)
            if v is not None:
                try:
                    return float(v)
                except (TypeError, ValueError):
                    pass
        return None

    return {
        "sharpe":   _f("sharpe", "sharpeRatio"),
        "fitness":  _f("fitness", "fitnessScore"),
        "turnover": _f("turnover"),
        "returns":  _f("returns", "annualReturn"),
        "drawdown": _f("maxDrawdown", "drawdown"),
    }

# ═══════════════════════════════════════════════════════════════════════════════
# 7. REPORTER  (CSV + Excel)
# ═══════════════════════════════════════════════════════════════════════════════

COLS = [
    "expression", "field_id", "sharpe", "fitness", "turnover",
    "returns", "drawdown", "alpha_id", "sim_id", "finished_at",
]


def export_csv(rows: list, path: str) -> None:
    """Append passing alphas to a persistent CSV file.

    Creates the file with a header row if it does not exist yet;
    appends rows without repeating the header on subsequent runs.
    """
    import os
    write_header = not os.path.isfile(path) or os.path.getsize(path) == 0
    with open(path, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLS, extrasaction="ignore")
        if write_header:
            w.writeheader()
        for r in rows:
            w.writerow(dict(r))
    log.info("Appended %d passing alpha(s) -> %s", len(rows), path)


# ═══════════════════════════════════════════════════════════════════════════════
# 8. CATEGORY ASSIGNMENT PROMPT
# ═══════════════════════════════════════════════════════════════════════════════

def prompt_category_assignment(
    engines: List["TemplateEngine"],
    categorized_fields: Dict[str, List[Dict]],
) -> List[List[List[str]]]:
    """Interactively assign a data category + sample size to each placeholder.

    Returns
    -------
    List[List[List[str]]]
        Outer list  — one entry per engine (template).
        Middle list — one entry per placeholder in that engine.
        Inner list  — the chosen (and possibly sampled) field IDs.
        Static templates get an empty middle list [].
    """
    cat_names = list(categorized_fields.keys())
    cat_ids: Dict[str, List[str]] = {
        cat: [
            fid for f in fields
            if (fid := f.get("id") or f.get("fieldId", ""))
        ]
        for cat, fields in categorized_fields.items()
    }
    all_ids: List[str] = [fid for ids in cat_ids.values() for fid in ids]

    result: List[List[List[str]]] = []
    for ei, engine in enumerate(engines, 1):
        if not engine.placeholders:
            print(f"\nTemplate {ei}: {engine.template}")
            print("  (no placeholder — will be submitted as-is)")
            result.append([])
            continue

        print(f"\nTemplate {ei}: {engine.template}")
        per_placeholder: List[List[str]] = []
        for ph in engine.placeholders:
            # ── Category selection ───────────────────────────────────────────
            print(f"  Assign category for {ph}:")
            print(f"    0. all fields  ({len(all_ids)} total)")
            for ci, cat in enumerate(cat_names, 1):
                print(f"    {ci}. {cat}  ({len(cat_ids[cat])} fields)")
            while True:
                try:
                    choice = int(input(f"  Enter number [0-{len(cat_names)}]: ").strip())
                    if 0 <= choice <= len(cat_names):
                        break
                    print(f"  Please enter a number between 0 and {len(cat_names)}.")
                except (ValueError, EOFError):
                    print("  Invalid input — defaulting to 0 (all fields).")
                    choice = 0
                    break

            if choice == 0:
                pool = all_ids
                log.info("  %s -> all fields (%d)", ph, len(pool))
            else:
                chosen_cat = cat_names[choice - 1]
                pool = cat_ids[chosen_cat]
                log.info("  %s -> category '%s' (%d fields)", ph, chosen_cat, len(pool))

            # ── Sample-size selection ────────────────────────────────────────
            print(f"  How many fields to randomly sample from this pool of {len(pool)}?")
            print(f"  (Enter 0 to use all {len(pool)} fields)")
            while True:
                try:
                    sample_n = int(input("  Sample size [0 = all]: ").strip())
                    if 0 <= sample_n <= len(pool):
                        break
                    print(f"  Please enter a number between 0 and {len(pool)}.")
                except (ValueError, EOFError):
                    print("  Invalid input — defaulting to 0 (use all).")
                    sample_n = 0
                    break

            if sample_n == 0:
                per_placeholder.append(pool)
                log.info("    -> using all %d fields", len(pool))
            else:
                sampled = random.sample(pool, sample_n)
                per_placeholder.append(sampled)
                log.info("    -> randomly sampled %d / %d fields", sample_n, len(pool))

        result.append(per_placeholder)
    return result


# ═══════════════════════════════════════════════════════════════════════════════
# 9. PER-TEMPLATE LANE RUNNER
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_alpha_details(client, alpha_id: str) -> Optional[Dict]:
    """GET /alphas/{alpha_id} and return the full alpha record, or None on error.

    The progress-URL response only contains status + id; the IS stats (sharpe,
    fitness, turnover, etc.) are only available at this separate endpoint.
    """
    url = f"{BRAIN_BASE}/alphas/{alpha_id}"
    for attempt in range(3):
        try:
            r = _get_session(client).get(url)
            if r.status_code == 429:
                wait = int(r.headers.get("Retry-After", 60))
                log.warning(
                    "Rate limited fetching alpha details. Waiting %ds ...", wait
                )
                time.sleep(wait)
                continue
            r.raise_for_status()
            data = r.json()
            log.debug("Alpha details for %s: %s", alpha_id, data)
            return data
        except Exception as exc:
            log.warning(
                "Could not fetch alpha details for %s (attempt %d/3): %s",
                alpha_id, attempt + 1, exc,
            )
            # fall through to next attempt
    return None


def _parse_is_metrics(raw: Dict) -> Dict:
    """Parse IS stats from a /alphas/{id} response.

    Handles multiple nesting patterns seen in the BRAIN API:
      raw["is"]["sharpe"], raw["is"]["stats"]["sharpe"],
      raw["stats"]["sharpe"], raw["sharpe"], etc.
    """
    def _candidates(key: str, *aliases: str) -> Optional[float]:
        all_keys = (key,) + aliases
        # search these sub-dicts in priority order
        sources = [
            raw.get("is") or {},
            (raw.get("is") or {}).get("stats") or {},
            raw.get("stats") or {},
            raw,
        ]
        for src in sources:
            for k in all_keys:
                v = src.get(k)
                if v is not None:
                    try:
                        return float(v)
                    except (TypeError, ValueError):
                        pass
        return None

    return {
        "sharpe":   _candidates("sharpe",   "sharpeRatio"),
        "fitness":  _candidates("fitness",  "fitnessScore"),
        "turnover": _candidates("turnover"),
        "returns":  _candidates("returns",  "annualReturn"),
        "drawdown": _candidates("maxDrawdown", "drawdown"),
    }


def _record_result(
    db: StateDB,
    expr: str,
    fid: str,
    result: Dict,
    min_sharpe: float,
    min_fitness: float,
    label: str,
    min_sharpe_low: Optional[float] = None,
    client=None,
) -> None:
    """Parse a completed simulation result and persist it to the DB."""
    # The progress-URL response contains the SIMULATION id under "id" and the
    # actual ALPHA id under "alpha" (sometimes a string, sometimes a dict
    # like {"id": "..."}).  Older API versions may use "alphaId".  We must
    # use the alpha id (NOT the simulation id) for /alphas/{...}.
    alpha_field = result.get("alpha")
    if isinstance(alpha_field, dict):
        alpha_id = alpha_field.get("id", "")
    else:
        alpha_id = alpha_field or result.get("alphaId") or ""
    if not alpha_id:
        log.warning(
            "%s  No alpha id in sim result.  Result keys: %s",
            label, list(result.keys()),
        )

    # Always fetch the full alpha record from /alphas/{alpha_id} — the
    # progress-URL response only contains status + id and never has IS stats.
    metrics = None
    if client and alpha_id:
        log.info("Fetching IS stats from /alphas/%s ...", alpha_id)
        full = fetch_alpha_details(client, alpha_id)
        if full:
            metrics = _parse_is_metrics(full)
            if all(v is None for v in metrics.values()):
                log.warning(
                    "Alpha %s: /alphas/ response had no parseable metrics. "
                    "Raw keys: %s", alpha_id, list(full.keys())
                )
                metrics = None

    # Fall back to parsing the simulation result itself (older API versions)
    if metrics is None:
        metrics = parse_metrics(result)

    sharpe  = metrics.get("sharpe")  if metrics.get("sharpe")  is not None else 0.0
    fitness = metrics.get("fitness") if metrics.get("fitness") is not None else 0.0
    sharpe_ok  = sharpe >= min_sharpe or (
        min_sharpe_low is not None and sharpe <= min_sharpe_low
    )
    fitness_ok = fitness >= min_fitness
    passed     = sharpe_ok or fitness_ok  # OR: either threshold alone is enough

    if passed:
        parts = []
        if sharpe_ok:
            parts.append("sharpe")
        if fitness_ok:
            parts.append("fitness")
        reason = "PASS(" + "+".join(parts) + ")"
    else:
        reason = "fail"

    log.info(
        "%s  Field=%-20s  Sharpe=%.3f(thr %.2f)  Fitness=%.3f(thr %.2f)  %s",
        label, fid, sharpe, min_sharpe, fitness, min_fitness, reason,
    )
    db.mark_done(expr, metrics, passed, alpha_id=alpha_id)


def _run_template_lane(
    template_idx: int,
    engine: "TemplateEngine",
    db: StateDB,
    client,
    sim_settings: Dict,
    submit_lock: threading.Lock,
    submission_delay: float,
    poll_interval: int,
    timeout: int,
    min_sharpe: float,
    min_fitness: float,
    min_sharpe_low: Optional[float] = None,
) -> None:
    """Run all simulations for one template sequentially.

    On resume, any already-submitted rows are polled first, then remaining
    pending rows are submitted and polled one by one.  The submit_lock is
    held only during the brief client.simulate() call so other lanes can
    interleave their submits while this lane is waiting for poll results.
    """
    label = f"Lane {template_idx + 1} [{engine.template[:35]}]"

    # Resume: poll any rows that were submitted but not yet polled
    for row in db.submitted_for_template(template_idx):
        expr   = row["expression"]
        fid    = row["field_id"]
        log.info("%s  Resuming poll for: %s", label, expr[:70])
        result = poll_simulation(client, row["sim_id"], poll_interval, timeout)
        if result is None:
            db.mark_failed(expr, "timeout_or_error")
            continue
        _record_result(
            db, expr, fid, result, min_sharpe, min_fitness, label,
            min_sharpe_low, client=client,
        )

    # Submit + poll pending rows one at a time
    rows  = db.pending_for_template(template_idx)
    total = len(rows)
    log.info("%s  %d expression(s) to simulate.", label, total)
    for i, row in enumerate(rows, 1):
        expr = row["expression"]
        fid  = row["field_id"]
        log.info("%s  [%d/%d] Submitting: %s", label, i, total, expr[:70])
        try:
            with submit_lock:
                sim_result = simulate_with_retry(
                    client, expr, settings=sim_settings
                )
            db.mark_submitted(expr, sim_result.progress_url)
        except Exception as exc:
            log.error("%s  Submit failed: %s", label, exc)
            db.mark_failed(expr, str(exc))
            continue

        time.sleep(submission_delay)
        result = poll_simulation(
            client, sim_result.progress_url, poll_interval, timeout
        )
        if result is None:
            db.mark_failed(expr, "timeout_or_error")
            continue
        _record_result(
            db, expr, fid, result, min_sharpe, min_fitness, label,
            min_sharpe_low, client=client,
        )

    log.info("%s  done.", label)


# ═══════════════════════════════════════════════════════════════════════════════
# 10. MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

def run(
    templates: List[str],
    credentials: Optional[str],
    universe: str,
    region: str,
    neutralization: str,
    instrument_type: str,
    delay: int,
    min_sharpe: float,
    min_fitness: float,
    submission_delay: float,
    poll_interval: int,
    timeout: int,
    output_prefix: str,
    db_path: str,
    resume: bool,
    min_sharpe_low: Optional[float] = None,
    data_fields_file: Optional[str] = None,
) -> None:
    client  = build_client(credentials)
    engines = [TemplateEngine(t) for t in templates]
    db      = StateDB(db_path)

    try:
        _run_inner(
            client, engines, db,
            universe=universe,
            region=region,
            neutralization=neutralization,
            instrument_type=instrument_type,
            delay=delay,
            min_sharpe=min_sharpe,
            min_fitness=min_fitness,
            min_sharpe_low=min_sharpe_low,
            submission_delay=submission_delay,
            poll_interval=poll_interval,
            timeout=timeout,
            output_prefix=output_prefix,
            resume=resume,
            data_fields_file=data_fields_file,
        )
    finally:
        db.close()


def _run_inner(
    client: BrainClient,
    engines: List[TemplateEngine],
    db: StateDB,
    *,
    universe: str,
    region: str,
    neutralization: str,
    instrument_type: str,
    delay: int,
    min_sharpe: float,
    min_fitness: float,
    submission_delay: float,
    poll_interval: int,
    timeout: int,
    output_prefix: str,
    resume: bool,
    min_sharpe_low: Optional[float] = None,
    data_fields_file: Optional[str] = None,
) -> None:
    # ── Enqueue templates ─────────────────────────────────────────────────────
    if not resume or not db.pending():
        if not resume:
            existing = len(db.pending())
            if existing:
                log.info(
                    "Clearing %d stale row(s) from a previous run "
                    "(pass --resume to continue that run instead).",
                    existing,
                )
                db.clear()

        # Load data fields — prompt for category+sample when categorised
        categorized_fields: Optional[Dict[str, List[Dict]]] = None
        flat_field_ids: List[str] = []
        if data_fields_file:
            log.info("Loading data fields from %s ...", data_fields_file)
            with open(data_fields_file) as _f:
                raw = json.load(_f)
            if isinstance(raw, dict):
                categorized_fields = raw
                flat = [f for cat in raw.values() for f in cat]
            else:
                flat = raw
            flat_field_ids = [
                fid for f in flat
                if (fid := f.get("id") or f.get("fieldId", ""))
            ]
            log.info(
                "Loaded %d field ID(s) across %s.", len(flat_field_ids),
                f"{len(raw)} categories" if isinstance(raw, dict) else "1 flat list",
            )

        # Categorised JSON → interactive prompt per placeholder
        # Flat JSON        → use all fields for every placeholder (no prompt)
        if categorized_fields is not None:
            field_lists_per_engine = prompt_category_assignment(engines, categorized_fields)
        else:
            field_lists_per_engine = [
                [flat_field_ids] * len(engine.placeholders) if engine.placeholders else []
                for engine in engines
            ]

        pending_rows: List[Tuple[str, str, int]] = []
        for tidx, (engine, field_lists) in enumerate(zip(engines, field_lists_per_engine)):
            if engine.mode == "static":
                expr = engine.template
                ok, reason = engine.validate(expr)
                if ok:
                    pending_rows.append((expr, "static", tidx))
                else:
                    log.warning(
                        "Template %d skipped (%s): %s", tidx + 1, expr[:60], reason
                    )
            else:
                if not field_lists or not any(field_lists):
                    log.warning(
                        "Template %d has placeholder(s) %s but no fields were selected. "
                        "Skipping.", tidx + 1, engine.placeholders,
                    )
                    continue
                combos = engine.all_combinations(field_lists)
                for expr, fids in combos:
                    ok, reason = engine.validate(expr)
                    if not ok:
                        log.debug("Skip %s: %s", fids, reason)
                        continue
                    pending_rows.append(
                        (expr, "|".join(fids) if fids else "static", tidx)
                    )

        db.bulk_upsert_pending(pending_rows)
        log.info("Enqueued %d expression(s).", len(pending_rows))
    else:
        log.info("Resuming — %d pending expression(s) in DB.", len(db.pending()))

    # ── Simulation settings ───────────────────────────────────────────────────
    sim_settings: Dict = {
        "instrumentType": instrument_type,
        "region":         region,
        "universe":       universe,
        "delay":          delay,
        "decay":          0,
        "neutralization": neutralization,
        "truncation":     0.08,
        "pasteurization": "ON",
        "unitHandling":   "VERIFY",
        "nanHandling":    "OFF",
        "language":       "FASTEXPR",
        "visualization":  False,
    }

    # ── Per-template parallel lanes ───────────────────────────────────────────
    # Each template gets its own dedicated lane (thread).  Within a lane,
    # submit and poll are sequential so exactly one simulation per template
    # is in-flight at any time.  Lanes run in parallel across templates.
    # The submit_lock serialises the brief client.simulate() call across lanes.
    submit_lock = threading.Lock()
    log.info("Running %d template lane(s) in parallel ...", len(engines))
    with ThreadPoolExecutor(max_workers=len(engines)) as executor:
        futures = {
            executor.submit(
                _run_template_lane,
                tidx, engine, db, client, sim_settings,
                submit_lock, submission_delay, poll_interval, timeout,
                min_sharpe, min_fitness, min_sharpe_low,
            ): tidx
            for tidx, engine in enumerate(engines)
        }
        for future in as_completed(futures):
            tidx = futures[future]
            try:
                future.result()
            except Exception as exc:
                log.error("Lane %d raised an unhandled exception: %s", tidx + 1, exc)

    # ── Export ────────────────────────────────────────────────────────────────
    all_rows    = db.all_done()
    passed_rows = [r for r in all_rows if r["passed"]]
    csv_path    = f"{output_prefix}.csv"   # fixed name — appended each run

    if passed_rows:
        export_csv(passed_rows, csv_path)
    else:
        log.info("No passing alphas this run — %s not updated.", csv_path)

    log.info(
        "Done. %d/%d alphas passed filters. Results: %s",
        len(passed_rows), len(all_rows), csv_path,
    )

# ═══════════════════════════════════════════════════════════════════════════════
# 9. HELPERS
# ═══════════════════════════════════════════════════════════════════════════════

def _sha(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()

def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds")

# ═══════════════════════════════════════════════════════════════════════════════
# 10. CLI
# ═══════════════════════════════════════════════════════════════════════════════

def main() -> None:
    p = argparse.ArgumentParser(
        description="WorldQuant BRAIN Automatic Alpha Submitter",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument(
        "--template", action="append", default=None, dest="templates_list",
        help=(
            "Alpha expression template. Can be given up to 3 times for multiple templates. "
            "Always quote it to protect curly braces from the shell. "
            'Examples:  --template "ts_rank({DATA},10)"  '
            '           --template "rank({DATA1},10)+rank({DATA2},5)"  '
            '           --template "ts_rank(close,10)" (no placeholder = submit as-is). '
            "Use --templates-file to supply many templates from a file."
        ),
    )
    p.add_argument(
        "--templates-file", default=None,
        help=(
            "Path to a file with one template expression per line. "
            "Lines starting with # are treated as comments. "
            "Generate this file with template_generator.py."
        ),
    )
    p.add_argument(
        "--data-fields-file", default=None,
        help=(
            "Path to a JSON file of data fields (produced by brain_data_fetcher.py). "
            "Required when templates contain {DATA} / {DATA1} placeholders. "
            "Accepts a flat list [{...}, ...] or a categorised dict "
            "{'fundamental': [...], ...}."
        ),
    )
    p.add_argument(
        "--credentials", default="credentials.json",
        help=(
            "Path to JSON file with {username, password}. "
            "Defaults to credentials.json in the current directory."
        ),
    )
    p.add_argument(
        "--interactive", action="store_true",
        help="Force interactive credential prompt (ignores --credentials)",
    )
    p.add_argument("--universe",        default="TOP3000")
    p.add_argument("--region",          default="USA")
    p.add_argument(
        "--neutralization", default="SUBINDUSTRY",
        choices=["MARKET", "INDUSTRY", "SUBINDUSTRY", "NONE",
                 "market", "industry", "subindustry", "none"],
    )
    p.add_argument(
        "--instrument-type", default="EQUITY",
        help="Instrument type for data field query (e.g. EQUITY, FUTURES)",
    )
    p.add_argument("--delay",            default=1, type=int)
    p.add_argument(
        "--sharpe", default=1.25, type=float,
        help="Upper Sharpe threshold — alpha passes if sharpe >= this value",
    )
    p.add_argument(
        "--sharpe-low", default=None, type=float,
        help=(
            "Optional lower (negative) Sharpe threshold.  When set, an alpha "
            "also passes the Sharpe test if sharpe <= this value.  "
            "Example: --sharpe 0.8 --sharpe-low -0.8"
        ),
    )
    p.add_argument(
        "--fitness", default=1.0, type=float,
        help="Minimum fitness score to pass",
    )
    p.add_argument(
        "--submission-delay", default=2.0, type=float,
        help="Seconds between submissions (rate limit buffer)",
    )
    p.add_argument(
        "--poll-interval", default=15, type=int,
        help="Seconds between polling attempts",
    )
    p.add_argument(
        "--timeout", default=600, type=int,
        help="Max seconds to wait for a single simulation",
    )
    p.add_argument(
        "--output", default="brain_results",
        help=(
            "Output file prefix. Passing alphas are appended to "
            "{prefix}.csv each run, so all results accumulate in one file. "
            "Use a different prefix per category to keep them separate."
        ),
    )
    p.add_argument(
        "--db", default="brain_runs.db",
        help="SQLite state database path",
    )
    p.add_argument(
        "--resume", action="store_true",
        help="Resume a previous run (skip re-fetching data fields)",
    )
    p.add_argument(
        "--test-auth", action="store_true",
        help="Authenticate and verify API access, then exit (no submissions)",
    )

    args = p.parse_args()

    # Build the templates list from --template (repeatable) and/or --templates-file
    templates: List[str] = []
    if args.templates_file:
        with open(args.templates_file) as _f:
            templates = [
                ln.strip() for ln in _f
                if ln.strip() and not ln.strip().startswith("#")
            ]
        log.info("Loaded %d templates from %s.", len(templates), args.templates_file)
    templates.extend(args.templates_list or [])

    if len(templates) > 3:
        log.warning("More than 3 templates provided; only the first 3 will be used.")
        templates = templates[:3]

    if not args.test_auth and not templates:
        p.error(
            "Provide at least one of --template or --templates-file.\n"
            '  Example:  --template "ts_rank({DATA}, 10)"\n'
            "  Or:       --templates-file templates.txt\n"
            "  Tip: generate templates.txt with template_generator.py"
        )

    # --interactive overrides --credentials
    credentials = None if args.interactive else args.credentials

    # ── Test-auth mode ────────────────────────────────────────────────────────
    if args.test_auth:
        client = build_client(credentials)
        r = _get_session(client).get(
            f"{BRAIN_BASE}/data-fields",
            params={
                "instrumentType": args.instrument_type,
                "region":         args.region,
                "universe":       args.universe,
                "delay":          args.delay,
                "language":       "FASTEXPR",
                "limit":          1,
            },
        )
        if r.status_code == 400:
            log.error("data-fields 400 error. Response: %s", r.text)
        r.raise_for_status()
        total = r.json().get("count", "unknown")
        log.info(
            "API access verified. %s data fields available for %s / %s.",
            total, args.region, args.universe,
        )
        sys.exit(0)

    run(
        templates        = templates,
        credentials      = credentials,
        universe         = args.universe,
        region           = args.region,
        neutralization   = args.neutralization,
        instrument_type  = args.instrument_type,
        delay            = args.delay,
        min_sharpe       = args.sharpe,
        min_sharpe_low   = args.sharpe_low,
        min_fitness      = args.fitness,
        submission_delay = args.submission_delay,
        poll_interval    = args.poll_interval,
        timeout          = args.timeout,
        output_prefix    = args.output,
        db_path          = args.db,
        resume           = args.resume,
        data_fields_file = args.data_fields_file,
    )


if __name__ == "__main__":
    main()
