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
from openpyxl import Workbook
from openpyxl.styles import Alignment, Font, PatternFill
from openpyxl.utils import get_column_letter

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
# 2. DATA FIELD FETCHER  (uses autobrain-sim's authenticated session)
# ═══════════════════════════════════════════════════════════════════════════════

def get_data_fields(
    client: BrainClient,
    instrument_type: str = "EQUITY",
    region: str = "USA",
    universe: str = "TOP3000",
    delay: int = 1,
    category: str = "",
) -> List[Dict]:
    """Return all available data fields matching the given settings."""
    fields: List[Dict] = []
    offset, limit = 0, 20

    while True:
        params: Dict = {
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
            log.warning("Rate limited fetching data fields. Waiting %ds ...", wait)
            time.sleep(wait)
            continue
        if r.status_code == 400:
            log.error("data-fields 400 error. Response: %s", r.text)
        r.raise_for_status()

        page = r.json()
        results = page.get("results", [])
        fields.extend(results)
        if len(results) < limit:
            break
        offset += limit
        time.sleep(0.3)  # gentle paging

    log.info("Fetched %d data fields.", len(fields))
    return fields

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
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLS, extrasaction="ignore")
        w.writeheader()
        for r in rows:
            w.writerow(dict(r))
    log.info("CSV saved -> %s", path)


def export_excel(
    rows: list,
    path: str,
    min_sharpe: float,
    min_fitness: float,
    min_sharpe_low: Optional[float] = None,
) -> None:
    wb = Workbook()

    # Sheet 1: Passed alphas
    ws = wb.active
    ws.title = "Passed Alphas"
    _write_sheet(
        ws, [r for r in rows if r["passed"]],
        min_sharpe, min_fitness, min_sharpe_low,
    )

    # Sheet 2: All results
    ws2 = wb.create_sheet("All Results")
    _write_sheet(ws2, rows, min_sharpe, min_fitness, min_sharpe_low)

    wb.save(path)
    log.info("Excel saved -> %s", path)


def _write_sheet(
    ws, rows: list, min_sharpe: float, min_fitness: float,
    min_sharpe_low: Optional[float] = None,
) -> None:
    HEADERS = [
        "Expression", "Field ID", "Sharpe", "Fitness",
        "Turnover", "Ann. Return", "Max Drawdown",
        "Alpha ID", "Sim URL", "Finished At",
    ]
    COL_WIDTHS = [55, 22, 10, 10, 10, 12, 14, 28, 50, 22]

    # ── Header row ───────────────────────────────────────────────────────────
    header_fill = PatternFill("solid", start_color="1F4E79")
    header_font = Font(color="FFFFFF", bold=True, name="Arial", size=10)
    for ci, (h, w) in enumerate(zip(HEADERS, COL_WIDTHS), 1):
        cell = ws.cell(row=1, column=ci, value=h)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center", vertical="center")
        ws.column_dimensions[get_column_letter(ci)].width = w
    ws.row_dimensions[1].height = 22
    ws.freeze_panes = "A2"

    # ── Data rows ─────────────────────────────────────────────────────────────
    green_fill  = PatternFill("solid", start_color="E2EFDA")   # pass
    yellow_fill = PatternFill("solid", start_color="FFF2CC")   # marginal
    red_fill    = PatternFill("solid", start_color="FCE4D6")   # fail
    num_font    = Font(name="Arial", size=9)
    txt_font    = Font(name="Arial", size=9)
    mono_font   = Font(name="Courier New", size=8)

    for ri, row in enumerate(rows, 2):
        sharpe  = row["sharpe"]  or 0.0
        fitness = row["fitness"] or 0.0
        passed  = bool(row["passed"])

        near_upper = sharpe >= min_sharpe * 0.8
        near_lower = min_sharpe_low is not None and sharpe <= min_sharpe_low * 0.8
        if passed:
            row_fill = green_fill
        elif (near_upper or near_lower) and fitness >= min_fitness * 0.8:
            row_fill = yellow_fill
        else:
            row_fill = red_fill

        values = [
            row["expression"],
            row["field_id"],
            row["sharpe"],
            row["fitness"],
            row["turnover"],
            row["returns"],
            row["drawdown"],
            row["alpha_id"],
            row["sim_id"],
            row["finished_at"],
        ]
        for ci, val in enumerate(values, 1):
            cell = ws.cell(row=ri, column=ci, value=val)
            cell.fill = row_fill
            if ci == 1:
                cell.font = mono_font
            elif ci in (3, 4, 5, 6, 7):
                cell.font = num_font
                cell.number_format = "0.000"
                cell.alignment = Alignment(horizontal="right")
            else:
                cell.font = txt_font

    # ── Summary row ───────────────────────────────────────────────────────────
    # Capture last_data_row BEFORE writing the summary so formulas are correct.
    if rows:
        last_data_row = ws.max_row
        sr = last_data_row + 2
        ws.cell(row=sr, column=1, value="SUMMARY").font = Font(bold=True, name="Arial")
        ws.cell(row=sr, column=2, value=f"{len(rows)} alphas")
        ws.cell(row=sr, column=3, value=f"=MAX(C2:C{last_data_row})")
        ws.cell(row=sr, column=4, value=f"=MAX(D2:D{last_data_row})")

        sharpe_desc = f">= {min_sharpe}"
        if min_sharpe_low is not None:
            sharpe_desc += f" or <= {min_sharpe_low}"
        ws.cell(
            row=ws.max_row + 2, column=1,
            value=(
                f"Filters applied:  Sharpe {sharpe_desc}"
                f"  |  min Fitness >= {min_fitness}"
            ),
        ).font = Font(italic=True, color="555555", name="Arial", size=8)

# ═══════════════════════════════════════════════════════════════════════════════
# 8. CATEGORY ASSIGNMENT PROMPT
# ═══════════════════════════════════════════════════════════════════════════════

def prompt_category_assignment(
    engines: List[TemplateEngine],
    categorized_fields: Dict[str, List[Dict]],
) -> List[List[List[str]]]:
    """Interactively assign a data category to each placeholder in each template.

    Prints a numbered menu once per placeholder and reads stdin.  Choosing 0
    uses all fields regardless of category.

    Returns
    -------
    List[List[List[str]]]
        Outer list  — one entry per engine (template).
        Middle list — one entry per placeholder in that engine.
        Inner list  — the field IDs for that placeholder.
        Static templates (no placeholders) get an empty middle list [].
    """
    cat_names = list(categorized_fields.keys())
    # Build field-ID list per category (skip blanks)
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
            print(f"  Assign category for {ph}:")
            print(f"    0. all fields  ({len(all_ids)} total)")
            for ci, cat in enumerate(cat_names, 1):
                print(f"    {ci}. {cat}  ({len(cat_ids[cat])} fields)")
            while True:
                try:
                    raw = input(f"  Enter number [0-{len(cat_names)}]: ").strip()
                    choice = int(raw)
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
                chosen = cat_names[choice - 1]
                pool = cat_ids[chosen]
                log.info("  %s -> category '%s' (%d fields)", ph, chosen, len(pool))

            # Ask how many fields to randomly sample from this pool.
            # 0 = use all (run as normal).
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
    for _attempt in range(3):
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
            return r.json()
        except Exception as exc:
            log.warning("Could not fetch alpha details for %s: %s", alpha_id, exc)
            return None
    return None


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
    metrics  = parse_metrics(result)
    alpha_id = result.get("id") or result.get("alphaId") or ""

    # The progress-URL response often omits IS stats; fetch the full alpha record
    # from /alphas/{id} whenever all metrics come back as None.
    if client and alpha_id and all(v is None for v in metrics.values()):
        log.debug("Fetching IS stats from /alphas/%s ...", alpha_id)
        full = fetch_alpha_details(client, alpha_id)
        if full:
            metrics = parse_metrics(full)
    sharpe   = metrics.get("sharpe")  or 0.0
    fitness  = metrics.get("fitness") or 0.0
    sharpe_ok = sharpe >= min_sharpe or (
        min_sharpe_low is not None and sharpe <= min_sharpe_low
    )
    passed = sharpe_ok and fitness >= min_fitness
    log.info(
        "%s  Field=%-20s  Sharpe=%.3f  Fitness=%.3f  %s",
        label, fid, sharpe, fitness, "PASS" if passed else "fail",
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
    field_category: str,
    submission_delay: float,
    poll_interval: int,
    timeout: int,
    output_prefix: str,
    db_path: str,
    resume: bool,
    data_fields_file: Optional[str] = None,
    min_sharpe_low: Optional[float] = None,
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
            field_category=field_category,
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
    field_category: str,
    submission_delay: float,
    poll_interval: int,
    timeout: int,
    output_prefix: str,
    resume: bool,
    data_fields_file: Optional[str] = None,
    min_sharpe_low: Optional[float] = None,
) -> None:
    # ── Fetch & enqueue data fields ───────────────────────────────────────────
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
        if data_fields_file:
            log.info("Loading data fields from %s ...", data_fields_file)
            with open(data_fields_file) as _f:
                raw = json.load(_f)
            # Detect format: categorized dict vs legacy flat list
            if isinstance(raw, dict):
                categorized_fields = raw
                flat_fields: List[Dict] = [
                    f for cat_fields in raw.values() for f in cat_fields
                ]
                total_loaded = sum(len(v) for v in raw.values())
                log.info(
                    "Loaded %d fields across %d categories from cache.",
                    total_loaded, len(raw),
                )
            else:
                categorized_fields = None
                flat_fields = raw
                log.info("Loaded %d data fields from cache.", len(flat_fields))
        else:
            log.info("Fetching data fields from BRAIN ...")
            flat_fields = get_data_fields(
                client,
                instrument_type=instrument_type,
                region=region,
                universe=universe,
                delay=delay,
                category=field_category,
            )
            categorized_fields = None

        flat_field_ids: List[str] = [
            fid for f in flat_fields
            if (fid := f.get("id") or f.get("fieldId", ""))
        ]

        # Prompt user to assign categories to placeholders when categorized
        # data is available.
        if categorized_fields is not None:
            field_lists_per_engine = prompt_category_assignment(
                engines, categorized_fields
            )
        else:
            # Legacy / no-category mode: use flat list for every placeholder
            field_lists_per_engine = [
                [flat_field_ids] * len(engine.placeholders) if engine.placeholders else []
                for engine in engines
            ]

        log.info(
            "Enqueueing combinations for %d template(s) x %d fields ...",
            len(engines), len(flat_field_ids),
        )
        pending_rows: List[Tuple[str, str, int]] = []
        for tidx, (engine, field_lists) in enumerate(
            zip(engines, field_lists_per_engine)
        ):
            if engine.mode == "static":
                combos = engine.all_combinations([])
            else:
                # Warn when the cartesian product is very large
                n_combos = 1
                for fl in field_lists:
                    n_combos *= len(fl)
                if len(engine.placeholders) > 1 and n_combos > 1000:
                    log.warning(
                        "Template '%s': %d combinations is very large. "
                        "Consider assigning a narrower category.",
                        engine.template[:60], n_combos,
                    )
                combos = engine.all_combinations(field_lists)
            for expr, fids in combos:
                ok, reason = engine.validate(expr)
                if not ok:
                    log.debug("Skip %s: %s", fids, reason)
                    continue
                pending_rows.append(
                    (expr, "|".join(fids) if fids else "static", tidx)
                )
        log.info("Inserting %d expressions into DB ...", len(pending_rows))
        db.bulk_upsert_pending(pending_rows)
        log.info("Enqueued %d expressions total.", len(db.pending()))
    else:
        log.info("Resuming -- %d pending expressions in DB.", len(db.pending()))

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
    ts         = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_rows   = db.all_done()
    csv_path   = f"{output_prefix}_{ts}.csv"
    excel_path = f"{output_prefix}_{ts}.xlsx"

    export_csv(all_rows, csv_path)
    export_excel(all_rows, excel_path, min_sharpe, min_fitness, min_sharpe_low)

    # Per-criterion files — one for each threshold independently
    def _sharpe_ok(s):
        v = s or 0.0
        return v >= min_sharpe or (min_sharpe_low is not None and v <= min_sharpe_low)

    sharpe_rows  = [r for r in all_rows if _sharpe_ok(r["sharpe"])]
    fitness_rows = [r for r in all_rows if (r["fitness"] or 0.0) >= min_fitness]

    if sharpe_rows:
        sharpe_path = f"sharpe_passed_{ts}.xlsx"
        export_excel(sharpe_rows, sharpe_path, min_sharpe, min_fitness, min_sharpe_low)
        log.info("Sharpe-passed  -> %s  (%d alphas)", sharpe_path, len(sharpe_rows))

    if fitness_rows:
        fitness_path = f"fitness_passed_{ts}.xlsx"
        export_excel(fitness_rows, fitness_path, min_sharpe, min_fitness, min_sharpe_low)
        log.info("Fitness-passed -> %s  (%d alphas)", fitness_path, len(fitness_rows))

    passed_count = sum(1 for r in all_rows if r["passed"])
    log.info(
        "Done. %d/%d alphas passed all filters. Results: %s | %s",
        passed_count, len(all_rows), csv_path, excel_path,
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
            "Path to a cached JSON file of data fields produced by "
            "brain_data_fetcher.py. Skips the API fetch when provided."
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
        "--category", default="",
        help="Filter data fields by category (e.g. 'fundamental')",
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
        help="Output file prefix (timestamp appended)",
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
        field_category   = args.category,
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
