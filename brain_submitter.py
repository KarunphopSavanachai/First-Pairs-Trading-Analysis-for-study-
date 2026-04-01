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
import re
import sqlite3
import sys
import time
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
            raise ValueError(
                "Template must contain {DATA} (single) or {DATA1},{DATA2},{DATA3} (multi).\n"
                "Examples:\n"
                '  --template "ts_rank({DATA}, 10)"\n'
                '  --template "rank({DATA1}, 10) + rank({DATA2}, 5)"\n'
                "Shell tip: always wrap the template in double-quotes so your shell\n"
                "does not strip or expand the { } characters."
            )
        self.template = template

    def generate(self, *field_ids: str) -> str:
        """Replace each placeholder with the corresponding field ID."""
        expr = self.template
        for ph, fid in zip(self.placeholders, field_ids):
            expr = expr.replace(ph, fid)
        return expr

    def all_combinations(
        self, fields: List[str]
    ) -> List[Tuple[str, List[str]]]:
        """Return (expression, [field_ids]) for every combination.

        Single mode      →  N   pairs  (one per field)
        2 placeholders   →  N²  pairs  (cartesian product)
        3 placeholders   →  N³  pairs
        """
        results: List[Tuple[str, List[str]]] = []
        for combo in itertools.product(fields, repeat=len(self.placeholders)):
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
    rows: list, path: str, min_sharpe: float, min_fitness: float
) -> None:
    wb = Workbook()

    # Sheet 1: Passed alphas
    ws = wb.active
    ws.title = "Passed Alphas"
    _write_sheet(ws, [r for r in rows if r["passed"]], min_sharpe, min_fitness)

    # Sheet 2: All results
    ws2 = wb.create_sheet("All Results")
    _write_sheet(ws2, rows, min_sharpe, min_fitness)

    wb.save(path)
    log.info("Excel saved -> %s", path)


def _write_sheet(ws, rows: list, min_sharpe: float, min_fitness: float) -> None:
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

        if passed:
            row_fill = green_fill
        elif sharpe >= min_sharpe * 0.8 and fitness >= min_fitness * 0.8:
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

        ws.cell(
            row=ws.max_row + 2, column=1,
            value=(
                f"Filters applied:  min Sharpe >= {min_sharpe}"
                f"  |  min Fitness >= {min_fitness}"
            ),
        ).font = Font(italic=True, color="555555", name="Arial", size=8)

# ═══════════════════════════════════════════════════════════════════════════════
# 8. MAIN ORCHESTRATOR
# ═══════════════════════════════════════════════════════════════════════════════

def run(
    template: str,
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
) -> None:
    client = build_client(credentials)
    engine = TemplateEngine(template)
    db     = StateDB(db_path)

    try:
        _run_inner(
            client, engine, db,
            universe=universe,
            region=region,
            neutralization=neutralization,
            instrument_type=instrument_type,
            delay=delay,
            min_sharpe=min_sharpe,
            min_fitness=min_fitness,
            field_category=field_category,
            submission_delay=submission_delay,
            poll_interval=poll_interval,
            timeout=timeout,
            output_prefix=output_prefix,
            resume=resume,
        )
    finally:
        db.close()


def _run_inner(
    client: BrainClient,
    engine: TemplateEngine,
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
) -> None:
    # ── Fetch & enqueue data fields ───────────────────────────────────────────
    if not resume or not db.pending():
        log.info("Fetching data fields from BRAIN ...")
        fields = get_data_fields(
            client,
            instrument_type=instrument_type,
            region=region,
            universe=universe,
            delay=delay,
            category=field_category,
        )
        field_ids = [
            fid for f in fields
            if (fid := f.get("id") or f.get("fieldId", ""))
        ]
        n_combos = len(field_ids) ** len(engine.placeholders)
        if len(engine.placeholders) > 1:
            log.info(
                "%d fields x %d placeholders = %d combinations to enqueue.",
                len(field_ids), len(engine.placeholders), n_combos,
            )
            if n_combos > 1000:
                log.warning(
                    "%d combinations is very large. Consider using --category "
                    "to filter data fields and keep runtime manageable.",
                    n_combos,
                )

        for expr, fids in engine.all_combinations(field_ids):
            ok, reason = engine.validate(expr)
            if not ok:
                log.debug("Skip %s: %s", fids, reason)
                continue
            db.upsert_pending(expr, "|".join(fids))
        log.info("Enqueued %d expressions.", len(db.pending()))
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

    # ── Submission loop ───────────────────────────────────────────────────────
    pending = db.pending()
    log.info("Submitting %d alphas ...", len(pending))

    for i, row in enumerate(pending, 1):
        expr = row["expression"]
        fid  = row["field_id"]

        log.info("[%d/%d] Submitting: %s", i, len(pending), expr[:80])
        try:
            sim_result = client.simulate(expr, settings=sim_settings)
            db.mark_submitted(expr, sim_result.progress_url)
        except Exception as e:
            log.error("Submit failed for %s: %s", fid, e)
            db.mark_failed(expr, str(e))
            continue

        time.sleep(submission_delay)  # rate-limit buffer

    # ── Polling loop ──────────────────────────────────────────────────────────
    submitted = db.submitted()
    log.info("Polling %d submitted simulations ...", len(submitted))

    for row in submitted:
        expr         = row["expression"]
        progress_url = row["sim_id"]  # stored as progress_url during submission
        fid          = row["field_id"]

        log.info("Polling simulation for field %s ...", fid)
        result = poll_simulation(client, progress_url, poll_interval, timeout)

        if result is None:
            db.mark_failed(expr, "timeout_or_error")
            continue

        metrics  = parse_metrics(result)
        alpha_id = result.get("id") or result.get("alphaId") or ""
        sharpe   = metrics.get("sharpe")  or 0.0
        fitness  = metrics.get("fitness") or 0.0
        passed   = sharpe >= min_sharpe and fitness >= min_fitness

        log.info(
            "  -> Sharpe=%.3f  Fitness=%.3f  %s",
            sharpe, fitness, "PASS" if passed else "fail",
        )
        db.mark_done(expr, metrics, passed, alpha_id=alpha_id)

    # ── Export ────────────────────────────────────────────────────────────────
    ts        = datetime.now().strftime("%Y%m%d_%H%M%S")
    all_rows  = db.all_done()
    csv_path   = f"{output_prefix}_{ts}.csv"
    excel_path = f"{output_prefix}_{ts}.xlsx"

    export_csv(all_rows, csv_path)
    export_excel(all_rows, excel_path, min_sharpe, min_fitness)

    passed_count = sum(1 for r in all_rows if r["passed"])
    log.info(
        "Done. %d/%d alphas passed filters. Results: %s | %s",
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
        "--template", default=None,
        help=(
            'Alpha expression with {DATA} placeholder. '
            'Always quote it to protect curly braces from the shell. '
            'Examples:  "ts_rank({DATA},10)"  |  "rank({DATA1},10)+rank({DATA2},5)" '
            '(required unless --test-auth is used)'
        ),
    )
    p.add_argument(
        "--credentials", default=None,
        help=(
            "Path to JSON file with {username, password}. "
            "If omitted, autobrain-sim reads ~/.brain_credentials or prompts interactively."
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
        help="Minimum Sharpe ratio to pass",
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

    # --template is required for normal runs but not for --test-auth
    if not args.test_auth and not args.template:
        p.error(
            "--template is required.\n"
            '  Example:  --template "ts_rank({DATA}, 10)"\n'
            "  Tip: always wrap the template in double-quotes so the shell\n"
            "  does not interpret { } as brace expansion."
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
        template         = args.template,
        credentials      = credentials,
        universe         = args.universe,
        region           = args.region,
        neutralization   = args.neutralization,
        instrument_type  = args.instrument_type,
        delay            = args.delay,
        min_sharpe       = args.sharpe,
        min_fitness      = args.fitness,
        field_category   = args.category,
        submission_delay = args.submission_delay,
        poll_interval    = args.poll_interval,
        timeout          = args.timeout,
        output_prefix    = args.output,
        db_path          = args.db,
        resume           = args.resume,
    )


if __name__ == "__main__":
    main()
