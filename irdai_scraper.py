#!/usr/bin/env python3
"""
IRDAI Agent Locator — Full Async Production Scraper v2
=======================================================
Scrapes ALL insurance agents across India from the IRDAI Agency Portal.

CRITICAL: The IRDAI API hard-caps results at 15 per query, regardless of the
requested page size. To get complete data, we use PIN code as the primary key
and query each (PIN × InsuranceType × Insurer) combination individually.

Strategy:
    PIN Code → Insurance Type → Insurer → LocateAgent API

Inputs:  pins_master.csv (list of all Indian PIN codes)
Outputs: CSV files per insurance type + deduplicated master CSV/Parquet

Usage:
    python irdai_scraper.py --test                              # Quick test (5 PINs, 1 type, 1 insurer)
    python irdai_scraper.py --types 1 --max-pins 100            # General only, first 100 PINs
    python irdai_scraper.py                                     # Full scrape (all PINs × all types × all insurers)
    python irdai_scraper.py --merge-only                        # Just merge existing raw CSVs
    python irdai_scraper.py --resume                            # Resume interrupted scrape
    python irdai_scraper.py --list-states                       # Show all states, districts, and PIN counts
    python irdai_scraper.py --state "MAHARASHTRA"               # Scrape only Maharashtra (1,600 PINs)
    python irdai_scraper.py --state "DELHI" --types 1 2         # Delhi, General + Life only
    python irdai_scraper.py --state "KARNATAKA" --district "BENGALURU URBAN"  # Single district
"""

import asyncio
import csv
import json
import logging
import os
import sys
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from xml.etree import ElementTree as ET

import httpx
import pandas as pd

# ─── Configuration ───────────────────────────────────────────────────────────

BASE_URL = "https://agencyportal.irdai.gov.in"
DATA_API = f"{BASE_URL}/_WebService/General/DataLoader.asmx"
AGENT_API = f"{BASE_URL}/_WebService/PublicAccess/AgentLocator.asmx/LocateAgent"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "Origin": BASE_URL,
    "Referer": f"{BASE_URL}/PublicAccess/AgentLocator.aspx",
    "X-Requested-With": "XMLHttpRequest",
}

# Tuning knobs
CONCURRENCY = 20          # Max parallel requests
TIMEOUT = 45              # Per-request timeout (seconds)
MAX_RETRIES = 4           # Retry count on failure
RETRY_BACKOFF = 2.0       # Exponential backoff base
API_RESULT_CAP = 15       # Server-side hard limit per query
RATE_LIMIT_DELAY = 0.05   # Min delay between requests

# Paths
PINS_FILE = Path("pins_master.csv")
OUTPUT_DIR = Path("data")
RAW_DIR = OUTPUT_DIR / "raw"
PROGRESS_FILE = OUTPUT_DIR / "progress.json"
TRUNCATED_FILE = OUTPUT_DIR / "truncated_queries.csv"

# Column names for output CSV
AGENT_COLUMNS = [
    "AgentID_Internal", "AgentName", "LicenseNo", "IRDA_URN", "Agent_ID",
    "InsuranceType", "Insurer", "DP_ID", "State", "District",
    "PINCode", "ValidFrom", "ValidTo", "AbsorbedAgent", "PhoneNo", "MobileNo"
]

# ─── Logging ─────────────────────────────────────────────────────────────────

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler("irdai_scraper.log", mode="a"),
        ],
    )
    # Suppress httpx debug spam
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    return logging.getLogger("irdai")

log = setup_logging()

# ─── Data Classes ────────────────────────────────────────────────────────────

@dataclass
class InsuranceType:
    id: str
    name: str

@dataclass
class Insurer:
    id: int
    code: str
    name: str
    type_id: str

@dataclass
class ScrapeStats:
    total_queries: int = 0
    successful: int = 0
    empty: int = 0
    failed: int = 0
    truncated: int = 0      # Queries that hit the 15-result cap
    total_agents: int = 0
    pins_done: int = 0
    pins_total: int = 0
    start_time: float = field(default_factory=time.time)

    def summary(self):
        elapsed = time.time() - self.start_time
        rate = self.total_queries / max(elapsed, 1)
        pct = (self.pins_done / max(self.pins_total, 1)) * 100
        return (
            f"PINs: {self.pins_done:,}/{self.pins_total:,} ({pct:.1f}%) | "
            f"Queries: {self.total_queries:,} ({rate:.0f}/s) | "
            f"Agents: {self.total_agents:,} | "
            f"Truncated: {self.truncated} | Failed: {self.failed} | "
            f"Time: {elapsed:.0f}s"
        )

# ─── XML Parsers ─────────────────────────────────────────────────────────────

def parse_insurance_types(xml_text: str) -> list[InsuranceType]:
    root = ET.fromstring(xml_text)
    types = []
    for table in root.findall("Table"):
        name = table.findtext("VcParamValueDisplay", "").strip()
        tid = table.findtext("BintParamConstantValue", "").strip()
        if tid:
            types.append(InsuranceType(id=tid, name=name))
    return types


def parse_insurers(xml_text: str, type_id: str) -> list[Insurer]:
    root = ET.fromstring(xml_text)
    insurers = []
    for table in root.findall("Table"):
        uid = int(table.findtext("intTblMstInsurerUserID", "0"))
        code = table.findtext("varInsurerID", "").strip()
        name = table.findtext("varName", "").strip()
        insurers.append(Insurer(id=uid, code=code, name=name, type_id=type_id))
    return insurers


def parse_agents(xml_text: str) -> tuple[int, list[list[str]]]:
    """Parse LocateAgent XML response. Returns (total_count, list_of_row_cells)."""
    root = ET.fromstring(xml_text)
    total = int(root.findtext("total", "0"))
    rows = []
    for row in root.findall("row"):
        cells = [cell.text.strip() if cell.text else "" for cell in row.findall("cell")]
        if cells:
            rows.append(cells)
    return total, rows

# ─── PIN Code Loader ─────────────────────────────────────────────────────────

def load_pin_codes(
    filepath: Path,
    states: Optional[list[str]] = None,
    districts: Optional[list[str]] = None,
) -> list[str]:
    """Load unique PIN codes from the master CSV, optionally filtered by state/district."""
    if not filepath.exists():
        log.error(f"PIN codes file not found: {filepath}")
        log.error("Please provide pins_master.csv with a 'pincode' column.")
        sys.exit(1)

    # Pre-compute filter sets for efficiency (avoids rebuilding per row)
    states_upper = {s.upper() for s in states} if states else None
    districts_upper = {d.upper() for d in districts} if districts else None

    pins = set()
    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pin = row.get("pincode", "").strip()
            if not (pin and pin.isdigit() and len(pin) == 6):
                continue

            # State filter (case-insensitive)
            if states_upper:
                state = row.get("statename", "").strip().upper()
                if state not in states_upper:
                    continue

            # District filter (case-insensitive)
            if districts_upper:
                district = row.get("district", "").strip().upper()
                if district not in districts_upper:
                    continue

            pins.add(pin)

    sorted_pins = sorted(pins)

    filter_desc = ""
    if states:
        filter_desc += f" | States: {', '.join(states)}"
    if districts:
        filter_desc += f" | Districts: {', '.join(districts)}"
    log.info(f"Loaded {len(sorted_pins):,} unique PIN codes from {filepath}{filter_desc}")
    return sorted_pins


def list_states_and_districts(filepath: Path):
    """Print all states and their districts with PIN counts."""
    if not filepath.exists():
        log.error(f"PIN codes file not found: {filepath}")
        sys.exit(1)

    state_districts: dict[str, dict[str, set]] = defaultdict(lambda: defaultdict(set))
    state_pins: dict[str, set] = defaultdict(set)

    with open(filepath, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            pin = row.get("pincode", "").strip()
            state = row.get("statename", "").strip()
            district = row.get("district", "").strip()
            if pin and pin.isdigit() and len(pin) == 6 and state:
                state_pins[state].add(pin)
                if district:
                    state_districts[state][district].add(pin)

    print(f"\n{'='*70}")
    print(f"AVAILABLE STATES & DISTRICTS (from {filepath.name})")
    print(f"{'='*70}\n")

    for state in sorted(state_pins.keys()):
        pin_count = len(state_pins[state])
        districts = state_districts[state]
        print(f"  {state}  ({pin_count:,} PINs, {len(districts)} districts)")
        for dist in sorted(districts.keys()):
            print(f"      {dist}: {len(districts[dist]):,} PINs")
        print()

    total_pins = sum(len(v) for v in state_pins.values())
    print(f"Total: {len(state_pins)} states/UTs, {total_pins:,} PINs")
    print(f"\nUsage examples:")
    print(f'  python irdai_scraper.py --state "MAHARASHTRA"')
    print(f'  python irdai_scraper.py --state "DELHI" --types 1 2')
    print(f'  python irdai_scraper.py --state "KARNATAKA" --district "BENGALURU URBAN"')
    print()

# ─── Progress Tracking (Resume Support) ──────────────────────────────────────

class ProgressTracker:
    """Track completed PINs for resume capability."""

    def __init__(self, filepath: Path):
        self.filepath = filepath
        self.completed: set[str] = set()  # "pin_typeId" keys
        self._load()

    def _load(self):
        if self.filepath.exists():
            try:
                with open(self.filepath) as f:
                    data = json.load(f)
                self.completed = set(data.get("completed", []))
                log.info(f"Loaded progress: {len(self.completed):,} completed combos")
            except Exception:
                self.completed = set()

    def save(self):
        os.makedirs(self.filepath.parent, exist_ok=True)
        with open(self.filepath, "w") as f:
            json.dump({"completed": list(self.completed)}, f)

    def is_done(self, pin: str, type_id: str) -> bool:
        return f"{pin}_{type_id}" in self.completed

    def mark_done(self, pin: str, type_id: str):
        self.completed.add(f"{pin}_{type_id}")

    def pending_count(self, pins: list[str], type_ids: list[str]) -> int:
        return sum(
            1 for p in pins for t in type_ids
            if not self.is_done(p, t)
        )

# ─── Async HTTP Helpers ──────────────────────────────────────────────────────

async def fetch_with_retry(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    url: str,
    *,
    content: Optional[str] = None,
    content_type: str = "application/xml; charset=UTF-8",
    max_retries: int = MAX_RETRIES,
) -> Optional[str]:
    """Fetch URL with semaphore, retries, and exponential backoff."""
    headers = {**HEADERS, "Content-Type": content_type}

    for attempt in range(max_retries + 1):
        async with sem:
            try:
                resp = await client.post(url, content=content, headers=headers)
                resp.raise_for_status()
                return resp.text
            except (httpx.HTTPError, httpx.TimeoutException) as e:
                err_type = type(e).__name__
                err_msg = str(e) or "no details"
                if attempt < max_retries:
                    wait = RETRY_BACKOFF ** (attempt + 1)
                    log.warning(f"Retry {attempt+1}/{max_retries}: {err_type}: {err_msg} (wait {wait:.1f}s)")
                    await asyncio.sleep(wait)
                else:
                    log.error(f"Failed after {max_retries} retries: {err_type}: {err_msg} | URL: {url}")
                    return None

# ─── Metadata Loaders ────────────────────────────────────────────────────────

async def load_metadata(client: httpx.AsyncClient, sem: asyncio.Semaphore):
    """Load all insurance types and their insurers."""
    log.info("Loading metadata from IRDAI API...")

    # Insurance types
    xml = await fetch_with_retry(client, sem, f"{DATA_API}/GetInsurerType", content="{}")
    types = parse_insurance_types(xml) if xml else []
    log.info(f"  Insurance types: {[(t.id, t.name) for t in types]}")

    # Insurers per type
    insurers_by_type: dict[str, list[Insurer]] = {}
    for t in types:
        xml = await fetch_with_retry(
            client, sem, f"{DATA_API}/GetInsurer",
            content=f"{{InsuranceType:'{t.id}'}}",
            content_type="application/json; charset=UTF-8",
        )
        if xml:
            ins = parse_insurers(xml, t.id)
            insurers_by_type[t.id] = ins
            log.info(f"  {t.name}: {len(ins)} insurers")

    total_insurers = sum(len(v) for v in insurers_by_type.values())
    log.info(f"  Total insurers across all types: {total_insurers}")
    return types, insurers_by_type

# ─── CSV Writer (Thread-Safe) ────────────────────────────────────────────────

class CSVWriter:
    """Streaming CSV writer with flush control."""

    def __init__(self, filepath: Path):
        self.filepath = filepath
        os.makedirs(filepath.parent, exist_ok=True)
        self.file = open(filepath, "a", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file)
        # Write header only if file is new/empty
        if filepath.stat().st_size == 0:
            self.writer.writerow(AGENT_COLUMNS)
        self.count = 0
        self._flush_counter = 0

    def write_rows(self, rows: list[list[str]]):
        for row in rows:
            padded = (row + [""] * len(AGENT_COLUMNS))[:len(AGENT_COLUMNS)]
            self.writer.writerow(padded)
            self.count += 1
            self._flush_counter += 1
        if self._flush_counter >= 100:
            self.file.flush()
            self._flush_counter = 0

    def close(self):
        self.file.flush()
        self.file.close()
        log.info(f"Wrote {self.count:,} rows to {self.filepath}")

# ─── Truncation Logger ───────────────────────────────────────────────────────

class TruncationLogger:
    """Log queries that hit the 15-result cap (potentially incomplete data)."""

    def __init__(self, filepath: Path):
        self.filepath = filepath
        os.makedirs(filepath.parent, exist_ok=True)
        self.file = open(filepath, "a", newline="", encoding="utf-8")
        self.writer = csv.writer(self.file)
        if filepath.stat().st_size == 0:
            self.writer.writerow(["PIN", "InsuranceType", "InsurerID", "InsurerName", "Total", "Returned"])

    def log(self, pin: str, type_id: str, insurer_id: int, insurer_name: str, total: int, returned: int):
        self.writer.writerow([pin, type_id, insurer_id, insurer_name, total, returned])
        self.file.flush()

    def close(self):
        self.file.close()

# ─── Core Scraping Logic ─────────────────────────────────────────────────────

async def scrape_pin_type_insurer(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    pin: str,
    type_id: str,
    insurer: Insurer,
) -> tuple[list[list[str]], int, bool, bool]:
    """
    Scrape agents for one (PIN, InsuranceType, Insurer) combination.
    Returns (rows, total_declared, was_truncated, was_failed).
    """
    # customquery format: Name,LicenseNo,AgentID,InsuranceType,InsurerID,StateID,DistrictID,PIN
    custom = f"%2C%2C%2C{type_id}%2C{insurer.id}%2C%2C%2C{pin}"
    payload = (
        f"page=1&rp=9999&sortname=AgentName&sortorder=asc"
        f"&query=&qtype=&customquery={custom}"
    )

    xml = await fetch_with_retry(
        client, sem, AGENT_API,
        content=payload,
        content_type="application/x-www-form-urlencoded",
    )

    if not xml:
        return [], 0, False, True  # failed=True

    try:
        total, rows = parse_agents(xml)
    except ET.ParseError as e:
        log.error(f"XML parse error PIN={pin} Type={type_id} Insurer={insurer.id}: {e}")
        return [], 0, False, True  # failed=True

    truncated = (total >= API_RESULT_CAP and len(rows) >= API_RESULT_CAP)
    return rows, total, truncated, False


async def scrape_pin(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    stats: ScrapeStats,
    csv_writers: dict[str, CSVWriter],
    trunc_logger: TruncationLogger,
    pin: str,
    type_id: str,
    type_name: str,
    insurers: list[Insurer],
):
    """Scrape all insurers for one PIN + one insurance type."""
    pin_agents = 0

    for insurer in insurers:
        rows, total, truncated, failed = await scrape_pin_type_insurer(
            client, sem, pin, type_id, insurer,
        )

        stats.total_queries += 1

        if failed:
            stats.failed += 1
            continue

        if not rows:
            stats.empty += 1
            continue

        stats.successful += 1
        stats.total_agents += len(rows)
        pin_agents += len(rows)

        # Write to type-specific CSV
        csv_writers[type_id].write_rows(rows)

        if truncated:
            stats.truncated += 1
            trunc_logger.log(pin, type_id, insurer.id, insurer.name, total, len(rows))
            log.warning(
                f"  [!] TRUNCATED: PIN={pin} {type_name}/{insurer.name}: "
                f"got {len(rows)}/{total} (cap={API_RESULT_CAP})"
            )

        # Tiny delay to be polite
        await asyncio.sleep(RATE_LIMIT_DELAY)

    return pin_agents


async def process_pin_batch(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    stats: ScrapeStats,
    csv_writers: dict[str, CSVWriter],
    trunc_logger: TruncationLogger,
    progress: ProgressTracker,
    pins_batch: list[str],
    types: list[InsuranceType],
    insurers_by_type: dict[str, list[Insurer]],
):
    """Process a batch of PINs across all types."""
    tasks = []

    for pin in pins_batch:
        for t in types:
            if progress.is_done(pin, t.id):
                continue

            insurers = insurers_by_type.get(t.id, [])
            if not insurers:
                continue

            tasks.append(
                scrape_pin_for_type(
                    client, sem, stats, csv_writers, trunc_logger,
                    progress, pin, t, insurers,
                )
            )

    if tasks:
        await asyncio.gather(*tasks)


async def scrape_pin_for_type(
    client: httpx.AsyncClient,
    sem: asyncio.Semaphore,
    stats: ScrapeStats,
    csv_writers: dict[str, CSVWriter],
    trunc_logger: TruncationLogger,
    progress: ProgressTracker,
    pin: str,
    t: InsuranceType,
    insurers: list[Insurer],
):
    """Scrape one PIN for one type, then mark as done."""
    await scrape_pin(
        client, sem, stats, csv_writers, trunc_logger,
        pin, t.id, t.name, insurers,
    )
    progress.mark_done(pin, t.id)
    stats.pins_done += 1

# ─── Main Pipeline ───────────────────────────────────────────────────────────

async def run_pipeline(
    pins_file: Path = PINS_FILE,
    types_filter: Optional[list[str]] = None,
    insurers_filter: Optional[list[int]] = None,
    max_pins: Optional[int] = None,
    max_insurers_per_type: Optional[int] = None,
    concurrency: int = CONCURRENCY,
    resume: bool = False,
    batch_size: int = 50,
    state_filter: Optional[list[str]] = None,
    district_filter: Optional[list[str]] = None,
):
    """
    Main scraping pipeline.

    Args:
        pins_file: Path to CSV with 'pincode' column
        types_filter: Insurance type IDs to process (e.g., ['1'] for General)
        insurers_filter: Insurer IDs to process
        max_pins: Limit number of PINs (for testing)
        max_insurers_per_type: Limit insurers per type (for testing)
        concurrency: Max parallel requests
        resume: If True, skip already-completed combos
        batch_size: PINs to process per batch before saving progress
        state_filter: State names to filter PINs (e.g., ['MAHARASHTRA'])
        district_filter: District names to filter PINs (e.g., ['MUMBAI'])
    """
    # Build output directory (state-specific if filtered)
    if state_filter and len(state_filter) == 1:
        state_tag = state_filter[0].upper().replace(" ", "_")
        out_dir = OUTPUT_DIR / state_tag
        raw_dir = out_dir / "raw"
        progress_file = out_dir / "progress.json"
        truncated_file = out_dir / "truncated_queries.csv"
    else:
        out_dir = OUTPUT_DIR
        raw_dir = RAW_DIR
        progress_file = PROGRESS_FILE
        truncated_file = TRUNCATED_FILE

    os.makedirs(raw_dir, exist_ok=True)

    # Load PIN codes (with state/district filter)
    all_pins = load_pin_codes(pins_file, states=state_filter, districts=district_filter)
    if max_pins:
        all_pins = all_pins[:max_pins]

    if not all_pins:
        log.error("No PIN codes matched the given filters. Use --list-states to see available states/districts.")
        return None

    # Progress tracker
    progress = ProgressTracker(progress_file) if resume else ProgressTracker(Path("/dev/null"))
    if not resume and progress_file.exists():
        progress_file.unlink()

    stats = ScrapeStats()
    sem = asyncio.Semaphore(concurrency)

    async with httpx.AsyncClient(
        timeout=httpx.Timeout(TIMEOUT),
        follow_redirects=True,
        limits=httpx.Limits(max_connections=concurrency + 5, max_keepalive_connections=concurrency),
    ) as client:
        # Step 1: Load metadata
        types, insurers_by_type = await load_metadata(client, sem)

        # Apply filters
        if types_filter:
            types = [t for t in types if t.id in types_filter]

        for t_id in list(insurers_by_type.keys()):
            ins_list = insurers_by_type[t_id]
            if insurers_filter:
                ins_list = [i for i in ins_list if i.id in insurers_filter]
            if max_insurers_per_type:
                ins_list = ins_list[:max_insurers_per_type]
            insurers_by_type[t_id] = ins_list

        # Calculate scope
        total_insurers = sum(len(insurers_by_type.get(t.id, [])) for t in types)
        total_combos = len(all_pins) * total_insurers
        stats.pins_total = len(all_pins) * len(types)

        log.info(f"\n{'='*70}")
        log.info(f"IRDAI AGENT LOCATOR SCRAPER v2")
        log.info(f"{'='*70}")
        log.info(f"PINs: {len(all_pins):,}")
        log.info(f"Types: {len(types)} ({', '.join(t.name for t in types)})")
        log.info(f"Insurers: {total_insurers}")
        log.info(f"Total query combinations: {total_combos:,}")
        log.info(f"Concurrency: {concurrency}")
        if state_filter:
            log.info(f"State filter: {', '.join(state_filter)}")
        if district_filter:
            log.info(f"District filter: {', '.join(district_filter)}")
        log.info(f"Output dir: {out_dir}")

        if resume:
            pending = progress.pending_count(all_pins, [t.id for t in types])
            log.info(f"Resuming: {stats.pins_total - pending:,} already done, {pending:,} pending")
            stats.pins_total = pending

        log.info(f"{'='*70}\n")

        # Step 2: Open CSV writers (one per type, append mode)
        csv_writers: dict[str, CSVWriter] = {}
        for t in types:
            csv_path = raw_dir / f"agents_{t.name.lower()}.csv"
            # Create empty file if new
            if not csv_path.exists():
                csv_path.touch()
            csv_writers[t.id] = CSVWriter(csv_path)

        # Truncation logger
        if not truncated_file.exists():
            truncated_file.parent.mkdir(parents=True, exist_ok=True)
            truncated_file.touch()
        trunc_logger = TruncationLogger(truncated_file)

        # Step 3: Process PINs in batches
        try:
            for batch_start in range(0, len(all_pins), batch_size):
                batch = all_pins[batch_start:batch_start + batch_size]

                await process_pin_batch(
                    client, sem, stats, csv_writers, trunc_logger,
                    progress, batch, types, insurers_by_type,
                )

                # Save progress periodically
                if resume and batch_start % (batch_size * 5) == 0:
                    progress.save()

                # Log progress
                if batch_start % (batch_size * 2) == 0 or batch_start + batch_size >= len(all_pins):
                    log.info(f"Progress: {stats.summary()}")

        except KeyboardInterrupt:
            log.warning("\nInterrupted! Saving progress...")
        finally:
            # Clean up
            for w in csv_writers.values():
                w.close()
            trunc_logger.close()
            if resume:
                progress.save()

        # Step 4: Summary
        log.info(f"\n{'='*70}")
        log.info(f"SCRAPING COMPLETE")
        log.info(f"{'='*70}")
        log.info(stats.summary())

        if stats.truncated > 0:
            log.warning(
                f"\n[!] {stats.truncated} queries hit the 15-result API cap. "
                f"See {truncated_file} for details."
            )

    return stats


async def merge_outputs(state_filter: Optional[list[str]] = None):
    """Merge all raw CSV files into a deduplicated master file."""
    if state_filter and len(state_filter) == 1:
        state_tag = state_filter[0].upper().replace(" ", "_")
        out_dir = OUTPUT_DIR / state_tag
        raw_dir = out_dir / "raw"
    else:
        out_dir = OUTPUT_DIR
        raw_dir = RAW_DIR

    log.info(f"Merging output files from {raw_dir}...")
    raw_files = sorted(raw_dir.glob("agents_*.csv"))

    if not raw_files:
        log.warning("No raw files found to merge!")
        return None

    dfs = []
    for f in raw_files:
        try:
            df = pd.read_csv(f, dtype=str)
            if len(df) > 0:
                dfs.append(df)
                log.info(f"  {f.name}: {len(df):,} rows")
        except Exception as e:
            log.error(f"  Error reading {f}: {e}")

    if not dfs:
        log.warning("No data found in raw files!")
        return None

    master = pd.concat(dfs, ignore_index=True)
    log.info(f"Total before dedup: {len(master):,}")

    # Clean up whitespace
    for col in master.columns:
        master[col] = master[col].str.strip()

    # Deduplicate on IRDA_URN + LicenseNo (primary identifiers)
    before = len(master)
    master.drop_duplicates(subset=["IRDA_URN", "LicenseNo"], keep="first", inplace=True)
    log.info(f"Total after dedup: {len(master):,} (removed {before - len(master):,} dupes)")

    # Save
    if state_filter and len(state_filter) == 1:
        tag = state_filter[0].upper().replace(" ", "_")
        master_csv = out_dir / f"agents_{tag}_master.csv"
    else:
        master_csv = out_dir / "irdai_agents_master.csv"
    master.to_csv(master_csv, index=False)
    log.info(f"Master CSV: {master_csv} ({len(master):,} rows)")

    try:
        master_parquet = master_csv.with_suffix(".parquet")
        master.to_parquet(master_parquet, index=False)
        log.info(f"Master Parquet: {master_parquet}")
    except Exception as e:
        log.warning(f"Could not save Parquet: {e}")

    return master


# ─── CLI ─────────────────────────────────────────────────────────────────────

def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="IRDAI Agent Locator Scraper v2 - PIN-based granular extraction",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python irdai_scraper.py --test                                   Quick test (5 PINs)
  python irdai_scraper.py --list-states                            Show all states & districts
  python irdai_scraper.py --state "MAHARASHTRA"                    Scrape one state
  python irdai_scraper.py --state "DELHI" "GOA"                    Scrape multiple states
  python irdai_scraper.py --state "KARNATAKA" --district "MYSURU"  Scrape one district
  python irdai_scraper.py --state "DELHI" --types 1 2              State + type filter
  python irdai_scraper.py --types 1                                General insurance only
  python irdai_scraper.py --resume --state "MAHARASHTRA"           Resume state scrape
  python irdai_scraper.py --merge-only --state "MAHARASHTRA"       Merge state CSVs
  python irdai_scraper.py                                          FULL SCRAPE (all data)
        """,
    )
    parser.add_argument("--pins-file", type=Path, default=PINS_FILE,
                        help=f"Path to PIN codes CSV (default: {PINS_FILE})")
    parser.add_argument("--types", nargs="*",
                        help="Insurance type IDs: 1=General, 2=Life, 3=Health")
    parser.add_argument("--insurers", nargs="*", type=int,
                        help="Specific insurer IDs to scrape")
    parser.add_argument("--state", nargs="*",
                        help='State name(s) to scrape, e.g. --state "MAHARASHTRA" "DELHI"')
    parser.add_argument("--district", nargs="*",
                        help='District name(s) to scrape, e.g. --district "MUMBAI" "PUNE"')
    parser.add_argument("--list-states", action="store_true",
                        help="List all available states, districts, and PIN counts, then exit")
    parser.add_argument("--max-pins", type=int,
                        help="Limit number of PINs (for testing)")
    parser.add_argument("--max-insurers", type=int,
                        help="Limit insurers per type (for testing)")
    parser.add_argument("--concurrency", type=int, default=CONCURRENCY,
                        help=f"Max parallel requests (default: {CONCURRENCY})")
    parser.add_argument("--batch-size", type=int, default=50,
                        help="PINs per batch (default: 50)")
    parser.add_argument("--resume", action="store_true",
                        help="Resume from last saved progress")
    parser.add_argument("--merge-only", action="store_true",
                        help="Only merge existing raw CSV files")
    parser.add_argument("--test", action="store_true",
                        help="Quick test: 5 PINs, 1 type, 1 insurer")

    args = parser.parse_args()

    # List states mode
    if args.list_states:
        list_states_and_districts(args.pins_file)
        return

    # Warn if --district used without --state (could match across states)
    if args.district and not args.state:
        log.warning(
            "[!] --district without --state will match that district name across ALL states. "
            "Consider adding --state for precision."
        )

    if args.merge_only:
        asyncio.run(merge_outputs(state_filter=args.state))
        return

    if args.test:
        log.info("=" * 50)
        log.info("TEST MODE: 5 PINs x 1 type x 1 insurer")
        log.info("=" * 50)
        asyncio.run(run_pipeline(
            pins_file=args.pins_file,
            types_filter=["1"],
            max_pins=5,
            max_insurers_per_type=1,
            concurrency=5,
            state_filter=args.state,
            district_filter=args.district,
        ))
    else:
        asyncio.run(run_pipeline(
            pins_file=args.pins_file,
            types_filter=args.types,
            insurers_filter=args.insurers,
            max_pins=args.max_pins,
            max_insurers_per_type=args.max_insurers,
            concurrency=args.concurrency,
            resume=args.resume,
            batch_size=args.batch_size,
            state_filter=args.state,
            district_filter=args.district,
        ))

    # Merge after scraping
    asyncio.run(merge_outputs(state_filter=args.state))


if __name__ == "__main__":
    main()
