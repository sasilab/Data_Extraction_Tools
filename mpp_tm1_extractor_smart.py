#!/usr/bin/env python3
"""
Smart MPP tm_1 extractor

1) Index archives in --base-dir by reading XML <info>:
     <item key="startdate" value="YYYY-MM-DD HH:MM:SS"/>
     <item key="enddate"   value="YYYY-MM-DD HH:MM:SS"/>
2) For each requested timestamp from a CSV (e.g., sma_all_files_extended.csv),
   find the archive whose [startdate, enddate] contains it.
3) Parse only those archives, extract rows where:
     - item key="timestamp" == requested timestamp
     - item key="inverter_id" == <inverter-id>  (default: "4")
     - then read item key="tm_1"
4) Write CSV + Pickle with columns: timestamp, tm_1

Supports .zip (first .xml inside), .xml.gz, and raw .xml.

Usage:
  python mpp_tm1_extractor_smart.py \
    --base-dir /path/to/mpp \
    --timestamps-csv /path/to/sma_all_files_extended.csv \
    --ts-column timestamp \
    --out-csv /path/to/out_tm1.csv \
    --out-pkl /path/to/out_tm1.pkl \
    [--inverter-id 4]
"""

from pathlib import Path
import sys, argparse, gzip, zipfile, io, re
import xml.etree.ElementTree as ET
from typing import Iterable, Dict, Set, Optional, List, Tuple
import pandas as pd
from datetime import datetime

# ---------- Helpers: timestamp parsing/normalization ----------

TS_FMT = "%Y-%m-%d %H:%M:%S"

def parse_ts_any(s: str) -> datetime:
    """
    Accepts 'YYYY-MM-DD HH:MM:SS' or 'YYYYMMDDHHMMSS'.
    Returns a datetime.
    """
    s = s.strip()
    if not s:
        raise ValueError("Empty timestamp string")
    # Remove quotes if present
    if (s[0] == s[-1]) and s[0] in ("'", '"'):
        s = s[1:-1].strip()
    # Try dash+space format first
    try:
        return datetime.strptime(s, TS_FMT)
    except Exception:
        pass
    # Try compact 14-digit format
    if re.fullmatch(r"\d{14}", s):
        return datetime.strptime(s, "%Y%m%d%H%M%S")
    # Try some CSV auto-parsed variants (e.g., pandas date)
    # Let pandas parse if it's weird; then format back
    try:
        return pd.to_datetime(s).to_pydatetime()
    except Exception as e:
        raise ValueError(f"Unsupported timestamp format: {s}") from e

def ts_norm_str(dt: datetime) -> str:
    """Normalized string for matching XML items."""
    return dt.strftime(TS_FMT)

# ---------- File opening ----------

def open_xml_stream(file_path: Path):
    """
    Returns a binary file-like object containing XML bytes ready for iterparse.
    """
    name = file_path.name.lower()
    if name.endswith(".xml.gz"):
        return gzip.open(file_path, "rb")
    if name.endswith(".zip"):
        zf = zipfile.ZipFile(file_path, "r")
        # choose the first .xml entry (you can tweak this to pick by name)
        xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not xml_names:
            raise ValueError(f"No XML found inside zip: {file_path}")
        data = zf.read(xml_names[0])
        return io.BytesIO(data)
    if name.endswith(".xml"):
        return open(file_path, "rb")
    raise ValueError(f"Unsupported file type: {file_path}")

# ---------- XML parsing (lightweight for <info>, streaming for <row>) ----------

def read_info_times(xml_stream) -> Tuple[Optional[str], Optional[str]]:
    """
    Parse only until </info> to fetch startdate and enddate (strings).
    Returns (startdate_str, enddate_str) as seen in XML, or (None, None).
    """
    start_str = end_str = None
    # We parse for 'end' events and stop after closing </info>
    context = ET.iterparse(xml_stream, events=("end",))
    for event, elem in context:
        if elem.tag == "item":
            k = (elem.attrib.get("key") or "").strip().lower()
            v = elem.attrib.get("value")
            if k == "startdate":
                start_str = v
            elif k == "enddate":
                end_str = v
        elif elem.tag == "info":
            # Done with info section
            elem.clear()
            break
        elem.clear()
    return start_str, end_str

def iter_rows(xml_stream) -> Iterable[ET.Element]:
    """Yield <row> elements from an XML stream efficiently."""
    context = ET.iterparse(xml_stream, events=("end",))
    for _, elem in context:
        if elem.tag == "row":
            yield elem
            elem.clear()

def row_items_to_dict(row_elem: ET.Element) -> Dict[str, Optional[str]]:
    """Convert <item key="..." value="..."/> to a dict with lowercase keys."""
    d: Dict[str, Optional[str]] = {}
    for item in row_elem.findall("item"):
        k = (item.attrib.get("key") or "").strip().lower()
        v = item.attrib.get("value")
        d[k] = v
    return d

# ---------- Index archives by [startdate, enddate] ----------

def index_archives(base_dir: Path) -> List[Tuple[datetime, datetime, Path]]:
    """
    Returns a list of (start_dt, end_dt, path) for each archive discovered.
    """
    candidates = (
        list(base_dir.rglob("*.zip")) +
        list(base_dir.rglob("*.xml.gz")) +
        list(base_dir.rglob("*.xml"))
    )
    idx: List[Tuple[datetime, datetime, Path]] = []
    for fp in sorted(candidates, key=lambda p: p.name.lower()):
        try:
            with open_xml_stream(fp) as stream:
                start_str, end_str = read_info_times(stream)
            if not start_str or not end_str:
                # If <info> not present, skip or fallback to name-based heuristic
                # Here we skip to stay accurate
                print(f"[WARN] No start/end in {fp.name}; skipping file for indexing.", file=sys.stderr)
                continue
            start_dt = parse_ts_any(start_str)
            end_dt = parse_ts_any(end_str)
            idx.append((start_dt, end_dt, fp))
        except Exception as e:
            print(f"[WARN] Indexing failed for {fp}: {e}", file=sys.stderr)
    return idx

def find_covering_file(ts_dt: datetime, index: List[Tuple[datetime, datetime, Path]]) -> Optional[Path]:
    """
    Find the first archive whose [start_dt, end_dt] includes ts_dt.
    """
    for start_dt, end_dt, fp in index:
        if start_dt <= ts_dt <= end_dt:
            return fp
    return None

# ---------- Extraction ----------

def extract_from_archive(fp: Path, wanted_ts: Set[str], inverter_id_value: str) -> Dict[str, Optional[str]]:
    """
    Parse <row> of archive `fp`, and return mapping timestamp->tm_1 for the subset in `wanted_ts`.
    """
    results: Dict[str, Optional[str]] = {}
    with open_xml_stream(fp) as stream:
        for row in iter_rows(stream):
            d = row_items_to_dict(row)
            ts = (d.get("timestamp") or "").strip()
            if ts not in wanted_ts:
                continue
            if (d.get("inverter_id") or "").strip() != inverter_id_value:
                continue
            results[ts] = d.get("tm_1")
            # optional: early stop if found all for this file
            if len(results) == len(wanted_ts):
                # not entirely safe if duplicates; but good enough
                pass
    return results

# ---------- Main flow ----------

def load_requested_timestamps(csv_path: Path, ts_column: Optional[str]) -> List[str]:
    df = pd.read_csv(csv_path)
    # Auto-detect column if not provided
    if not ts_column:
        candidates = [c for c in df.columns if "timestamp" in c.lower()]
        if not candidates:
            raise ValueError("Could not auto-detect a timestamp column. Use --ts-column.")
        ts_column = candidates[0]
    if ts_column not in df.columns:
        raise ValueError(f"Column '{ts_column}' not found in {csv_path.name}. Columns: {list(df.columns)}")

    ts_list: List[str] = []
    for val in df[ts_column].tolist():
        if pd.isna(val):
            continue
        try:
            ts_list.append(ts_norm_str(parse_ts_any(str(val))))
        except Exception:
            # If a row has a bad timestamp, skip but warn
            print(f"[WARN] Skipping unparseable timestamp value: {val}", file=sys.stderr)
    return ts_list

def write_outputs(mapping: Dict[str, Optional[str]], out_csv: Path, out_pkl: Path):
    rows = [{"timestamp": k, "tm_1": mapping.get(k)} for k in sorted(mapping)]
    df = pd.DataFrame(rows)
    # numeric conversion
    df["tm_1"] = pd.to_numeric(df["tm_1"], errors="coerce")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_pkl.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    df.to_pickle(out_pkl)

def main(argv=None):
    ap = argparse.ArgumentParser(description="Extract tm_1 at specified timestamps from MPP XML archives by first locating the right archive via <info startdate/enddate>.")
    ap.add_argument("--base-dir", required=True, help="Path to the mpp folder containing .zip/.xml.gz/.xml (searched recursively).")
    ap.add_argument("--timestamps-csv", required=True, help="Path to sma_all_files_extended.csv (or a CSV with a timestamp column).")
    ap.add_argument("--ts-column", default=None, help="Name of the timestamp column in the CSV. If omitted, auto-detects a column containing 'timestamp'.")
    ap.add_argument("--out-csv", required=True, help="Output CSV path.")
    ap.add_argument("--out-pkl", required=True, help="Output Pickle path (pandas DataFrame).")
    ap.add_argument("--inverter-id", default="4", help="Inverter ID to match (default: 4).")
    args = ap.parse_args(argv)

    base_dir = Path(args.base_dir).expanduser().resolve()
    ts_csv = Path(args.timestamps_csv).expanduser().resolve()
    out_csv = Path(args.out_csv).expanduser().resolve()
    out_pkl = Path(args.out_pkl).expanduser().resolve()
    inverter_id_value = str(args.inverter_id).strip()

    # 1) Load + normalize timestamps from CSV
    ts_list = load_requested_timestamps(ts_csv, args.ts_column)
    if not ts_list:
        print("No valid timestamps found in CSV.", file=sys.stderr)
        sys.exit(1)

    # 2) Build archive index by [startdate, enddate]
    index = index_archives(base_dir)
    if not index:
        print("No archives with valid start/end times were indexed. Check your mpp folder.", file=sys.stderr)
        sys.exit(1)

    # 3) Map timestamps to the correct archive
    #    We'll group timestamps by archive to minimize parsing.
    per_file: Dict[Path, Set[str]] = {}
    missing_routing: List[str] = []

    for ts_str in ts_list:
        ts_dt = parse_ts_any(ts_str)
        fp = find_covering_file(ts_dt, index)
        if fp is None:
            # Could fallback to filename-based heuristic here if desired.
            missing_routing.append(ts_str)
            continue
        per_file.setdefault(fp, set()).add(ts_str)

    if missing_routing:
        print(f"[WARN] {len(missing_routing)} timestamps did not map to any archive via <info>. They will appear as missing in outputs.", file=sys.stderr)

    # 4) Parse only needed archives and collect tm_1
    results: Dict[str, Optional[str]] = {ts: None for ts in ts_list}  # default None for all
    for fp, wanted_ts in per_file.items():
        try:
            found = extract_from_archive(fp, wanted_ts, inverter_id_value=inverter_id_value)
            results.update(found)
        except Exception as e:
            print(f"[WARN] Failed extracting from {fp.name}: {e}", file=sys.stderr)

    # 5) Write outputs
    write_outputs(results, out_csv, out_pkl)

    total = len(results)
    missing_vals = sum(1 for v in results.values() if v in (None, ""))
    print(f"Done. {total} rows written. {missing_vals} missing tm_1 (or not found).")

if __name__ == "__main__":
    main()
