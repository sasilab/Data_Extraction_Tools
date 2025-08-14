#!/usr/bin/env python3
"""
Smart MPP tm_1 extractor (robust)

Improvements vs previous version:
- Accepts a *file* path in --base-dir (treats it as a single archive to parse).
- Verbose logging (use --debug).
- Indexing fallback: if <info> block is missing, derive [start,end] by scanning <row> timestamps.
- Better diagnostics: lists which archives were indexed and their ranges.

What it does:
1) Build an index of archives (zip/xml.gz/xml) under --base-dir (or handle a single file).
   Index uses <info> startdate/enddate, or falls back to min/max <row> timestamps.
2) Map requested timestamps (from your CSV) to the right archive by that time range.
3) Open only the necessary archives and extract rows where:
   - key="timestamp" == requested timestamp
   - key="inverter_id" == <inverter-id> (default 4)
   - read key="tm_1"
4) Write CSV + Pickle with columns: timestamp, tm_1
"""

from pathlib import Path
import sys, argparse, gzip, zipfile, io, re
import xml.etree.ElementTree as ET
from typing import Iterable, Dict, Set, Optional, List, Tuple, Union
import pandas as pd
from datetime import datetime

TS_FMT = "%Y-%m-%d %H:%M:%S"

def log(msg: str, debug: bool = False, force: bool = False):
    if debug or force:
        print(msg, file=sys.stderr)

# ---------- Timestamp helpers ----------

def parse_ts_any(s: str) -> datetime:
    s = str(s).strip()
    if not s:
        raise ValueError("Empty timestamp")
    if (s[0] == s[-1]) and s[0] in ('"', "'"):
        s = s[1:-1].strip()
    try:
        return datetime.strptime(s, TS_FMT)
    except Exception:
        pass
    if re.fullmatch(r"\d{14}", s):
        return datetime.strptime(s, "%Y%m%d%H%M%S")
    return pd.to_datetime(s).to_pydatetime()

def ts_norm_str(dt: datetime) -> str:
    return dt.strftime(TS_FMT)

# ---------- File opening ----------

def open_xml_stream(file_path: Path):
    name = file_path.name.lower()
    if name.endswith(".xml.gz"):
        return gzip.open(file_path, "rb")
    if name.endswith(".zip"):
        zf = zipfile.ZipFile(file_path, "r")
        xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not xml_names:
            raise ValueError(f"No XML found inside zip: {file_path}")
        data = zf.read(xml_names[0])
        return io.BytesIO(data)
    if name.endswith(".xml"):
        return open(file_path, "rb")
    raise ValueError(f"Unsupported file type: {file_path}")

# ---------- XML parsing ----------

def read_info_times(xml_stream) -> Tuple[Optional[str], Optional[str]]:
    start_str = end_str = None
    context = ET.iterparse(xml_stream, events=("end",))
    for _, elem in context:
        if elem.tag == "item":
            k = (elem.attrib.get("key") or "").strip().lower()
            v = elem.attrib.get("value")
            if k == "startdate":
                start_str = v
            elif k == "enddate":
                end_str = v
        elif elem.tag == "info":
            elem.clear()
            break
        elem.clear()
    return start_str, end_str

def iter_rows(xml_stream) -> Iterable[ET.Element]:
    context = ET.iterparse(xml_stream, events=("end",))
    for _, elem in context:
        if elem.tag == "row":
            yield elem
            elem.clear()
        else:
            elem.clear()

def row_items_to_dict(row_elem: ET.Element) -> Dict[str, Optional[str]]:
    d: Dict[str, Optional[str]] = {}
    for item in row_elem.findall("item"):
        k = (item.attrib.get("key") or "").strip().lower()
        v = item.attrib.get("value")
        d[k] = v
    return d

def derive_range_from_rows(fp: Path) -> Tuple[Optional[datetime], Optional[datetime]]:
    """If <info> is missing, scan rows to get min/max timestamp."""
    ts_min = None
    ts_max = None
    with open_xml_stream(fp) as stream:
        for row in iter_rows(stream):
            d = row_items_to_dict(row)
            ts = (d.get("timestamp") or "").strip()
            if not ts:
                continue
            try:
                dt = parse_ts_any(ts)
            except Exception:
                continue
            ts_min = dt if ts_min is None else min(ts_min, dt)
            ts_max = dt if ts_max is None else max(ts_max, dt)
    return ts_min, ts_max

# ---------- Index archives ----------

def discover_candidates(base: Path) -> List[Path]:
    if base.is_file():
        return [base]
    # recursively
    return (
        list(base.rglob("*.zip")) +
        list(base.rglob("*.xml.gz")) +
        list(base.rglob("*.xml"))
    )

def index_archives(base: Path, debug: bool = False) -> List[Tuple[datetime, datetime, Path]]:
    candidates = discover_candidates(base)
    if not candidates:
        log(f"[WARN] No archives found under: {base}", debug=True, force=True)
        return []

    idx: List[Tuple[datetime, datetime, Path]] = []
    for fp in sorted(candidates, key=lambda p: p.name.lower()):
        try:
            # Try <info>
            with open_xml_stream(fp) as stream:
                start_str, end_str = read_info_times(stream)
            if start_str and end_str:
                start_dt = parse_ts_any(start_str)
                end_dt = parse_ts_any(end_str)
                idx.append((start_dt, end_dt, fp))
                log(f"[INDEX] {fp.name} -> {ts_norm_str(start_dt)} .. {ts_norm_str(end_dt)}", debug)
                continue

            # Fallback: derive by scanning row timestamps
            log(f"[WARN] No <info> in {fp.name}; deriving range from rows...", debug=True, force=True)
            ts_min, ts_max = derive_range_from_rows(fp)
            if ts_min and ts_max:
                idx.append((ts_min, ts_max, fp))
                log(f"[INDEX-FALLBACK] {fp.name} -> {ts_norm_str(ts_min)} .. {ts_norm_str(ts_max)}", debug)
            else:
                log(f"[WARN] Could not derive range from {fp.name}; skipping.", debug=True, force=True)

        except Exception as e:
            log(f"[WARN] Indexing failed for {fp.name}: {e}", debug=True, force=True)
    if not idx:
        log("[WARN] Index is empty after scanning. Check your --base-dir path and archives.", debug=True, force=True)
    else:
        log(f"[INFO] Indexed {len(idx)} archive(s).", debug=True, force=True)
    return idx

def find_covering_file(ts_dt: datetime, index: List[Tuple[datetime, datetime, Path]]) -> Optional[Path]:
    for start_dt, end_dt, fp in index:
        if start_dt <= ts_dt <= end_dt:
            return fp
    return None

# ---------- Extraction ----------

def extract_from_archive(fp: Path, wanted_ts: Set[str], inverter_id_value: str, debug: bool=False) -> Dict[str, Optional[str]]:
    results: Dict[str, Optional[str]] = {}
    with open_xml_stream(fp) as stream:
        for row in iter_rows(stream):
            d = row_items_to_dict(row)
            ts = (d.get("timestamp") or "").strip()
            if ts not in wanted_ts:
                continue
            if (d.get("inverter_id") or "").strip() != inverter_id_value:
                continue
            tm1 = d.get("tm_1")
            results[ts] = tm1
    if results:
        log(f"[FOUND] {fp.name}: matched {len(results)} timestamp(s).", debug=True, force=True)
    return results

# ---------- Main ----------

def load_requested_timestamps(csv_path: Path, ts_column: Optional[str], debug: bool=False) -> List[str]:
    df = pd.read_csv(csv_path)
    if not ts_column:
        candidates = [c for c in df.columns if "timestamp" in c.lower()]
        if not candidates:
            raise ValueError("Could not auto-detect a timestamp column. Use --ts-column.")
        ts_column = candidates[0]
        log(f"[INFO] Auto-detected timestamp column: {ts_column}", debug=True, force=True)
    if ts_column not in df.columns:
        raise ValueError(f"Column '{ts_column}' not found in {csv_path.name}. Columns: {list(df.columns)}")

    ts_list: List[str] = []
    for val in df[ts_column].tolist():
        if pd.isna(val):
            continue
        try:
            ts_list.append(ts_norm_str(parse_ts_any(str(val))))
        except Exception:
            log(f"[WARN] Skipping unparseable timestamp value: {val}", debug=True, force=True)
    log(f"[INFO] Loaded {len(ts_list)} timestamp(s) from CSV.", debug=True, force=True)
    return ts_list

def write_outputs(mapping: Dict[str, Optional[str]], out_csv: Path, out_pkl: Path):
    rows = [{"timestamp": k, "tm_1": mapping.get(k)} for k in sorted(mapping)]
    df = pd.DataFrame(rows)
    df["tm_1"] = pd.to_numeric(df["tm_1"], errors="coerce")
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_pkl.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_csv, index=False)
    df.to_pickle(out_pkl)

def main(argv=None):
    ap = argparse.ArgumentParser(description="Extract tm_1 at specified timestamps from MPP XML archives with robust indexing.")
    ap.add_argument("--base-dir", required=True, help="Path to the mpp folder (recursively scanned) OR a single archive file (.zip/.xml.gz/.xml).")
    ap.add_argument("--timestamps-csv", required=True, help="Path to sma_all_files_extended.csv (or any CSV with timestamp column).")
    ap.add_argument("--ts-column", default=None, help="Name of the timestamp column. If omitted, auto-detects a column containing 'timestamp'.")
    ap.add_argument("--out-csv", required=True, help="Output CSV path.")
    ap.add_argument("--out-pkl", required=True, help="Output Pickle path (pandas DataFrame).")
    ap.add_argument("--inverter-id", default="4", help="Inverter ID to match (default: 4).")
    ap.add_argument("--debug", action="store_true", help="Verbose logging.")
    args = ap.parse_args(argv)

    base = Path(args.base_dir).expanduser().resolve()
    ts_csv = Path(args.timestamps_csv).expanduser().resolve()
    out_csv = Path(args.out_csv).expanduser().resolve()
    out_pkl = Path(args.out_pkl).expanduser().resolve()
    inverter_id_value = str(args.inverter_id).strip()
    debug = args.debug

    ts_list = load_requested_timestamps(ts_csv, args.ts_column, debug=debug)
    if not ts_list:
        print("No valid timestamps found in CSV.", file=sys.stderr)
        sys.exit(1)

    index = index_archives(base, debug=debug)
    if not index:
        print("No archives indexed. Check --base-dir path and that it contains .zip/.xml.gz/.xml.", file=sys.stderr)
        sys.exit(1)

    per_file: Dict[Path, Set[str]] = {}
    missing_route: List[str] = []
    for ts_str in ts_list:
        ts_dt = parse_ts_any(ts_str)
        fp = find_covering_file(ts_dt, index)
        if fp is None:
            missing_route.append(ts_str)
            continue
        per_file.setdefault(fp, set()).add(ts_str)

    if missing_route:
        log(f"[WARN] {len(missing_route)} timestamp(s) did not map to any archive range. They will appear as missing.", debug=True, force=True)

    results: Dict[str, Optional[str]] = {ts: None for ts in ts_list}
    for fp, wanted_ts in per_file.items():
        try:
            found = extract_from_archive(fp, wanted_ts, inverter_id_value=inverter_id_value, debug=debug)
            results.update(found)
        except Exception as e:
            log(f"[WARN] Failed extracting from {fp.name}: {e}", debug=True, force=True)

    write_outputs(results, out_csv, out_pkl)
    total = len(results)
    missing_vals = sum(1 for v in results.values() if v in (None, ""))
    print(f"Done. {total} rows written. {missing_vals} missing tm_1 (or not found).")

if __name__ == "__main__":
    main()
