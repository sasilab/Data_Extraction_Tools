#!/usr/bin/env python3
"""
Fast MPP tm_1 extractor with date cutoffs and flexible timestamp matching.

Matching modes:
- minute (default): match any row whose timestamp falls in the same minute as the requested timestamp
- exact: require exact second equality
- tolerance: match if |row_ts - requested_ts| <= --second-tolerance seconds (default 59)

Strategy:
1) Load timestamps from CSV, normalize to "YYYY-MM-DD HH:MM:SS".
2) Optional cutoffs via --date-min/--date-max (accept YYYY-MM or YYYY-MM-DD or full).
3) For each timestamp, generate candidate archive names using t + minute offsets (default 2..6 min),
   for seconds 00..59; try extensions .xml.gz, .zip, .xml.
4) For existing candidates, read <info> startdate/enddate (or fallback to min/max row timestamps)
   to confirm the timestamp is inside the file's range.
5) Parse only confirmed files; for each matching row (timestamp & inverter_id), read tm_1.
"""

from pathlib import Path
import sys, argparse, gzip, zipfile, io, re, calendar
import xml.etree.ElementTree as ET
from typing import Dict, Set, Optional, List, Tuple
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

TS_FMT = "%Y-%m-%d %H:%M:%S"
EXTS = (".xml.gz", ".zip", ".xml")

# ------------ time helpers ------------
def parse_ts_any(s: str) -> datetime:
    s = str(s).strip()
    if not s:
        raise ValueError("Empty timestamp")
    if (s[0] == s[-1]) and s[0] in ("'", '"'):
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

def parse_cutoff(s: Optional[str], is_max: bool) -> Optional[datetime]:
    if not s:
        return None
    s = s.strip()
    m = re.fullmatch(r"(\d{4})-(\d{2})", s)  # YYYY-MM
    if m:
        y = int(m.group(1)); mo = int(m.group(2))
        if is_max:
            last_day = calendar.monthrange(y, mo)[1]
            return datetime(y, mo, last_day, 23, 59, 59)
        return datetime(y, mo, 1, 0, 0, 0)
    try:
        return datetime.strptime(s, TS_FMT)
    except Exception:
        try:
            d = datetime.strptime(s, "%Y-%m-%d")
            return d.replace(hour=23, minute=59, second=59) if is_max else d
        except Exception:
            return pd.to_datetime(s).to_pydatetime()

# ------------ file/open helpers ------------
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

def iter_rows(xml_stream):
    context = ET.iterparse(xml_stream, events=("end",))
    for _, elem in context:
        if elem.tag == "row":
            yield elem
            elem.clear()
        else:
            elem.clear()

def row_items_to_dict(row_elem: ET.Element) -> Dict[str, Optional[str]]:
    return { (item.attrib.get("key") or "").strip().lower(): item.attrib.get("value")
             for item in row_elem.findall("item") }

def derive_range_from_rows(fp: Path) -> Tuple[Optional[datetime], Optional[datetime]]:
    ts_min = ts_max = None
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

# ------------ core ------------
def candidate_names_for_timestamp(dt: datetime,
                                  minute_offsets: List[int],
                                  seconds_range: range) -> List[str]:
    names = []
    for off in minute_offsets:
        base = dt + timedelta(minutes=off)
        for s in seconds_range:
            names.append(base.strftime(f"%Y%m%d%H%M{ s:02d }").replace(" ", ""))
    return names

def confirm_coverage(fp: Path) -> Tuple[Optional[datetime], Optional[datetime]]:
    with open_xml_stream(fp) as stream:
        start_str, end_str = read_info_times(stream)
    if start_str and end_str:
        return parse_ts_any(start_str), parse_ts_any(end_str)
    return derive_range_from_rows(fp)

def extract_matches(fp: Path,
                    requested_ts_list: List[Tuple[str, datetime]],
                    inverter_id_value: str,
                    match_mode: str,
                    second_tolerance: int) -> Dict[str, Optional[str]]:
    """
    requested_ts_list: list of (original_ts_str, dt)
    """
    out: Dict[str, Optional[str]] = {}

    # Fast index by minute if mode == 'minute'
    minute_index: Dict[Tuple[int,int,int,int,int], List[str]] = {}
    if match_mode == "minute":
        for ts_str, dt in requested_ts_list:
            key = (dt.year, dt.month, dt.day, dt.hour, dt.minute)
            minute_index.setdefault(key, []).append(ts_str)
    elif match_mode == "exact":
        exact_set = { ts_str for ts_str, _ in requested_ts_list }
    else:
        # tolerance mode: leave as list, we'll scan linearly (requested per file is usually small)
        pass

    with open_xml_stream(fp) as stream:
        for row in iter_rows(stream):
            d = row_items_to_dict(row)
            if (d.get("inverter_id") or "").strip() != inverter_id_value:
                continue
            ts_row = (d.get("timestamp") or "").strip()
            if not ts_row:
                continue
            try:
                dt_row = parse_ts_any(ts_row)
            except Exception:
                continue

            if match_mode == "minute":
                key = (dt_row.year, dt_row.month, dt_row.day, dt_row.hour, dt_row.minute)
                for ts_str in minute_index.get(key, []):
                    # don't overwrite if already filled
                    if ts_str not in out:
                        out[ts_str] = d.get("tm_1")
            elif match_mode == "exact":
                ts_row_norm = ts_norm_str(dt_row)
                if ts_row_norm in exact_set and ts_row_norm not in out:
                    out[ts_row_norm] = d.get("tm_1")
            else:  # tolerance
                for ts_str, dt_req in requested_ts_list:
                    if ts_str in out:
                        continue
                    delta = abs((dt_row - dt_req).total_seconds())
                    if delta <= second_tolerance:
                        out[ts_str] = d.get("tm_1")
    return out

def main():
    ap = argparse.ArgumentParser(description="Fast extractor using filename guessing + info confirmation, with date cutoffs and flexible matching.")
    ap.add_argument("--base-dir", required=True, help="Folder containing archives OR a single archive file.")
    ap.add_argument("--timestamps-csv", required=True, help="CSV with timestamps (e.g. sma_all_files_extended.csv).")
    ap.add_argument("--ts-column", default=None, help="Name of the CSV column containing timestamps. Auto-detects if omitted.")
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-pkl", required=True)
    ap.add_argument("--inverter-id", default="4")
    ap.add_argument("--minute-offset-start", type=int, default=2, help="Start minute offset from ts (inclusive).")
    ap.add_argument("--minute-offset-end", type=int, default=6, help="End minute offset from ts (inclusive).")
    ap.add_argument("--date-min", default=None, help="Min timestamp to include (YYYY-MM or YYYY-MM-DD or full).")
    ap.add_argument("--date-max", default=None, help="Max timestamp to include (YYYY-MM or YYYY-MM-DD or full).")
    ap.add_argument("--match", choices=["minute","exact","tolerance"], default="minute",
                    help="How to match CSV timestamps to XML rows. Default: minute.")
    ap.add_argument("--second-tolerance", type=int, default=59, help="Used when --match tolerance.")
    ap.add_argument("--max-workers", type=int, default=8, help="Threads for confirming/extracting.")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    base = Path(args.base_dir).expanduser().resolve()
    ts_csv = Path(args.timestamps_csv).expanduser().resolve()
    out_csv = Path(args.out_csv).expanduser().resolve()
    out_pkl = Path(args.out_pkl).expanduser().resolve()
    inverter_id_value = str(args.inverter_id).strip()
    debug = args.debug

    # Load timestamps
    df = pd.read_csv(ts_csv)
    if args.ts_column:
        col = args.ts_column
    else:
        cands = [c for c in df.columns if "timestamp" in c.lower()]
        if not cands:
            print("Could not auto-detect a timestamp column. Use --ts-column.", file=sys.stderr)
            sys.exit(1)
        col = cands[0]

    dt_min = parse_cutoff(args.date_min, is_max=False)
    dt_max = parse_cutoff(args.date_max, is_max=True)

    requested: List[Tuple[str, datetime]] = []
    for v in df[col].tolist():
        if pd.isna(v):
            continue
        try:
            dt = parse_ts_any(v)
            if dt_min and dt < dt_min: continue
            if dt_max and dt > dt_max: continue
            requested.append((ts_norm_str(dt), dt))
        except Exception:
            if debug:
                print(f"[WARN] Bad timestamp: {v}", file=sys.stderr)

    if not requested:
        print("No valid timestamps after applying date filters.", file=sys.stderr)
        sys.exit(1)

    # Build candidate file set by name guessing
    minute_offsets = list(range(args.minute_offset_start, args.minute_offset_end + 1))
    seconds_range = range(0, 60)

    base_dir = base.parent if base.is_file() else base
    if base.is_file():
        candidates = {base}
    else:
        names = set()
        for _, dt in requested:
            for n in candidate_names_for_timestamp(dt, minute_offsets, seconds_range):
                names.add(n)
        candidates: Set[Path] = set()
        for n in names:
            for ext in EXTS:
                p = base_dir / f"{n}{ext}"
                if p.exists():
                    candidates.add(p)
        if debug:
            print(f"[INFO] Found {len(candidates)} candidate archive(s) by name.", file=sys.stderr)

    # Confirm coverage (parallel)
    cover_map: Dict[Path, Tuple[Optional[datetime], Optional[datetime]]] = {}
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {ex.submit(confirm_coverage, fp): fp for fp in candidates}
        for f in as_completed(futs):
            fp = futs[f]
            try:
                cover_map[fp] = f.result()
            except Exception as e:
                if debug:
                    print(f"[WARN] Could not read info from {fp.name}: {e}", file=sys.stderr)

    # Group timestamps by covering file
    per_file: Dict[Path, List[Tuple[str, datetime]]] = {}
    for ts_str, dt in requested:
        for fp, (s, e) in cover_map.items():
            if s and e and s <= dt <= e:
                per_file.setdefault(fp, []).append((ts_str, dt))

    if debug:
        total_ts = sum(len(v) for v in per_file.values())
        print(f"[INFO] Routed {total_ts}/{len(requested)} timestamps to {len(per_file)} file(s).", file=sys.stderr)

    # Extract (parallel)
    results: Dict[str, Optional[str]] = {ts_str: None for ts_str, _ in requested}
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {
            ex.submit(extract_matches, fp, wanted, inverter_id_value, args.match, args.second_tolerance): (fp, wanted)
            for fp, wanted in per_file.items()
        }
        for f in as_completed(futs):
            try:
                found = f.result()
                results.update(found)
            except Exception as e:
                if debug:
                    fp, _ = futs[f]
                    print(f"[WARN] Extract failed for {fp.name}: {e}", file=sys.stderr)

    # Write outputs
    rows = [{"timestamp": k, "tm_1": results.get(k)} for k in sorted(results)]
    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_pkl.parent.mkdir(parents=True, exist_ok=True)
    df_out = pd.DataFrame(rows)
    df_out["tm_1"] = pd.to_numeric(df_out["tm_1"], errors="coerce")
    df_out.to_csv(out_csv, index=False)
    df_out.to_pickle(out_pkl)

    missing = sum(1 for v in results.values() if v in (None, ""))
    print(f"Done. {len(results)} rows written. {missing} missing tm_1 (or not found).")

if __name__ == "__main__":
    main()
