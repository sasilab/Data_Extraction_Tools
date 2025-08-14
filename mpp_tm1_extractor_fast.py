#!/usr/bin/env python3
"""
Fast MPP tm_1 extractor

Strategy:
1) For each requested timestamp t, generate candidate archive *names* using t + minute offsets
   (default 2..6 minutes). For each minute, try all seconds 00..59. Extensions tried: .xml.gz, .zip, .xml
2) For each *existing* candidate file, open once to read <info> startdate/enddate and confirm t lies in range.
3) Parse only confirmed files to extract rows with:
     - key="timestamp" == t
     - key="inverter_id" == <inverter-id> (default: 4)
     - read key="tm_1"
Outputs: CSV + Pickle with columns: timestamp, tm_1
"""

from pathlib import Path
import sys, argparse, gzip, zipfile, io, re
import xml.etree.ElementTree as ET
from typing import Dict, Set, Optional, List, Tuple
import pandas as pd
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

TS_FMT = "%Y-%m-%d %H:%M:%S"
EXTS = (".xml.gz", ".zip", ".xml")

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
    d: Dict[str, Optional[str]] = {}
    for item in row_elem.findall("item"):
        k = (item.attrib.get("key") or "").strip().lower()
        v = item.attrib.get("value")
        d[k] = v
    return d

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
    # returns (start, end) as datetime if available
    with open_xml_stream(fp) as stream:
        start_str, end_str = read_info_times(stream)
    if start_str and end_str:
        return parse_ts_any(start_str), parse_ts_any(end_str)
    # fallback: derive by scanning timestamps in rows (expensive but only for found candidates)
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

def extract_matches(fp: Path, wanted_ts: Set[str], inverter_id_value: str) -> Dict[str, Optional[str]]:
    out: Dict[str, Optional[str]] = {}
    with open_xml_stream(fp) as stream:
        for row in iter_rows(stream):
            d = row_items_to_dict(row)
            ts = (d.get("timestamp") or "").strip()
            if ts not in wanted_ts:
                continue
            if (d.get("inverter_id") or "").strip() != inverter_id_value:
                continue
            out[ts] = d.get("tm_1")
    return out

def main():
    ap = argparse.ArgumentParser(description="Fast extractor using filename guessing + info confirmation.")
    ap.add_argument("--base-dir", required=True, help="Folder containing archives OR a single archive file.")
    ap.add_argument("--timestamps-csv", required=True, help="CSV with timestamps (e.g. sma_all_files_extended.csv).")
    ap.add_argument("--ts-column", default=None, help="Name of the CSV column containing timestamps. Auto-detects if omitted.")
    ap.add_argument("--out-csv", required=True)
    ap.add_argument("--out-pkl", required=True)
    ap.add_argument("--inverter-id", default="4")
    ap.add_argument("--minute-offset-start", type=int, default=2, help="Start minute offset from ts (inclusive).")
    ap.add_argument("--minute-offset-end", type=int, default=6, help="End minute offset from ts (inclusive).")
    ap.add_argument("--max-workers", type=int, default=8, help="Threads for confirming/extracting.")
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    base = Path(args.base_dir).expanduser().resolve()
    ts_csv = Path(args.timestamps-csv).expanduser().resolve()  # type: ignore[attr-defined]
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
    ts_list: List[str] = []
    for v in df[col].tolist():
        if pd.isna(v):
            continue
        try:
            ts_list.append(ts_norm_str(parse_ts_any(v)))
        except Exception:
            if debug:
                print(f"[WARN] Bad timestamp: {v}", file=sys.stderr)

    if not ts_list:
        print("No valid timestamps.", file=sys.stderr); sys.exit(1)

    # Build candidate file set by name guessing
    minute_offsets = list(range(args.minute_offset_start, args.minute_offset_end + 1))
    seconds_range = range(0, 60)

    base_dir = base.parent if base.is_file() else base
    if base.is_file():
        # If single file, just use that if it exists
        candidates = {base}
    else:
        # Generate names and check existence
        names = set()
        for ts in ts_list:
            dt = parse_ts_any(ts)
            names.update(candidate_names_for_timestamp(dt, minute_offsets, seconds_range))
        candidates: Set[Path] = set()
        for n in names:
            for ext in EXTS:
                p = base_dir / f"{n}{ext}"
                if p.exists():
                    candidates.add(p)
        if debug:
            print(f"[INFO] Found {len(candidates)} candidate archive(s) by name.", file=sys.stderr)

    # Confirm coverage ranges (parallel)
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
    per_file: Dict[Path, Set[str]] = {}
    for ts in ts_list:
        dt = parse_ts_any(ts)
        for fp, (s, e) in cover_map.items():
            if s and e and s <= dt <= e:
                per_file.setdefault(fp, set()).add(ts)

    if debug:
        total_ts = sum(len(v) for v in per_file.values())
        print(f"[INFO] Routed {total_ts}/{len(ts_list)} timestamps to {len(per_file)} file(s).", file=sys.stderr)

    # Extract (parallel over files)
    results: Dict[str, Optional[str]] = {ts: None for ts in ts_list}
    with ThreadPoolExecutor(max_workers=args.max_workers) as ex:
        futs = {ex.submit(extract_matches, fp, wanted, inverter_id_value): (fp, wanted)
                for fp, wanted in per_file.items()}
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
