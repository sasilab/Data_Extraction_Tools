#!/usr/bin/env python3
"""
MPP tm_1 extractor

Scans a folder (e.g., MPP) for compressed XML files (.xml.gz or .zip with XML inside),
matches rows by timestamp and inverter_id==4, and outputs a CSV and Pickle with
columns: timestamp, tm_1.

The XML is expected to look like:

  <row ...>
    <item key="timestamp" value="YYYY-MM-DD HH:MM:SS"/>
    <item key="inverter_id" value="4"/>
    <item key="tm_1" value="53.5"/>
    ...
  </row>

Usage:
  python mpp_tm1_extractor.py \
    --base-dir /path/to/MPP \
    --timestamps-file /path/to/timestamps.txt \
    --out-csv /path/to/out.csv \
    --out-pkl /path/to/out.pkl

The timestamps file can be:
  - A plain text file with one timestamp string per line, or
  - A CSV with a column named "timestamp".

Notes:
  - Matching is done as an exact string equality on the "timestamp" field
    (trimmed). Make sure your timestamps match the XML format, e.g.:
        2025-08-14 13:51:00
  - Both .xml.gz and .zip files are supported. For .zip, the first .xml entry
    is parsed.
"""

from pathlib import Path
import sys, argparse, gzip, zipfile, io, csv, pickle
import xml.etree.ElementTree as ET
from typing import Iterable, Dict, Tuple, Set, Optional
import pandas as pd

def read_timestamps(path: Path) -> Set[str]:
    """
    Load desired timestamps as a set of normalized strings.
    Accepts either a plain .txt (one per line) or a .csv with a 'timestamp' column.
    """
    if not path.exists():
        raise FileNotFoundError(f"Timestamps file not found: {path}")
    ts_set: Set[str] = set()
    if path.suffix.lower() == ".csv":
        df = pd.read_csv(path)
        if "timestamp" not in df.columns:
            raise ValueError("CSV must contain a 'timestamp' column.")
        for val in df["timestamp"]:
            if pd.isna(val):
                continue
            ts_set.add(str(val).strip())
    else:
        # treat as text
        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                s = line.strip()
                if s:
                    ts_set.add(s)
    return ts_set

def open_xml_stream(file_path: Path) -> io.BufferedReader:
    """
    Given a compressed file path (.xml.gz or .zip with an XML file inside),
    return a binary file-like object with *XML bytes* ready for ElementTree.iterparse.
    """
    suf = file_path.suffix.lower()
    if suf == ".gz" and file_path.name.lower().endswith(".xml.gz"):
        return gzip.open(file_path, "rb")
    elif suf == ".zip":
        zf = zipfile.ZipFile(file_path, "r")
        # pick the first XML entry
        xml_names = [n for n in zf.namelist() if n.lower().endswith(".xml")]
        if not xml_names:
            raise ValueError(f"No XML found inside zip: {file_path}")
        data = zf.read(xml_names[0])
        return io.BytesIO(data)
    else:
        # allow raw .xml for convenience
        if suf == ".xml":
            return open(file_path, "rb")
        raise ValueError(f"Unsupported file type: {file_path} (expected .xml.gz or .zip)")


def iter_rows(xml_stream: io.BufferedReader) -> Iterable[ET.Element]:
    """
    Yield <row> elements from an XML stream efficiently and clear them to save memory.
    """
    context = ET.iterparse(xml_stream, events=("end",))
    for event, elem in context:
        if elem.tag == "row":
            yield elem
            elem.clear()


def row_items_to_dict(row_elem: ET.Element) -> Dict[str, Optional[str]]:
    """
    Convert child <item key="..." value="..."/> elements into a dict
    with lowercase keys.
    """
    d: Dict[str, Optional[str]] = {}
    for item in row_elem.findall("item"):
        k = (item.attrib.get("key") or "").strip().lower()
        v = item.attrib.get("value")
        d[k] = v
    return d


def collect_tm1_for_timestamps(
    base_dir: Path, desired_ts: Set[str], inverter_id_value: str = "4"
) -> Dict[str, Optional[str]]:
    """
    Walk the base_dir for all .xml.gz and .zip files.
    For each, parse rows and, for rows with matching timestamp and inverter_id==4,
    record tm_1. Returns a mapping: timestamp -> tm_1 (string or None if absent).
    """
    results: Dict[str, Optional[str]] = {}
    wanted = set(desired_ts)  # copy so we can remove as we find
    # Discover candidate files
    candidates = list(base_dir.rglob("*.xml.gz")) + list(base_dir.rglob("*.zip")) + list(base_dir.rglob("*.xml"))
    # Sort for deterministic order (by name); not strictly required
    candidates.sort(key=lambda p: p.name.lower())

    for fp in candidates:
        if not wanted:
            break  # found all we need
        try:
            with open_xml_stream(fp) as stream:
                for row in iter_rows(stream):
                    d = row_items_to_dict(row)
                    ts = (d.get("timestamp") or "").strip()
                    if not ts or ts not in wanted:
                        continue
                    if (d.get("inverter_id") or "").strip() != inverter_id_value:
                        continue
                    tm1 = d.get("tm_1")
                    results[ts] = tm1
                    # Once found for this timestamp, we can remove it
                    wanted.discard(ts)
                    if not wanted:
                        break
        except Exception as e:
            print(f"[WARN] Skipping {fp}: {e}", file=sys.stderr)
    return results


def write_outputs(mapping: Dict[str, Optional[str]], out_csv: Path, out_pkl: Path) -> None:
    # Build DataFrame sorted by timestamp string
    rows = [{"timestamp": k, "tm_1": mapping.get(k)} for k in sorted(mapping.keys())]
    df = pd.DataFrame(rows)
    # Try converting tm_1 to float if possible
    with pd.option_context("mode.chained_assignment", None):
        try:
            df["tm_1"] = pd.to_numeric(df["tm_1"], errors="coerce")
        except Exception:
            pass
    df.to_csv(out_csv, index=False)
    with out_pkl.open("wb") as f:
        pickle.dump(df, f)


def main(argv=None):
    ap = argparse.ArgumentParser(description="Extract tm_1 for inverter_id=4 at specific timestamps from MPP XML archives.")
    ap.add_argument("--base-dir", required=True, help="Path to the MPP folder containing .xml.gz / .zip files (searched recursively).")
    ap.add_argument("--timestamps-file", required=True, help="Path to a .txt (one per line) or .csv (with 'timestamp' column).")
    ap.add_argument("--out-csv", required=True, help="Output CSV path.")
    ap.add_argument("--out-pkl", required=True, help="Output Pickle path (pandas DataFrame).")
    ap.add_argument("--inverter-id", default="4", help="Inverter ID to match (default: 4).")

    args = ap.parse_args(argv)
    base_dir = Path(args.base_dir).expanduser().resolve()
    ts_file = Path(args.timestamps_file).expanduser().resolve()
    out_csv = Path(args.out_csv).expanduser().resolve()
    out_pkl = Path(args.out_pkl).expanduser().resolve()

    desired_ts = read_timestamps(ts_file)
    if not desired_ts:
        print("No timestamps to search for; check your timestamps file.", file=sys.stderr)
        sys.exit(1)

    mapping = collect_tm1_for_timestamps(base_dir, desired_ts, inverter_id_value=str(args.inverter_id))
    # For any missing timestamps, ensure they appear with tm_1=None
    for ts in desired_ts:
        mapping.setdefault(ts, None)

    out_csv.parent.mkdir(parents=True, exist_ok=True)
    out_pkl.parent.mkdir(parents=True, exist_ok=True)
    write_outputs(mapping, out_csv, out_pkl)

    print(f"Wrote CSV: {out_csv}")
    print(f"Wrote Pickle: {out_pkl}")
    missing = [ts for ts, v in mapping.items() if v in (None, "")]
    if missing:
        print(f"Note: {len(missing)} timestamps found without tm_1 (or missing).", file=sys.stderr)

if __name__ == "__main__":
    main()
