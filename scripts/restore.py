#!/usr/bin/env python3
"""Restore BOTH volumes from one whole-group bundle.

The consistency rule is enforced here: the manifest's round id must match the
round id stamped by each volume's own ``_snapshot_report.json`` inside the
zip.  Mixing snapshots from different backup rounds is refused.

Usage:
  scripts/restore.py <bundle.zip> --index-dir DIR --blob-dir DIR [--force]
"""
from __future__ import annotations

import argparse
import json
import sys
import zipfile
from pathlib import Path


def _safe_extract_members(zf: zipfile.ZipFile, prefix: str, dest: Path, force: bool) -> int:
    count = 0
    for info in zf.infolist():
        name = info.filename
        if not name.startswith(prefix):
            continue
        rel = name[len(prefix):]
        if not rel or rel.endswith("/"):
            continue
        target = (dest / rel).resolve()
        if dest.resolve() not in target.parents and target != dest.resolve():
            raise RuntimeError(f"unsafe path in zip: {name}")
        if target.exists() and not force:
            raise FileExistsError(f"{target} exists; pass --force to overwrite")
        target.parent.mkdir(parents=True, exist_ok=True)
        with zf.open(info) as src, open(target, "wb") as out:
            out.write(src.read())
        count += 1
    return count


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("bundle", type=Path)
    ap.add_argument("--index-dir", type=Path, required=True)
    ap.add_argument("--blob-dir", type=Path, required=True)
    ap.add_argument("--force", action="store_true")
    args = ap.parse_args()

    with zipfile.ZipFile(args.bundle) as zf:
        names = set(zf.namelist())
        if "manifest.json" not in names:
            print("ERROR: not a group bundle (manifest.json missing)", file=sys.stderr)
            return 2
        manifest = json.loads(zf.read("manifest.json"))
        round_id = manifest["round_id"]

        stamps = {}
        for vol, stamp_name in (
            ("index", "index/_snapshot_report.json"),
            ("blobs", "blobs/_snapshot_report.json"),
        ):
            if stamp_name not in names:
                print(f"ERROR: {stamp_name} missing from bundle", file=sys.stderr)
                return 2
            stamps[vol] = json.loads(zf.read(stamp_name)).get("round_id")

        print(f"manifest round : {round_id}")
        print(f"index stamp    : {stamps['index']}")
        print(f"blobs stamp    : {stamps['blobs']}")
        if stamps["index"] != round_id or stamps["blobs"] != round_id:
            print("ERROR: round mismatch — refusing to mix snapshots from different rounds",
                  file=sys.stderr)
            return 3
        print("round check OK: both volume snapshots belong to the same backup round")

        args.index_dir.mkdir(parents=True, exist_ok=True)
        args.blob_dir.mkdir(parents=True, exist_ok=True)
        n_idx = _safe_extract_members(zf, "index/", args.index_dir, args.force)
        n_blob = _safe_extract_members(zf, "blobs/", args.blob_dir, args.force)

    print(f"restored {n_idx} index files -> {args.index_dir}")
    print(f"restored {n_blob} blob files  -> {args.blob_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
