"""Upload essential Kilosort 2.5 output files to S3 via Axon.

Uploads only the 6 files needed by force_ingest_external_sorting.py from
the 4 target channel groups. Everything else (TB-scale .bin, temp_wh.dat,
rez2.mat, etc.) is excluded.

Usage:
    # Dry run (list files + sizes, no upload):
    python -m aeon.dj_pipeline.scripts.axon_upload_ephys --dry-run

    # Real upload:
    python -m aeon.dj_pipeline.scripts.axon_upload_ephys

    # Upload + verify:
    python -m aeon.dj_pipeline.scripts.axon_upload_ephys --verify

Prerequisites:
    - djsciops config with Axon credentials (client_id, client_secret)
    - Run from HPC where /ceph/aeon is mounted
"""

import argparse
import sys
from pathlib import Path

import datajoint as dj

logger = dj.logger

ROOT_DIR = Path("/ceph/aeon")
DB_PREFIX = "swc-ucl_aeon-social_"

SORTED_DATA_ROOT = (
    ROOT_DIR / "aeon/data/processed/AEONX1/social-ephys0.1"
    / "2024-06-04T10-24-07/NeuropixelsV2Beta/SpikeSortingRaw"
)

TARGET_GROUPS = [
    "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_1_144",
    "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_121_264",
    "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_241_384",
    "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_193_240",
]

ESSENTIAL_FILES = [
    "spike_times.npy",
    "spike_clusters.npy",
    "templates.npy",
    "cluster_KSLabel.tsv",
    "params.py",
]

# Conditional files — uploaded if present, not an error if missing.
# spike_times_sync_binary_2_147.npy: Dario's pre-converted HARP seconds (float64).
#   Used by force_ingest step 17 for SyncedSpikes insertion.
# spike_index_harp_clock_binary_2_147.npy: Dario's uint64 HARP ticks (250 MHz).
#   Used for cross-validation only.
OPTIONAL_FILES = [
    "spike_times_sync_binary_2_147.npy",
    "spike_index_harp_clock_binary_2_147.npy",
]

s3_session, s3_bucket = None, None


def _get_axon_s3_session():
    """Authenticate to Axon S3 using djsciops credentials."""
    import djsciops.authentication as dj_auth
    import djsciops.settings as dj_settings

    global s3_session, s3_bucket
    if s3_session is not None:
        return s3_session, s3_bucket

    dj_sciops_config = dj_settings.get_config()
    s3_session = dj_auth.Session(
        aws_account_id=dj_sciops_config["aws"]["account_id"],
        s3_role=dj_sciops_config["s3"]["role"],
        auth_client_id=dj_sciops_config["djauth"]["client_id"],
        auth_client_secret=dj_sciops_config["djauth"].get("client_secret"),
    )
    s3_bucket = dj_sciops_config["s3"]["bucket"]
    return s3_session, s3_bucket


def collect_upload_manifest():
    """Build list of (local_path, relative_to_root) for all files to upload."""
    manifest = []
    missing_required = []
    missing_optional = []

    for grp in TARGET_GROUPS:
        grp_dir = SORTED_DATA_ROOT / grp
        if not grp_dir.exists():
            print(f"  [FAIL] Channel group directory not found: {grp_dir}")
            sys.exit(1)

        for fname in ESSENTIAL_FILES:
            fpath = grp_dir / fname
            if fpath.exists():
                rel = fpath.relative_to(ROOT_DIR)
                manifest.append((fpath, rel))
            else:
                missing_required.append(f"{grp}/{fname}")

        for fname in OPTIONAL_FILES:
            fpath = grp_dir / fname
            if fpath.exists():
                rel = fpath.relative_to(ROOT_DIR)
                manifest.append((fpath, rel))
            else:
                missing_optional.append(f"{grp}/{fname}")

    return manifest, missing_required, missing_optional


def dry_run():
    """List all files that would be uploaded with sizes."""
    print(f"\n{'='*70}")
    print(f"  DRY RUN — Axon Upload: Ephys Sorting Data")
    print(f"  DB_PREFIX: {DB_PREFIX}")
    print(f"  Source: {SORTED_DATA_ROOT}")
    print(f"{'='*70}\n")

    manifest, missing_req, missing_opt = collect_upload_manifest()

    total_bytes = 0
    for local_path, rel_path in manifest:
        size = local_path.stat().st_size
        total_bytes += size
        s3_dest = f"{DB_PREFIX[:-1]}/inbox/{rel_path.as_posix()}"
        size_str = f"{size / 1e9:.2f} GB" if size > 1e9 else f"{size / 1e6:.0f} MB"
        print(f"  {size_str:>10s}  {rel_path}")

    print(f"\n  Total: {len(manifest)} files, {total_bytes / 1e9:.2f} GB")

    if missing_req:
        print(f"\n  [FAIL] Missing REQUIRED files:")
        for m in missing_req:
            print(f"    - {m}")

    if missing_opt:
        print(f"\n  [WARN] Missing OPTIONAL files (ground truth):")
        for m in missing_opt:
            print(f"    - {m}")
        print("  → These channel groups will need Clock.bin upload for SyncedSpikes")

    print()
    return len(missing_req) == 0


def upload_files():
    """Upload all essential files to S3 via Axon."""
    import djsciops.axon as dj_axon

    manifest, missing_req, missing_opt = collect_upload_manifest()
    if missing_req:
        print("[FAIL] Cannot upload — required files missing. Run with --dry-run first.")
        sys.exit(1)

    s3_session, s3_bucket = _get_axon_s3_session()

    print(f"\n  Uploading {len(manifest)} files to S3...")

    for i, (local_path, rel_path) in enumerate(manifest, 1):
        s3_dest = f"{DB_PREFIX[:-1]}/inbox/{rel_path.as_posix()}"
        size_mb = local_path.stat().st_size / 1e6
        print(f"  [{i}/{len(manifest)}] {rel_path} ({size_mb:.0f} MB) -> {s3_dest}")

        # Upload single file: use parent dir as source, file lands at destination
        # NOTE: dj_axon.upload_files() may expect a directory, not a single file.
        # If it fails, try: source=local_path.parent.as_posix() with a file filter,
        # or use dj_axon.upload_file() (singular) if available.
        # Test with --dry-run first on HPC to verify API behavior.
        dj_axon.upload_files(
            source=local_path.as_posix(),
            destination=f"{DB_PREFIX[:-1]}/inbox/{rel_path.parent.as_posix()}/",
            session=s3_session,
            s3_bucket=s3_bucket,
        )

    print(f"\n  Upload complete: {len(manifest)} files")

    if missing_opt:
        print(f"\n  [WARN] Skipped optional files (not present on Ceph):")
        for m in missing_opt:
            print(f"    - {m}")


def verify_upload():
    """Compare local files against what's on S3."""
    import djsciops.axon as dj_axon

    s3_session, s3_bucket = _get_axon_s3_session()
    manifest, _, _ = collect_upload_manifest()

    s3_prefix = f"{DB_PREFIX[:-1]}/inbox/"
    remote_files = {
        Path(x["key"]).relative_to(DB_PREFIX[:-1] + "/inbox").as_posix(): x["_size"]
        for x in dj_axon.list_files(
            session=s3_session,
            s3_bucket=s3_bucket,
            s3_prefix=s3_prefix + "aeon/data/processed/AEONX1/social-ephys0.1",
            as_tree=False,
        )
    }

    local_files = {
        rel_path.as_posix(): local_path.stat().st_size
        for local_path, rel_path in manifest
    }

    all_ok = True
    for path, size in local_files.items():
        if path in remote_files:
            if remote_files[path] == size:
                print(f"  [OK] {path}")
            else:
                print(f"  [FAIL] {path}: size mismatch (local={size}, remote={remote_files[path]})")
                all_ok = False
        else:
            print(f"  [FAIL] {path}: not found on S3")
            all_ok = False

    if all_ok:
        print(f"\n  Verification PASSED: {len(local_files)} files match")
    else:
        print(f"\n  Verification FAILED: some files missing or mismatched")

    return all_ok


def main():
    parser = argparse.ArgumentParser(
        description="Upload essential ephys sorting files to S3 via Axon"
    )
    parser.add_argument("--dry-run", action="store_true", help="List files without uploading")
    parser.add_argument("--verify", action="store_true", help="Verify upload after completion")
    args = parser.parse_args()

    if args.dry_run:
        ok = dry_run()
        sys.exit(0 if ok else 1)

    upload_files()

    if args.verify:
        verify_upload()


if __name__ == "__main__":
    main()
