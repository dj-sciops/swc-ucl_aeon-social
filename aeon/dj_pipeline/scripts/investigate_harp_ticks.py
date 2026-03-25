"""Investigate HARP tick format, probe_type, and Chs_193_240 alternatives.

Read-only — no DB writes, no file writes.

Usage:
    python aeon/dj_pipeline/scripts/investigate_harp_ticks.py
"""

import numpy as np
from pathlib import Path

SORTED_ROOT = Path(
    "/ceph/aeon/aeon/data/processed/AEONX1/social-ephys0.1/"
    "2024-06-04T10-24-07/NeuropixelsV2Beta/SpikeSortingRaw"
)

GROUPS = [
    "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_1_144",
    "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_121_264",
    "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_241_384",
    "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_193_240",
]


def main():
    # =========================================================================
    # Question 1: What are the uint64 HARP tick values, and can we convert them?
    # =========================================================================
    print("=" * 70)
    print("  Q1: HARP tick format investigation")
    print("=" * 70)

    grp_dir = SORTED_ROOT / GROUPS[0]  # Use Chs_1_144 as reference

    # Load the uint64 ground truth
    gt_file = grp_dir / "spike_index_harp_clock_binary_2_147.npy"
    gt = np.load(gt_file, mmap_mode="r")
    print(f"\n  Ground truth file: spike_index_harp_clock_binary_2_147.npy")
    print(f"  dtype={gt.dtype}, shape={gt.shape}")
    print(f"  First 5:  {gt.flat[:5]}")
    print(f"  Last 5:   {gt.flat[-5:]}")
    print(f"  Min:      {gt.flat[:].min()}")
    print(f"  Max:      {gt.flat[:].max()}")
    print(f"  Range:    {gt.flat[:].max() - gt.flat[:].min()}")

    # Try common tick rates to see what gives sensible durations
    tick_range = float(gt.flat[:].max() - gt.flat[:].min())
    print(f"\n  Duration at various tick rates:")
    for rate_name, rate in [("1 Hz", 1), ("1 kHz", 1e3), ("32 kHz", 32e3),
                             ("1 MHz", 1e6), ("32 MHz", 32e6), ("250 MHz", 250e6)]:
        duration_s = tick_range / rate
        duration_h = duration_s / 3600
        print(f"    {rate_name:>10s}: {duration_s:>12.1f} s  = {duration_h:>6.1f} hours")

    # The recording is ~6 days (Jun 4-10), so ~144 hours.
    # Whichever tick rate gives ~144 hours is likely correct.

    # Check if there are already-converted files we could use instead
    print(f"\n  Other sync files in {GROUPS[0]}:")
    alt_files = [
        "spike_times_sync.npy",
        "spike_times_sync_binary.npy",
        "spike_times_sync_binary_2_147.npy",
        "spike_times_sync_date.parquet",
        "spike_times_sync_date_binary_2_147.parquet",
    ]
    for fname in alt_files:
        fpath = grp_dir / fname
        if fpath.exists():
            size_mb = fpath.stat().st_size / 1e6
            if fname.endswith(".npy"):
                arr = np.load(fpath, mmap_mode="r")
                print(f"  FOUND: {fname} ({size_mb:.0f} MB, dtype={arr.dtype}, shape={arr.shape})")
                print(f"         First 3: {arr.flat[:3]}")
                print(f"         Last 3:  {arr.flat[-3:]}")
            else:
                print(f"  FOUND: {fname} ({size_mb:.0f} MB) [parquet — not loading]")
        else:
            print(f"  MISSING: {fname}")

    # =========================================================================
    # Question 2: What's available for Chs_193_240?
    # =========================================================================
    print(f"\n{'=' * 70}")
    print(f"  Q2: What files exist for Chs_193_240?")
    print(f"{'=' * 70}")

    grp_240 = SORTED_ROOT / GROUPS[3]
    if grp_240.exists():
        print(f"\n  Directory exists: {grp_240}")
        all_files = sorted(grp_240.iterdir())
        for f in all_files:
            if f.is_file():
                size_mb = f.stat().st_size / 1e6
                size_str = f"{size_mb:.0f} MB" if size_mb >= 1 else f"{f.stat().st_size} bytes"
                print(f"    {size_str:>12s}  {f.name}")
    else:
        print(f"  Directory NOT FOUND: {grp_240}")

    # =========================================================================
    # Question 3: What probe_type strings exist in the DB?
    # =========================================================================
    print(f"\n{'=' * 70}")
    print(f"  Q3: Probe type strings in production DB")
    print(f"{'=' * 70}")

    import datajoint as dj
    from aeon.dj_pipeline import ephys

    # Check ProbeType table
    print(f"\n  ProbeType table entries:")
    try:
        probe_types = ephys.ProbeType.fetch(as_dict=True)
        for pt in probe_types:
            print(f"    - {pt}")
    except Exception as e:
        print(f"    Error: {e}")

    # Check what probe_type is used in EphysChunk for our experiment
    print(f"\n  probe_type values in EphysChunk (social-ephys0.1-aeon3):")
    try:
        pts = (ephys.EphysChunk & {"experiment_name": "social-ephys0.1-aeon3"}).fetch(
            "probe_type", limit=3
        )
        for pt in pts:
            print(f"    - '{pt}'")
    except Exception as e:
        print(f"    Error: {e}")

    # Check what electrode_config_name is used
    print(f"\n  electrode_config_name values in EphysChunk:")
    try:
        ecs = np.unique(
            (ephys.EphysChunk & {"experiment_name": "social-ephys0.1-aeon3"}).fetch(
                "electrode_config_name"
            )
        )
        for ec in ecs:
            print(f"    - '{ec}'")
    except Exception as e:
        print(f"    Error: {e}")

    print()


if __name__ == "__main__":
    main()
