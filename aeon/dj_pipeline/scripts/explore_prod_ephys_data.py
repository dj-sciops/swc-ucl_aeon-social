"""Explore what ephys data exists in the production database (aeon-db2).

Run this on HPC where dj_local_conf.json points to aeon-db2 with prefix "aeon_".
Reports what's available for export to the Works deployment.

Usage:
    python -m aeon.dj_pipeline.scripts.explore_prod_ephys_data
"""

import json
from pathlib import Path

import numpy as np

EXPERIMENT_NAME = "social-ephys0.1-aeon3"


def main():
    import datajoint as dj

    # --- DB connection info ---
    host = dj.config.get("database.host", "unknown")
    prefix = dj.config["custom"].get("database.prefix", "aeon_")
    print(f"\n{'='*70}")
    print(f"  Exploring production DB: {host}  (prefix: {prefix})")
    print(f"  Experiment: {EXPERIMENT_NAME}")
    print(f"{'='*70}\n")

    from aeon.dj_pipeline import acquisition, ephys

    # --- 1. Experiment ---
    print("1. Experiment table")
    exp = acquisition.Experiment & {"experiment_name": EXPERIMENT_NAME}
    n = len(exp)
    print(f"   Experiment entries: {n}")
    if n:
        print(f"   {exp.fetch1()}")
    else:
        print("   *** Experiment not found — nothing else will work ***")
        return
    print()

    # --- 2. Experiment.Directory ---
    print("2. Experiment.Directory")
    dirs = (acquisition.Experiment.Directory & {"experiment_name": EXPERIMENT_NAME}).fetch(
        as_dict=True
    )
    print(f"   Entries: {len(dirs)}")
    for d in dirs:
        print(f"   - type={d['directory_type']}, repo={d['repository_name']}, path={d['directory_path']}")
    print()

    # --- 3. Behavioral Chunk (acquisition.Chunk) ---
    print("3. Behavioral Chunk (acquisition.Chunk)")
    chunks = acquisition.Chunk & {"experiment_name": EXPERIMENT_NAME}
    n_chunks = len(chunks)
    print(f"   Total chunks: {n_chunks}")
    if n_chunks:
        first, last = chunks.fetch(
            "chunk_start", order_by="chunk_start", limit=1
        ), chunks.fetch("chunk_start", order_by="chunk_start DESC", limit=1)
        print(f"   First chunk_start: {first[0]}")
        print(f"   Last chunk_start:  {last[0]}")
    print()

    # --- 4. Epoch ---
    print("4. Epoch")
    epochs = acquisition.Epoch & {"experiment_name": EXPERIMENT_NAME}
    n_epochs = len(epochs)
    print(f"   Epochs: {n_epochs}")
    if n_epochs:
        for ep in epochs.fetch(as_dict=True, order_by="epoch_start", limit=5):
            print(f"   - start={ep['epoch_start']}")
    print()

    # --- 5. EphysEpoch ---
    print("5. EphysEpoch")
    try:
        ee = ephys.EphysEpoch & {"experiment_name": EXPERIMENT_NAME}
        n_ee = len(ee)
        print(f"   EphysEpoch entries: {n_ee}")
        if n_ee:
            for row in ee.fetch(as_dict=True, limit=3):
                print(f"   - epoch_start={row['epoch_start']}")
    except Exception as e:
        print(f"   Error querying EphysEpoch: {e}")
    print()

    # --- 6. EphysEpoch.Insertion ---
    print("6. EphysEpoch.Insertion")
    try:
        ins = ephys.EphysEpoch.Insertion & {"experiment_name": EXPERIMENT_NAME}
        n_ins = len(ins)
        print(f"   Insertion entries: {n_ins}")
        if n_ins:
            for row in ins.fetch(as_dict=True, limit=5):
                print(f"   - epoch={row['epoch_start']}, ins#={row['insertion_number']}, "
                      f"probe_label={row.get('probe_label', '?')}")
    except Exception as e:
        print(f"   Error: {e}")
    print()

    # --- 7. ProbeInsertion ---
    print("7. ProbeInsertion")
    try:
        pi = ephys.ProbeInsertion & {"experiment_name": EXPERIMENT_NAME}
        n_pi = len(pi)
        print(f"   ProbeInsertion entries: {n_pi}")
        if n_pi:
            for row in pi.fetch(as_dict=True):
                print(f"   - subject={row['subject']}, ins#={row['insertion_number']}, "
                      f"probe={row.get('probe', '?')}")
    except Exception as e:
        print(f"   Error: {e}")
    print()

    # --- 8. EphysChunk (THE BIG ONE) ---
    print("8. EphysChunk")
    try:
        ec = ephys.EphysChunk & {"experiment_name": EXPERIMENT_NAME}
        n_ec = len(ec)
        print(f"   EphysChunk entries: {n_ec}")
        if n_ec:
            all_chunks = ec.fetch(as_dict=True, order_by="chunk_start")
            print(f"   First chunk_start: {all_chunks[0]['chunk_start']}")
            print(f"   Last chunk_start:  {all_chunks[-1]['chunk_start']}")
            print(f"   Last chunk_end:    {all_chunks[-1]['chunk_end']}")
            print(f"   Sample entry: {all_chunks[0]}")
        else:
            print("   *** No EphysChunk data — cannot export chunk metadata ***")
    except Exception as e:
        print(f"   Error: {e}")
    print()

    # --- 9. EphysChunk.File ---
    print("9. EphysChunk.File")
    try:
        ecf = ephys.EphysChunk.File & {"experiment_name": EXPERIMENT_NAME}
        n_ecf = len(ecf)
        print(f"   File entries: {n_ecf}")
        if n_ecf:
            sample = ecf.fetch(as_dict=True, limit=5, order_by="chunk_start")
            for row in sample:
                print(f"   - chunk={row['chunk_start']}, file={row['file_name']}")
    except Exception as e:
        print(f"   Error: {e}")
    print()

    # --- 10. EphysChunk.SyncModel ---
    print("10. EphysChunk.SyncModel")
    try:
        sm = ephys.EphysChunk.SyncModel & {"experiment_name": EXPERIMENT_NAME}
        n_sm = len(sm)
        print(f"   SyncModel entries: {n_sm}")
        if n_sm:
            # Don't fetch the actual model blob — just the metadata
            sample = sm.proj("onix_ts_start", "onix_ts_end", "harp_start").fetch(
                as_dict=True, limit=3, order_by="chunk_start"
            )
            for row in sample:
                print(f"   - chunk={row['chunk_start']}, harp_start={row.get('harp_start')}, "
                      f"onix_range=[{row['onix_ts_start']}, {row['onix_ts_end']}]")
    except Exception as e:
        print(f"   Error: {e}")
    print()

    # --- 11. EphysBlock ---
    print("11. EphysBlock")
    try:
        eb = ephys.EphysBlock & {"experiment_name": EXPERIMENT_NAME}
        n_eb = len(eb)
        print(f"   EphysBlock entries: {n_eb}")
        if n_eb:
            for row in eb.fetch(as_dict=True):
                print(f"   - block: {row['block_start']} -> {row['block_end']}")
    except Exception as e:
        print(f"   Error: {e}")
    print()

    # --- 12. Spike sorting tables (quick check) ---
    from aeon.dj_pipeline import spike_sorting

    print("12. Spike sorting pipeline status")
    tables_to_check = [
        ("SortingTask", spike_sorting.SortingTask),
        ("PreProcessing", spike_sorting.PreProcessing),
        ("SpikeSorting", spike_sorting.SpikeSorting),
        ("PostProcessing", spike_sorting.PostProcessing),
        ("SortedSpikes", spike_sorting.SortedSpikes),
        ("SyncedSpikes", spike_sorting.SyncedSpikes),
        ("UnitMatching", spike_sorting.UnitMatching),
    ]
    for name, table in tables_to_check:
        try:
            n = len(table & {"experiment_name": EXPERIMENT_NAME})
            status = f"{n} entries" if n > 0 else "empty"
            print(f"   {name:20s}: {status}")
        except Exception as e:
            print(f"   {name:20s}: error — {e}")
    print()

    # --- Summary and recommendations ---
    print("=" * 70)
    print("  SUMMARY & RECOMMENDATIONS")
    print("=" * 70)

    if n_ec > 0:
        print("\n  [GOOD] EphysChunk data EXISTS on production DB.")
        print("  → We can export chunk metadata (chunk_start, chunk_end) for Works.")
        print("  → Run this script with --export to dump the data as JSON.")
    else:
        print("\n  [MISSING] No EphysChunk data on production DB.")
        print("  → We need an alternative source for chunk boundaries.")
        if n_chunks > 0:
            print(f"  → Behavioral Chunk data exists ({n_chunks} chunks) — could derive from that.")
        else:
            print("  → No behavioral chunks either — will need to compute from Dario's files or raw data.")

    if n_sm > 0:
        print(f"\n  [GOOD] SyncModel data EXISTS ({n_sm} entries).")
        print("  → We can export sync models for Works (avoids need for Clock.bin + HarpSync).")
    else:
        print("\n  [MISSING] No SyncModel data.")
        print("  → Will need to use Dario's pre-computed HARP timestamps directly.")

    # --- 13. Dario's ground truth files ---
    print("\n13. Dario's ground truth files on Ceph")
    gt_filename = "spike_index_harp_clock_binary_2_147.npy"
    sorted_root = Path(
        "/ceph/aeon/aeon/data/processed/AEONX1/social-ephys0.1/"
        "2024-06-04T10-24-07/NeuropixelsV2Beta/SpikeSortingRaw"
    )
    target_groups = [
        "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_1_144",
        "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_121_264",
        "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_241_384",
        "NeuropixelsV2Beta_ProbeA_Chunks_2_147Chs_193_240",
    ]
    for grp in target_groups:
        gt_path = sorted_root / grp / gt_filename
        if gt_path.exists():
            size_mb = gt_path.stat().st_size / 1e6
            # Load a small sample to check dtype
            arr = np.load(gt_path, mmap_mode="r")
            print(f"   {grp}:")
            print(f"     FOUND — {size_mb:.0f} MB, dtype={arr.dtype}, shape={arr.shape}")
            print(f"     First 5 values: {arr.flat[:5]}")
        else:
            print(f"   {grp}:")
            print(f"     MISSING — {gt_path}")
    print()

    print()


def export_chunk_data():
    """Export EphysChunk + SyncModel metadata as JSON for import to Works."""
    import datajoint as dj
    from aeon.dj_pipeline import ephys

    ec = ephys.EphysChunk & {"experiment_name": EXPERIMENT_NAME}
    if not ec:
        print("No EphysChunk data to export")
        return

    chunks = ec.fetch(as_dict=True, order_by="chunk_start")

    # Convert datetimes to strings for JSON serialization
    for c in chunks:
        for k, v in c.items():
            if hasattr(v, "isoformat"):
                c[k] = v.isoformat()

    output = {
        "source_db": dj.config.get("database.host", "unknown"),
        "source_prefix": dj.config["custom"].get("database.prefix", ""),
        "experiment_name": EXPERIMENT_NAME,
        "n_chunks": len(chunks),
        "chunks": chunks,
    }

    # Also export sync model metadata (without the actual model blobs — those are large)
    sm = ephys.EphysChunk.SyncModel & {"experiment_name": EXPERIMENT_NAME}
    if sm:
        sync_meta = sm.proj("onix_ts_start", "onix_ts_end", "harp_start").fetch(
            as_dict=True, order_by="chunk_start"
        )
        for s in sync_meta:
            for k, v in s.items():
                if hasattr(v, "isoformat"):
                    s[k] = v.isoformat()
        output["sync_model_count"] = len(sync_meta)
        output["sync_models_metadata"] = sync_meta
    else:
        output["sync_model_count"] = 0

    out_path = Path("prod_ephys_chunk_export.json")
    with open(out_path, "w") as f:
        json.dump(output, f, indent=2, default=str)

    print(f"Exported {len(chunks)} chunks to {out_path}")
    if sm:
        print(f"Exported {len(sync_meta)} sync model metadata entries")
    print("NOTE: Sync model blobs NOT exported (too large for JSON). "
          "Use --export-sync-models for full export with pickle.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Explore production ephys data")
    parser.add_argument(
        "--export", action="store_true", help="Export EphysChunk data as JSON"
    )
    args = parser.parse_args()

    if args.export:
        export_chunk_data()
    else:
        main()
