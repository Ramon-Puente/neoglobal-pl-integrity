
import pandas as pd
import numpy as np
from pathlib import Path
import time
import shutil
import sys
from sdv.lite import SingleTablePreset
from decimal import Decimal, getcontext

# Set precision for Decimal operations
getcontext().prec = 19

def main():
    """
    Main function to load trained models, generate bulk synthetic data in batches,
    introduce distributed anomalies, and save the data as partitioned Parquet files.
    """
    # --- 1. Configuration ---
    # Check for interactive mode
    is_interactive = '--interactive' in sys.argv
    
    NUM_ROWS = 1_500_000
    BATCH_SIZE = 150_000
    NUM_BATCHES = NUM_ROWS // BATCH_SIZE
    
    ANOMALY_RATE = 0.001
    NUM_ANOMALIES = int(NUM_ROWS * ANOMALY_RATE)
    NUM_DELETIONS = 500
    NUM_MODIFICATIONS = NUM_ANOMALIES - NUM_DELETIONS

    BASE_PATH = Path("/mnt/ssd_raw")
    MODEL_PATH = BASE_PATH / "models"
    STRIPE_MODEL_PATH = MODEL_PATH / "stripe_model.pkl"
    NETSUITE_MODEL_PATH = MODEL_PATH / "netsuite_model.pkl"

    STRIPE_OUTPUT_DIR = BASE_PATH / "stripe_bulk"
    NETSUITE_OUTPUT_DIR = BASE_PATH / "netsuite_bulk"

    print("--- Phase 5 (Batched): High-Density Synthesis ---")
    if is_interactive:
        print(">>> RUNNING IN INTERACTIVE MODE <<<")
    print(f"Total rows to generate per system: {NUM_ROWS:,}")
    print(f"Batch Size: {BATCH_SIZE:,}, Number of Batches: {NUM_BATCHES}")
    print(f"Output Directories: {STRIPE_OUTPUT_DIR}, {NETSUITE_OUTPUT_DIR}")
    
    # --- 2. Setup Output Directories ---
    print("\n[1/5] Preparing output directories...")
    for dir_path in [STRIPE_OUTPUT_DIR, NETSUITE_OUTPUT_DIR]:
        if dir_path.exists():
            shutil.rmtree(dir_path)
            print(f"   - Removed existing directory: {dir_path}")
        dir_path.mkdir(parents=True)
        print(f"   - Created directory: {dir_path}")

    # --- 3. Load Trained Models ---
    print(f"\n[2/5] Loading models from {MODEL_PATH}...")
    start_time = time.time()
    try:
        stripe_synthesizer = SingleTablePreset.load(STRIPE_MODEL_PATH)
        netsuite_synthesizer = SingleTablePreset.load(NETSUITE_MODEL_PATH)
        print(f"   ...Models loaded successfully in {time.time() - start_time:.2f} seconds.")
    except FileNotFoundError as e:
        print(f"   [FATAL ERROR] Could not load models: {e}. Please ensure Phase 4 was completed.")
        return

    # --- 4. Pre-calculate Anomaly Distribution ---
    print("\n[3/5] Pre-calculating anomaly distribution...")
    all_indices = np.arange(NUM_ROWS)
    np.random.shuffle(all_indices)
    
    anomaly_indices = all_indices[:NUM_ANOMALIES]
    deletion_indices = set(anomaly_indices[:NUM_DELETIONS])
    modification_indices = set(anomaly_indices[NUM_DELETIONS:])
    print(f"   - Total anomalies planned: {len(deletion_indices) + len(modification_indices):,}")

    # --- 5. Main Batch Generation Loop ---
    print("\n[4/5] Starting batch generation loop...")
    total_start_time = time.time()

    for i in range(NUM_BATCHES):
        batch_start_time = time.time()
        print(f"\n   - Generating Batch {i+1} of {NUM_BATCHES}...")
        
        # --- Generate Data ---
        stripe_batch = stripe_synthesizer.sample(num_rows=BATCH_SIZE)
        netsuite_batch = netsuite_synthesizer.sample(num_rows=BATCH_SIZE)

        # --- Link Data & Enforce Constraints ---
        # The models should have learned the relationship, but we enforce it for perfect matching.
        stripe_batch.reset_index(drop=True, inplace=True)
        netsuite_batch.reset_index(drop=True, inplace=True)
        netsuite_batch['external_id'] = stripe_batch['charge_id']
        
        # Enforce DECIMAL(19, 4) by converting to Decimal objects
        # The underlying Parquet writer (pyarrow) will handle this correctly.
        stripe_batch['amount'] = stripe_batch['amount'].apply(lambda x: Decimal(f'{x:.4f}'))
        # ENFORCE AMOUNT MATCH FOR NON-ANOMALOUS ROWS
        # Make netsuite_amount equal to stripe_amount before introducing anomalies
        netsuite_batch['credit_usd'] = stripe_batch['amount'].apply(lambda x: Decimal(f'{x:.4f}'))
        netsuite_batch['debit_usd'] = Decimal('0.0000') # Ensure debit is zero
        
        # Enforce datetime types
        stripe_batch['created'] = pd.to_datetime(stripe_batch['created_utc'])
        netsuite_batch['transaction_date'] = pd.to_datetime(netsuite_batch['created_utc'])

        # --- Apply Anomalies for Current Batch ---
        batch_start_index = i * BATCH_SIZE
        batch_end_index = batch_start_index + BATCH_SIZE
        
        # Modifications
        mods_in_batch = [idx for idx in modification_indices if batch_start_index <= idx < batch_end_index]
        if mods_in_batch:
            local_indices = [idx - batch_start_index for idx in mods_in_batch]
            changes = np.random.choice([Decimal('-0.01'), Decimal('0.01')], size=len(local_indices))
            netsuite_batch.loc[local_indices, 'credit_usd'] += changes

        # Deletions (applied after mods to preserve indices)
        dels_in_batch = [idx for idx in deletion_indices if batch_start_index <= idx < batch_end_index]
        if dels_in_batch:
            local_indices_to_drop = [idx - batch_start_index for idx in dels_in_batch]
            netsuite_batch.drop(index=local_indices_to_drop, inplace=True)

        # --- Save Batch to Partitioned Parquet ---
        stripe_batch.to_parquet(STRIPE_OUTPUT_DIR / f"part-{i}.parquet")
        netsuite_batch.to_parquet(NETSUITE_OUTPUT_DIR / f"part-{i}.parquet")
        
        batch_time = time.time() - batch_start_time
        print(f"   ...Batch {i+1} of {10} complete. ({batch_time:.2f} seconds)")

        # --- Interactive Pause ---
        if is_interactive and (i + 1) < NUM_BATCHES:
            print("\n   --- PAUSED ---")
            print(f"   Verify the output in: {STRIPE_OUTPUT_DIR} and {NETSUITE_OUTPUT_DIR}")
            try:
                input("   Press Enter in the script's terminal to continue to the next batch, or Ctrl+C to stop...")
            except KeyboardInterrupt:
                print("\n\nExiting synthesis process as requested.")
                return

    print(f"\n[5/5] Batch generation complete.")
    print(f"   - Total execution time: {(time.time() - total_start_time) / 60:.2f} minutes.")
    print(f"   - Stripe data saved in: {STRIPE_OUTPUT_DIR}")
    print(f"   - NetSuite data saved in: {NETSUITE_OUTPUT_DIR}")
    print("\n--- Batched Synthesis Complete ---")

if __name__ == "__main__":
    main()
