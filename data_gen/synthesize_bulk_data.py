
import pandas as pd
import numpy as np
from pathlib import Path
import time
from sdv.lite import SingleTablePreset

def main():
    """
    Main function to load trained models, generate bulk synthetic data,
    introduce anomalies, and save the data in Parquet format.
    """
    # --- Configuration ---
    NUM_ROWS = 1_500_000
    ANOMALY_RATE = 0.001
    NUM_ANOMALIES = int(NUM_ROWS * ANOMALY_RATE)
    NUM_DELETIONS = 500
    NUM_MODIFICATIONS = NUM_ANOMALIES - NUM_DELETIONS

    BASE_PATH = Path("/mnt/ssd_raw")
    MODEL_PATH = BASE_PATH / "models"
    STRIPE_MODEL_PATH = MODEL_PATH / "stripe_model.pkl"
    NETSUITE_MODEL_PATH = MODEL_PATH / "netsuite_model.pkl"

    print("--- Phase 5: High-Density Synthesis ---")
    print(f"Number of rows to generate per system: {NUM_ROWS:,}")
    print(f"Anomaly Rate: {ANOMALY_RATE:%}")
    print(f"Total Anomalous NetSuite Records: {NUM_ANOMALIES:,}")
    print(f" - Rows to delete: {NUM_DELETIONS:,}")
    print(f" - Rows to modify (amount): {NUM_MODIFICATIONS:,}")
    
    # --- 1. Load Trained Models ---
    print(f"\n[1/5] Loading models from {MODEL_PATH}...")
    start_time = time.time()
    try:
        stripe_synthesizer = SingleTablePreset.load(STRIPE_MODEL_PATH)
        netsuite_synthesizer = SingleTablePreset.load(NETSUITE_MODEL_PATH)
        print(f"   ...Models loaded successfully in {time.time() - start_time:.2f} seconds.")
    except FileNotFoundError:
        print(f"   [ERROR] Models not found. Make sure '{STRIPE_MODEL_PATH}' and '{NETSUITE_MODEL_PATH}' exist.")
        return

    # --- 2. Generate Bulk Data ---
    # We generate Stripe data first, then use its transaction IDs for NetSuite to ensure they match.
    print(f"\n[2/5] Generating {NUM_ROWS:,} synthetic Stripe records...")
    start_time = time.time()
    stripe_bulk_data = stripe_synthesizer.sample(num_rows=NUM_ROWS)
    print(f"   ...Stripe data generated in {time.time() - start_time:.2f} seconds.")

    # Ensure a common key exists for matching. We'll use 'charge_id' from Stripe.
    if 'charge_id' not in stripe_bulk_data.columns:
        print("[ERROR] 'charge_id' not found in generated Stripe data. Cannot create matching NetSuite data.")
        return
        
    print(f"\n[3/5] Generating {NUM_ROWS:,} matching synthetic NetSuite records...")
    start_time = time.time()
    # To create a perfect match, we sample and then enforce the primary key relationship.
    netsuite_bulk_data = netsuite_synthesizer.sample(num_rows=NUM_ROWS)
    netsuite_bulk_data['external_id'] = stripe_bulk_data['charge_id']
    # Re-align columns for consistency
    if 'charge_id' in netsuite_bulk_data.columns and 'external_id' in netsuite_bulk_data.columns:
         netsuite_bulk_data = netsuite_bulk_data.drop(columns=['charge_id'])
    
    print(f"   ...NetSuite data generated in {time.time() - start_time:.2f} seconds.")

    # --- 4. Introduce Anomalies into NetSuite Data ---
    print("\n[4/5] Introducing anomalies into NetSuite data...")
    start_time = time.time()

    # Get a random sample of indices for anomalies
    anomaly_indices = np.random.choice(netsuite_bulk_data.index, NUM_ANOMALIES, replace=False)
    
    # Indices for deletion
    deletion_indices = anomaly_indices[:NUM_DELETIONS]
    
    # Indices for modification (ensure they are not the same as deleted ones)
    modification_indices = anomaly_indices[NUM_DELETIONS:]

    # 4a. Delete 500 NetSuite entries
    netsuite_bulk_data.drop(index=deletion_indices, inplace=True)
    print(f"   - Deleted {len(deletion_indices)} rows.")

    # 4b. Change amounts on 1,000 NetSuite entries by +/- $0.01
    # Create an array of random +/- 0.01 changes
    changes = np.random.choice([-0.01, 0.01], size=len(modification_indices))
    netsuite_bulk_data.loc[modification_indices, 'amount'] += changes
    print(f"   - Modified amounts for {len(modification_indices)} rows.")
    
    print(f"   ...Anomalies introduced in {time.time() - start_time:.2f} seconds.")

    # --- 5. Save to Parquet ---
    print("\n[5/5] Saving final bulk data to Parquet format...")
    start_time = time.time()

    stripe_output_path = BASE_PATH / "stripe_bulk.parquet"
    netsuite_output_path = BASE_PATH / "netsuite_bulk.parquet"

    stripe_bulk_data.to_parquet(stripe_output_path)
    netsuite_bulk_data.to_parquet(netsuite_output_path)

    print(f"   - Stripe data saved to: {stripe_output_path}")
    print(f"   - NetSuite data saved to: {netsuite_output_path}")
    print(f"   ...Files saved in {time.time() - start_time:.2f} seconds.")
    print("\n--- Synthesis Complete ---")

if __name__ == "__main__":
    main()
