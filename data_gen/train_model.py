import os
import pandas as pd
from sdv.lite import SingleTablePreset
from sdv.metadata import SingleTableMetadata

def train_generative_models():
    """
    Loads seed data, trains separate generative models for Stripe and NetSuite data,
    and saves the model artifacts.
    """
    # 1. Setup: Define paths and create artifact directory
    base_path = os.getenv('DATA_PATH_RAW', '/mnt/ssd_raw')
    model_output_path = os.path.join(base_path, 'models')
    
    print("--- AI Model Training Script ---")
    print(f"Reading data from: {base_path}")
    print(f"Saving models to: {model_output_path}")
    
    os.makedirs(model_output_path, exist_ok=True)

    stripe_csv_path = os.path.join(base_path, 'stripe_raw.csv')
    netsuite_csv_path = os.path.join(base_path, 'netsuite_raw.csv')

    # --- Model A (Stripe) ---
    print("\n--- Training Stripe Model ---")
    try:
        stripe_df = pd.read_csv(stripe_csv_path)
        print("Loaded stripe_raw.csv successfully.")

        # Define metadata to specify the primary key
        stripe_metadata = SingleTableMetadata()
        stripe_metadata.detect_from_dataframe(data=stripe_df)
        stripe_metadata.update_column(
            column_name='charge_id',
            sdtype='id'
        )
        stripe_metadata.set_primary_key(column_name='charge_id')
        
        stripe_model = SingleTablePreset(metadata=stripe_metadata, name='FAST_ML')
        
        print("Training Stripe model...")
        stripe_model.fit(data=stripe_df)
        
        stripe_model_save_path = os.path.join(model_output_path, 'stripe_model.pkl')
        stripe_model.save(filepath=stripe_model_save_path)
        print(f"Stripe model saved successfully to {stripe_model_save_path}")

    except FileNotFoundError:
        print(f"Error: Could not find {stripe_csv_path}. Please generate seed data first.")
        return

    # --- Model B (NetSuite) ---
    print("\n--- Training NetSuite Model ---")
    try:
        netsuite_df = pd.read_csv(netsuite_csv_path)
        print("Loaded netsuite_raw.csv successfully.")

        # Define metadata for the NetSuite table
        netsuite_metadata = SingleTableMetadata()
        netsuite_metadata.detect_from_dataframe(data=netsuite_df)
        netsuite_metadata.update_column(
            column_name='external_id',
            sdtype='id'
        )
        netsuite_metadata.set_primary_key(column_name='external_id')

        netsuite_model = SingleTablePreset(metadata=netsuite_metadata, name='FAST_ML')

        print("Training NetSuite model...")
        netsuite_model.fit(data=netsuite_df)

        netsuite_model_save_path = os.path.join(model_output_path, 'netsuite_model.pkl')
        netsuite_model.save(filepath=netsuite_model_save_path)
        print(f"NetSuite model saved successfully to {netsuite_model_save_path}")

    except FileNotFoundError:
        print(f"Error: Could not find {netsuite_csv_path}. Please generate seed data first.")

    print("\n--- Model training complete. ---")


if __name__ == "__main__":
    train_generative_models()
