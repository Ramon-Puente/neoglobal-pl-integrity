
import os
import pandas as pd
import datetime
import random
import string
from decimal import Decimal, getcontext

# Set precision for Decimal operations
getcontext().prec = 28

def generate_seed_data(num_records=100):
    """
    Generates a set of perfectly matching Stripe and NetSuite transaction records.
    """
    # 1. Path Handling: Read from environment variable or use default
    output_path = os.getenv('DATA_PATH_RAW', '/mnt/ssd_raw')
    print(f"--- Verification: Data will be saved to: {output_path} ---")

    # Ensure the output directory exists
    os.makedirs(output_path, exist_ok=True)

    stripe_data = []
    netsuite_data = []

    for _ in range(num_records):
        # --- Stream A (Stripe) Data ---
        charge_id = f"ch_{''.join(random.choices(string.ascii_letters + string.digits, k=24))}"
        
        # Use Decimal for financial precision
        amount = Decimal(random.uniform(50.00, 5000.00)).quantize(Decimal('0.0001'))
        
        stripe_created_utc = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(
            days=random.randint(1, 10),
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59)
        )

        stripe_record = {
            'charge_id': charge_id,
            'amount': amount,
            'currency': 'USD',
            'created_utc': stripe_created_utc.isoformat()
        }
        stripe_data.append(stripe_record)

        # --- Stream B (NetSuite) Data ---
        netsuite_lag = datetime.timedelta(
            hours=random.randint(1, 4),
            minutes=random.randint(0, 59)
        )
        netsuite_created_utc = stripe_created_utc + netsuite_lag

        netsuite_record = {
            'external_id': charge_id, # Matches Stripe charge_id
            'account_code': 4000,
            'credit_usd': amount,     # Matches Stripe amount
            'debit_usd': Decimal('0.0000'),
            'memo': f"Stripe: {charge_id}",
            'created_utc': netsuite_created_utc.isoformat()
        }
        netsuite_data.append(netsuite_record)

    # Create pandas DataFrames
    stripe_df = pd.DataFrame(stripe_data)
    netsuite_df = pd.DataFrame(netsuite_data)

    # 3. Output to CSV
    stripe_df.to_csv(os.path.join(output_path, 'stripe_raw.csv'), index=False)
    netsuite_df.to_csv(os.path.join(output_path, 'netsuite_raw.csv'), index=False)
    
    print(f"Successfully generated {len(stripe_df)} Stripe records.")
    print(f"Successfully generated {len(netsuite_df)} NetSuite records.")
    print("--- Seed data generation complete. ---")


if __name__ == "__main__":
    generate_seed_data()
