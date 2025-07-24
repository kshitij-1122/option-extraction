import pandas as pd

def read_mapping_csv():
    filepath = r"C:\Users\ktandon\OneDrive - Hartree Partners\Desktop\Options_testing\mapping.csv"
    try:
        df = pd.read_csv(filepath)
        print(f"✅ Successfully loaded mapping.csv — shape: {df.shape}")
        return df
    except Exception as e:
        print(f"❌ Error reading mapping.csv: {e}")
        return pd.DataFrame()
    


import pandas as pd
from connections import connect_crate_db

def fetch_expiry_data_with_exchange(df, start_date="2025-07-21", end_date="2025-12-31"):
    """
    For each unique opt_symbol in the input DataFrame, dynamically query CrateDB for instrument_key matches
    and attach the associated exchange from the original df to the resulting CrateDB rows.

    Parameters:
        df (pd.DataFrame): Input DataFrame with at least 'opt_symbol' and 'exchange' columns.
        start_date (str): Start date for expiration filter (default: '2025-07-21').
        end_date (str): End date for expiration filter (default: '2025-12-31').
        always_fresh (bool): If True, always return a new DataFrame (even if no results). Default: True.

    Returns:
        pd.DataFrame: Combined result from CrateDB queries with exchange mapped back in, or a fresh DataFrame if no results.
    """
    always_fresh = True  # Set to True to always return a new DataFrame if no results

    if "opt_symbol" not in df.columns or "exchange" not in df.columns:
        raise ValueError("Input DataFrame must contain 'opt_symbol' and 'exchange' columns.")

    unique_mappings = df.drop_duplicates(subset=["opt_symbol", "exchange"])
    results = []

    conn = connect_crate_db()
    try:
        for _, row in unique_mappings.iterrows():
            opt_symbol = row["opt_symbol"]
            exchange = row["exchange"]

            like_pattern = f"{opt_symbol} ______ P%"

            query = f"""
            SELECT DISTINCT 
                properties['UnderlyingInstrument']['instrument_key'] AS future_key,
                properties['UnderlyingInstrument']['ExpirationDate'] AS future_expiry,
                properties['ExpirationDate'] AS option_expiry
            FROM settles.instruments
            WHERE instrument_key LIKE '{like_pattern}'
                AND properties['ExpirationDate'] BETWEEN '{start_date}' AND '{end_date}'
                AND properties['UnderlyingInstrument']['ExpirationDate'] IS NOT NULL
            ORDER BY option_expiry
                """

            try:
                sub_df = pd.read_sql(query, conn)
                sub_df["exchange"] = exchange
                results.append(sub_df)
            except Exception as e:
                print(f"❌ Failed to fetch for symbol {opt_symbol}: {e}")
    finally:
        conn.dispose()

    required_columns = [
        "opt_symbol", "future_key", "future_expiry", "option_expiry",
        "scheme", "tempest_code", "commodity_code", "exchange"
    ]

    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_csv = f"expiry_data_output_{timestamp}.csv"

    if results:
        combined = pd.concat(results, ignore_index=True)
        # Add missing columns from the input DataFrame if available
        for col in ["opt_symbol", "scheme", "tempest_code", "commodity_code"]:
            if col in df.columns and col not in combined.columns:
                # Map from unique_mappings to combined using exchange as key if possible
                mapping = df.set_index("exchange")[col].to_dict()
                combined[col] = combined["exchange"].map(mapping)
            elif col not in combined.columns:
                combined[col] = None
        # Ensure all required columns are present
        for col in required_columns:
            if col not in combined.columns:
                combined[col] = None
        combined = combined[required_columns]
        combined.to_csv(output_csv, index=False)
        print(f"✅ Exported expiry data to {output_csv} (shape: {combined.shape})")
        return combined
    else:
        print("⚠️ No results returned from CrateDB. Returning a fresh DataFrame.")
        empty_df = pd.DataFrame(columns=required_columns)
        empty_df.to_csv(output_csv, index=False)
        print(f"⚠️ Exported empty expiry data to {output_csv}")
        return empty_df
    


if __name__ == "__main__":
    # Example usage
    df = read_mapping_csv()
    if not df.empty:
        expiry_data = fetch_expiry_data_with_exchange(df)
        print(f"Fetched expiry data shape: {expiry_data.shape}")
    else:
        print("No mapping data available to fetch expiry data.")
