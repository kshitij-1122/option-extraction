import pandas as pd

def read_option_price_results_european():
    """
    Reads the option_price_results_European.csv file and returns the DataFrame.
    """
    csv_path = r"C:\Users\ktandon\OneDrive - Hartree Partners\Desktop\Options_testing\option_price_results_European.csv"
    try:
        df = pd.read_csv(csv_path)
        print(f"✅ Successfully loaded: {csv_path}")
        print(f"Shape: {df.shape}")
        print(f"Columns: {list(df.columns)}")
        print(df.head())
        return df
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return None




def generate_opt_symbol_column(df):
    """
    Creates a new column 'opt_symbol_code' by combining:
    crate_ticks, ym_key, first letter of option_type, and strike (no space before strike).
    
    Example output: 'TFO 202508 P25'
    """
    df = df.copy()
    
    df["opt_symbol_code"] = (
        df["crate_ticks"].astype(str).str.strip() + " " +
        df["ym_key"].astype(str).str.strip() + " " +
        df["option_type"].astype(str).str.upper().str[0] +
        df["strike"].astype(str).str.replace(".0", "", regex=False)  # Remove ".0" if int
    )
    
    return df


import pandas as pd
from connections import connect_crate_db

def fetch_settlements_for_symbols(df, trade_date="2025-07-21"):
    """
    Fetch settlement prices from CrateDB for each opt_symbol_code and source in the DataFrame.

    Parameters:
        df (pd.DataFrame): Input DataFrame with columns ['opt_symbol_code', 'source']
        trade_date (str): Date for which to pull settlements (YYYY-MM-DD)

    Returns:
        pd.DataFrame: Combined settlement results from CrateDB
    """
    if "opt_symbol_code" not in df.columns or "source" not in df.columns:
        raise ValueError("DataFrame must contain 'opt_symbol_code' and 'source' columns")

    conn = connect_crate_db()
    results = []

    try:
        for idx, row in df.iterrows():
            opt_code = row["opt_symbol_code"]
            source = row["source"]

            if pd.isnull(opt_code) or pd.isnull(source):
                continue

            # Exact match query
            query = f"""
                SELECT *
                FROM settles."values"
                WHERE instrument_key = '{opt_code}'
                  AND field = 'Price'
                  AND label = 'Settlement'
                  AND source = '{source}'
                  AND date = '{trade_date}'
                ORDER BY date DESC
            """

            try:
                sub_df = pd.read_sql(query, conn)
                sub_df["opt_symbol_code"] = opt_code
                sub_df["source"] = source
                results.append(sub_df)
            except Exception as e:
                print(f"❌ Failed for {opt_code} ({source}): {e}")
    finally:
        conn.dispose()

    # Export results to Excel
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_excel = f"settlements_output_{timestamp}.xlsx"

    if results:
        combined = pd.concat(results, ignore_index=True)
        combined.to_excel(output_excel, index=False)
        print(f"✅ Exported settlements to {output_excel} (shape: {combined.shape})")
        return combined
    else:
        print("⚠️ No results retrieved from CrateDB.")
        empty_df = pd.DataFrame()
        empty_df.to_excel(output_excel, index=False)
        print(f"⚠️ Exported empty settlements to {output_excel}")
        return empty_df

    
if __name__ == "__main__":
    # Example usage
    df = read_option_price_results_european()
    if df is not None:
        df = generate_opt_symbol_column(df)
        settlements_df = fetch_settlements_for_symbols(df)
        print(settlements_df.head())