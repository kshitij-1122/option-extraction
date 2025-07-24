import pandas as pd

csv_path = r"C:\Users\ktandon\OneDrive - Hartree Partners\Desktop\Options_testing\aggregated_valuations_202507241548.csv"

def read_csv():
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

if __name__ == "__main__":
    read_csv()
