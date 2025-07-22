import pandas as pd
import logging

# Configure logging for this module
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)




import requests
import time

def call_ivol_api_and_add_to_df(df, as_of_date="2025-07-21", scheme="European", model="BSM", sleep_between=0.1):
    df = df.copy()
    ivols = []

    base_url = "https://options-api.mosaic.hartreepartners.com/options/api/v1/getIVol"

    for idx, row in df.iterrows():
        try:
            expiration_date = pd.to_datetime(row["option_expiry"]).strftime("%Y-%m-%d")
            strike = float(row["strike"])
            parity = str(row["option_type"]).capitalize()
            future_value = float(row["future_value"])
            value = float(row["value"])
            rf_rate = float(row["rf_rate"])

            url = f"{base_url}/{as_of_date}/{expiration_date}/{strike}/{parity}/{future_value}/{value}/{rf_rate}"
            params = {
                "scheme": scheme,
                "model": model
            }

            response = requests.get(url, params=params)
            response.raise_for_status()
            ivol = float(response.text)
            ivols.append(ivol)

        except Exception as e:
            logger.warning(f"Row {idx} failed: {e}")
            ivols.append(None)  # fallback for error
        time.sleep(sleep_between)  # prevent API overload

    df["computed_ivol"] = ivols
    return df






def transform_to_option_api_payloads(df, as_of_date="2025-07-21", scheme="American", model="BSM", output_csv="option_price_results.csv"):
    logger.info("Starting transformation of DataFrame to option API payloads")
    logger.info(f"Input DataFrame shape: {df.shape}")
    logger.info(f"Parameters - as_of_date: {as_of_date}, scheme: {scheme}, model: {model}")
    
    df = df.copy()
    logger.debug("Created copy of input DataFrame")
    
    # Log initial data quality check
    initial_rows = len(df)
    null_future_value_count = df["future_value"].isnull().sum()
    logger.info(f"Initial rows: {initial_rows}")
    logger.info(f"Rows with null future_value: {null_future_value_count}")
    
    df = df[df["future_value"].notnull()]
    rows_after_filter = len(df)
    logger.info(f"Rows after filtering null future_value: {rows_after_filter}")
    
    if rows_after_filter == 0:
        logger.warning("No rows remaining after filtering null future_value!")
        return [], df
    
    null_rf_rate_count = df["rf_rate"].isnull().sum()
    zero_rf_rate_count = (df["rf_rate"] == 0.0).sum()
    logger.info(f"Rows with null rf_rate: {null_rf_rate_count}")
    logger.info(f"Rows with zero rf_rate: {zero_rf_rate_count}")
    
    df["rf_rate"] = df["rf_rate"].fillna(0.0434)
    df.loc[df["rf_rate"] == 0.0, "rf_rate"] = 0.0434
    replaced_rf_rate_count = null_rf_rate_count + zero_rf_rate_count
    if replaced_rf_rate_count > 0:
        logger.info(f"Replaced {replaced_rf_rate_count} null/zero rf_rate values with 0.0434")

    payloads = []
    successful_transformations = 0
    failed_transformations = 0

    logger.info(f"Starting row-by-row transformation for {len(df)} rows")
    
    for idx, row in df.iterrows():
        try:
            logger.debug(f"Processing row {idx} - exposure: {row.get('exposure', 'N/A')}")
            payload = {
                "as_of_date": as_of_date,
                "expiration_date": str(row["option_expiry"]),
                "strike": float(row["strike"]),
                "parity": str(row["option_type"]).capitalize(),
                "future_value": float(row["future_value"]),
                "ivol": float(row["ivol"]),
                "rf_rate": float(row["rf_rate"]),
                "scheme": scheme,
                "model": model,
                "exposure": row["exposure"]
            }

            payloads.append(payload)
            successful_transformations += 1
        except Exception as e:
            logger.error(f"Failed to transform row {idx} (exposure: {row.get('exposure', 'N/A')}): {str(e)}")
            logger.error(f"Row data: {dict(row)}")
            failed_transformations += 1
            continue

    logger.info(f"Transformation completed: {successful_transformations} successful, {failed_transformations} failed")

    # API Call Phase: Compute Option Price
    logger.info("Starting option pricing API calls...")
    base_url = "https://options-api.mosaic.hartreepartners.com/options/api/v1/getPriceVanilla"
    computed_values = []

    for idx, p in enumerate(payloads):
        try:
            url = f"{base_url}/{p['as_of_date']}/{p['expiration_date']}/{p['strike']}/{p['parity']}/{p['future_value']}/{p['ivol']}/{p['rf_rate']}"
            params = {
                "scheme": p["scheme"],
                "model": p["model"]
            }
            response = requests.get(url, params=params)
            response.raise_for_status()
            price = float(response.text)
            computed_values.append(price)
            logger.debug(f"Row {idx}: Option price = {price}")
        except Exception as e:
            logger.warning(f"Row {idx}: Failed to get price: {str(e)}")
            computed_values.append(None)
        time.sleep(0.1)  # Throttle requests

    df["computed_value"] = computed_values

    # Save to CSV
    df.to_csv(output_csv, index=False)
    logger.info(f"Saved results with computed option prices to {output_csv}")
    
    return payloads, df
