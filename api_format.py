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
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def call_ivol_api_and_add_to_df(df, as_of_date="2025-07-21", scheme="American", model="BSM", sleep_between=0.1):
    df = df.copy()
    ivols = []

    logger.info("STEP: Cleaning rf_rate column")

    # Step 1: Clean rf_rate
    null_rf_rate_count = df["rf_rate"].isnull().sum()
    zero_rf_rate_count = (df["rf_rate"] == 0.0).sum()
    logger.info(f"Rows with null rf_rate: {null_rf_rate_count}")
    logger.info(f"Rows with zero rf_rate: {zero_rf_rate_count}")

    df["rf_rate"] = df["rf_rate"].fillna(0.0434)
    df.loc[df["rf_rate"] == 0.0, "rf_rate"] = 0.0434

    replaced_rf_rate_count = null_rf_rate_count + zero_rf_rate_count
    if replaced_rf_rate_count > 0:
        logger.info(f"Replaced {replaced_rf_rate_count} null/zero rf_rate values with 0.0434")

    logger.info("STEP: Calling getIVol API row-by-row")

    base_url = "https://options-api.mosaic.hartreepartners.com/options/api/v1/getIVol"

    for idx, row in df.iterrows():
        try:
            # Check for required fields
            required_fields = ["option_expiry", "strike", "option_type", "future_value", "market_price", "rf_rate"]
            missing_fields = [f for f in required_fields if pd.isnull(row.get(f))]

            if missing_fields:
                logger.warning(f"Row {idx} skipped due to missing fields: {missing_fields}")
                ivols.append(None)
                continue

            # Format expiration date as Y-M-D (no leading zeros)
            expiry = pd.to_datetime(row["option_expiry"])
            expiration_date = f"{expiry.year}-{expiry.month}-{expiry.day}"

            # Prepare inputs
            strike = float(row["strike"])
            parity = str(row["option_type"]).capitalize()
            future_value = float(row["future_value"])
            value = float(row["market_price"])
            rf_rate = float(row["rf_rate"])

            # Construct URL
            url = f"{base_url}/{as_of_date}/{expiration_date}/{strike}/{parity}/{future_value}/{value}/{rf_rate}"
            params = {
                "scheme": scheme,
                "model": model
            }

            logger.debug(f"Row {idx} → URL: {url} with params {params}")

            # Make the request (SSL verification disabled for internal certs)
            response = requests.get(url, params=params, verify=False)
            response.raise_for_status()

            # Parse response
            ivol = float(response.text)
            ivols.append(ivol)

        except Exception as e:
            logger.warning(f"Row {idx} failed: {e}")
            ivols.append(None)

        time.sleep(sleep_between)

    df["computed_ivol"] = ivols
    return df




import pandas as pd
import requests
import time
import logging

logger = logging.getLogger(__name__)

def transform_to_option_api_payloads(df, as_of_date="2025-07-21", scheme="American", model="BSM", output_csv="option_price_results_American.csv"):
    logger.info("Starting transformation of DataFrame to option API payloads")
    logger.info(f"Input DataFrame shape: {df.shape}")
    logger.info(f"Parameters - as_of_date: {as_of_date}, scheme: {scheme}, model: {model}")
    
    df = df.copy()
    
    # Filter out rows with missing future_value
    null_future_value_count = df["future_value"].isnull().sum()
    logger.info(f"Rows with null future_value: {null_future_value_count}")
    df = df[df["future_value"].notnull()]
    
    if df.empty:
        logger.warning("No rows remaining after filtering null future_value!")
        return [], df
    
    # Replace missing or zero rf_rate
    null_rf_rate_count = df["rf_rate"].isnull().sum()
    zero_rf_rate_count = (df["rf_rate"] == 0.0).sum()
    df["rf_rate"] = df["rf_rate"].fillna(0.0434)
    df.loc[df["rf_rate"] == 0.0, "rf_rate"] = 0.0434
    logger.info(f"Replaced {null_rf_rate_count + zero_rf_rate_count} null/zero rf_rate values with 0.0434")
    
    payloads = []
    successful_transformations = 0
    failed_transformations = 0

    logger.info(f"Starting row-by-row payload creation for {len(df)} rows")

    for idx, row in df.iterrows():
        try:
            # Validate required fields
            required_fields = ["option_expiry", "strike", "option_type", "future_value", "computed_ivol", "rf_rate"]
            missing_fields = [f for f in required_fields if pd.isnull(row.get(f))]
            if missing_fields:
                logger.warning(f"Row {idx} skipped due to missing fields: {missing_fields}")
                failed_transformations += 1
                continue

            # Format expiration date as YYYY-M-D
            expiry = pd.to_datetime(row["option_expiry"])
            expiration_date = f"{expiry.year}-{expiry.month}-{expiry.day}"

            payload = {
                "as_of_date": as_of_date,
                "expiration_date": expiration_date,
                "strike": float(row["strike"]),
                "parity": str(row["option_type"]).capitalize(),
                "future_value": float(row["future_value"]),
                "ivol": float(row["computed_ivol"]),
                "rf_rate": float(row["rf_rate"]),
                "scheme": scheme,
                "model": model,
                "exposure": row.get("exposure")
            }

            payloads.append(payload)
            successful_transformations += 1
        except Exception as e:
            logger.error(f"Failed to transform row {idx}: {str(e)}")
            failed_transformations += 1

    logger.info(f"Payload creation completed: {successful_transformations} successful, {failed_transformations} failed")
    
    # Step 2: Call API for each payload
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
            response = requests.get(url, params=params, verify=False)
            response.raise_for_status()

            # ✅ FIXED: parse JSON and extract price
            response_json = response.json()
            price = float(response_json["price"])

            computed_values.append(price)
            logger.debug(f"Row {idx}: Option price = {price}")

        except Exception as e:
            logger.warning(f"Row {idx}: Failed to get price: {str(e)}")
            computed_values.append(None)

        time.sleep(0.1)  # Throttle

    df = df.iloc[:len(computed_values)].copy()
    df["computed_value"] = computed_values

    # Save to CSV
    df.to_csv(output_csv, index=False)
    logger.info(f"Saved results with computed option prices to {output_csv}")

    return payloads, df
