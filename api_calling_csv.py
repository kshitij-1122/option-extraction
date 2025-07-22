import requests
import pandas as pd
import logging

# Configure logging for this module
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def call_option_api(payloads):
    logger.info(f"Starting API calls for {len(payloads)} payloads")
    base_url = "https://options-api.mosaic.hartreepartners.com/options/api/v1/getPriceVanilla"
    results = []
    
    successful_calls = 0
    failed_calls = 0

    for i, p in enumerate(payloads):
        logger.info(f"Processing payload {i+1}/{len(payloads)} - exposure: {p.get('exposure', 'N/A')}")
        
        url = f"{base_url}/{p['as_of_date']}/{p['expiration_date']}/{p['strike']}/{p['parity']}/{p['future_value']}/{p['ivol']}/{p['rf_rate']}"
        params = {
            "scheme": p["scheme"],
            "model": p["model"]
        }
        
        logger.debug(f"API URL: {url}")
        logger.debug(f"API params: {params}")

        try:
            logger.info(f"Making API request for exposure {p.get('exposure', 'N/A')}")
            response = requests.get(url, params=params)
            response.raise_for_status()
            
            result_data = response.json()
            result_data["exposure"] = p["exposure"]
            results.append(result_data)
            successful_calls += 1
            
            logger.info(f"Successfully received API response for exposure {p['exposure']}")
            logger.debug(f"Response data keys: {list(result_data.keys())}")
            
        except requests.exceptions.RequestException as e:
            logger.error(f"API request failed for exposure {p.get('exposure', 'N/A')}: {str(e)}")
            logger.error(f"URL: {url}")
            logger.error(f"Params: {params}")
            
            error_result = {
                "error": str(e),
                "exposure": p["exposure"]
            }
            results.append(error_result)
            failed_calls += 1
        except Exception as e:
            logger.error(f"Unexpected error for exposure {p.get('exposure', 'N/A')}: {str(e)}")
            error_result = {
                "error": str(e),
                "exposure": p["exposure"]
            }
            results.append(error_result)
            failed_calls += 1

    logger.info(f"API calling completed. Successful: {successful_calls}, Failed: {failed_calls}")
    return results


def merge_and_export_results(df, api_results, output_file="option_api_results.csv"):
    logger.info(f"Starting merge and export process")
    logger.info(f"Input DataFrame shape: {df.shape}")
    logger.info(f"API results count: {len(api_results)}")
    
    try:
        results_df = pd.DataFrame(api_results)
        logger.info(f"Created results DataFrame with shape: {results_df.shape}")
        logger.info(f"Results DataFrame columns: {list(results_df.columns)}")

        # Check for errors in API results
        error_count = results_df['error'].notna().sum() if 'error' in results_df.columns else 0
        logger.info(f"API results with errors: {error_count}")

        # Merge on exposure
        logger.info("Performing merge on 'exposure' column")
        merged = df.merge(results_df, on="exposure", how="left")
        logger.info(f"Merged DataFrame shape: {merged.shape}")
        logger.info(f"Merged DataFrame columns: {list(merged.columns)}")

        # Export to CSV
        logger.info(f"Exporting results to: {output_file}")
        merged.to_csv(output_file, index=False)
        
        logger.info(f"Successfully saved {len(merged)} results to {output_file}")
        print(f"Saved {len(merged)} results to {output_file}")
        
        return merged
        
    except Exception as e:
        logger.error(f"Error in merge and export process: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        raise

