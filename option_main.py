import pandas as pd
from connections import connect_back_office_applictions
from api_format import transform_to_option_api_payloads, call_ivol_api_and_add_to_df
from get_data import expiry_date, align_option_expiries 
import logging
from read_aggregated_valuations import read_csv

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def options_main():
    logger.info("=" * 60)
    logger.info("STARTING OPTIONS MAIN WORKFLOW")
    logger.info("=" * 60)

    try:
        # Step 1: Get data
        logger.info("STEP 1: Retrieving data from database")
        df = read_csv()
        logger.info(f"Retrieved data with shape: {df.shape}")
        logger.info(f"Data columns: {list(df.columns)}")

        if df.empty:
            logger.error("No data retrieved from database! Terminating workflow.")
            return

        # Step 1.5: Expiry date
        logger.info("STEP 1.5: Getting expiry date for options")
        expiry = expiry_date()
        logger.info(f"Retrieved expiry data with shape: {expiry.shape}")

        # Step 2: Align option expiries
        logger.info("STEP 2: Aligning option expiries")
        aligned_df = align_option_expiries(df, expiry)
        logger.info(f"DataFrame shape after expiry alignment: {aligned_df.shape}")

        # Step 3: Manual entries (optional)
        # logger.info("STEP 3: Applying manual entries")
        # final_df = manual_entries(aligned_df)
        # logger.info(f"DataFrame shape after manual entries: {final_df.shape}")

        # Since manual_entries is commented, use aligned_df as final_df
        final_df = aligned_df.copy()

        # Step 4: Call ivol API
        if not final_df.empty:
            logger.info("STEP 4: Calling ivol API and adding results to DataFrame")
            logger.info(f"Input DataFrame shape before ivol API: {final_df.shape}")

            final_df = call_ivol_api_and_add_to_df(final_df)

            logger.info(f"DataFrame shape after ivol API calls: {final_df.shape}")
        else:
            logger.warning("Skipping IVOL API call — final_df is empty.")

        # Step 5: Transform to payloads
        if not final_df.empty:
            logger.info("STEP 5: Transforming DataFrame to API payloads")
            payloads, transformed_df = transform_to_option_api_payloads(final_df)
            logger.info(f"Generated {len(payloads)} API payloads")
            logger.info(f"Transformed DataFrame shape: {transformed_df.shape}")
        else:
            logger.warning("Skipping transformation — final_df is empty.")
            payloads, transformed_df = [], pd.DataFrame()

        # Step 6 and 7: Placeholder
        logger.info("STEP 6: API calls (currently commented out)")
        logger.info("STEP 7: Merge and export (currently commented out)")

        # Final summary
        logger.info("=" * 60)
        logger.info("OPTIONS MAIN WORKFLOW COMPLETED SUCCESSFULLY")
        logger.info(f"Final summary:")
        logger.info(f"  - Initial data rows: {len(df)}")
        logger.info(f"  - Final processed rows: {len(transformed_df)}")
        logger.info(f"  - API payloads generated: {len(payloads)}")
        logger.info("=" * 60)

        return payloads, transformed_df

    except Exception as e:
        logger.error("=" * 60)
        logger.error("OPTIONS MAIN WORKFLOW FAILED")
        logger.error(f"Error: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        logger.exception("Full traceback:")
        logger.error("=" * 60)
        raise

if __name__ == "__main__":
    logger.info("Starting options main execution")
    try:
        result = options_main()
        if result:
            payloads, transformed_df = result
            logger.info("Main execution completed successfully")
            logger.info(f"Generated {len(payloads)} payloads and {len(transformed_df)} transformed rows")
        else:
            logger.warning("Main execution completed but returned no results")
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        raise
