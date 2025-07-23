import pandas as pd
from connections import connect_back_office_applictions
import logging

# Configure logging for this module
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def get_data():
    logger.info("Starting data retrieval process")
    
    query = """
        SELECT DISTINCT
            strategy_id,
            exposure,
            end_date,
            market_price,
            instrument_type,
            future_value,
            option_type,
            strike,
            rf_rate
        FROM 
            position.aggregated_valuations av
        WHERE 
            valuation_date = '2025-07-21'
            AND instrument_type = 'Option'
            AND position_type = 'exposure'
            AND strategy_id IN (
                '124', '143', '160', '162', '5189', '734', '735',
                '774',
                'LN-NG-EB',
                '4809', '525', '694', '739', '740',
                '634', '635', '636', '637', '741',
                '343', '345', '348', '349', '4908', '4909', '742', '743', 'LN-NG-GS',
                '238',
                '5653', '5654', '5655', '647', '648',
                '231', '232', '239', '284', '4275', '744',
                '387', '388', '389', '390', '413', '414', '751', '752',
                '5192', '5193', '5196', '5685', '749', '750',
                '1504', '185', '187', '289', '5686', '753', 'LN-NG-PG Cross Commodity-PG',
                '791', '792',
                '5421', '5422', '5423', '5426', '5427', '5428', '5429',
                '175',
                '681', '682', '683',
                '222', '224', '280', '291', '404', '4283', 'LN-NG-VG'
            );
    """
    
    logger.info("Executing query to retrieve options data")
    logger.debug(f"Query: {query}")
    
    conn = connect_back_office_applictions()
    
    try:
        logger.info("Executing SQL query with pandas")
        df = pd.read_sql(query, conn)
        logger.info(f"Successfully retrieved {len(df)} rows of data")
        logger.info(f"DataFrame columns: {list(df.columns)}")
        
        if df.empty:
            logger.warning("Query returned no data!")
        else:
            logger.info(f"Data shape: {df.shape}")
            
    except Exception as e:
        logger.error(f"Error executing query: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        raise
    finally:
        logger.info("Disposing database connection")
        conn.dispose()
        
    logger.info("Data retrieval completed successfully")
    return df


from connections import connect_crate_db  # assuming this function is defined



def expiry_date():
    # Define all instrument_key LIKE patterns
    like_patterns = [
        "B ______ P%",     # TTF
        "TFO ______ P%",   # EUA
        "LO ______ P%",    # WTI
        "CB5 ______ P%",   # Brent
        "ON ______ P%",    # Gas
        "EUA ______ P%",   # EUA
    ]

    # Query template
    base_query = """
        SELECT DISTINCT 
            properties['UnderlyingInstrument']['instrument_key'] AS future_key,
            properties['UnderlyingInstrument']['ExpirationDate'] AS future_expiry,
            properties['ExpirationDate'] AS option_expiry
        FROM settles.instruments
        WHERE instrument_key LIKE '{pattern}'
        AND properties['ExpirationDate'] > '2025-07-21'
    """

    # Combine all queries with UNION ALL
    union_queries = "\nUNION ALL\n".join([base_query.format(pattern=p) for p in like_patterns])

    final_query = f"""
        {union_queries}
        ORDER BY option_expiry
    """

    # Execute query
    conn = connect_crate_db()
    try:
        df = pd.read_sql(final_query, conn)
    finally:
        conn.dispose()

    # ✅ Clean the DataFrame: remove rows with any nulls
    df = df.dropna(subset=["future_key", "future_expiry", "option_expiry"])

    return df



def align_option_expiries(positions_df, expiry_df):
    import pandas as pd

    # Step 1: Exposure → symbol mapping
    exposure_map = {
        "TTF Curve": "TTF",
        "IPEBRT25Z": "B",
        "ICEEUA25Z": "EUA",
        "NYMWTI26F": "CL",
        "ICEV25CCA25Z": "CB5",
        "NG-HenryHub-EXCH": "NG",
        "EUA Monthly Curve": "EUA"
    }

    # Step 2: Preprocess expiry_df
    expiry_df = expiry_df.copy()
    expiry_df["symbol"] = expiry_df["future_key"].str.extract(r"^(\S+)")
    expiry_df["ym_key"] = expiry_df["future_key"].str.extract(r"(\d{6})")
    expiry_df["ym_key"] = expiry_df["ym_key"].astype("Int64")  # numeric YYYYMM

    # Step 3: Preprocess positions_df
    positions_df = positions_df.copy()
    positions_df["symbol"] = positions_df["exposure"].map(exposure_map)
    positions_df["ym_key"] = (
        pd.to_datetime(positions_df["end_date"]).dt.year * 100 +
        pd.to_datetime(positions_df["end_date"]).dt.month
    ).astype("Int64")

    # Step 4: Join on symbol and numeric ym_key
    merged = pd.merge(
        positions_df,
        expiry_df[["symbol", "ym_key", "option_expiry"]],
        on=["symbol", "ym_key"],
        how="left"
    )
    # output_file = "aligned_option_expiries.csv"
    # merged.to_csv(output_file, index=False)
    # logger.info(f"Saved merged DataFrame to {output_file} with shape: {merged.shape}")
    return merged




def manual_entries(df):
    df = df.copy()

    # Set future_value to 28.07 for 'ICEV25CCA25Z'
    df.loc[df["exposure"] == "ICEV25CCA25Z", "future_value"] = 28.07

    # Set future_value to 69.83 for 'EUA Monthly Curve'
    df.loc[df["exposure"] == "EUA Monthly Curve", "future_value"] = 69.83

    # Export the DataFrame to Excel for inspection with a timestamp
    from datetime import datetime
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_excel = f"manual_entries_output_{timestamp}.xlsx"
    df.to_excel(output_excel, index=False)
    logger.info(f"Exported DataFrame with manual entries to {output_excel} (shape: {df.shape})")
    return df








if __name__ == "__main__":
    logger.info("Starting main execution")
    try:
        data = get_data()
        logger.info(f"Retrieved data with shape: {data.shape}")
        print(data.head())
        logger.info("Main execution completed successfully")
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        logger.error(f"Exception type: {type(e).__name__}")
        raise