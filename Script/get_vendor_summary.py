import pandas as pd
import numpy as np
import logging
from sqlalchemy import create_engine

# -------------------------------------------------------------
#  LOGGING CONFIGURATION
# -------------------------------------------------------------
logging.basicConfig(
    filename="logs/get_vendor_summary_mysql.log",
    level=logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)


# Create SQLAlchemy engine for MySQL
engine = create_engine("mysql+pymysql://root:Sudhu%40123@127.0.0.1/inventory")

logging.info("Connected to MySQL database")

# -------------------------------------------------------------
#  FUNCTION TO INGEST DATAFRAME INTO DATABASE
# -------------------------------------------------------------
def ingest_db(df, table_name, engine):
    """This function ingests a dataframe into MySQL table."""
    try:
        vendor_sales_summary.to_sql(
            'vendor_sales_summary',
            con=engine,
            if_exists='replace',
            index=False,
            chunksize=5000,
            method='multi')
        logging.info(f"Table {table_name} successfully created with {len(df)} rows.")
    except Exception as e:
        logging.error(f"Error while ingesting data: {e}")
        print(e)

# -------------------------------------------------------------
#  FUNCTION TO CREATE VENDOR SUMMARY
# -------------------------------------------------------------
def create_vendor_summary(engine):
    """Merges different tables to create vendor summary."""
    query = """
    WITH FreightSummary AS (
        SELECT 
            VendorNumber, 
            SUM(Freight) AS FreightCost 
        FROM vendor_invoice 
        GROUP BY VendorNumber
    ), 
    PurchaseSummary AS (
        SELECT 
            p.VendorNumber,
            p.VendorName,
            p.Brand,
            p.Description,
            p.PurchasePrice,
            pp.Price AS ActualPrice,
            pp.Volume,
            SUM(p.Quantity) AS TotalPurchaseQuantity,
            SUM(p.Dollars) AS TotalPurchaseDollars
        FROM purchases p
        JOIN purchase_prices pp
            ON p.Brand = pp.Brand
        WHERE p.PurchasePrice > 0
        GROUP BY p.VendorNumber, p.VendorName, p.Brand, p.Description, p.PurchasePrice, pp.Price, pp.Volume
    ), 
    SalesSummary AS (
        SELECT 
            VendorNo,
            Brand,
            SUM(SalesQuantity) AS TotalSalesQuantity,
            SUM(SalesDollars) AS TotalSalesDollars,
            SUM(SalesPrice) AS TotalSalesPrice,
            SUM(ExciseTax) AS TotalExciseTax
        FROM sales
        GROUP BY VendorNo, Brand
    )
    SELECT 
        ps.VendorNumber,
        ps.VendorName,
        ps.Brand,
        ps.Description,
        ps.PurchasePrice,
        ps.ActualPrice,
        ps.Volume,
        ps.TotalPurchaseQuantity,
        ps.TotalPurchaseDollars,
        ss.TotalSalesQuantity,
        ss.TotalSalesDollars,
        ss.TotalSalesPrice,
        ss.TotalExciseTax,
        fs.FreightCost
    FROM PurchaseSummary ps
    LEFT JOIN SalesSummary ss 
        ON ps.VendorNumber = ss.VendorNo 
        AND ps.Brand = ss.Brand
    LEFT JOIN FreightSummary fs 
        ON ps.VendorNumber = fs.VendorNumber
    ORDER BY ps.TotalPurchaseDollars DESC;
    """

    vendor_sales_summary = pd.read_sql_query(query, engine)
    return vendor_sales_summary

# -------------------------------------------------------------
#  FUNCTION TO CLEAN DATA
# -------------------------------------------------------------
def clean_data(df):
    """This function cleans and enriches the vendor summary."""
    df.fillna(0, inplace=True)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)

    df['VendorName'] = df['VendorName'].str.strip()
    df['Description'] = df['Description'].str.strip()

    # Convert datatypes safely
    float_cols = ['PurchasePrice', 'ActualPrice', 'Volume', 'TotalSalesDollars', 'TotalPurchaseDollars']
    for col in float_cols:
        df[col] = df[col].astype(float)

    int_cols = ['VendorNumber', 'TotalSalesQuantity', 'TotalPurchaseQuantity']
    for col in int_cols:
        df[col] = df[col].astype(int)

    # Derived business metrics
    df['GrossProfit'] = df['TotalSalesDollars'] - df['TotalPurchaseDollars']
    df['ProfitMargin'] = np.where(df['TotalSalesDollars'] != 0, 
                                  (df['GrossProfit'] / df['TotalSalesDollars']) * 100, 0)
    df['StockTurnover'] = np.where(df['TotalPurchaseQuantity'] != 0,
                                   df['TotalSalesQuantity'] / df['TotalPurchaseQuantity'], 0)
    df['SalesToPurchaseRatio'] = np.where(df['TotalPurchaseDollars'] != 0,
                                          df['TotalSalesDollars'] / df['TotalPurchaseDollars'], 0)
    return df

# -------------------------------------------------------------
#  MAIN EXECUTION
# -------------------------------------------------------------
if __name__ == '__main__':
    logging.info('Creating Vendor Summary Table.....')
    summary_df = create_vendor_summary(engine)
    logging.info(summary_df.head())

    logging.info('Cleaning Data.....')
    clean_df = clean_data(summary_df)
    logging.info(clean_df.head())

    logging.info('Ingesting data into MySQL.....')
    ingest_db(clean_df, 'vendor_sales_summary', engine)
    logging.info('Completed ')
