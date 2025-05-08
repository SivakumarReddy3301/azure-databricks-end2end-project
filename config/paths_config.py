# configs/paths_config.py

# Azure ADLS base containers
BRONZE_CONTAINER = "abfss://bronze@myprojecte2estorage.dfs.core.windows.net"
SILVER_CONTAINER = "abfss://silver@myprojecte2estorage.dfs.core.windows.net"
GOLD_CONTAINER   = "abfss://gold@myprojecte2estorage.dfs.core.windows.net"
SOURCE_CONTAINER = "abfss://source@myprojecte2estorage.dfs.core.windows.net"

# Bronze layer paths
BRONZE_CUSTOMERS_PATH = f"{BRONZE_CONTAINER}/customers"
BRONZE_ORDERS_PATH    = f"{BRONZE_CONTAINER}/orders"
BRONZE_PRODUCTS_PATH  = f"{BRONZE_CONTAINER}/products"

# Silver layer paths
SILVER_CUSTOMERS_PATH = f"{SILVER_CONTAINER}/customers"
SILVER_ORDERS_PATH    = f"{SILVER_CONTAINER}/orders"
SILVER_PRODUCTS_PATH  = f"{SILVER_CONTAINER}/products"

# Gold layer paths (for external tables if needed)
GOLD_CUSTOMERS_PATH = f"{GOLD_CONTAINER}/dim_customers"
GOLD_ORDERS_PATH    = f"{GOLD_CONTAINER}/fact_orders"
GOLD_PRODUCTS_PATH  = f"{GOLD_CONTAINER}/dim_products"

# Unity Catalog table references
CATALOG_NAME = "databricks_catalog"

TABLES = {
    "bronze_customers": f"{CATALOG_NAME}.bronze.customers_bronze",
    "silver_customers": f"{CATALOG_NAME}.silver.customers_silver",
    "gold_customers": f"{CATALOG_NAME}.gold.dim_customers",
    "gold_orders":    f"{CATALOG_NAME}.gold.fact_orders",
    "gold_products":  f"{CATALOG_NAME}.gold.dim_products"
}
