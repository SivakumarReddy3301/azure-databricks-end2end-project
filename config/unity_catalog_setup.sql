-- 1. Storage Credential (via Access Connector)
CREATE STORAGE CREDENTIAL if not exists azure_adls_sc
WITH AZURE_MANAGED_IDENTITY 'your-managed-identity-resource-id';

-- 2. External Locations
CREATE EXTERNAL LOCATION bronze_loc
URL 'abfss://bronze@myprojecte2estorage.dfs.core.windows.net/'
WITH CREDENTIAL azure_adls_sc;

CREATE EXTERNAL LOCATION silver_loc
URL 'abfss://silver@myprojecte2estorage.dfs.core.windows.net/'
WITH CREDENTIAL azure_adls_sc;

CREATE EXTERNAL LOCATION gold_loc
URL 'abfss://gold@myprojecte2estorage.dfs.core.windows.net/'
WITH CREDENTIAL azure_adls_sc;

-- 3. Optional: Create a schema/catalog for organization
CREATE CATALOG IF NOT EXISTS databricks_catalog;
USE CATALOG databricks_catalog;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
