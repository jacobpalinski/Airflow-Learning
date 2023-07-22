COPY clean_store_transactions (STORE_ID, STORE_LOCATION, PRODUCT_CATEGORY, PRODUCT_ID, MRP, CP, DISCOUNT, SP, DATE)
FROM '/store_files_postgres/clean_store_transactions.csv'
WITH (FORMAT csv, HEADER true);