CREATE OR REPLACE TABLE PRODUCT_DB.DW_EXCEPTION.PRODUCTS_EXCEPTIONS(  -- EXCEPTION TABLE
    PRODUCT_ID VARCHAR,
    PRODUCT_NAME VARCHAR,
    PRODUCT_DESCRIPTION VARCHAR,
    PRODUCT_EAN VARCHAR, 
    PRODUCT_UPC VARCHAR, 
    NET_PRICE VARCHAR, 
    TAXES VARCHAR,
    PRICE_INC_TAX VARCHAR,
    CREATED_TS VARCHAR,
    DW_CREATE_TS TIMESTAMP_LTZ DEFAULT CURRENT_TIMESTAMP,
    DW_SOURCE_NM VARCHAR,
    EXCEPTION_REASON VARCHAR
);