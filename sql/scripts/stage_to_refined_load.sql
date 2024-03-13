CREATE OR REPLACE PROCEDURE PRODUCT_DB.DW_APPL.SP_PRODUCT_STAGE_LOAD_TO_REFINED_PRODUCTS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
  database VARCHAR(128) := 'PRODUCT_DB';
  stage_schema VARCHAR(128) := 'DW_APPL';
  stage_name VARCHAR(128) := 'PRODUCT_STAGE';
  refined_schema VARCHAR(128) := 'DW_R_PRODUCT';
  refined_table VARCHAR(128) := 'PRODUCTS';
  copy_query VARCHAR;
BEGIN
  -- Generate the COPY INTO command dynamically using the declared variables
  copy_query := '
    COPY INTO ' || database || '.' || refined_schema || '.' || refined_table || ' (
      PRODUCT_ID,
      PRODUCT_NAME,
      PRODUCT_DESCRIPTION,
      PRODUCT_EAN,
      PRODUCT_UPC,
      NET_PRICE,
      TAXES,
      PRICE_INC_TAX,
      CREATED_TS,
      DW_CREATE_TS,
      DW_SOURCE_NM
    )
    FROM (
        SELECT
            $1 AS PRODUCT_ID, 
            $2 AS PRODUCT_NAME, 
            $3 AS PRODUCT_DESCRIPTION, 
            $4 AS PRODUCT_EAN, 
            $5 AS PRODUCT_UPC, 
            $6 AS NET_PRICE, 
            $7 AS TAXES,
            $8 AS PRICE_INC_TAX,
            $9 AS CREATED_TS,
            CURRENT_TIMESTAMP AS DW_CREATE_TS,
            SPLIT_PART(REPLACE(METADATA$FILENAME, '' '', ''_''),''/'',4) AS DW_SOURCE_NM
        FROM @' || database || '.' || stage_schema || '.' || stage_name || '/' || TO_VARCHAR(CURRENT_DATE(), 'YYYY/MM/DD') || '/' 
    || ') ON_ERROR = ''CONTINUE'';';

    BEGIN
        EXECUTE IMMEDIATE copy_query;
        COMMIT;
        RETURN 'SUCCESS';  -- Return success message if execution succeeds
    EXCEPTION
        WHEN OTHER THEN 
            ROLLBACK;
            RAISE;
    END;
END;
$$;