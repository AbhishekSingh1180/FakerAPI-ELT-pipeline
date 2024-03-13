CREATE OR REPLACE PROCEDURE PRODUCT_DB.DW_APPL.SP_PRODUCT_REFINED_LOAD_TO_CONFIRMED_PRODUCTS()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
      database VARCHAR(128) := 'PRODUCT_DB';
      refined_schema VARCHAR(128) := 'DW_R_PRODUCT';
      refined_table VARCHAR(128) := 'PRODUCTS';
      stream_schema VARCHAR(128) := 'DW_APPL';
      stream_name VARCHAR(128) := 'PRODUCTS_R_STREAM';
      temp_schema VARCHAR(128) := 'DW_TEMP';
      work_table VARCHAR(128) := 'PRODUCTS_WORK';
      rerun_table VARCHAR(128) := 'PRODUCTS_RERUN';
      timeout_table VARCHAR(128) := 'PRODUCTS_TIMEOUT';
      exception_schema VARCHAR(128) := 'DW_EXCEPTION';
      exception_table VARCHAR(128) := 'PRODUCTS_EXCEPTIONS';
      confirmed_schema VARCHAR(128) := 'DW_C_PRODUCT';
      confirmed_table VARCHAR(128) := 'PRODUCTS';
      create_rerun_table VARCHAR;
      create_timeout_table VARCHAR;
      create_work_table VARCHAR;
      truncate_rerun_table VARCHAR;
      truncate_timeout_table VARCHAR;
      insert_into_timeout_table VARCHAR;
      insert_into_rerun_table VARCHAR;
      handle_exception_records VARCHAR;
      insert_new_records VARCHAR;
      update_target_table VARCHAR;
BEGIN

    create_rerun_table := 'CREATE TRANSIENT TABLE IF NOT EXISTS ' || database || '.' || temp_schema || '.' || rerun_table || ' LIKE ' 
                           || database || '.' || refined_schema || '.' || refined_table || ';';

    create_timeout_table := 'CREATE TRANSIENT TABLE IF NOT EXISTS ' || database || '.' || temp_schema || '.' || timeout_table || ' LIKE ' 
                           || database || '.' || refined_schema || '.' || refined_table || ';';

    create_work_table := 'CREATE OR REPLACE TRANSIENT TABLE ' || database || '.' || temp_schema || '.' || work_table || ' AS
                          SELECT 
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
                          FROM ' || database || '.' || stream_schema || '.' || stream_name || 
                          ' UNION ALL 
                          SELECT * FROM ' || database || '.' || temp_schema || '.' || rerun_table || 
                          ' UNION ALL
                          SELECT * FROM ' || database || '.' || temp_schema || '.' || timeout_table || ';';
    
    truncate_timeout_table := 'TRUNCATE TABLE '|| database || '.' || temp_schema || '.' || timeout_table || ';';

    truncate_rerun_table := 'TRUNCATE TABLE '|| database || '.' || temp_schema || '.' || rerun_table || ';';
    
    insert_into_timeout_table := 'INSERT INTO ' || database || '.' || temp_schema || '.' || timeout_table || '
                                  SELECT * FROM ' || database || '.' || temp_schema || '.' || work_table || ';';
    
    insert_into_rerun_table := 'INSERT INTO ' || database || '.' || temp_schema || '.' || rerun_table || '
                                SELECT * FROM ' || database || '.' || temp_schema || '.' || work_table || ';';
    
    handle_exception_records := 'INSERT INTO ' || database || '.' || exception_schema || '.' || exception_table || '
                                    SELECT 
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
                                        DW_SOURCE_NM,
                                        ''PRODUCT_ID IS NULL'' AS EXCEPTION_REASON
                                    FROM ' || database || '.' || temp_schema || '.' || work_table ||
                                    ' WHERE PRODUCT_ID IS NULL;';

    insert_new_records := 'INSERT INTO ' || database || '.' || confirmed_schema || '.' || confirmed_table || '
                           SELECT 
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
                                CURRENT_TIMESTAMP() AS DW_LAST_UPDATE_TS, 
                                DW_SOURCE_NM,
                                CURRENT_DATE() AS DW_FIRST_EFFECTIVE_DT,
                                ''9999-12-31'' AS DW_LAST_EFFECTIVE_DT,
                                1 AS DW_CURRENT_VERSION_IND,
                                0 AS DW_LOGICAL_DELETE_IND
                            FROM ' || database || '.' || temp_schema || '.' || work_table ||
                            ' WHERE PRODUCT_ID IS NOT NULL;';
    
    update_target_table := 'UPDATE ' || database || '.' || confirmed_schema || '.' || confirmed_table || ' TGT 
                            SET 
                                DW_LAST_UPDATE_TS = CURRENT_TIMESTAMP(),
                                DW_LAST_EFFECTIVE_DT = CURRENT_DATE(),
                                DW_CURRENT_VERSION_IND = 0,
                                DW_LOGICAL_DELETE_IND = 1 
                            FROM
                            (
                                SELECT 
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
                                FROM ' || database || '.' || confirmed_schema || '.' || confirmed_table ||
                                ' WHERE DW_CURRENT_VERSION_IND = 1
                             QUALIFY ROW_NUMBER() OVER(PARTITION BY PRODUCT_ID ORDER BY CREATED_TS DESC) > 1 
                            ) SRC 
                            WHERE TGT.PRODUCT_ID = SRC.PRODUCT_ID 
                            AND TGT.PRODUCT_NAME = TGT.PRODUCT_NAME 
                            AND TGT.PRODUCT_DESCRIPTION = SRC.PRODUCT_DESCRIPTION 
                            AND TGT.PRODUCT_EAN = SRC.PRODUCT_EAN 
                            AND TGT.PRODUCT_UPC = SRC.PRODUCT_UPC 
                            AND TGT.NET_PRICE = SRC.NET_PRICE 
                            AND TGT.TAXES = SRC.TAXES 
                            AND TGT.PRICE_INC_TAX = SRC.PRICE_INC_TAX 
                            AND TGT.CREATED_TS = SRC.CREATED_TS 
                            AND TGT.DW_CREATE_TS = SRC.DW_CREATE_TS 
                            AND TGT.DW_SOURCE_NM = SRC.DW_SOURCE_NM;';
    BEGIN
        EXECUTE IMMEDIATE create_rerun_table;
        EXECUTE IMMEDIATE create_timeout_table;
        EXECUTE IMMEDIATE create_work_table;
        EXECUTE IMMEDIATE truncate_rerun_table;
        EXECUTE IMMEDIATE insert_into_timeout_table;
        BEGIN
            EXECUTE IMMEDIATE handle_exception_records;
            EXECUTE IMMEDIATE insert_new_records;
            EXECUTE IMMEDIATE update_target_table;
            EXECUTE IMMEDIATE truncate_timeout_table;
            COMMIT;
        EXCEPTION
            WHEN OTHER THEN
                ROLLBACK;
                RAISE;
        RETURN 'SUCCESS';
        END;
    EXCEPTION
        WHEN OTHER THEN
            EXECUTE IMMEDIATE insert_into_rerun_table;
            EXECUTE IMMEDIATE truncate_timeout_table;
            RAISE;
    END;
END;
$$;