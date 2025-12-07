SELECT
    sls_ord_num,
    sls_prd_key,
    sls_cust_id,
    TRY_CAST(sls_order_dt::VARCHAR AS DATE) AS sls_order_dt,
    TRY_CAST(sls_ship_dt::VARCHAR AS DATE) AS sls_ship_dt,
    TRY_CAST(sls_due_dt::VARCHAR AS DATE) AS sls_due_dt,
    CASE 
        WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
        THEN sls_quantity * ABS(sls_price)
        ELSE sls_sales
    END AS sls_sales,
    sls_quantity,
    CASE 
        WHEN sls_price IS NULL OR sls_price <= 0 
        THEN sls_sales / NULLIF(sls_quantity,0)
        ELSE sls_price
    END AS sls_price
FROM {{ source('bronze', 'CRM_SALES_DETAILS') }}