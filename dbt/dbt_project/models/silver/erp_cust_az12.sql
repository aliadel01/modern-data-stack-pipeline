SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTR(cid, 4) ELSE cid END AS cid,
    CASE WHEN bdate > CURRENT_DATE THEN NULL ELSE bdate END AS bdate,
    CASE
        WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
        WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
        ELSE 'n/a'
    END AS gen
FROM {{ source('bronze', 'ERP_CUST_AZ12') }}
