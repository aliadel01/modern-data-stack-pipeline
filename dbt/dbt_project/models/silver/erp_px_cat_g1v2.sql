SELECT
    id,
    cat,
    subcat,
    maintenance
FROM {{ source('bronze', 'ERP_PX_CAT_G1V2') }}