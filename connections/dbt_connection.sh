airflow connections add 'dbt_cloud_conn' \
    --conn-json '{
        "conn_type": "HTTP",
        "host": "cloud.getdbt.com"
    }'