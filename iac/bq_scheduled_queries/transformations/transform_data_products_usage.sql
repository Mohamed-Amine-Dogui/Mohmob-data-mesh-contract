MERGE INTO `${dataset}.${table}` AS target
    USING (
    WITH
        extract_snapshot AS (
                SELECT
                    dp_id,
                    dpc_id,
                    date,
                    STRING(JSON_QUERY(dpc_output_port, '$.projectId')) AS c_projectId,
                    STRING(JSON_QUERY(dpc_output_port, '$.dataset')) AS c_dataset,
                    STRING(JSON_QUERY(dpc_output_port, '$.table')) AS c_table
                FROM `${dataset}.metadata_snapshot`
                WHERE STRING(JSON_QUERY(dpc_output_port, '$.system')) = 'bigquery'
                AND date <= CURRENT_DATE() -- date is always latest
        ),

        filtered_users AS (
                SELECT
                    dp_id,
                    dpc_id,
                    DATE(TIMESTAMP_TRUNC(query_time, DAY)) as date,
                    ARRAY_AGG(DISTINCT CASE WHEN is_user_flag then principalEmail ELSE NULL END IGNORE NULLS) AS users_array,
                    ARRAY_AGG(DISTINCT CASE WHEN NOT is_user_flag AND principalEmail NOT LIKE '%looker%' then principalEmail ELSE NULL END IGNORE NULLS ) AS service_accounts_array,
                    ARRAY_AGG(DISTINCT CASE WHEN NOT is_user_flag AND principalEmail LIKE '%looker%' then principalEmail ELSE NULL END IGNORE NULLS) AS looker_users_array,
                    ARRAY_LENGTH(ARRAY_AGG(DISTINCT CASE WHEN is_user_flag then principalEmail ELSE NULL END IGNORE NULLS)) AS total_users,
                    ARRAY_LENGTH(ARRAY_AGG(DISTINCT CASE WHEN NOT is_user_flag AND principalEmail NOT LIKE '%looker%' THEN principalEmail ELSE NULL END IGNORE NULLS)) AS total_sas,
                    ARRAY_LENGTH(ARRAY_AGG(DISTINCT CASE WHEN NOT is_user_flag AND principalEmail LIKE '%looker%' THEN principalEmail ELSE NULL END IGNORE NULLS))  AS total_looker_users,
                FROM extract_snapshot AS contracts
                LEFT JOIN `mo-bi-layer-prod-gay6.operational.ops_bq_model_usage`
                ON table_catalog = c_projectId AND table_schema = c_dataset AND table_name = c_table
                WHERE NOT is_sa_self_ref_flag
                GROUP BY dp_id, dpc_id, is_user_flag, date)

    SELECT
        dp_id,
        dpc_id,
        date,
        ARRAY_CONCAT_AGG(users_array) AS users,
        ARRAY_CONCAT_AGG(service_accounts_array) AS sas,
        ARRAY_CONCAT_AGG(looker_users_array) AS looker_users,
        SUM(IFNULL(total_sas,0)) AS total_sas,
        SUM(IFNULL(total_users,0)) AS total_users,
        SUM(IFNULL(total_looker_users,0)) AS total_looker_users
    FROM filtered_users
    GROUP BY dp_id, dpc_id, date
        ) AS source

    ON  target.dp_id = source.dp_id
        AND target.dpc_id = source.dpc_id
        AND target.date = source.date
        AND target.date <= CURRENT_DATE()
        WHEN MATCHED THEN
    UPDATE SET
        users = source.users,
        looker_users = source.looker_users,
        sas = source.sas,
        total_sas = source.total_sas,
        total_users = source.total_users,
        total_looker_users = source.total_looker_users
        WHEN NOT MATCHED THEN
    INSERT (
            dp_id,
            dpc_id,
            date,
            users,
            looker_users,
            sas,
            total_sas,
            total_users,
            total_looker_users)
    VALUES (
            source.dp_id,
            source.dpc_id,
            source.date,
            source.users,
            source.looker_users,
            source.sas,
            source.total_sas,
            source.total_users,
            source.total_looker_users);