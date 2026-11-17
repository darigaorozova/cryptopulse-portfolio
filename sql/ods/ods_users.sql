-- stg_users → ods_users (справочник целиком): upsert по user_id.

INSERT INTO orozova_ods.ods_users
    (user_id, country_code, signup_date, plan, age_group, acquisition_channel, _loaded_at)
SELECT DISTINCT ON (trim(user_id))
       trim(user_id),
       upper(trim(country_code)),
       trim(signup_date)::date,
       lower(trim(plan)),
       nullif(trim(age_group), ''),
       nullif(trim(acquisition_channel), ''),
       now()
FROM orozova_stg.stg_users
WHERE nullif(trim(user_id), '') IS NOT NULL
ORDER BY trim(user_id), _loaded_at DESC
ON CONFLICT (user_id) DO UPDATE SET
    country_code = EXCLUDED.country_code, signup_date = EXCLUDED.signup_date, plan = EXCLUDED.plan,
    age_group = EXCLUDED.age_group, acquisition_channel = EXCLUDED.acquisition_channel, _loaded_at = now();
