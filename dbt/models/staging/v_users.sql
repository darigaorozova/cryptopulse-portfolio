select u.user_id, u.country_code, u.plan, k.kyc_status
from {{ source('ods', 'ods_users') }} u
left join {{ source('ods', 'ods_users_kyc') }} k using (user_id)
