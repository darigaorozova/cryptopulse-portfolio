-- Демонстрация ФТ-9: изменить строку в «базе сайта» и через ≤ 30 секунд увидеть её в ods.
-- 1) было:
SELECT 'source' AS where_, user_id, kyc_status, updated_at FROM orozova.users_kyc WHERE user_id = 'U00042'
UNION ALL
SELECT 'ods', user_id, kyc_status, updated_at FROM orozova_ods.ods_users_kyc WHERE user_id = 'U00042';

-- 2) меняем статус в источнике (переключаем verified ↔ rejected):
UPDATE orozova.users_kyc
SET kyc_status = CASE WHEN kyc_status = 'verified' THEN 'rejected' ELSE 'verified' END
WHERE user_id = 'U00042';

-- 3) через несколько секунд выполнить ещё раз запрос из п.1 — в ods то же новое значение.
