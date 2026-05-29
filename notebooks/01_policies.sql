-- Aplicar las políticas a la tabla streaming
ALTER TABLE fintech_finpay.silver.users_silver 
  ALTER COLUMN full_name SET MASK fintech_finpay.silver.mask_full_name;

ALTER TABLE fintech_finpay.silver.users_silver 
  ALTER COLUMN document_id SET MASK fintech_finpay.silver.mask_document_id;

ALTER TABLE fintech_finpay.silver.users_silver 
  ALTER COLUMN email SET MASK fintech_finpay.silver.mask_email;

ALTER TABLE fintech_finpay.silver.users_silver 
  ALTER COLUMN phone SET MASK fintech_finpay.silver.mask_phone;

-- Aplicar la política RLS a la tabla
ALTER TABLE fintech_finpay.silver.users_silver 
SET ROW FILTER fintech_finpay.silver.users_rls ON (segment);