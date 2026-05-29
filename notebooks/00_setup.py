# Databricks notebook source
# MAGIC %md
# MAGIC # CREACION DE CATALOGO Y ESQUEMA

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 1. Crear catálogo
# MAGIC CREATE CATALOG IF NOT EXISTS fintech_finpay;
# MAGIC
# MAGIC -- 2. Crear schemas dentro del catálogo
# MAGIC CREATE SCHEMA IF NOT EXISTS fintech_finpay.default;
# MAGIC CREATE SCHEMA IF NOT EXISTS fintech_finpay.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS fintech_finpay.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS fintech_finpay.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS fintech_finpay.observability;
# MAGIC
# MAGIC -- 3. Crear volumen de landing en el schema default
# MAGIC CREATE VOLUME IF NOT EXISTS fintech_finpay.default.vol_landing;

# COMMAND ----------

# MAGIC %md
# MAGIC # CREACION DE DIRECTORIOS

# COMMAND ----------

# 4. (Opcional) Crear subdirectorios dentro del volumen
dbutils.fs.mkdirs("/Volumes/fintech_finpay/default/vol_landing/transactions")
dbutils.fs.mkdirs("/Volumes/fintech_finpay/default/vol_landing/merchants")
dbutils.fs.mkdirs("/Volumes/fintech_finpay/default/vol_landing/users")
dbutils.fs.mkdirs("/Volumes/fintech_finpay/default/vol_landing/metadata")

# COMMAND ----------

# MAGIC %md
# MAGIC # ASIGNACION DE PERMISOS A ROLES

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rol ingeniería
# MAGIC GRANT USE CATALOG ON CATALOG fintech_finpay TO `ingenieria`;
# MAGIC GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA fintech_finpay.bronze TO `ingenieria`;
# MAGIC GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA fintech_finpay.silver TO `ingenieria`;
# MAGIC GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA fintech_finpay.gold TO `ingenieria`;
# MAGIC GRANT USE SCHEMA, CREATE TABLE, MODIFY ON SCHEMA fintech_finpay.observability TO `ingenieria`;
# MAGIC
# MAGIC -- Rol riesgo
# MAGIC GRANT USE CATALOG ON CATALOG fintech_finpay TO `riesgo`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA fintech_finpay.silver TO `riesgo`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA fintech_finpay.gold TO `riesgo`;
# MAGIC
# MAGIC -- Rol auditoría
# MAGIC GRANT USE CATALOG ON CATALOG fintech_finpay TO `auditoria`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA fintech_finpay.gold TO `auditoria`;
# MAGIC GRANT USE SCHEMA, SELECT ON SCHEMA fintech_finpay.observability TO `auditoria`;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # CREACION DE FUNCIONES MASK

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Masking para full_name
# MAGIC CREATE OR REPLACE FUNCTION fintech_finpay.silver.mask_full_name(val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE WHEN is_account_group_member('ingenieria') THEN val ELSE '***MASKED***' END;
# MAGIC
# MAGIC -- Masking para document_id (últimos 4 dígitos)
# MAGIC CREATE OR REPLACE FUNCTION fintech_finpay.silver.mask_document_id(val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE WHEN is_account_group_member('ingenieria') THEN val ELSE CONCAT('****', RIGHT(val,4)) END;
# MAGIC
# MAGIC -- Masking para email (inicial + dominio)
# MAGIC CREATE OR REPLACE FUNCTION fintech_finpay.silver.mask_email(val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE WHEN is_account_group_member('ingenieria') THEN val 
# MAGIC        ELSE CONCAT(SUBSTRING(val,1,1),'*****',SUBSTRING(val,INSTR(val,'@'))) END;
# MAGIC
# MAGIC -- Masking para phone (código país + últimos 3 dígitos)
# MAGIC CREATE OR REPLACE FUNCTION fintech_finpay.silver.mask_phone(val STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE WHEN is_account_group_member('ingenieria') THEN val 
# MAGIC        ELSE CONCAT('+', SUBSTRING(val,2,3),'*******',RIGHT(val,3)) END;

# COMMAND ----------

# MAGIC %md
# MAGIC # CREACION DE FUNCION RLS

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Row-Level Security Function
# MAGIC CREATE OR REPLACE FUNCTION fintech_finpay.silver.users_rls(segment STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN CASE 
# MAGIC   WHEN is_account_group_member('ingenieria') THEN true
# MAGIC   WHEN is_account_group_member('riesgo') THEN segment IN ('premium', 'estandar')
# MAGIC   WHEN is_account_group_member('auditoria') THEN segment = 'premium'
# MAGIC   ELSE false
# MAGIC END;