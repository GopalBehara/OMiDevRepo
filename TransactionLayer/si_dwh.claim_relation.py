# Databricks notebook source
# MAGIC %sql
# MAGIC truncate table `omi-catalog`.si_dwh.claim_relation

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC  insert
# MAGIC       into
# MAGIC     `omi-catalog`.si_dwh.claim_relation (AUD_BATCH_ID,
# MAGIC       AUD_IU_FLAG,
# MAGIC       AUD_SRC_SYS_CD,
# MAGIC       AUD_SRC_SYS_ID,
# MAGIC       AUD_SRC_SYS_NM,
# MAGIC       AUD_SRC_SYS_UPDT_VER_ID,
# MAGIC       AUD_SUB_BATCH_ID,
# MAGIC       CLAIM_ID,
# MAGIC       CLAIM_RELATION_ID,
# MAGIC       CLAIM_RELATION_NUM,
# MAGIC       CLAIM_RELATIONSHIP_TYPE_CD,
# MAGIC       CLAIM_RELATIONSHIP_TYPE_DESC,
# MAGIC       CLAIM_REL_END_DT,
# MAGIC       CLAIM_REL_START_DT,
# MAGIC       COMPANY_ID,
# MAGIC       INSERT_DATE,
# MAGIC       INSERT_USER,
# MAGIC       LAST_UPDATE_DATE,
# MAGIC       LAST_UPDATE_USER,
# MAGIC       RELATED_CLAIM_ID,
# MAGIC       RELATED_CLAIM_NUM,
# MAGIC AUD_TRAN_DT)
# MAGIC select
# MAGIC       '?Batch_ID'::int as AUD_BATCH_ID,
# MAGIC       0 as AUD_IU_FLAG,
# MAGIC       null::VARCHAR(255) as AUD_SRC_SYS_CD,
# MAGIC       concat(CLA_SUBCASE.CLA_CASE_NO, '|', CLA_SUBCASE_NO)::VARCHAR(255) as AUD_SRC_SYS_ID,
# MAGIC       'TIA' as AUD_SRC_SYS_NM,
# MAGIC       null::numeric(12,
# MAGIC       0) as AUD_SRC_SYS_UPDT_VER_ID,
# MAGIC       '?Sub_Batch_ID'::int as AUD_SUB_BATCH_ID,
# MAGIC       cd_claim.CLAIM_ID::varchar(255) as CLAIM_ID,
# MAGIC       concat(CLA_SUBCASE.CLA_CASE_NO, '|', CLA_SUBCASE_NO, '|', '101', '|', 'TIA')::VARCHAR(255) as CLAIM_RELATION_ID,
# MAGIC       concat(CLA_SUBCASE.CLA_CASE_NO, '|', CLA_SUBCASE_NO)::VARCHAR(255) as CLAIM_RELATION_NUM,
# MAGIC       1 as CLAIM_RELATIONSHIP_TYPE_CD,
# MAGIC       'Claim-SubClaim' as CLAIM_RELATIONSHIP_TYPE_DESC,
# MAGIC       null::DATE as CLAIM_REL_END_DT,
# MAGIC       null::DATE as CLAIM_REL_START_DT,
# MAGIC       1 as COMPANY_ID,
# MAGIC       cc.TIMESTAMP::TIMESTAMP as INSERT_DATE,
# MAGIC       UD1.USER_ID::varchar(255) as INSERT_USER,
# MAGIC       CLA_SUBCASE.TIA_COMMIT_DATE::TIMESTAMP as LAST_UPDATE_DATE,
# MAGIC       USER_DETAILS1.USER_ID::VARCHAR(255) as LAST_UPDATE_USER,
# MAGIC       CLAIMS_DETAILS.CLAIM_ID::VARCHAR(255) as RELATED_CLAIM_ID,
# MAGIC       CLAIMS_DETAILS.CLAIM_NUM::INTEGER as RELATED_CLAIM_NUM,
# MAGIC       CLA_SUBCASE.record_timestamp AS AUD_TRAN_DT
# MAGIC from
# MAGIC       `omi-catalog`.tia_ods.CLA_SUBCASE CLA_SUBCASE
# MAGIC 		left join `omi-catalog`.tia_ods.CLA_CASE cc
# MAGIC 			on CLA_SUBCASE.CLA_CASE_NO = cc.CLA_CASE_NO
# MAGIC 		left join `omi-catalog`.si_dwh.claims_details claims_details on
# MAGIC       CLAIMS_DETAILS.CLAIM_ID = concat(CLA_SUBCASE.CLA_CASE_NO,'|', CLA_SUBCASE.CLA_SUBCASE_NO,'|','101','|','TIA')
# MAGIC 		left join `omi-catalog`.si_dwh.USER_DETAILS USER_DETAILS1 on
# MAGIC       CLA_SUBCASE.RECORD_USERID = USER_DETAILS1.USER_NUM
# MAGIC left join `omi-catalog`.si_dwh.claims_details cd_claim 
# MAGIC on cd_claim.CLAIM_ID = concat(CLA_SUBCASE.CLA_CASE_NO,'|','101','|','TIA')
# MAGIC left join `omi-catalog`.si_dwh.USER_DETAILS UD1
# MAGIC on CLA_SUBCASE.USERID = UD1.USER_NUM
# MAGIC where concat(CLA_SUBCASE.CLA_CASE_NO, '|', CLA_SUBCASE_NO, '|', '101', '|', 'TIA') <> '4096704|1200000|101|TIA';

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table `omi-catalog`.si_dwh.claim_relation

# COMMAND ----------

# MAGIC %sql
# MAGIC --Duplicate check
# MAGIC select 
# MAGIC user_id,
# MAGIC count (*)
# MAGIC from `omi-catalog`.si_dwh.USER_DETAILS 
# MAGIC --where valid_to ='9999-12-31 00:00:00'
# MAGIC group by
# MAGIC user_id 
# MAGIC having count(*)>1
