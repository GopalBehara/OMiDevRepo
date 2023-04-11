# Databricks notebook source
import re
 
#############################################################
# Setting default values
#############################################################
schema_si_dwh = "`omi-catalog`.si_dwh"
schema_si_dm = "`omi-catalog`.si_dm"
schema_si_dm_stg = "`omi-catalog`.si_dm_stg"
schema_tia_ods = "`omi-catalog`.tia_ods"
 
default_timestamp = "to_timestamp('1900-01-01 00:00:00')"
default_date = "to_date('9999-12-31', 'yyyy-mm-dd')"
 
v_last_ts = "to_timestamp('1900-01-01 00:00:00')"
gv_cus_id = 12345
gv_ses_id = 12345
v_policy_status = "\'P\'"
p_policy_no = "1111"
job_start_time = "to_timestamp('1900-01-01 00:00:00')"
v_business_start_date = "to_timestamp('1900-01-01 00:00:00')"
 
v_aud_src_sys_nm = " "
v_aud_batch_id = 1
v_aud_sub_batch_id = 1
v_aud_ins_batch_id = 1
v_aud_ins_sub_batch_id = 1
 
incr_load_ind = 0

# COMMAND ----------

spark.sql(
f"""insert
into {schema_si_dwh}.genins_con_ver_lob_omi(
contract_id,
	company_id,
	contract_version_id,
	contract_lob_id,
	aggregate_deductable,
	aggregate_deductable_desc,
	agr_add_ref_premium,
	agr_premium,
	agr_lta_discount_percent,
	agr_lta_signed_date,
	agr_lta_start_date,
	agr_lta_expiry_date,
	fee_code,
	aud_src_sys_nm,
	aud_src_sys_id,
	aud_src_sys_cd,
	aud_src_sys_updt_ver_id,
	aud_batch_id,
	aud_sub_batch_id,
	aud_iu_flag,
	last_update_date,
	last_update_user,
	center_cd
)(
  select
    genins_con_ver_lob_omi.contract_id,
    genins_con_ver_lob_omi.company_id,
    genins_con_ver_lob_omi.contract_version_id,
    genins_con_ver_lob_omi.contract_lob_id,
    genins_con_ver_lob_omi.aggregate_deductable,
    genins_con_ver_lob_omi.aggregate_deductable_desc,
    genins_con_ver_lob_omi.agr_add_ref_premium,
    genins_con_ver_lob_omi.agr_premium,
    genins_con_ver_lob_omi.agr_lta_discount_percent,
    genins_con_ver_lob_omi.agr_lta_signed_date,
    genins_con_ver_lob_omi.agr_lta_start_date,
    genins_con_ver_lob_omi.agr_lta_expiry_date,
    genins_con_ver_lob_omi.fee_code,
    genins_con_ver_lob_omi.aud_src_sys_nm,
    genins_con_ver_lob_omi.aud_src_sys_id,
    null as aud_src_sys_cd,
    null as aud_src_sys_updt_ver_id,
    genins_con_ver_lob_omi.aud_batch_id,
    genins_con_ver_lob_omi.aud_sub_batch_id,
    genins_con_ver_lob_omi.aud_iu_flag,
    genins_con_ver_lob_omi.last_update_date,
    genins_con_ver_lob_omi.last_update_user,
    genins_con_ver_lob_omi.center_cd
  from
    (
      select
        genins_con_ver_lob.contract_id as contract_id,
        genins_con_ver_lob.company_id as company_id,
        genins_con_ver_lob.contract_version_id as contract_version_id,
        genins_con_ver_lob.contract_lob_id as contract_lob_id,
        agreement_line.c03 as aggregate_deductable,
        mst_currency_code.description as aggregate_deductable_desc,
        cast(agreement_line.n02 as decimal(38, 20)) as agr_add_ref_premium,
        cast(agreement_line.n03 as decimal(38, 20)) as agr_premium,
        cast(agreement_line.n07 as decimal(38, 20)) as agr_lta_discount_percent,
        agreement_line.d04 as agr_lta_signed_date,
        agreement_line.d05 as agr_lta_start_date,
        agreement_line.d06 as agr_lta_expiry_date,
        agreement_line.c02 as fee_code,
        concat('tia', '') as aud_src_sys_nm,
        genins_con_ver_lob.aud_src_sys_id as aud_src_sys_id,
        1 as aud_batch_id,
        1 as aud_sub_batch_id,
        0 as aud_iu_flag,
        agreement_line.tia_commit_date as last_update_date,
        genins_con_ver_lob.last_update_user as last_update_user,
        agreement_line.center_code as center_cd
      from
        {schema_si_dwh}.genins_con_ver_lob
        inner join { schema_tia_ods }.agreement_line agreement_line on agreement_line.agr_line_seq_no = genins_con_ver_lob.lob_version_num
        and agreement_line.agr_line_no = genins_con_ver_lob.lob_num
        left join (
          select
            val.mst_val_si_cd code,
            val.mst_val_si_desc description,
            mst.src_sys_mst_tbl_nm
          from
            { schema_si_dwh }.mst_mapping_table_list mst,
            { schema_si_dwh }.mst_table_val_list val
          where
            mst.mst_tbl_si_id = val.mst_tbl_si_cd
            and mst.src_sys_mst_tbl_nm = 'yesno'
            and mst.src_sys_nm = 'tia'
            and val.mst_val_si_language = 'en'
        ) mst_currency_code on agreement_line.c03 = mst_currency_code.code
    ) genins_con_ver_lob_omi
)""").show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select CONTRACT_ID,COMPANY_ID,
# MAGIC CONTRACT_VERSION_ID,CONTRACT_LOB_ID,count(*) 
# MAGIC from `omi-catalog`.si_dwh.genins_con_ver_lob_omi
# MAGIC group by CONTRACT_ID,COMPANY_ID,
# MAGIC CONTRACT_VERSION_ID,CONTRACT_LOB_ID 
# MAGIC having count(*) > 1

# COMMAND ----------

# MAGIC %sql
# MAGIC select CONTRACT_ID,COMPANY_ID,
# MAGIC CONTRACT_VERSION_ID,CONTRACT_LOB_ID
# MAGIC from `omi-catalog`.si_dwh.genins_con_ver_lob_omi
# MAGIC WHERE CONTRACT_ID IS NULL OR COMPANY_ID IS NULL OR CONTRACT_VERSION_ID IS NULL OR 
# MAGIC 	  CONTRACT_LOB_ID IS NULL

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table `omi-catalog`.si_dwh.genins_con_ver_lob_omi

# COMMAND ----------

# MAGIC %sql
# MAGIC insert
# MAGIC 	into
# MAGIC 	`omi-catalog`.si_dwh.genins_con_ver_lob_omi(CONTRACT_ID,
# MAGIC 	COMPANY_ID,
# MAGIC 	CONTRACT_VERSION_ID,
# MAGIC 	CONTRACT_LOB_ID,
# MAGIC 	aggregate_deductable,
# MAGIC 	aggregate_deductable_desc,
# MAGIC 	agr_add_ref_premium,
# MAGIC 	agr_premium,
# MAGIC 	agr_lta_discount_percent,
# MAGIC 	agr_lta_signed_date,
# MAGIC 	agr_lta_start_date,
# MAGIC 	agr_lta_expiry_date,
# MAGIC 	fee_code,
# MAGIC 	AUD_SRC_SYS_NM,
# MAGIC 	AUD_SRC_SYS_ID,
# MAGIC 	AUD_BATCH_ID,
# MAGIC 	AUD_SUB_BATCH_ID,
# MAGIC 	AUD_IU_FLAG,
# MAGIC 	LAST_UPDATE_DATE,
# MAGIC 	LAST_UPDATE_USER,
# MAGIC 	center_cd,
# MAGIC     aud_src_sys_cd,
# MAGIC     aud_src_sys_updt_ver_id
# MAGIC 	)
# MAGIC 	
# MAGIC 	(
# MAGIC 	select
# MAGIC 		genins_con_ver_lob_omi.CONTRACT_ID,
# MAGIC 		genins_con_ver_lob_omi.COMPANY_ID,
# MAGIC 		genins_con_ver_lob_omi.CONTRACT_VERSION_ID,
# MAGIC 		genins_con_ver_lob_omi.CONTRACT_LOB_ID,
# MAGIC 		genins_con_ver_lob_omi.AGGREGATE_DEDUCTABLE,
# MAGIC 		genins_con_ver_lob_omi.AGGREGATE_DEDUCTABLE_DESC,
# MAGIC 		genins_con_ver_lob_omi.AGR_ADD_REF_PREMIUM,
# MAGIC 		genins_con_ver_lob_omi.AGR_PREMIUM,
# MAGIC 		genins_con_ver_lob_omi.AGR_LTA_DISCOUNT_PERCENT,
# MAGIC 		genins_con_ver_lob_omi.AGR_LTA_SIGNED_DATE,
# MAGIC 		genins_con_ver_lob_omi.AGR_LTA_START_DATE,
# MAGIC 		genins_con_ver_lob_omi.AGR_LTA_EXPIRY_DATE,
# MAGIC 		genins_con_ver_lob_omi.FEE_CODE,
# MAGIC 		genins_con_ver_lob_omi.AUD_SRC_SYS_NM,
# MAGIC 		genins_con_ver_lob_omi.AUD_SRC_SYS_ID,
# MAGIC 		genins_con_ver_lob_omi.AUD_BATCH_ID,
# MAGIC 		genins_con_ver_lob_omi.AUD_SUB_BATCH_ID,
# MAGIC 		genins_con_ver_lob_omi.AUD_IU_FLAG,
# MAGIC 		genins_con_ver_lob_omi.LAST_UPDATE_DATE,
# MAGIC 		genins_con_ver_lob_omi.LAST_UPDATE_USER,
# MAGIC 		genins_con_ver_lob_omi.center_cd,
# MAGIC         null as aud_src_sys_cd,
# MAGIC         null as aud_src_sys_updt_ver_id
# MAGIC 	from
# MAGIC 		(
# MAGIC 		select
# MAGIC 			genins_con_ver_lob.contract_id::VARCHAR(50) as CONTRACT_ID,
# MAGIC 			genins_con_ver_lob.company_id::INT as COMPANY_ID,
# MAGIC 			genins_con_ver_lob.contract_version_id::VARCHAR(50) as CONTRACT_VERSION_ID,
# MAGIC 			genins_con_ver_lob.CONTRACT_LOB_ID as CONTRACT_LOB_ID,
# MAGIC 			agreement_line.C03::VARCHAR(255) as AGGREGATE_DEDUCTABLE,
# MAGIC 			mst_currency_code.description::VARCHAR(255) as AGGREGATE_DEDUCTABLE_DESC,
# MAGIC 			agreement_line.n02::numeric(38,
# MAGIC 			20) as AGR_ADD_REF_PREMIUM,
# MAGIC 			agreement_line.n03::numeric(38,
# MAGIC 			20) as AGR_PREMIUM,
# MAGIC 			agreement_line.n07::numeric(38,
# MAGIC 			20) as AGR_LTA_DISCOUNT_PERCENT,
# MAGIC 			agreement_line.d04::timestamp as AGR_LTA_SIGNED_DATE,
# MAGIC 			agreement_line.d05::timestamp as AGR_LTA_START_DATE,
# MAGIC 			agreement_line.d06::timestamp as AGR_LTA_EXPIRY_DATE,
# MAGIC 			agreement_line.C02::VARCHAR(255) as FEE_CODE,
# MAGIC 			concat('TIA', '')::VARCHAR(10) as AUD_SRC_SYS_NM,
# MAGIC 			genins_con_ver_lob.AUD_SRC_SYS_ID as AUD_SRC_SYS_ID,
# MAGIC 			'?Batch_ID'::int as AUD_BATCH_ID,
# MAGIC 			'?Sub_Batch_ID'::int as AUD_SUB_BATCH_ID,
# MAGIC 			0 as AUD_IU_FLAG,
# MAGIC 			agreement_line.tia_commit_date::TIMESTAMP as LAST_UPDATE_DATE,
# MAGIC 			genins_con_ver_lob.LAST_UPDATE_USER as LAST_UPDATE_USER,
# MAGIC 			agreement_line.center_code as center_cd
# MAGIC 		from
# MAGIC 			`omi-catalog`.si_dwh.genins_con_ver_lob
# MAGIC 		inner join `omi-catalog`.tia_ods.agreement_line agreement_line on
# MAGIC 			agreement_line.agr_line_seq_no = genins_con_ver_lob.LOB_VERSION_NUM
# MAGIC 			and agreement_line.AGR_LINE_NO = genins_con_ver_lob.LOB_NUM
# MAGIC 		left join (
# MAGIC 			select
# MAGIC 				val.mst_val_si_cd code,
# MAGIC 				val.mst_val_si_desc description,
# MAGIC 				mst.src_sys_mst_tbl_nm
# MAGIC 			from
# MAGIC 				`omi-catalog`.si_dwh.mst_mapping_table_list mst,
# MAGIC 				`omi-catalog`.si_dwh.mst_table_val_list val
# MAGIC 			where
# MAGIC 				mst.mst_tbl_si_id = val.mst_tbl_si_cd
# MAGIC 				and mst.src_sys_mst_tbl_nm = 'YESNO'
# MAGIC 				and mst.src_sys_nm = 'TIA'
# MAGIC 				and val.mst_val_si_language = 'en') mst_currency_code on
# MAGIC 			agreement_line.C03 = mst_currency_code.code ) genins_con_ver_lob_omi )

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct company_id from `omi-catalog`.si_dwh.genins_con_ver_lob

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'TGT' TBL_NAME, COUNT(*) COUNT
# MAGIC FROM `omi-catalog`.SI_DWH.genins_con_ver_lob_omi
# MAGIC 
# MAGIC UNION ALL
# MAGIC 
# MAGIC SELECT 'SRC' TBL_NAME, COUNT(*) COUNT 
# MAGIC FROM (select gcvl.CONTRACT_ID AS CONTRACT_ID,
# MAGIC gcvl.COMPANY_ID AS COMPANY_ID,
# MAGIC gcvl.CONTRACT_VERSION_ID AS CONTRACT_VERSION_ID,
# MAGIC gcvl.CONTRACT_LOB_ID AS CONTRACT_LOB_ID ,
# MAGIC al.C03 AS AGGREGATE_DEDUCTABLE,
# MAGIC mst.description AS AGGREGATE_DEDUCTABLE_DESC,
# MAGIC al.N02 AS AGR_ADD_REF_PREMIUM,
# MAGIC al.N03 AS AGR_PREMIUM,
# MAGIC al.N07 AS AGR_LTA_DISCOUNT_PERCENT,
# MAGIC al.D04 AS AGR_LTA_SIGNED_DATE,                                               
# MAGIC al.D05  AS AGR_LTA_START_DATE,
# MAGIC al.D06 AS AGR_LTA_EXPIRY_DATE,
# MAGIC al.C02 AS FEE_CODE,
# MAGIC gcvl.AUD_SRC_SYS_NM AS AUD_SRC_SYS_NM,
# MAGIC gcvl.AUD_SRC_SYS_ID AS AUD_SRC_SYS_ID,
# MAGIC --AS AUD_BATCH_ID,
# MAGIC --AS AUD_SUB_BATCH_ID,
# MAGIC al.tia_commit_date AS LAST_UPDATE_DATE,
# MAGIC ud.user_id AS LAST_UPDATE_USER
# MAGIC from `omi-catalog`.SI_DWH.genins_con_ver_lob gcvl 
# MAGIC inner join `omi-catalog`.tia_ods.agreement_line al on
# MAGIC            al.agr_line_seq_no = gcvl.LOB_VERSION_NUM
# MAGIC left outer join `omi-catalog`.si_dwh.user_details ud on al.record_userid = ud.user_num 
# MAGIC left outer join (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  			from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  			where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'YESNO' 
# MAGIC  			and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST on  al.C03=MST.code)tc

# COMMAND ----------

# MAGIC %sql
# MAGIC -------------------------------------------------------------------------------nullcheck-------------------------------------------------------
# MAGIC select CONTRACT_ID,COMPANY_ID,
# MAGIC CONTRACT_VERSION_ID,CONTRACT_LOB_ID
# MAGIC from `omi-catalog`.si_dwh.genins_con_ver_lob_omi
# MAGIC WHERE CONTRACT_ID IS NULL OR COMPANY_ID IS NULL OR CONTRACT_VERSION_ID IS NULL OR 
# MAGIC 	  CONTRACT_LOB_ID IS NULL
# MAGIC 	  

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.SI_DWH.genins_con_ver_lob where company_id is null

# COMMAND ----------

# MAGIC %sql
# MAGIC select gcvl.CONTRACT_ID AS CONTRACT_ID,
# MAGIC gcvl.COMPANY_ID AS COMPANY_ID,
# MAGIC gcvl.CONTRACT_VERSION_ID AS CONTRACT_VERSION_ID,
# MAGIC gcvl.CONTRACT_LOB_ID AS CONTRACT_LOB_ID ,
# MAGIC al.C03 AS AGGREGATE_DEDUCTABLE,
# MAGIC mst.description AS AGGREGATE_DEDUCTABLE_DESC,
# MAGIC al.N02 AS AGR_ADD_REF_PREMIUM,
# MAGIC al.N03 AS AGR_PREMIUM,
# MAGIC al.N07 AS AGR_LTA_DISCOUNT_PERCENT,
# MAGIC al.D04 AS AGR_LTA_SIGNED_DATE,                                               
# MAGIC al.D05  AS AGR_LTA_START_DATE,
# MAGIC al.D06 AS AGR_LTA_EXPIRY_DATE,
# MAGIC al.C02 AS FEE_CODE,
# MAGIC gcvl.AUD_SRC_SYS_NM AS AUD_SRC_SYS_NM,
# MAGIC gcvl.AUD_SRC_SYS_ID AS AUD_SRC_SYS_ID,
# MAGIC --al2.batch_id AS AUD_BATCH_ID,
# MAGIC --AS AUD_SUB_BATCH_ID,
# MAGIC al.tia_commit_date AS LAST_UPDATE_DATE,
# MAGIC al.center_code as center_code,
# MAGIC ud.user_id AS LAST_UPDATE_USER
# MAGIC from `omi-catalog`.SI_DWH.genins_con_ver_lob gcvl 
# MAGIC left join `omi-catalog`.tia_ods.agreement_line al on
# MAGIC            al.agr_line_seq_no = gcvl.LOB_VERSION_NUM and  al.agr_line_no  = gcvl.LOB_NUM 
# MAGIC left outer join `omi-catalog`.si_dwh.user_details ud on al.record_userid = ud.user_num 
# MAGIC left outer join (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  			from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  			where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'YESNO' 
# MAGIC  			and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST on  al.C03=MST.code
# MAGIC 
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC 
# MAGIC select CONTRACT_ID,
# MAGIC COMPANY_ID,
# MAGIC CONTRACT_VERSION_ID,
# MAGIC CONTRACT_LOB_ID ,
# MAGIC AGGREGATE_DEDUCTABLE,
# MAGIC AGGREGATE_DEDUCTABLE_DESC,
# MAGIC AGR_ADD_REF_PREMIUM,
# MAGIC AGR_PREMIUM,
# MAGIC AGR_LTA_DISCOUNT_PERCENT,
# MAGIC AGR_LTA_SIGNED_DATE,
# MAGIC AGR_LTA_START_DATE,
# MAGIC AGR_LTA_EXPIRY_DATE,
# MAGIC FEE_CODE,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC LAST_UPDATE_DATE,
# MAGIC center_cd,
# MAGIC LAST_UPDATE_USER
# MAGIC from `omi-catalog`.si_dwh.genins_con_ver_lob_omi 

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*) from `omi-catalog`.si_dwh.genins_con_ver_lob_omi
# MAGIC union all
# MAGIC select count(*) from `omi-catalog`.si_dwh.genins_con_ver_lob

# COMMAND ----------

# MAGIC %sql
# MAGIC select gcvl.CONTRACT_ID AS CONTRACT_ID,
# MAGIC gcvl.COMPANY_ID AS COMPANY_ID,
# MAGIC gcvl.CONTRACT_VERSION_ID AS CONTRACT_VERSION_ID,
# MAGIC gcvl.CONTRACT_LOB_ID AS CONTRACT_LOB_ID ,
# MAGIC al.C03 AS AGGREGATE_DEDUCTABLE,
# MAGIC mst.description AS AGGREGATE_DEDUCTABLE_DESC,
# MAGIC al.N02 AS AGR_ADD_REF_PREMIUM,
# MAGIC al.N03 AS AGR_PREMIUM,
# MAGIC al.N07 AS AGR_LTA_DISCOUNT_PERCENT,
# MAGIC al.D04 AS AGR_LTA_SIGNED_DATE,                                               
# MAGIC al.D05  AS AGR_LTA_START_DATE,
# MAGIC al.D06 AS AGR_LTA_EXPIRY_DATE,
# MAGIC al.C02 AS FEE_CODE,
# MAGIC gcvl.AUD_SRC_SYS_NM AS AUD_SRC_SYS_NM,
# MAGIC gcvl.AUD_SRC_SYS_ID AS AUD_SRC_SYS_ID,
# MAGIC --al2.batch_id AS AUD_BATCH_ID,
# MAGIC --AS AUD_SUB_BATCH_ID,
# MAGIC al.tia_commit_date AS LAST_UPDATE_DATE,
# MAGIC --al.center_code
# MAGIC ud.user_id AS LAST_UPDATE_USER
# MAGIC from `omi-catalog`.SI_DWH.genins_con_ver_lob gcvl 
# MAGIC left join `omi-catalog`.tia_ods.agreement_line al on
# MAGIC            al.agr_line_seq_no = gcvl.LOB_VERSION_NUM and  al.agr_line_no  = gcvl.LOB_NUM 
# MAGIC left outer join `omi-catalog`.si_dwh.user_details ud on al.record_userid = ud.user_num 
# MAGIC left outer join (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  			from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  			where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'YESNO' 
# MAGIC  			and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST on  al.C03=MST.code
# MAGIC 
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC 
# MAGIC select CONTRACT_ID,
# MAGIC COMPANY_ID,
# MAGIC CONTRACT_VERSION_ID,
# MAGIC CONTRACT_LOB_ID ,
# MAGIC AGGREGATE_DEDUCTABLE,
# MAGIC AGGREGATE_DEDUCTABLE_DESC,
# MAGIC AGR_ADD_REF_PREMIUM,
# MAGIC AGR_PREMIUM,
# MAGIC AGR_LTA_DISCOUNT_PERCENT,
# MAGIC AGR_LTA_SIGNED_DATE,
# MAGIC AGR_LTA_START_DATE,
# MAGIC AGR_LTA_EXPIRY_DATE,
# MAGIC FEE_CODE,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC LAST_UPDATE_DATE,
# MAGIC LAST_UPDATE_USER
# MAGIC from `omi-catalog`.si_dwh.genins_con_ver_lob_omi 

# COMMAND ----------


