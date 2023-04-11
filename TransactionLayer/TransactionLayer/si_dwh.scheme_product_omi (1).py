# Databricks notebook source
# MAGIC %sql
# MAGIC truncate table `omi-catalog`.si_dwh.scheme_product_omi

# COMMAND ----------

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
    into {schema_si_dwh}.scheme_product_omi( 
    scheme_product_omi_id,
	seq_no,
	scheme_seq_no,
	prod_no,
	prod_name,
	prod_id,
	cancel_code,
	cancel_code_desc,
	gs_code,
	gs_name,
	prod_type,
	monthly_divisor,
	tp_provider,
	administrator,
	prod_definition,
	prod_group,
	claim_mandate_ind,
	sasria_scheme_yn,
	swiftcare_yn,
	bordereaux_yn,
	bordereaux_import,
	eft_yn,
	qs_ind,
	bord_provisional,
	division,
	old_scheme_prod_no,
	prefix,
	trans_type,
	linked_schd_prod_id,
	schd_auto_reversal_switch,
	stride_enabled_yn,
	reporting_line,
	agreement_type,
	admin_fee_effective_date,
	start_date,
	end_date,
	cancelation_date,
	timestamp,
	userid,
	record_timestamp,
	record_userid,
	stride_enabled_from,
	binder_fee,
	claim_mandate,
	admin_fee_percentage,
	fee_percentage,
	budget_percentage,
	broker_service_fee,
	aud_src_sys_nm,
	aud_src_sys_id,
	aud_src_sys_cd,
	aud_batch_id,
	aud_sub_batch_id,
	last_update_date,
	last_update_user
)
    select
	 concat('|',it03m_scheme_product.seq_no , it03m_scheme_product.scheme_seq_no ,   '101',  'TIA') as scheme_product_omi_id,
	 cast(it03m_scheme_product.seq_no as decimal(38,20)) as seq_no,
	 cast(it03m_scheme_product.scheme_seq_no as decimal(38,20)) as scheme_seq_no,
	 it03m_scheme_product.scheme_prod_no as prod_no,
	 it03m_scheme_product.scheme_prod_name as prod_name,
	 it03m_scheme_product.prod_id as prod_id,
	 it03m_scheme_product.cancel_code as cancel_code,
	 case
                when it03m_scheme_product.cancel_code = 0
                or it03m_scheme_product.cancel_code is null then 'Active'
                else 'Cancelled'
            end as cancel_code_desc,
	 it03m_scheme.scheme_cd as gs_code,
            it03m_scheme.scheme_name as gs_name,
	case
                when it03m_scheme_product.scheme_prod_definition = '10' then 'UMA'
                else mst_i03m_prod_type.mst_val_si_desc
            end as prod_type,
	it03m_scheme_product.monthly_divisor as monthly_divisor,		
	 mst_i03m_tp_provider.mst_val_si_desc as tp_provider,
	 mst_i03m_administrator.mst_val_si_desc as administrator,
	  mst_i03m_prod_definition.mst_val_si_desc as prod_definition,
            mst_i03m_prod_group.mst_val_si_desc as prod_group,
			it03m_scheme_product.claim_mandate_ind as claim_mandate_ind,
			 it03m_scheme_product.sasria_scheme_yn as sasria_scheme_yn,
			 it03m_scheme_product.swiftcare_yn as swiftcare_yn,
			 it03m_scheme_product.bordereaux_yn as bordereaux_yn,
			 it03m_scheme_product.bordereaux_import as bordereaux_import,
			  it03m_scheme_product.eft_yn as eft_yn,
			  it03m_scheme_product.qs_ind as qs_ind,
			  it03m_scheme_product.bord_provisional as bord_provisional,
			  it03m_scheme_product.division as division,
			  it03m_scheme_product.old_scheme_prod_no as old_scheme_prod_no,
			  it03m_scheme_product.prefix as prefix,
			  it03m_scheme_product.trans_type as trans_type,
			  it03m_scheme_product.linked_schd_prod_id as linked_schd_prod_id,
			  it03m_scheme_product.schd_auto_reversal_switch as schd_auto_reversal_switch,
			  it03m_scheme_product.stride_enabled_yn as stride_enabled_yn,
			  it03m_scheme_product.reporting_line as reporting_line,
			  case
                when it03m_scheme_product.reporting_line = '1' then 'BINDER'
                when it03m_scheme_product.reporting_line = '2' then 'FULL BINDER'
                when it03m_scheme_product.reporting_line = '3' then 'OUTSOURCE'
                when it03m_scheme_product.reporting_line = '4' then 'UMA'
                else null
            end as agreement_type,
			it03m_scheme_product.admin_fee_effective_date as admin_fee_effective_date,
			it03m_scheme_product.start_date as start_date,
			it03m_scheme_product.end_date as end_date,
			it03m_scheme_product.cacelation_date as cancelation_date,
			it03m_scheme_product.timestamp as timestamp,
			it03m_scheme_product.userid as userid,
			it03m_scheme_product.record_timestamp as record_timestamp,
			it03m_scheme_product.record_userid as record_userid,
			it03m_scheme_product.stride_enabled_from as stride_enabled_from,
			cast(coalesce(it03m_scheme_product.binder_fee, 0) as decimal(38,20)) as binder_fee,
			cast(coalesce(it03m_scheme_product.claim_mandate, 0) as decimal(38,20)) as claim_mandate,
			cast(coalesce(it03m_scheme_product.binder_fee, 0) + coalesce(it03m_scheme_product.claim_mandate, 0) as decimal(38,20)) as admin_fee_percentage,
			cast(it03m_scheme_product.admin_fee_percentage as decimal(38,20)) as fee_percentage,
			cast(it03m_scheme_product.budget_percentage as decimal(38,20))as budget_percentage,
			cast(coalesce(it03m_scheme_product.broker_service_fee, 0) as decimal(38,20)) as broker_service_fee,
			concat('TIA', '') as aud_src_sys_nm,
			concat_ws('|',it03m_scheme_product.seq_no,it03m_scheme_product.scheme_seq_no , '101', 'TIA') as aud_src_sys_id,
            null as aud_src_sys_cd,
			46 as aud_batch_id,
			50 as aud_sub_batch_id,
			it03m_scheme_product.tia_commit_date as last_update_date,
			user_details.user_id as last_update_user
        from
            {schema_tia_ods}.it03m_scheme_product it03m_scheme_product
        left join {schema_si_dwh}.mst_table_val_list mst_i03m_administrator on
            it03m_scheme_product.administrator = mst_i03m_administrator.mst_val_si_cd
            and mst_i03m_administrator.mst_tbl_nm_shrt = 'I03M_ADMINISTRATOR'
            and mst_i03m_administrator.mst_val_si_language = 'en'
        inner join {schema_tia_ods}.it03m_scheme it03m_scheme on
            it03m_scheme_product.scheme_seq_no = it03m_scheme.seq_no
        left join {schema_si_dwh}.user_details user_details on
            it03m_scheme_product.record_userid = user_details.user_num
        left join {schema_si_dwh}.mst_table_val_list mst_i03m_prod_definition on
            it03m_scheme_product.scheme_prod_definition = mst_i03m_prod_definition.mst_val_si_cd
            and mst_i03m_prod_definition.mst_tbl_nm_shrt = 'I03M_PROD_DEFINITION'
            and mst_i03m_prod_definition.mst_val_si_language = 'en'
        left join {schema_si_dwh}.mst_table_val_list mst_i03m_prod_group on
            it03m_scheme_product.scheme_prod_group = mst_i03m_prod_group.mst_val_si_cd
            and mst_i03m_prod_group.mst_tbl_nm_shrt = 'I03M_PROD_GROUP'
            and mst_i03m_prod_group.mst_val_si_language = 'en'
        left join {schema_si_dwh}.mst_table_val_list mst_i03m_prod_type on
            it03m_scheme_product.scheme_prod_type = mst_i03m_prod_type.mst_val_si_cd
            and mst_i03m_prod_type.mst_tbl_nm_shrt = 'I03M_PROD_TYPE'
            and mst_i03m_prod_type.mst_val_si_language = 'en'
        left join {schema_si_dwh}.mst_table_val_list mst_i03m_tp_provider on
            it03m_scheme_product.tp_provider = mst_i03m_tp_provider.mst_val_si_cd
            and mst_i03m_tp_provider.mst_tbl_nm_shrt = 'I03M_TP_PROVIDER'
            and mst_i03m_tp_provider.mst_val_si_language = 'en'"""
).show()


# COMMAND ----------

# MAGIC %sql
# MAGIC insert
# MAGIC 	into
# MAGIC 	`omi-catalog`.si_dwh.scheme_product_omi(ADMIN_FEE_EFFECTIVE_DATE,
# MAGIC 	ADMIN_FEE_PERCENTAGE,
# MAGIC 	ADMINISTRATOR,
# MAGIC 	AGREEMENT_TYPE,
# MAGIC 	AUD_BATCH_ID,
# MAGIC 	AUD_SRC_SYS_ID,
# MAGIC 	AUD_SRC_SYS_NM,
# MAGIC 	AUD_SUB_BATCH_ID,
# MAGIC 	BINDER_FEE,
# MAGIC 	BORDEREAUX_IMPORT,
# MAGIC 	BORDEREAUX_YN,
# MAGIC 	BORD_PROVISIONAL,
# MAGIC 	BROKER_SERVICE_FEE,
# MAGIC 	BUDGET_PERCENTAGE,
# MAGIC 	CANCELATION_DATE,
# MAGIC 	CANCEL_CODE,
# MAGIC 	CANCEL_CODE_DESC,
# MAGIC 	CLAIM_MANDATE,
# MAGIC 	CLAIM_MANDATE_IND,
# MAGIC 	DIVISION,
# MAGIC 	EFT_YN,
# MAGIC 	END_DATE,
# MAGIC 	FEE_PERCENTAGE,
# MAGIC 	GS_CODE,
# MAGIC 	GS_NAME,
# MAGIC 	LAST_UPDATE_DATE,
# MAGIC 	LAST_UPDATE_USER,
# MAGIC 	LINKED_SCHD_PROD_ID,
# MAGIC 	MONTHLY_DIVISOR,
# MAGIC 	OLD_SCHEME_PROD_NO,
# MAGIC 	PREFIX,
# MAGIC 	PROD_DEFINITION,
# MAGIC 	PROD_GROUP,
# MAGIC 	PROD_ID,
# MAGIC 	PROD_NAME,
# MAGIC 	PROD_NO,
# MAGIC 	PROD_TYPE,
# MAGIC 	QS_IND,
# MAGIC 	RECORD_TIMESTAMP,
# MAGIC 	RECORD_USERID,
# MAGIC 	REPORTING_LINE,
# MAGIC 	SASRIA_SCHEME_YN,
# MAGIC 	SCHD_AUTO_REVERSAL_SWITCH,
# MAGIC 	SCHEME_PRODUCT_OMI_ID,
# MAGIC 	SCHEME_SEQ_NO,
# MAGIC 	SEQ_NO,
# MAGIC 	START_DATE,
# MAGIC 	STRIDE_ENABLED_FROM,
# MAGIC 	STRIDE_ENABLED_YN,
# MAGIC 	SWIFTCARE_YN,
# MAGIC 	TIMESTAMP,
# MAGIC 	TP_PROVIDER,
# MAGIC 	TRANS_TYPE,
# MAGIC 	USERID,
# MAGIC     aud_src_sys_cd)
# MAGIC 		select
# MAGIC 			it03m_scheme_product.admin_fee_effective_date::TIMESTAMP as ADMIN_FEE_EFFECTIVE_DATE,
# MAGIC 			coalesce(it03m_scheme_product.binder_fee, 0) + coalesce(it03m_scheme_product.claim_mandate, 0)::numeric(38,
# MAGIC 			20) as ADMIN_FEE_PERCENTAGE,
# MAGIC 			mst_i03m_administrator.mst_val_si_desc::VARCHAR(500) as ADMINISTRATOR,
# MAGIC 			case
# MAGIC 				when it03m_scheme_product.reporting_line = '1' then 'BINDER'
# MAGIC 				when it03m_scheme_product.reporting_line = '2' then 'FULL BINDER'
# MAGIC 				when it03m_scheme_product.reporting_line = '3' then 'OUTSOURCE'
# MAGIC 				when it03m_scheme_product.reporting_line = '4' then 'UMA'
# MAGIC 				else null
# MAGIC 			end::VARCHAR(20) as AGREEMENT_TYPE,
# MAGIC 			46 as AUD_BATCH_ID,
# MAGIC 			concat(it03m_scheme_product.seq_no, '|', it03m_scheme_product.scheme_seq_no, '|' , '101', '|', 'TIA')::VARCHAR(50) as AUD_SRC_SYS_ID,
# MAGIC 			concat('TIA', '')::VARCHAR(10) as AUD_SRC_SYS_NM,
# MAGIC 			50 as AUD_SUB_BATCH_ID,
# MAGIC 			coalesce(it03m_scheme_product.binder_fee, 0)::numeric(38,
# MAGIC 			20) as BINDER_FEE,
# MAGIC 			it03m_scheme_product.bordereaux_import::VARCHAR(1) as BORDEREAUX_IMPORT,
# MAGIC 			it03m_scheme_product.bordereaux_yn::VARCHAR(1) as BORDEREAUX_YN,
# MAGIC 			it03m_scheme_product.bord_provisional::VARCHAR(1) as BORD_PROVISIONAL,
# MAGIC 			coalesce(it03m_scheme_product.broker_service_fee, 0)::numeric(38,
# MAGIC 			20) as BROKER_SERVICE_FEE,
# MAGIC 			it03m_scheme_product.budget_percentage::numeric(38,
# MAGIC 			20) as BUDGET_PERCENTAGE,
# MAGIC 			it03m_scheme_product.cacelation_date::TIMESTAMP as CANCELATION_DATE,
# MAGIC 			it03m_scheme_product.cancel_code::BIGINT as CANCEL_CODE,
# MAGIC 			case
# MAGIC 				when it03m_scheme_product.cancel_code = 0
# MAGIC 				or it03m_scheme_product.cancel_code is null then 'Active'
# MAGIC 				else 'Cancelled'
# MAGIC 			end::VARCHAR(50) as CANCEL_CODE_DESC,
# MAGIC 			coalesce(it03m_scheme_product.claim_mandate, 0)::numeric(38,
# MAGIC 			20) as CLAIM_MANDATE,
# MAGIC 			it03m_scheme_product.claim_mandate_ind::VARCHAR(1) as CLAIM_MANDATE_IND,
# MAGIC 			it03m_scheme_product.division::VARCHAR(5) as DIVISION,
# MAGIC 			it03m_scheme_product.eft_yn::VARCHAR(1) as EFT_YN,
# MAGIC 			it03m_scheme_product.end_date::TIMESTAMP as END_DATE,
# MAGIC 			it03m_scheme_product.admin_fee_percentage::numeric(38,
# MAGIC 			20) as FEE_PERCENTAGE,
# MAGIC 			it03m_scheme.scheme_cd::VARCHAR(100) as GS_CODE,
# MAGIC 			it03m_scheme.scheme_name::VARCHAR(200) as GS_NAME,
# MAGIC 			it03m_scheme_product.tia_commit_date::TIMESTAMP as LAST_UPDATE_DATE,
# MAGIC 			user_details.user_id::VARCHAR(50) as LAST_UPDATE_USER,
# MAGIC 			it03m_scheme_product.linked_schd_prod_id::VARCHAR(8) as LINKED_SCHD_PROD_ID,
# MAGIC 			it03m_scheme_product.monthly_divisor::VARCHAR(10) as MONTHLY_DIVISOR,
# MAGIC 			it03m_scheme_product.old_scheme_prod_no::VARCHAR(100) as OLD_SCHEME_PROD_NO,
# MAGIC 			it03m_scheme_product.prefix::VARCHAR(2000) as PREFIX,
# MAGIC 			mst_i03m_prod_definition.mst_val_si_desc::VARCHAR(500) as PROD_DEFINITION,
# MAGIC 			mst_i03m_prod_group.mst_val_si_desc::VARCHAR(500) as PROD_GROUP,
# MAGIC 			it03m_scheme_product.prod_id::VARCHAR(8) as PROD_ID,
# MAGIC 			it03m_scheme_product.scheme_prod_name::VARCHAR(2000) as PROD_NAME,
# MAGIC 			it03m_scheme_product.scheme_prod_no::VARCHAR(100) as PROD_NO,
# MAGIC 			case
# MAGIC 				when it03m_scheme_product.scheme_prod_definition = '10' then 'UMA'
# MAGIC 				else mst_i03m_prod_type.mst_val_si_desc
# MAGIC 			end::VARCHAR(500) as PROD_TYPE,
# MAGIC 			it03m_scheme_product.qs_ind::VARCHAR(5) as QS_IND,
# MAGIC 			it03m_scheme_product.record_timestamp::TIMESTAMP as RECORD_TIMESTAMP,
# MAGIC 			it03m_scheme_product.record_userid::VARCHAR(8) as RECORD_USERID,
# MAGIC 			it03m_scheme_product.reporting_line::VARCHAR(10) as REPORTING_LINE,
# MAGIC 			it03m_scheme_product.sasria_scheme_yn::VARCHAR(1) as SASRIA_SCHEME_YN,
# MAGIC 			it03m_scheme_product.schd_auto_reversal_switch::VARCHAR(1) as SCHD_AUTO_REVERSAL_SWITCH,
# MAGIC 			concat(it03m_scheme_product.seq_no, '|', it03m_scheme_product.scheme_seq_no, '|' , '101', '|', 'TIA')::VARCHAR(50) as SCHEME_PRODUCT_OMI_ID,
# MAGIC 			it03m_scheme_product.scheme_seq_no::numeric(38,
# MAGIC 			20) as SCHEME_SEQ_NO,
# MAGIC 			it03m_scheme_product.seq_no::numeric(38,
# MAGIC 			20) as SEQ_NO,
# MAGIC 			it03m_scheme_product.start_date::TIMESTAMP as START_DATE,
# MAGIC 			it03m_scheme_product.stride_enabled_from::TIMESTAMP as STRIDE_ENABLED_FROM,
# MAGIC 			it03m_scheme_product.stride_enabled_yn::VARCHAR(1) as STRIDE_ENABLED_YN,
# MAGIC 			it03m_scheme_product.swiftcare_yn::VARCHAR(1) as SWIFTCARE_YN,
# MAGIC 			it03m_scheme_product.timestamp::TIMESTAMP as TIMESTAMP,
# MAGIC 			mst_i03m_tp_provider.mst_val_si_desc::VARCHAR(500) as TP_PROVIDER,
# MAGIC 			it03m_scheme_product.trans_type::VARCHAR(2) as TRANS_TYPE,
# MAGIC 			it03m_scheme_product.userid::VARCHAR(8) as USERID,
# MAGIC             null as aud_src_sys_cd
# MAGIC 		from
# MAGIC 			`omi-catalog`.tia_ods.it03m_scheme_product it03m_scheme_product
# MAGIC 		left join `omi-catalog`.si_dwh.mst_table_val_list mst_i03m_administrator on
# MAGIC 			it03m_scheme_product.administrator = mst_i03m_administrator.mst_val_si_cd
# MAGIC 			and mst_i03m_administrator.mst_tbl_nm_shrt = 'I03M_ADMINISTRATOR'
# MAGIC 			and mst_i03m_administrator.mst_val_si_language = 'en'
# MAGIC 		inner join `omi-catalog`.tia_ods.it03m_scheme it03m_scheme on
# MAGIC 			it03m_scheme_product.scheme_seq_no = it03m_scheme.seq_no
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_details on
# MAGIC 			it03m_scheme_product.record_userid = user_details.user_num
# MAGIC 		left join `omi-catalog`.si_dwh.mst_table_val_list mst_i03m_prod_definition on
# MAGIC 			it03m_scheme_product.scheme_prod_definition = mst_i03m_prod_definition.mst_val_si_cd
# MAGIC 			and mst_i03m_prod_definition.mst_tbl_nm_shrt = 'I03M_PROD_DEFINITION'
# MAGIC 			and mst_i03m_prod_definition.mst_val_si_language = 'en'
# MAGIC 		left join `omi-catalog`.si_dwh.mst_table_val_list mst_i03m_prod_group on
# MAGIC 			it03m_scheme_product.scheme_prod_group = mst_i03m_prod_group.mst_val_si_cd
# MAGIC 			and mst_i03m_prod_group.mst_tbl_nm_shrt = 'I03M_PROD_GROUP'
# MAGIC 			and mst_i03m_prod_group.mst_val_si_language = 'en'
# MAGIC 		left join `omi-catalog`.si_dwh.mst_table_val_list mst_i03m_prod_type on
# MAGIC 			it03m_scheme_product.scheme_prod_type = mst_i03m_prod_type.mst_val_si_cd
# MAGIC 			and mst_i03m_prod_type.mst_tbl_nm_shrt = 'I03M_PROD_TYPE'
# MAGIC 			and mst_i03m_prod_type.mst_val_si_language = 'en'
# MAGIC 		left join `omi-catalog`.si_dwh.mst_table_val_list mst_i03m_tp_provider on
# MAGIC 			it03m_scheme_product.tp_provider = mst_i03m_tp_provider.mst_val_si_cd
# MAGIC 			and mst_i03m_tp_provider.mst_tbl_nm_shrt = 'I03M_TP_PROVIDER'
# MAGIC 			and mst_i03m_tp_provider.mst_val_si_language = 'en'

# COMMAND ----------

# MAGIC %sql
# MAGIC select SCHEME_PRODUCT_OMI_ID,count(*) from `omi-catalog`.si_dwh.SCHEME_PRODUCT_OMI
# MAGIC group by SCHEME_PRODUCT_OMI_ID
# MAGIC having count(*)>1;

# COMMAND ----------

# MAGIC %sql
# MAGIC select SCHEME_PRODUCT_OMI_ID from `omi-catalog`.si_dwh.SCHEME_PRODUCT_OMI
# MAGIC where (SCHEME_PRODUCT_OMI_ID is null);

# COMMAND ----------

# MAGIC %sql
# MAGIC ---source Count check
# MAGIC select 'SRC',Count(*) from (select concat(cast(sp.seq_no as integer),'|', cast(sp.scheme_seq_no as integer) , '|' ,'101','|','TIA') SCHEME_PRODUCT_OMI_ID,
# MAGIC sp.seq_no,sp.scheme_seq_no ,sp.scheme_prod_no,sp.scheme_prod_name,sp.prod_id,sp.cancel_code,
# MAGIC case when sp.cancel_code =0 or sp.cancel_code is null then 'Active' else 'Cancelled' end as cancel_code_desc,sc.scheme_cd,sc.scheme_name,
# MAGIC case when sp.scheme_prod_definition = '10' then 'UMA' else mst1.description end prod_type,sp.monthly_divisor,
# MAGIC mst2.description,mst3.description,mst4.description,mst5.description,sp.claim_mandate_ind,sp.sasria_scheme_yn,sp.swiftcare_yn
# MAGIC ,sp.bordereaux_yn,bordereaux_import,sp.eft_yn,sp.qs_ind,sp.bord_provisional,sp.division,sp.old_scheme_prod_no,sp.prefix
# MAGIC ,sp.trans_type,sp.linked_schd_prod_id,sp.schd_auto_reversal_switch,sp.stride_enabled_yn,sp.reporting_line,
# MAGIC case when sp.reporting_line = '1' then 'BINDER'
# MAGIC when sp.reporting_line = '2' then 'FULL BINDER'
# MAGIC when sp.reporting_line = '3' then 'OUTSOURCE'
# MAGIC when sp.reporting_line = '4' then 'UMA'
# MAGIC Else null end as agreement_type,sp.admin_fee_effective_date,sp.start_date,sp.end_date,sp.cacelation_date,sp.timestamp
# MAGIC ,sp.userid,sp.record_timestamp,sp.record_userid,sp.stride_enabled_from,
# MAGIC coalesce(sp.binder_fee,0) as binder_fee,coalesce(sp.claim_mandate,0) claim_mandate,
# MAGIC coalesce(sp.binder_fee,0) + coalesce(sp.claim_mandate,0) as admin_fee_percentage,
# MAGIC sp.admin_fee_percentage,sp.budget_percentage,coalesce(sp.broker_service_fee,0) broker_service_fee,
# MAGIC 'TIA' as AUD_SRC_SYS_NM,concat(cast(sp.seq_no as integer),'|', cast(sp.scheme_seq_no as integer), '|' ,'101','|','TIA')	as AUD_SRC_SYS_ID,
# MAGIC sp.tia_commit_date,`omi-catalog`.si_dwh.USER_DETAILS.USER_ID
# MAGIC from `omi-catalog`.tia_ods.it03m_scheme_product sp
# MAGIC inner join `omi-catalog`.tia_ods.it03m_scheme sc on sp.scheme_seq_no = sc.seq_no
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_PROD_TYPE' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst1 on sp.scheme_prod_type=mst1.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_TP_PROVIDER' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst2 on sp.tp_provider=mst2.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_ADMINISTRATOR' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst3 on sp.ADMINISTRATOR=mst3.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_PROD_DEFINITION' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst4 on sp.scheme_prod_definition=mst4.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_PROD_GROUP' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst5 on sp.scheme_prod_group=mst5.code
# MAGIC left outer join `omi-catalog`.si_dwh.USER_DETAILS on sp.RECORD_USERID = USER_DETAILS.USER_NUM)A
# MAGIC --Target count check
# MAGIC union all
# MAGIC select 'TGT',Count(*) from (select SCHEME_PRODUCT_OMI_ID,seq_no,scheme_seq_no ,prod_no,prod_name,prod_id,cancel_code,cancel_code_desc,gs_code,gs_name,prod_type,monthly_divisor,
# MAGIC tp_provider,administrator,prod_definition,prod_group,claim_mandate_ind,sasria_scheme_yn,swiftcare_yn,bordereaux_yn,bordereaux_import,
# MAGIC eft_yn,qs_ind,bord_provisional,division,old_scheme_prod_no,prefix,trans_type,linked_schd_prod_id,schd_auto_reversal_switch,stride_enabled_yn,
# MAGIC reporting_line,agreement_type,admin_fee_effective_date,start_date,end_date,cancelation_date,timestamp,userid,record_timestamp,record_userid,
# MAGIC stride_enabled_from,binder_fee,claim_mandate,admin_fee_percentage,fee_percentage,budget_percentage,broker_service_fee,AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,LAST_UPDATE_DATE,LAST_UPDATE_USER from `omi-catalog`.si_dwh.SCHEME_PRODUCT_OMI)B;

# COMMAND ----------

# MAGIC %sql
# MAGIC -----------------------------------------------------SRC-TGT------------------------------------
# MAGIC 
# MAGIC select 
# MAGIC concat(cast(sp.seq_no as integer),'|', cast(sp.scheme_seq_no as integer) , '|' ,'101','|','TIA') SCHEME_PRODUCT_OMI_ID,
# MAGIC sp.seq_no,sp.scheme_seq_no ,sp.scheme_prod_no,sp.scheme_prod_name,sp.prod_id,sp.cancel_code,
# MAGIC case when sp.cancel_code =0 or sp.cancel_code is null then 'Active' else 'Cancelled' end as cancel_code_desc,sc.scheme_cd,sc.scheme_name,
# MAGIC case when sp.scheme_prod_definition = '10' then 'UMA' else mst1.description end prod_type,sp.monthly_divisor,
# MAGIC mst2.description,mst3.description,mst4.description,mst5.description,sp.claim_mandate_ind,sp.sasria_scheme_yn,sp.swiftcare_yn
# MAGIC ,sp.bordereaux_yn,bordereaux_import,sp.eft_yn,sp.qs_ind,sp.bord_provisional,sp.division,sp.old_scheme_prod_no,sp.prefix
# MAGIC ,sp.trans_type,sp.linked_schd_prod_id,sp.schd_auto_reversal_switch,sp.stride_enabled_yn,sp.reporting_line,
# MAGIC case when sp.reporting_line = '1' then 'BINDER'
# MAGIC when sp.reporting_line = '2' then 'FULL BINDER'
# MAGIC when sp.reporting_line = '3' then 'OUTSOURCE'
# MAGIC when sp.reporting_line = '4' then 'UMA'
# MAGIC Else null end as agreement_type,sp.admin_fee_effective_date,sp.start_date,sp.end_date,sp.cacelation_date,sp.timestamp
# MAGIC ,sp.userid,sp.record_timestamp,sp.record_userid,sp.stride_enabled_from,
# MAGIC round (coalesce(sp.binder_fee::numeric,0),20) as binder_fee,
# MAGIC round (coalesce(sp.claim_mandate::numeric,0),20) claim_mandate,
# MAGIC round (coalesce(sp.binder_fee::numeric,0) + coalesce(sp.claim_mandate::numeric,0),20) as admin_fee_percentage,
# MAGIC sp.admin_fee_percentage,sp.budget_percentage,
# MAGIC round(coalesce(sp.broker_service_fee::numeric,0),20) broker_service_fee,
# MAGIC 'TIA' as AUD_SRC_SYS_NM,concat(cast(sp.seq_no as integer),'|', cast(sp.scheme_seq_no as integer), '|' ,'101','|','TIA')	as AUD_SRC_SYS_ID,
# MAGIC sp.tia_commit_date,`omi-catalog`.si_dwh.USER_DETAILS.USER_ID
# MAGIC from `omi-catalog`.tia_ods.it03m_scheme_product sp
# MAGIC inner join `omi-catalog`.tia_ods.it03m_scheme sc on sp.scheme_seq_no = sc.seq_no
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_PROD_TYPE' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst1 on sp.scheme_prod_type=mst1.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_TP_PROVIDER' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst2 on sp.tp_provider=mst2.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_ADMINISTRATOR' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst3 on sp.ADMINISTRATOR=mst3.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_PROD_DEFINITION' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst4 on sp.scheme_prod_definition=mst4.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_PROD_GROUP' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst5 on sp.scheme_prod_group=mst5.code
# MAGIC left outer join `omi-catalog`.si_dwh.USER_DETAILS on sp.RECORD_USERID = USER_DETAILS.USER_NUM
# MAGIC --order by seq_no
# MAGIC except 
# MAGIC 
# MAGIC 
# MAGIC select 
# MAGIC SCHEME_PRODUCT_OMI_ID,
# MAGIC seq_no,scheme_seq_no ,prod_no,prod_name,prod_id,cancel_code,cancel_code_desc,gs_code,gs_name,prod_type,monthly_divisor,
# MAGIC tp_provider,administrator,prod_definition,prod_group,claim_mandate_ind,sasria_scheme_yn,swiftcare_yn,bordereaux_yn,bordereaux_import,
# MAGIC eft_yn,qs_ind,bord_provisional,division,old_scheme_prod_no,prefix,trans_type,linked_schd_prod_id,schd_auto_reversal_switch,stride_enabled_yn,
# MAGIC reporting_line,agreement_type,admin_fee_effective_date,start_date,end_date,cancelation_date,timestamp,userid,record_timestamp,record_userid,
# MAGIC stride_enabled_from,binder_fee,claim_mandate,admin_fee_percentage,fee_percentage,budget_percentage,broker_service_fee,AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,LAST_UPDATE_DATE,LAST_UPDATE_USER from `omi-catalog`.si_dwh.SCHEME_PRODUCT_OMI 
# MAGIC --order by seq_no;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'SRC',* from (select 
# MAGIC concat(cast(sp.seq_no as integer),'|', cast(sp.scheme_seq_no as integer) , '|' ,'101','|','TIA') SCHEME_PRODUCT_OMI_ID,
# MAGIC sp.seq_no,sp.scheme_seq_no ,sp.scheme_prod_no,sp.scheme_prod_name,sp.prod_id,sp.cancel_code,
# MAGIC case when sp.cancel_code =0 or sp.cancel_code is null then 'Active' else 'Cancelled' end as cancel_code_desc,sc.scheme_cd,sc.scheme_name,
# MAGIC case when sp.scheme_prod_definition = '10' then 'UMA' else mst1.description end prod_type,
# MAGIC sp.monthly_divisor,
# MAGIC mst2.description,mst3.description,mst4.description,mst5.description,
# MAGIC sp.claim_mandate_ind,sp.sasria_scheme_yn,sp.swiftcare_yn
# MAGIC ,sp.bordereaux_yn,bordereaux_import,sp.eft_yn,sp.qs_ind,sp.bord_provisional,sp.division,sp.old_scheme_prod_no,sp.prefix
# MAGIC ,sp.trans_type,sp.linked_schd_prod_id,sp.schd_auto_reversal_switch,sp.stride_enabled_yn,sp.reporting_line,
# MAGIC case when sp.reporting_line = '1' then 'BINDER'
# MAGIC when sp.reporting_line = '2' then 'FULL BINDER'
# MAGIC when sp.reporting_line = '3' then 'OUTSOURCE'
# MAGIC when sp.reporting_line = '4' then 'UMA'
# MAGIC Else null end as agreement_type,sp.admin_fee_effective_date,sp.start_date,sp.end_date,sp.cacelation_date,sp.timestamp
# MAGIC ,sp.userid,sp.record_timestamp,sp.record_userid,sp.stride_enabled_from,
# MAGIC round (coalesce(sp.binder_fee::numeric,0),20) as binder_fee,
# MAGIC round (coalesce(sp.claim_mandate::numeric,0),20) claim_mandate,
# MAGIC round (coalesce(sp.binder_fee::numeric,0) + coalesce(sp.claim_mandate::numeric,0),20) as admin_fee_percentage,
# MAGIC sp.admin_fee_percentage,sp.budget_percentage,
# MAGIC round(coalesce(sp.broker_service_fee::numeric,0),20) broker_service_fee,
# MAGIC 'TIA' as AUD_SRC_SYS_NM,concat(cast(sp.seq_no as integer),'|', cast(sp.scheme_seq_no as integer), '|' ,'101','|','TIA')	as AUD_SRC_SYS_ID,
# MAGIC sp.tia_commit_date,`omi-catalog`.si_dwh.USER_DETAILS.USER_ID
# MAGIC from `omi-catalog`.tia_ods.it03m_scheme_product sp
# MAGIC inner join `omi-catalog`.tia_ods.it03m_scheme sc on sp.scheme_seq_no = sc.seq_no
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_PROD_TYPE' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst1 on sp.scheme_prod_type=mst1.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_TP_PROVIDER' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst2 on sp.tp_provider=mst2.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_ADMINISTRATOR' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst3 on sp.ADMINISTRATOR=mst3.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_PROD_DEFINITION' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst4 on sp.scheme_prod_definition=mst4.code
# MAGIC left outer join (select val.mst_val_si_cd code,val.mst_val_si_desc description,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_mapping_table_list mst,`omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 				where mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				and mst.mst_tbl_nm_shrt='I03M_PROD_GROUP' and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst5 on sp.scheme_prod_group=mst5.code
# MAGIC left outer join `omi-catalog`.si_dwh.USER_DETAILS on sp.RECORD_USERID = USER_DETAILS.USER_NUM)m 
# MAGIC where SCHEME_PRODUCT_OMI_ID = '9170|46|101|TIA'
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 'TGT' ,SCHEME_PRODUCT_OMI_ID,
# MAGIC seq_no,scheme_seq_no ,prod_no,prod_name,prod_id,cancel_code,cancel_code_desc,gs_code,gs_name,prod_type,monthly_divisor,
# MAGIC tp_provider,administrator,prod_definition,prod_group,claim_mandate_ind,sasria_scheme_yn,swiftcare_yn,bordereaux_yn,bordereaux_import,
# MAGIC eft_yn,qs_ind,bord_provisional,division,old_scheme_prod_no,prefix,trans_type,linked_schd_prod_id,schd_auto_reversal_switch,stride_enabled_yn,
# MAGIC reporting_line,agreement_type,admin_fee_effective_date,start_date,end_date,cancelation_date,timestamp,userid,record_timestamp,record_userid,
# MAGIC stride_enabled_from,binder_fee,claim_mandate,admin_fee_percentage,fee_percentage,budget_percentage,broker_service_fee,AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,LAST_UPDATE_DATE,LAST_UPDATE_USER from `omi-catalog`.si_dwh.SCHEME_PRODUCT_OMI 
# MAGIC where SCHEME_PRODUCT_OMI_ID = '9170|46|101|TIA'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.si_dwh.scheme_product_omi

# COMMAND ----------

# MAGIC %sql
# MAGIC select concat(cast(sp.seq_no as integer),'|', cast(sp.scheme_seq_no as integer) , '|' ,'101','|','TIA') SCHEME_PRODUCT_OMI_ID,
# MAGIC sp.seq_no,sp.scheme_seq_no ,sp.scheme_prod_no,sp.scheme_prod_name,sp.prod_id,sp.cancel_code,
# MAGIC case when sp.cancel_code =0 or sp.cancel_code is null then 'Active' else 'Cancelled' end as cancel_code_desc,sc.scheme_cd,sc.scheme_name,
# MAGIC case when sp.scheme_prod_definition = '10' then 'UMA' else mst1.description end prod_type,sp.monthly_divisor,
# MAGIC mst2.description,mst3.description,mst4.description,mst5.description,sp.claim_mandate_ind,sp.sasria_scheme_yn,sp.swiftcare_yn
# MAGIC ,sp.bordereaux_yn,bordereaux_import,sp.eft_yn,sp.qs_ind,sp.bord_provisional,sp.division,sp.old_scheme_prod_no,sp.prefix
# MAGIC ,sp.trans_type,sp.linked_schd_prod_id,sp.schd_auto_reversal_switch,sp.stride_enabled_yn,sp.reporting_line,
# MAGIC case when sp.reporting_line = '1' then 'BINDER'
# MAGIC when sp.reporting_line = '2' then 'FULL BINDER'
# MAGIC when sp.reporting_line = '3' then 'OUTSOURCE'
# MAGIC when sp.reporting_line = '4' then 'UMA'
# MAGIC Else null end as agreement_type,sp.admin_fee_effective_date,sp.start_date,sp.end_date,sp.cacelation_date,sp.timestamp
# MAGIC ,sp.userid,sp.record_timestamp,sp.record_userid,sp.stride_enabled_from,
# MAGIC coalesce(sp.binder_fee,0) as binder_fee,coalesce(sp.claim_mandate,0) claim_mandate,
# MAGIC coalesce(sp.binder_fee,0) + coalesce(sp.claim_mandate,0) as admin_fee_percentage,
# MAGIC sp.admin_fee_percentage,sp.budget_percentage,coalesce(sp.broker_service_fee,0) broker_service_fee,
# MAGIC 'TIA' as AUD_SRC_SYS_NM,concat(cast(sp.seq_no as integer),'|', cast(sp.scheme_seq_no as integer), '|' ,'101','|','TIA')	as AUD_SRC_SYS_ID,
# MAGIC sp.tia_commit_date,`omi-catalog`.si_dwh.USER_DETAILS.USER_ID
# MAGIC from `omi-catalog`.tia_ods.it03m_scheme_product sp
# MAGIC inner join `omi-catalog`.tia_ods.it03m_scheme sc on sp.scheme_seq_no = sc.seq_no
# MAGIC left outer join (select  val.mst_val_si_cd code,val.mst_val_si_desc description--,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_table_val_list val --si_dwh.mst_mapping_table_list mst,
# MAGIC 				where-- mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				val.mst_tbl_nm_shrt='I03M_PROD_TYPE' --and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst1 on sp.scheme_prod_type=mst1.code
# MAGIC left outer join (select  val.mst_val_si_cd code,val.mst_val_si_desc description--,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_table_val_list val --si_dwh.mst_mapping_table_list mst,
# MAGIC 				where-- mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				val.mst_tbl_nm_shrt='I03M_TP_PROVIDER' --and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst2 on sp.tp_provider=mst2.code
# MAGIC left outer join (select  val.mst_val_si_cd code,val.mst_val_si_desc description--,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_table_val_list val --si_dwh.mst_mapping_table_list mst,
# MAGIC 				where-- mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				val.mst_tbl_nm_shrt='I03M_ADMINISTRATOR' --and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst3 on sp.ADMINISTRATOR=mst3.code
# MAGIC left outer join (select  val.mst_val_si_cd code,val.mst_val_si_desc description--,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_table_val_list val --si_dwh.mst_mapping_table_list mst,
# MAGIC 				where-- mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				val.mst_tbl_nm_shrt='I03M_PROD_DEFINITION' --and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst4 on sp.scheme_prod_definition=mst4.code
# MAGIC left outer join (select  val.mst_val_si_cd code,val.mst_val_si_desc description--,mst.mst_tbl_nm_shrt 
# MAGIC 				from `omi-catalog`.si_dwh.mst_table_val_list val --si_dwh.mst_mapping_table_list mst,
# MAGIC 				where-- mst.mst_tbl_si_id = val.mst_tbl_si_cd 
# MAGIC 				val.mst_tbl_nm_shrt='I03M_PROD_GROUP' --and mst.src_sys_nm = 'TIA' 
# MAGIC 				and val.mst_val_si_language = 'en') mst5 on sp.scheme_prod_group=mst5.code
# MAGIC left outer join `omi-catalog`.si_dwh.USER_DETAILS on sp.RECORD_USERID = USER_DETAILS.USER_NUM
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC select SCHEME_PRODUCT_OMI_ID,seq_no,scheme_seq_no ,prod_no,prod_name,prod_id,cancel_code,cancel_code_desc,gs_code,gs_name,prod_type,monthly_divisor,
# MAGIC tp_provider,administrator,prod_definition,prod_group,claim_mandate_ind,sasria_scheme_yn,swiftcare_yn,bordereaux_yn,bordereaux_import,
# MAGIC eft_yn,qs_ind,bord_provisional,division,old_scheme_prod_no,prefix,trans_type,linked_schd_prod_id,schd_auto_reversal_switch,stride_enabled_yn,
# MAGIC reporting_line,agreement_type,admin_fee_effective_date,start_date,end_date,cancelation_date,timestamp,userid,record_timestamp,record_userid,
# MAGIC stride_enabled_from,binder_fee,claim_mandate,admin_fee_percentage,fee_percentage,budget_percentage,broker_service_fee,AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,LAST_UPDATE_DATE,LAST_UPDATE_USER from `omi-catalog`.si_dwh.SCHEME_PRODUCT_OMI
# MAGIC --order by seq_no;

# COMMAND ----------


