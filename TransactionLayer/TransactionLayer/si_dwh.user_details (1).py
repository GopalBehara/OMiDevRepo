# Databricks notebook source
# MAGIC %sql
# MAGIC insert
# MAGIC 	into
# MAGIC 	`omi-catalog`.si_dwh.user_details(AUD_BATCH_ID,
# MAGIC 	AUD_IU_FLAG,
# MAGIC 	AUD_SRC_SYS_ID,
# MAGIC 	AUD_SRC_SYS_NM,
# MAGIC 	AUD_SUB_BATCH_ID,
# MAGIC 	AUD_TRAN_DT,
# MAGIC 	COMPANY_ID,
# MAGIC 	COMPANY_NAME,
# MAGIC 	COMPANY_NUM,
# MAGIC 	DESIGNATION,
# MAGIC 	DESIGNATION_CD,
# MAGIC 	ENTITY_ID,
# MAGIC 	ENTITY_NUM,
# MAGIC 	INSERT_DATE,
# MAGIC 	INSERT_USER,
# MAGIC 	INTERNAL_UNIT_TYPE_CD,
# MAGIC 	INTERNAL_UNIT_TYPE_DESC,
# MAGIC 	LAST_UPDATE_DATE,
# MAGIC 	LAST_UPDATE_USER,
# MAGIC 	PREF_LANG_CD,
# MAGIC 	USER_ID,
# MAGIC 	USER_ID_END_DT,
# MAGIC 	USER_ID_START_DT,
# MAGIC 	USER_NAME,
# MAGIC 	USER_NUM,
# MAGIC     branch_hierarchy_id,
# MAGIC     branch_cd,
# MAGIC     branch_desc,
# MAGIC     zone_cd,
# MAGIC     zone_desc,
# MAGIC     region_cd,
# MAGIC     region_desc,
# MAGIC     reporting_to_entity,
# MAGIC     joining_dt,
# MAGIC     password_txt,
# MAGIC     password_txt_dt,
# MAGIC     password_txt_1,
# MAGIC     password_txt_1_dt,
# MAGIC     password_txt_2,
# MAGIC     password_txt_2_dt,
# MAGIC     password_change_days,
# MAGIC     number_of_failed_login_attempt,
# MAGIC     last_login_dt,
# MAGIC     last_login_time,
# MAGIC     password_lock_ind,
# MAGIC     aud_src_sys_cd,
# MAGIC     aud_src_sys_updt_ver_id,
# MAGIC     is_active_ind,
# MAGIC     level_of_authority,
# MAGIC     email_address,
# MAGIC     is_underwriter_ind,
# MAGIC     restrict_to_teams_ind,
# MAGIC     is_service_associate_ind,
# MAGIC     account_locked_date,
# MAGIC     last_bad_login_attempt_date,
# MAGIC     must_change_password_ind,
# MAGIC     is_agent_ind,
# MAGIC     agent_num,
# MAGIC     is_agency_ind,
# MAGIC     agency_num,
# MAGIC     deleted_ind,
# MAGIC     use_single_sign_on_ind,
# MAGIC     last_recover_pass_attempt_date,
# MAGIC     load_balancing_factor,
# MAGIC     out_of_office_ind,
# MAGIC     restrict_to_assigned_cases_ind,
# MAGIC     record_user_id)
# MAGIC 		select
# MAGIC 			45 as AUD_BATCH_ID,
# MAGIC 			0 as AUD_IU_FLAG,
# MAGIC 			TOP_USER.USER_ID::VARCHAR(50) as AUD_SRC_SYS_ID,
# MAGIC 			concat('TIA', '')::VARCHAR(10) as AUD_SRC_SYS_NM,
# MAGIC 			1 as AUD_SUB_BATCH_ID,
# MAGIC 			TOP_USER.record_timestamp::TIMESTAMP as AUD_TRAN_DT,
# MAGIC 			TOP_USER.SITE_SEQ_NO::INTEGER as COMPANY_ID,
# MAGIC 			COMPANY.COMPANY_NAME::VARCHAR(255) as COMPANY_NAME,
# MAGIC 			TOP_USER.COMPANY_NO::INTEGER as COMPANY_NUM,
# MAGIC 			MST_JOB_NAME.DESCRIPTION::VARCHAR(500) as DESIGNATION,
# MAGIC 			TOP_USER.JOB_CODE::VARCHAR(255) as DESIGNATION_CD,
# MAGIC 			case
# MAGIC 				when name.id_no is not null then concat(name.id_no, '|', '101', '|', 'TIA')
# MAGIC 				else null
# MAGIC 			end::VARCHAR(50) as ENTITY_ID,
# MAGIC 			NAME.ID_NO::BIGINT as ENTITY_NUM,
# MAGIC 			TOP_USER.TIMESTAMP::TIMESTAMP as INSERT_DATE,
# MAGIC 			case
# MAGIC 				when top_user.userid is not null then concat(top_user.userid, '|', insert_user.ID_NO, '|', '101', '|', 'TIA')
# MAGIC 				else null
# MAGIC 			end::VARCHAR(50) as INSERT_USER,
# MAGIC 			TOP_USER.DEPT_NO::VARCHAR(255) as INTERNAL_UNIT_TYPE_CD,
# MAGIC 			DEPARTMENT.DEPT_NAME::VARCHAR(500) as INTERNAL_UNIT_TYPE_DESC,
# MAGIC 			TOP_USER.tia_commit_date::TIMESTAMP as LAST_UPDATE_DATE,
# MAGIC 			case
# MAGIC 				when top_user.record_userid is not null then concat(top_user.record_userid, '|', record_user.ID_NO, '|', '101', '|', 'TIA')
# MAGIC 				else null
# MAGIC 			end::VARCHAR(50) as LAST_UPDATE_USER,
# MAGIC 			TOP_USER.LANGUAGE::VARCHAR(10) as PREF_LANG_CD,
# MAGIC 			--concat(TOP_USER.USER_ID, '|', TOP_USER.ID_NO , '|', '101', '|', 'TIA')::VARCHAR(50) as USER_ID,
# MAGIC             concat(COALESCE (TOP_USER.USER_ID,''), '|', COALESCE (TOP_USER.ID_NO,'') , '|', '101', '|', 'TIA')::VARCHAR(50) as USER_ID,
# MAGIC            
# MAGIC 			TOP_USER.END_DATE::TIMESTAMP as USER_ID_END_DT,
# MAGIC 			TOP_USER.START_DATE::TIMESTAMP as USER_ID_START_DT,
# MAGIC 			TOP_USER.USER_NAME::VARCHAR(255) as USER_NAME,
# MAGIC 			TOP_USER.USER_ID::VARCHAR(8) as USER_NUM,
# MAGIC             null as branch_hierarchy_id,
# MAGIC             null as branch_cd,
# MAGIC             null as branch_desc,
# MAGIC             null as zone_cd,
# MAGIC             null as zone_desc,
# MAGIC             null as region_cd,
# MAGIC             null as region_desc,
# MAGIC             null as reporting_to_entity,
# MAGIC             null as joining_dt,
# MAGIC             null as password_txt,
# MAGIC             null as password_txt_dt,
# MAGIC             null as password_txt_1,
# MAGIC             null as password_txt_1_dt,
# MAGIC             null as password_txt_2,
# MAGIC             null as password_txt_2_dt,
# MAGIC             null as password_change_days,
# MAGIC             null as number_of_failed_login_attempt,
# MAGIC             null as last_login_dt,
# MAGIC             null as last_login_time,
# MAGIC             null as password_lock_ind,
# MAGIC             null as aud_src_sys_cd,
# MAGIC             null as aud_src_sys_updt_ver_id,
# MAGIC             null as is_active_ind,
# MAGIC             null as level_of_authority,
# MAGIC             null as email_address,
# MAGIC             null as is_underwriter_ind,
# MAGIC             null as restrict_to_teams_ind,
# MAGIC             null as is_service_associate_ind,
# MAGIC             null as account_locked_date,
# MAGIC             null as last_bad_login_attempt_date,
# MAGIC             null as must_change_password_ind,
# MAGIC             null as is_agent_ind,
# MAGIC             null as agent_num,
# MAGIC             null as is_agency_ind,
# MAGIC             null as agency_num,
# MAGIC             null as deleted_ind,
# MAGIC             null as use_single_sign_on_ind,
# MAGIC             null as last_recover_pass_attempt_date,
# MAGIC             null as load_balancing_factor,
# MAGIC             null as out_of_office_ind,
# MAGIC             null as restrict_to_assigned_cases_ind,
# MAGIC             null as record_user_id
# MAGIC 		from
# MAGIC 			`omi-catalog`.tia_ods.top_user TOP_USER
# MAGIC 		left join `omi-catalog`.tia_ods.company company on
# MAGIC 			TOP_USER.COMPANY_NO = COMPANY.COMPANY_NO
# MAGIC 		left join (
# MAGIC 			select
# MAGIC 				val.mst_val_si_cd code,
# MAGIC 				val.mst_val_si_desc description
# MAGIC 			from
# MAGIC 				`omi-catalog`.si_dwh.mst_mapping_table_list mst,
# MAGIC 				`omi-catalog`.si_dwh.mst_table_val_list val
# MAGIC 			where
# MAGIC 				mst.mst_tbl_si_id = val.mst_tbl_si_cd
# MAGIC 				and mst.src_sys_mst_tbl_nm = 'JOB_NAME'
# MAGIC 				and mst.src_sys_nm = 'TIA'
# MAGIC 				and val.mst_val_si_language = 'en') mst_job_name on
# MAGIC 			top_user.job_code = mst_job_name.code
# MAGIC 		left join `omi-catalog`.tia_ods.name name on
# MAGIC 			TOP_USER.ID_NO = NAME.ID_NO
# MAGIC 		left join `omi-catalog`.tia_ods.top_user insert_user on
# MAGIC 			TOP_USER.USERID = insert_user.USER_ID
# MAGIC 		left join `omi-catalog`.tia_ods.department department on
# MAGIC 			TOP_USER.DEPT_NO = DEPARTMENT.DEPT_NO
# MAGIC 			and TOP_USER.COMPANY_NO = DEPARTMENT.COMPANY_NO
# MAGIC 		left join `omi-catalog`.tia_ods.top_user record_user on
# MAGIC 			TOP_USER.RECORD_USERID = record_user.USER_ID;

# COMMAND ----------

# MAGIC %sql
# MAGIC select USER_ID,user_id1,user_num from (
# MAGIC select
# MAGIC 			45 as AUD_BATCH_ID,
# MAGIC 			0 as AUD_IU_FLAG,
# MAGIC 			TOP_USER.USER_ID::VARCHAR(50) as AUD_SRC_SYS_ID,
# MAGIC 			concat('TIA', '')::VARCHAR(10) as AUD_SRC_SYS_NM,
# MAGIC 			1 as AUD_SUB_BATCH_ID,
# MAGIC 			TOP_USER.record_timestamp::TIMESTAMP as AUD_TRAN_DT,
# MAGIC 			TOP_USER.SITE_SEQ_NO::INTEGER as COMPANY_ID,
# MAGIC 			COMPANY.COMPANY_NAME::VARCHAR(255) as COMPANY_NAME,
# MAGIC 			TOP_USER.COMPANY_NO::INTEGER as COMPANY_NUM,
# MAGIC 			MST_JOB_NAME.DESCRIPTION::VARCHAR(500) as DESIGNATION,
# MAGIC 			TOP_USER.JOB_CODE::VARCHAR(255) as DESIGNATION_CD,
# MAGIC 			case
# MAGIC 				when name.id_no is not null then concat(name.id_no, '|', '101', '|', 'TIA')
# MAGIC 				else null
# MAGIC 			end::VARCHAR(50) as ENTITY_ID,
# MAGIC 			NAME.ID_NO::INT as ENTITY_NUM,
# MAGIC 			TOP_USER.TIMESTAMP::TIMESTAMP as INSERT_DATE,
# MAGIC 			case
# MAGIC 				when top_user.userid is not null then concat(top_user.userid, '|', insert_user.ID_NO, '|', '101', '|', 'TIA')
# MAGIC 				else null
# MAGIC 			end::VARCHAR(50) as INSERT_USER,
# MAGIC 			TOP_USER.DEPT_NO::VARCHAR(255) as INTERNAL_UNIT_TYPE_CD,
# MAGIC 			DEPARTMENT.DEPT_NAME::VARCHAR(500) as INTERNAL_UNIT_TYPE_DESC,
# MAGIC 			TOP_USER.tia_commit_date::TIMESTAMP as LAST_UPDATE_DATE,
# MAGIC 			case
# MAGIC 				when top_user.record_userid is not null then concat(top_user.record_userid, '|', record_user.ID_NO, '|', '101', '|', 'TIA')
# MAGIC 				else null
# MAGIC 			end::VARCHAR(50) as LAST_UPDATE_USER,
# MAGIC 			TOP_USER.LANGUAGE::VARCHAR(10) as PREF_LANG_CD,
# MAGIC 			--concat(TOP_USER.USER_ID, '|', TOP_USER.ID_NO , '|', '101', '|', 'TIA')::VARCHAR(50) as USER_ID,
# MAGIC             concat(COALESCE (TOP_USER.USER_ID,''), '|', COALESCE (TOP_USER.ID_NO,'') , '|', '101', '|', 'TIA')::VARCHAR(50) as USER_ID,
# MAGIC              (TOP_USER.USER_ID||'|'||TOP_USER.ID_NO||'|'||'101'||'|'||'TIA')::VARCHAR(50) as USER_ID1,
# MAGIC 			TOP_USER.END_DATE::TIMESTAMP as USER_ID_END_DT,
# MAGIC 			TOP_USER.START_DATE::TIMESTAMP as USER_ID_START_DT,
# MAGIC 			TOP_USER.USER_NAME::VARCHAR(255) as USER_NAME,
# MAGIC 			TOP_USER.USER_ID::VARCHAR(8) as USER_NUM,
# MAGIC             null as branch_hierarchy_id,
# MAGIC             null as branch_cd,
# MAGIC             null as branch_desc,
# MAGIC             null as zone_cd,
# MAGIC             null as zone_desc,
# MAGIC             null as region_cd,
# MAGIC             null as region_desc,
# MAGIC             null as reporting_to_entity,
# MAGIC             null as joining_dt,
# MAGIC             null as password_txt,
# MAGIC             null as password_txt_dt,
# MAGIC             null as password_txt_1,
# MAGIC             null as password_txt_1_dt,
# MAGIC             null as password_txt_2,
# MAGIC             null as password_txt_2_dt,
# MAGIC             null as password_change_days,
# MAGIC             null as number_of_failed_login_attempt,
# MAGIC             null as last_login_dt,
# MAGIC             null as last_login_time,
# MAGIC             null as password_lock_ind,
# MAGIC             null as aud_src_sys_cd,
# MAGIC             null as aud_src_sys_updt_ver_id,
# MAGIC             null as is_active_ind,
# MAGIC             null as level_of_authority,
# MAGIC             null as email_address,
# MAGIC             null as is_underwriter_ind,
# MAGIC             null as restrict_to_teams_ind,
# MAGIC             null as is_service_associate_ind,
# MAGIC             null as account_locked_date,
# MAGIC             null as last_bad_login_attempt_date,
# MAGIC             null as must_change_password_ind,
# MAGIC             null as is_agent_ind,
# MAGIC             null as agent_num,
# MAGIC             null as is_agency_ind,
# MAGIC             null as agency_num,
# MAGIC             null as deleted_ind,
# MAGIC             null as use_single_sign_on_ind,
# MAGIC             null as last_recover_pass_attempt_date,
# MAGIC             null as load_balancing_factor,
# MAGIC             null as out_of_office_ind,
# MAGIC             null as restrict_to_assigned_cases_ind,
# MAGIC             null as record_user_id
# MAGIC 		from
# MAGIC 			`omi-catalog`.tia_ods.top_user TOP_USER
# MAGIC 		left join `omi-catalog`.tia_ods.company company on
# MAGIC 			TOP_USER.COMPANY_NO = COMPANY.COMPANY_NO
# MAGIC 		left join (
# MAGIC 			select
# MAGIC 				val.mst_val_si_cd code,
# MAGIC 				val.mst_val_si_desc description
# MAGIC 			from
# MAGIC 				`omi-catalog`.si_dwh.mst_mapping_table_list mst,
# MAGIC 				`omi-catalog`.si_dwh.mst_table_val_list val
# MAGIC 			where
# MAGIC 				mst.mst_tbl_si_id = val.mst_tbl_si_cd
# MAGIC 				and mst.src_sys_mst_tbl_nm = 'JOB_NAME'
# MAGIC 				and mst.src_sys_nm = 'TIA'
# MAGIC 				and val.mst_val_si_language = 'en') mst_job_name on
# MAGIC 			top_user.job_code = mst_job_name.code
# MAGIC 		left join `omi-catalog`.tia_ods.name name on
# MAGIC 			TOP_USER.ID_NO = NAME.ID_NO
# MAGIC 		left join `omi-catalog`.tia_ods.top_user insert_user on
# MAGIC 			TOP_USER.USERID = insert_user.USER_ID
# MAGIC 		left join `omi-catalog`.tia_ods.department department on
# MAGIC 			TOP_USER.DEPT_NO = DEPARTMENT.DEPT_NO
# MAGIC 			and TOP_USER.COMPANY_NO = DEPARTMENT.COMPANY_NO
# MAGIC 		left join `omi-catalog`.tia_ods.top_user record_user on
# MAGIC 			TOP_USER.RECORD_USERID = record_user.USER_ID) d;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC USER_ID,
# MAGIC USER_NUM,
# MAGIC COMPANY_ID from `omi-catalog`.si_dwh.user_details ud
# MAGIC where  USER_ID is null or USER_NUM is null or COMPANY_ID is null;

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC USER_ID,
# MAGIC USER_NUM,
# MAGIC COMPANY_ID,count(*) from `omi-catalog`.si_dwh.user_details ud 
# MAGIC group by  USER_ID,USER_NUM,COMPANY_ID
# MAGIC having count(*)>1; 

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table `omi-catalog`.si_dwh.user_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select 'SRC' as column_name, count (*)
# MAGIC FROM (SELECT 
# MAGIC concat(tu.USER_ID, '|', tu.ID_NO , '|', '101', '|', 'TIA')::VARCHAR(50) as USER_ID,
# MAGIC tu.USER_ID AS USER_NUM,
# MAGIC tu.SITE_SEQ_NO as COMPANY_ID,
# MAGIC case when n.id_no is not null then concat(n.id_no,'|','101','|','TIA') else null end as ENTITY_ID,
# MAGIC n.ID_NO as ENTITY_NUM,
# MAGIC tu.START_DATE as USER_ID_START_DT,
# MAGIC tu.USER_name as USER_name,
# MAGIC tu.DEPT_NO as INTERNAL_UNIT_TYPE_CD,
# MAGIC d.DEPT_name as INTERNAL_UNIT_TYPE_DESC,
# MAGIC tu.JOB_CODE as DESIGNATION_CD,
# MAGIC MST1.DESCRIPTION as DESIGNATION,
# MAGIC tu.END_DATE as USER_ID_END_DT,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC tu.USER_ID as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC tu.record_timestamp as AUD_TRAN_DT,
# MAGIC tu.tia_commit_date as LAST_UPDATE_DATE,
# MAGIC case when tu.record_userid is not null then concat(tu.record_userid,'|','101','|','TIA')
# MAGIC else null end as LAST_UPDATE_USER,
# MAGIC tu.TIMESTAMP as INSERT_DATE,
# MAGIC case when tu.userid is not null then concat(tu.userid,'|','101','|','TIA')
# MAGIC else null end as INSERT_USER ,
# MAGIC TU.LANGUAGE as PREF_LANG_CD,
# MAGIC C.COMPANY_NAME as COMPANY_NAME,
# MAGIC TU.COMPANY_NO as COMPANY_NUM
# MAGIC FROM `omi-catalog`.tia_ods.TOP_USER tu 
# MAGIC LEFT JOIN `omi-catalog`.tia_ods.NAME n ON tu.ID_NO = n.ID_NO
# MAGIC LEFT JOIN `omi-catalog`.tia_ods.department d on tu.DEPT_NO = d.DEPT_NO AND tu.COMPANY_NO = d.COMPANY_NO
# MAGIC left join `omi-catalog`.TIA_ODS.company c on TU.COMPANY_NO = C.COMPANY_NO
# MAGIC LEFT JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description 
# MAGIC  			from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  			where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'JOB_NAME'  
# MAGIC  			and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1 ON tu.job_code=MST1.code)A
# MAGIC  
# MAGIC  union all 
# MAGIC  
# MAGIC  select 'TGT' as column_name,count (*) 
# MAGIC  from `omi-catalog`.si_dwh.user_details ud

# COMMAND ----------

# MAGIC %sql
# MAGIC -----------------Source minus target
# MAGIC SELECT
# MAGIC --concat(tu.USER_ID, '|', tu.ID_NO , '|', '101', '|', 'TIA')::VARCHAR(100) as USER_ID,
# MAGIC concat(COALESCE (tu.USER_ID,''), '|', COALESCE (tu.ID_NO,'') , '|', '101', '|', 'TIA')::VARCHAR(50) as USER_ID,
# MAGIC tu.USER_ID AS USER_NUM,
# MAGIC tu.SITE_SEQ_NO as COMPANY_ID,
# MAGIC case when n.id_no is not null then concat(n.id_no,'|','101','|','TIA') else null end as ENTITY_ID,
# MAGIC n.ID_NO as ENTITY_NUM,
# MAGIC tu.START_DATE as USER_ID_START_DT,
# MAGIC tu.USER_name as USER_name,
# MAGIC tu.DEPT_NO as INTERNAL_UNIT_TYPE_CD,
# MAGIC d.DEPT_name as INTERNAL_UNIT_TYPE_DESC,
# MAGIC tu.JOB_CODE as DESIGNATION_CD,
# MAGIC MST1.DESCRIPTION as DESIGNATION,
# MAGIC tu.END_DATE as USER_ID_END_DT,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC tu.USER_ID as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC tu.record_timestamp as AUD_TRAN_DT,
# MAGIC tu.tia_commit_date as LAST_UPDATE_DATE,
# MAGIC case when tu.record_userid is not null then concat(tu.record_userid,'|',tur.id_no,'|','101','|','TIA')
# MAGIC else null end as LAST_UPDATE_USER,
# MAGIC tu.TIMESTAMP as INSERT_DATE,
# MAGIC case when tu.userid is not null then concat(tu.userid,'|',tui.id_no,'|','101','|','TIA')
# MAGIC else null end as INSERT_USER ,
# MAGIC TU.LANGUAGE as PREF_LANG_CD,
# MAGIC C.COMPANY_NAME as COMPANY_NAME,
# MAGIC TU.COMPANY_NO as COMPANY_NUM
# MAGIC FROM `omi-catalog`.tia_ods.TOP_USER tu 
# MAGIC LEFT JOIN `omi-catalog`.tia_ods.NAME n ON tu.ID_NO = n.ID_NO
# MAGIC LEFT JOIN `omi-catalog`.tia_ods.department d on tu.DEPT_NO = d.DEPT_NO AND tu.COMPANY_NO = d.COMPANY_NO
# MAGIC left join `omi-catalog`.TIA_ODS.company c on TU.COMPANY_NO = C.COMPANY_NO
# MAGIC LEFT JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description 
# MAGIC  			from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  			where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'JOB_NAME'  
# MAGIC  			and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1 ON tu.job_code=MST1.code
# MAGIC left join `omi-catalog`.tia_ods.top_user tui on tu.USERID = tui.USER_ID
# MAGIC left join `omi-catalog`.tia_ods.top_user tur on tu.RECORD_USERID = tur.USER_ID
# MAGIC 
# MAGIC   
# MAGIC except
# MAGIC 
# MAGIC select
# MAGIC USER_ID,
# MAGIC USER_NUM,
# MAGIC COMPANY_ID,
# MAGIC ENTITY_ID,
# MAGIC ENTITY_NUM,
# MAGIC USER_ID_START_DT,
# MAGIC USER_NAME,
# MAGIC INTERNAL_UNIT_TYPE_CD,
# MAGIC INTERNAL_UNIT_TYPE_DESC,
# MAGIC DESIGNATION_CD,
# MAGIC DESIGNATION,
# MAGIC USER_ID_END_DT,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC --AUD_IU_FLAG,
# MAGIC AUD_TRAN_DT,
# MAGIC LAST_UPDATE_DATE,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER,
# MAGIC PREF_LANG_CD,
# MAGIC COMPANY_NAME,
# MAGIC COMPANY_NUM
# MAGIC from `omi-catalog`.si_dwh.user_details ud

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC --concat(tu.USER_ID, '|', tu.ID_NO , '|', '101', '|', 'TIA')::VARCHAR(100) as USER_ID,
# MAGIC concat(COALESCE (tu.USER_ID,''), '|', COALESCE (tu.ID_NO,'') , '|', '101', '|', 'TIA')::VARCHAR(50) as USER_ID,
# MAGIC tu.USER_ID AS USER_NUM,
# MAGIC tu.SITE_SEQ_NO as COMPANY_ID,
# MAGIC case when n.id_no is not null then concat(n.id_no,'|','101','|','TIA') else null end as ENTITY_ID,
# MAGIC n.ID_NO as ENTITY_NUM,
# MAGIC tu.START_DATE as USER_ID_START_DT,
# MAGIC tu.USER_name as USER_name,
# MAGIC tu.DEPT_NO as INTERNAL_UNIT_TYPE_CD,
# MAGIC d.DEPT_name as INTERNAL_UNIT_TYPE_DESC,
# MAGIC tu.JOB_CODE as DESIGNATION_CD,
# MAGIC MST1.DESCRIPTION as DESIGNATION,
# MAGIC tu.END_DATE as USER_ID_END_DT,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC tu.USER_ID as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC tu.record_timestamp as AUD_TRAN_DT,
# MAGIC tu.tia_commit_date as LAST_UPDATE_DATE,
# MAGIC case when tu.record_userid is not null then concat(tu.record_userid,'|',tur.id_no,'|','101','|','TIA')
# MAGIC else null end as LAST_UPDATE_USER,
# MAGIC tu.TIMESTAMP as INSERT_DATE,
# MAGIC case when tu.userid is not null then concat(tu.userid,'|',tui.id_no,'|','101','|','TIA')
# MAGIC else null end as INSERT_USER ,
# MAGIC TU.LANGUAGE as PREF_LANG_CD,
# MAGIC C.COMPANY_NAME as COMPANY_NAME,
# MAGIC TU.COMPANY_NO as COMPANY_NUM
# MAGIC FROM `omi-catalog`.tia_ods.TOP_USER tu 
# MAGIC LEFT JOIN `omi-catalog`.tia_ods.NAME n ON tu.ID_NO = n.ID_NO
# MAGIC LEFT JOIN `omi-catalog`.tia_ods.department d on tu.DEPT_NO = d.DEPT_NO AND tu.COMPANY_NO = d.COMPANY_NO
# MAGIC left join `omi-catalog`.TIA_ODS.company c on TU.COMPANY_NO = C.COMPANY_NO
# MAGIC LEFT JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description 
# MAGIC  			from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  			where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'JOB_NAME'  
# MAGIC  			and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1 ON tu.job_code=MST1.code
# MAGIC left join `omi-catalog`.tia_ods.top_user tui on tu.USERID = tui.USER_ID
# MAGIC left join `omi-catalog`.tia_ods.top_user tur on tu.RECORD_USERID = tur.USER_ID%sql
# MAGIC -----------------Source minus target
# MAGIC SELECT 
# MAGIC --concat(tu.USER_ID, '|', tu.ID_NO , '|', '101', '|', 'TIA')::VARCHAR(100) as USER_ID,
# MAGIC concat(COALESCE (tu.USER_ID,''), '|', COALESCE (tu.ID_NO,'') , '|', '101', '|', 'TIA')::VARCHAR(50) as USER_ID,
# MAGIC tu.USER_ID AS USER_NUM,
# MAGIC tu.SITE_SEQ_NO as COMPANY_ID,
# MAGIC case when n.id_no is not null then concat(n.id_no,'|','101','|','TIA') else null end as ENTITY_ID,
# MAGIC n.ID_NO as ENTITY_NUM,
# MAGIC tu.START_DATE as USER_ID_START_DT,
# MAGIC tu.USER_name as USER_name,
# MAGIC tu.DEPT_NO as INTERNAL_UNIT_TYPE_CD,
# MAGIC d.DEPT_name as INTERNAL_UNIT_TYPE_DESC,
# MAGIC tu.JOB_CODE as DESIGNATION_CD,
# MAGIC MST1.DESCRIPTION as DESIGNATION,
# MAGIC tu.END_DATE as USER_ID_END_DT,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC tu.USER_ID as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC tu.record_timestamp as AUD_TRAN_DT,
# MAGIC tu.tia_commit_date as LAST_UPDATE_DATE,
# MAGIC case when tu.record_userid is not null then concat(tu.record_userid,'|',tur.id_no,'|','101','|','TIA')
# MAGIC else null end as LAST_UPDATE_USER,
# MAGIC tu.TIMESTAMP as INSERT_DATE,
# MAGIC case when tu.userid is not null then concat(tu.userid,'|',tui.id_no,'|','101','|','TIA')
# MAGIC else null end as INSERT_USER ,
# MAGIC TU.LANGUAGE as PREF_LANG_CD,
# MAGIC C.COMPANY_NAME as COMPANY_NAME,
# MAGIC TU.COMPANY_NO as COMPANY_NUM
# MAGIC FROM `omi-catalog`.tia_ods.TOP_USER tu 
# MAGIC LEFT JOIN `omi-catalog`.tia_ods.NAME n ON tu.ID_NO = n.ID_NO
# MAGIC LEFT JOIN `omi-catalog`.tia_ods.department d on tu.DEPT_NO = d.DEPT_NO AND tu.COMPANY_NO = d.COMPANY_NO
# MAGIC left join `omi-catalog`.TIA_ODS.company c on TU.COMPANY_NO = C.COMPANY_NO
# MAGIC LEFT JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description 
# MAGIC  			from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  			where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'JOB_NAME'  
# MAGIC  			and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1 ON tu.job_code=MST1.code
# MAGIC left join `omi-catalog`.tia_ods.top_user tui on tu.USERID = tui.USER_ID
# MAGIC left join `omi-catalog`.tia_ods.top_user tur on tu.RECORD_USERID = tur.USER_ID
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC select 
# MAGIC USER_ID,
# MAGIC USER_NUM,
# MAGIC COMPANY_ID,
# MAGIC ENTITY_ID,
# MAGIC ENTITY_NUM,
# MAGIC USER_ID_START_DT,
# MAGIC USER_NAME,
# MAGIC INTERNAL_UNIT_TYPE_CD,
# MAGIC INTERNAL_UNIT_TYPE_DESC,
# MAGIC DESIGNATION_CD,
# MAGIC DESIGNATION,
# MAGIC USER_ID_END_DT,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC --AUD_IU_FLAG,
# MAGIC AUD_TRAN_DT,
# MAGIC LAST_UPDATE_DATE,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER,
# MAGIC PREF_LANG_CD,
# MAGIC COMPANY_NAME,
# MAGIC COMPANY_NUM
# MAGIC from `omi-catalog`.si_dwh.user_details ud	

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.si_dwh.mst_mapping_table_list 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.si_dwh.mst_table_val_list

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.tia_ods.TOP_USER where USER_ID='TIA'

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC USER_ID,
# MAGIC USER_NUM,
# MAGIC COMPANY_ID,
# MAGIC ENTITY_ID,
# MAGIC ENTITY_NUM,
# MAGIC USER_ID_START_DT,
# MAGIC USER_NAME,
# MAGIC INTERNAL_UNIT_TYPE_CD,
# MAGIC INTERNAL_UNIT_TYPE_DESC,
# MAGIC DESIGNATION_CD,
# MAGIC DESIGNATION,
# MAGIC USER_ID_END_DT,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC --AUD_IU_FLAG,
# MAGIC AUD_TRAN_DT,
# MAGIC LAST_UPDATE_DATE,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER,
# MAGIC PREF_LANG_CD,
# MAGIC COMPANY_NAME,
# MAGIC COMPANY_NUM
# MAGIC from `omi-catalog`.si_dwh.user_details ud where USER_ID = 'MLOUW1|300068154|101|TIA'

# COMMAND ----------

# MAGIC %sql
# MAGIC -----------------------------Target minus source
# MAGIC 
# MAGIC select 
# MAGIC USER_ID,
# MAGIC USER_NUM,
# MAGIC COMPANY_ID,
# MAGIC ENTITY_ID,
# MAGIC ENTITY_NUM,
# MAGIC USER_ID_START_DT,
# MAGIC USER_NAME,
# MAGIC INTERNAL_UNIT_TYPE_CD,
# MAGIC INTERNAL_UNIT_TYPE_DESC,
# MAGIC DESIGNATION_CD,
# MAGIC DESIGNATION,
# MAGIC USER_ID_END_DT,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC --AUD_IU_FLAG,
# MAGIC AUD_TRAN_DT,
# MAGIC LAST_UPDATE_DATE,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER,
# MAGIC PREF_LANG_CD,
# MAGIC COMPANY_NAME,
# MAGIC COMPANY_NUM
# MAGIC from `omi-catalog`.si_dwh.user_details ud	
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC SELECT 
# MAGIC concat(tu.USER_ID, '|', tu.ID_NO , '|', '101', '|', 'TIA')::VARCHAR(50) as USER_ID,
# MAGIC tu.USER_ID AS USER_NUM,
# MAGIC tu.SITE_SEQ_NO as COMPANY_ID,
# MAGIC case when n.id_no is not null then concat(n.id_no,'|','101','|','TIA') else null end as ENTITY_ID,
# MAGIC n.ID_NO as ENTITY_NUM,
# MAGIC tu.START_DATE as USER_ID_START_DT,
# MAGIC tu.USER_name as USER_name,
# MAGIC tu.DEPT_NO as INTERNAL_UNIT_TYPE_CD,
# MAGIC d.DEPT_name as INTERNAL_UNIT_TYPE_DESC,
# MAGIC tu.JOB_CODE as DESIGNATION_CD,
# MAGIC MST1.DESCRIPTION as DESIGNATION,
# MAGIC tu.END_DATE as USER_ID_END_DT,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC tu.USER_ID as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC tu.record_timestamp as AUD_TRAN_DT,
# MAGIC tu.tia_commit_date as LAST_UPDATE_DATE,
# MAGIC case when tu.record_userid is not null then concat(tu.record_userid,'|','101','|','TIA')
# MAGIC else null end as LAST_UPDATE_USER,
# MAGIC tu.TIMESTAMP as INSERT_DATE,
# MAGIC case when tu.userid is not null then concat(tu.userid,'|','101','|','TIA')
# MAGIC else null end as INSERT_USER ,
# MAGIC TU.LANGUAGE as PREF_LANG_CD,
# MAGIC C.COMPANY_NAME as COMPANY_NAME,
# MAGIC TU.COMPANY_NO as COMPANY_NUM
# MAGIC FROM `omi-catalog`.tia_ods.TOP_USER tu 
# MAGIC LEFT JOIN `omi-catalog`.tia_ods.NAME n ON tu.ID_NO = n.ID_NO
# MAGIC LEFT JOIN `omi-catalog`.tia_ods.department d on tu.DEPT_NO = d.DEPT_NO AND tu.COMPANY_NO = d.COMPANY_NO
# MAGIC left join `omi-catalog`.TIA_ODS.company c on TU.COMPANY_NO = C.COMPANY_NO
# MAGIC LEFT JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description 
# MAGIC  			from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  			where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'JOB_NAME'  
# MAGIC  			and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1 ON tu.job_code=MST1.code

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.si_dwh.user_details 

# COMMAND ----------


