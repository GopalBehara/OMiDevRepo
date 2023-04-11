# Databricks notebook source
# MAGIC %sql
# MAGIC truncate table `omi-catalog`.si_dwh.gl_account

# COMMAND ----------

# MAGIC %sql
# MAGIC insert
# MAGIC 	into
# MAGIC 	`omi-catalog`.si_dwh.gl_account(AUD_BATCH_ID,
# MAGIC 	AUD_SUB_BATCH_ID,
# MAGIC 	COMPANY_ID,
# MAGIC 	GL_ACCOUNT_CATEGORY_CD,
# MAGIC 	GL_ACCOUNT_CATEGORY_DESC,
# MAGIC 	GL_ACCOUNT_DESC,
# MAGIC 	GL_ACCOUNT_ID,
# MAGIC 	GL_ACCOUNT_NUM,
# MAGIC 	GL_ACCOUNT_TYPE_CD,
# MAGIC 	GL_ACCOUNT_TYPE_DESC,
# MAGIC 	INSERT_DATE,
# MAGIC 	INSERT_USER,
# MAGIC 	LAST_UPDATE_DATE,
# MAGIC 	LAST_UPDATE_USER,
# MAGIC     parent_gl_account_num,
# MAGIC     gl_balance_type_cd,
# MAGIC     gl_balance_type_desc,
# MAGIC     gl_account_status_cd,
# MAGIC     gl_account_status_desc,
# MAGIC     gl_account_analysis_value_1,
# MAGIC     gl_account_analysis_value_2,
# MAGIC     gl_account_analysis_value_3,
# MAGIC     gl_account_analysis_value_4,
# MAGIC     gl_account_analysis_value_5,
# MAGIC     gl_account_start_dt,
# MAGIC     gl_account_end_dt,
# MAGIC     aud_src_sys_nm,
# MAGIC     aud_src_sys_id,
# MAGIC     aud_src_sys_cd,
# MAGIC     aud_src_sys_updt_ver_id,
# MAGIC     aud_iu_flag,
# MAGIC     gl_account_cd,
# MAGIC     aud_tran_dt)
# MAGIC 		select
# MAGIC 			46 as AUD_BATCH_ID,
# MAGIC 			25 as AUD_SUB_BATCH_ID,
# MAGIC 			101 as COMPANY_ID,
# MAGIC 			acc_gl_account.category::INTEGER as GL_ACCOUNT_CATEGORY_CD,
# MAGIC 			acc_cate_desc.description::varchar(500) as GL_ACCOUNT_CATEGORY_DESC,
# MAGIC 			acc_gl_account.gl_account_name::VARCHAR(500) as GL_ACCOUNT_DESC,
# MAGIC 			concat(gl_account_type, '|', gl_account_code, '|', '101', '|', 'TIA')::varchar(50) as GL_ACCOUNT_ID,
# MAGIC 			acc_gl_account.gl_account_code::varchar(10) as GL_ACCOUNT_NUM,
# MAGIC 			acc_gl_account.gl_account_type::INTEGER as GL_ACCOUNT_TYPE_CD,
# MAGIC 			acc_type_desc.description::varchar(500) as GL_ACCOUNT_TYPE_DESC,
# MAGIC 			acc_gl_account.timestamp::timestamp as INSERT_DATE,
# MAGIC 			user_details1.user_id::varchar(50) as INSERT_USER,
# MAGIC 			acc_gl_account.tia_commit_date::timestamp as LAST_UPDATE_DATE,
# MAGIC 			user_details.user_id::varchar(50) as LAST_UPDATE_USER,
# MAGIC             null as parent_gl_account_num,
# MAGIC             null as gl_balance_type_cd,
# MAGIC             null as gl_balance_type_desc,
# MAGIC             null as gl_account_status_cd,
# MAGIC             null as gl_account_status_desc,
# MAGIC             null as gl_account_analysis_value_1,
# MAGIC             null as gl_account_analysis_value_2,
# MAGIC             null as gl_account_analysis_value_3,
# MAGIC             null as gl_account_analysis_value_4,
# MAGIC             null as gl_account_analysis_value_5,
# MAGIC             null as gl_account_start_dt,
# MAGIC             null as gl_account_end_dt,
# MAGIC             null as aud_src_sys_nm,
# MAGIC             null as aud_src_sys_id,
# MAGIC             null as aud_src_sys_cd,
# MAGIC             null as aud_src_sys_updt_ver_id,
# MAGIC             null as aud_iu_flag,
# MAGIC             null as gl_account_cd,
# MAGIC             null as aud_tran_dt
# MAGIC 		from
# MAGIC 			`omi-catalog`.tia_ods.acc_gl_account acc_gl_account
# MAGIC 		left join (
# MAGIC 			select
# MAGIC 				val.mst_val_si_cd code,
# MAGIC 				val.mst_val_si_desc description
# MAGIC 			from
# MAGIC 				`omi-catalog`.si_dwh.mst_mapping_table_list mst,
# MAGIC 				`omi-catalog`.si_dwh.mst_table_val_list val
# MAGIC 			where
# MAGIC 				mst.mst_tbl_si_id = val.mst_tbl_si_cd
# MAGIC 				and mst.src_sys_mst_tbl_nm = 'ACC_GL_ACC_CATEGORY'
# MAGIC 				and mst.src_sys_nm = 'TIA'
# MAGIC 				and val.mst_val_si_language = 'en') acc_cate_desc on
# MAGIC 			acc_gl_account.gl_account_code = acc_cate_desc.code
# MAGIC 		left join (
# MAGIC 			select
# MAGIC 				val.mst_val_si_cd code,
# MAGIC 				val.mst_val_si_desc description
# MAGIC 			from
# MAGIC 				`omi-catalog`.si_dwh.mst_mapping_table_list mst,
# MAGIC 				`omi-catalog`.si_dwh.mst_table_val_list val
# MAGIC 			where
# MAGIC 				mst.mst_tbl_si_id = val.mst_tbl_si_cd
# MAGIC 				and mst.src_sys_mst_tbl_nm = 'ACC_GL_ACCOUNT_TYPE'
# MAGIC 				and mst.src_sys_nm = 'TIA'
# MAGIC 				and val.mst_val_si_language = 'en') acc_type_desc on
# MAGIC 			acc_gl_account.gl_account_code = acc_type_desc.code
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_details1 on
# MAGIC 			ACC_GL_ACCOUNT.USERID = USER_DETAILS1.USER_NUM
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_details on
# MAGIC 			ACC_GL_ACCOUNT.RECORD_USERID = USER_DETAILS.USER_NUM

# COMMAND ----------

# MAGIC %sql
# MAGIC select GL_ACCOUNT_ID,
# MAGIC GL_ACCOUNT_TYPE_CD
# MAGIC from `omi-catalog`.si_dwh.GL_ACCOUNT ga
# MAGIC group by
# MAGIC GL_ACCOUNT_ID,
# MAGIC GL_ACCOUNT_TYPE_CD
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC ---null check---
# MAGIC select 
# MAGIC GL_ACCOUNT_ID,
# MAGIC GL_ACCOUNT_TYPE_CD
# MAGIC from `omi-catalog`.si_dwh.GL_ACCOUNT
# MAGIC where gl_account_num !='dummy' and 
# MAGIC GL_ACCOUNT_ID  is null or GL_ACCOUNT_TYPE_CD  is null

# COMMAND ----------

# MAGIC %sql
# MAGIC ---------------------Count check 
# MAGIC 
# MAGIC 
# MAGIC select 'TGT' as TBL_NAME,count (*)
# MAGIC from `omi-catalog`.si_dwh.gl_account ga 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC select 'SRC' as TBL_NAME,count (*)
# MAGIC from (select * from (select concat(gl_account_type,'|',gl_account_code,'|','101','|','TIA') as GL_ACCOUNT_ID,
# MAGIC ag.gl_account_code as GL_ACCOUNT_NUM,
# MAGIC 101 as COMPANY_ID,
# MAGIC ag.gl_account_name as GL_ACCOUNT_DESC,
# MAGIC ag.gl_account_type::int as GL_ACCOUNT_TYPE_CD,
# MAGIC MST1.description as GL_ACCOUNT_TYPE_DESC,
# MAGIC ag.category::int as GL_ACCOUNT_CATEGORY_CD,
# MAGIC MSt2.description as GL_ACCOUNT_CATEGORY_DESC,
# MAGIC ag.tia_commit_date as LAST_UPDATE_DATE,
# MAGIC UD1.user_id as LAST_UPDATE_USER,
# MAGIC ag.timestamp as INSERT_DATE,
# MAGIC UD2.user_id as INSERT_USER
# MAGIC from `omi-catalog`.tia_ods.acc_gl_account ag
# MAGIC left outer join (select val.mst_val_si_cd code, val.mst_val_si_desc description
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'ACC_GL_ACCOUNT_TYPE'  
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1 
# MAGIC  on ag.gl_account_code=MST1.code
# MAGIC left outer join (select val.mst_val_si_cd code, val.mst_val_si_desc description
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'ACC_GL_ACC_CATEGORY'  
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST2
# MAGIC  on ag.gl_account_code=MST2.code
# MAGIC inner join `omi-catalog`.si_dwh.user_details UD1 on ag.RECORD_USERID = UD1.USER_NUM 
# MAGIC inner join `omi-catalog`.si_dwh.user_details UD2 on ag.USERID = UD2.USER_NUM)tt
# MAGIC )tc

# COMMAND ----------

# MAGIC %sql
# MAGIC ---------------------Source minus target
# MAGIC 
# MAGIC select * from (select concat(gl_account_type,'|',gl_account_code,'|','101','|','TIA') as GL_ACCOUNT_ID,
# MAGIC ag.gl_account_code as GL_ACCOUNT_NUM,
# MAGIC 101 as COMPANY_ID,
# MAGIC ag.gl_account_name as GL_ACCOUNT_DESC,
# MAGIC ag.gl_account_type::int as GL_ACCOUNT_TYPE_CD,
# MAGIC MST1.description as GL_ACCOUNT_TYPE_DESC,
# MAGIC ag.category::int as GL_ACCOUNT_CATEGORY_CD,
# MAGIC MSt2.description as GL_ACCOUNT_CATEGORY_DESC,
# MAGIC ag.tia_commit_date as LAST_UPDATE_DATE,
# MAGIC UD1.user_id as LAST_UPDATE_USER,
# MAGIC ag.timestamp as INSERT_DATE,
# MAGIC UD2.user_id as INSERT_USER
# MAGIC from `omi-catalog`.tia_ods.acc_gl_account ag
# MAGIC left join (select val.mst_val_si_cd code, val.mst_val_si_desc description
# MAGIC 		from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 		where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'ACC_GL_ACCOUNT_TYPE'  
# MAGIC 		and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1  on ag.gl_account_code=MST1.code
# MAGIC left join (select val.mst_val_si_cd code, val.mst_val_si_desc description
# MAGIC 		from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC 		where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'ACC_GL_ACC_CATEGORY'  
# MAGIC 		and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST2 on ag.gl_account_code=MST2.code
# MAGIC left join `omi-catalog`.si_dwh.user_details UD1 on ag.RECORD_USERID = UD1.USER_NUM 
# MAGIC left join `omi-catalog`.si_dwh.user_details UD2 on ag.USERID = UD2.USER_NUM)tt
# MAGIC --where TT.GL_ACCOUNT_TYPE_CD = 1
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC select 
# MAGIC GL_ACCOUNT_ID,
# MAGIC GL_ACCOUNT_NUM,
# MAGIC COMPANY_ID,
# MAGIC GL_ACCOUNT_DESC,
# MAGIC GL_ACCOUNT_TYPE_CD,
# MAGIC GL_ACCOUNT_TYPE_DESC,
# MAGIC GL_ACCOUNT_CATEGORY_CD,
# MAGIC GL_ACCOUNT_CATEGORY_DESC,
# MAGIC LAST_UPDATE_DATE,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER
# MAGIC from `omi-catalog`.si_dwh.gl_account ga
# MAGIC where gl_account_num != 'dummy' --and gl_account_type_cd = '1'

# COMMAND ----------

# MAGIC %sql
# MAGIC ----------------------Target minus source
# MAGIC select 
# MAGIC GL_ACCOUNT_ID,
# MAGIC GL_ACCOUNT_NUM,
# MAGIC COMPANY_ID,
# MAGIC GL_ACCOUNT_DESC,
# MAGIC GL_ACCOUNT_TYPE_CD,
# MAGIC GL_ACCOUNT_TYPE_DESC,
# MAGIC GL_ACCOUNT_CATEGORY_CD,
# MAGIC GL_ACCOUNT_CATEGORY_DESC,
# MAGIC LAST_UPDATE_DATE,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER
# MAGIC from `omi-catalog`.si_dwh.gl_account ga
# MAGIC where gl_account_num != 'dummy' --and gl_account_type_cd = '1'
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC select * from (select concat(gl_account_type,'|',gl_account_code,'|','101','|','TIA') as GL_ACCOUNT_ID,
# MAGIC ag.gl_account_code as GL_ACCOUNT_NUM,
# MAGIC 101 as COMPANY_ID,
# MAGIC ag.gl_account_name as GL_ACCOUNT_DESC,
# MAGIC ag.gl_account_type::int as GL_ACCOUNT_TYPE_CD,
# MAGIC MST1.description as GL_ACCOUNT_TYPE_DESC,
# MAGIC ag.category::int as GL_ACCOUNT_CATEGORY_CD,
# MAGIC MSt2.description as GL_ACCOUNT_CATEGORY_DESC,
# MAGIC ag.tia_commit_date as LAST_UPDATE_DATE,
# MAGIC UD1.user_id as LAST_UPDATE_USER,
# MAGIC ag.timestamp as INSERT_DATE,
# MAGIC UD2.user_id as INSERT_USER
# MAGIC from `omi-catalog`.tia_ods.acc_gl_account ag
# MAGIC left outer join (select val.mst_val_si_cd code, val.mst_val_si_desc description
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'ACC_GL_ACCOUNT_TYPE'  
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1 
# MAGIC  on ag.gl_account_code=MST1.code
# MAGIC left outer join (select val.mst_val_si_cd code, val.mst_val_si_desc description
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd  and mst.src_sys_mst_tbl_nm = 'ACC_GL_ACC_CATEGORY'  
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST2
# MAGIC  on ag.gl_account_code=MST2.code
# MAGIC inner join `omi-catalog`.si_dwh.user_details UD1 on ag.RECORD_USERID = UD1.USER_NUM 
# MAGIC inner join `omi-catalog`.si_dwh.user_details UD2 on ag.USERID = UD2.USER_NUM)tt
# MAGIC --where TT.GL_ACCOUNT_TYPE_CD = 1

# COMMAND ----------


