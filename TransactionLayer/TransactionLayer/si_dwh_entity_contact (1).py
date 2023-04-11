# Databricks notebook source
# MAGIC %sql
# MAGIC insert
# MAGIC 	into
# MAGIC 	`omi-catalog`.si_dwh.entity_contact(AUD_BATCH_ID,
# MAGIC 	AUD_IU_FLAG,
# MAGIC 	AUD_SRC_SYS_ID,
# MAGIC 	AUD_SRC_SYS_NM,
# MAGIC 	AUD_SUB_BATCH_ID,
# MAGIC 	AUD_TRAN_DT,
# MAGIC 	COMPANY_ID,
# MAGIC 	CONTACT_TYPE_CD,
# MAGIC 	CONTACT_TYPE_DESC,
# MAGIC 	entity_contact_id,
# MAGIC 	ENTITY_CONTACT_NUM,
# MAGIC 	entity_id,
# MAGIC 	INSERT_DATE,
# MAGIC 	INSERT_USER,
# MAGIC 	last_update_date,
# MAGIC 	LAST_UPDATE_USER,
# MAGIC 	TELEPHONE_NUM,
# MAGIC     entity_contact_cd,
# MAGIC     entity_contact_start_dt,
# MAGIC     entity_contact_end_dt,
# MAGIC     entity_contact_ver_id,
# MAGIC     entity_contact_ver_start_dt,
# MAGIC     entity_contact_ver_end_dt,
# MAGIC     pref_contact_ind,
# MAGIC     pref_contact_start_time,
# MAGIC     pref_contact_end_time,
# MAGIC     telephone_country_cd,
# MAGIC     telephone_country_nm,
# MAGIC     telephone_country_num,
# MAGIC     telephone_area_cd,
# MAGIC     telephone_area_nm,
# MAGIC     telephone_area_num,
# MAGIC     telephone_ext_num,
# MAGIC     fax_num,
# MAGIC     remarks,
# MAGIC     aud_src_sys_cd,
# MAGIC     aud_src_sys_updt_ver_id,
# MAGIC     name_id_no,
# MAGIC     telephone_type)
# MAGIC 		select
# MAGIC 			45 as AUD_BATCH_ID,
# MAGIC 			0 as AUD_IU_FLAG,
# MAGIC 			entity_details.entity_num::BIGINT as AUD_SRC_SYS_ID,
# MAGIC 			concat('TIA', '')::VARCHAR(10) as AUD_SRC_SYS_NM,
# MAGIC 			28 as AUD_SUB_BATCH_ID,
# MAGIC 			name_telephone.record_timestamp::TIMESTAMP as AUD_TRAN_DT,
# MAGIC 			entity_details.company_id::INTEGER as COMPANY_ID,
# MAGIC 			name_telephone.telephone_type::VARCHAR(10) as CONTACT_TYPE_CD,
# MAGIC 			mst_telephone_type.description::VARCHAR(500) as CONTACT_TYPE_DESC,
# MAGIC 			concat(name_id_no::varchar(10), '|', telephone_type::varchar(10), '|', '101', '|', 'TIA')::VARCHAR(25) as entity_contact_id,
# MAGIC 			concat(name_id_no::varchar(10), '|', telephone_type::varchar(10))::VARCHAR(50) as ENTITY_CONTACT_NUM,
# MAGIC 			entity_details.entity_id::VARCHAR(50) as entity_id,
# MAGIC 			name_telephone.timestamp::TIMESTAMP as INSERT_DATE,
# MAGIC 			user_det_userid.user_id::VARCHAR(50) as INSERT_USER,
# MAGIC 			name_telephone.tia_commit_date::TIMESTAMP as last_update_date,
# MAGIC 			user_det_record_userid.user_id::VARCHAR(50) as LAST_UPDATE_USER,
# MAGIC 			name_telephone.phone_no::VARCHAR(255) as TELEPHONE_NUM,
# MAGIC             null as entity_contact_cd,
# MAGIC             null as entity_contact_start_dt,
# MAGIC             null as entity_contact_end_dt,
# MAGIC             null as entity_contact_ver_id,
# MAGIC             null as entity_contact_ver_start_dt,
# MAGIC             null as entity_contact_ver_end_dt,
# MAGIC             null as pref_contact_ind,
# MAGIC             null as pref_contact_start_time,
# MAGIC             null as pref_contact_end_time,
# MAGIC             null as telephone_country_cd,
# MAGIC             null as telephone_country_nm,
# MAGIC             null as telephone_country_num,
# MAGIC             null as telephone_area_cd,
# MAGIC             null as telephone_area_nm,
# MAGIC             null as telephone_area_num,
# MAGIC             null as telephone_ext_num,
# MAGIC             null as fax_num,
# MAGIC             null as remarks,
# MAGIC             null as aud_src_sys_cd,
# MAGIC             null as aud_src_sys_updt_ver_id,
# MAGIC             null as name_id_no,
# MAGIC             null as telephone_type
# MAGIC 		from
# MAGIC 			`omi-catalog`.tia_ods.name_telephone name_telephone
# MAGIC 		inner join `omi-catalog`.si_dwh.entity_details entity_details on
# MAGIC 			NAME_TELEPHONE.NAME_ID_NO = ENTITY_DETAILS.ENTITY_NUM
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
# MAGIC 				and mst.src_sys_mst_tbl_nm = 'TELEPHONE_TYPE'
# MAGIC 				and mst.src_sys_nm = 'TIA'
# MAGIC 				and val.mst_val_si_language = 'en') mst_telephone_type on
# MAGIC 			name_telephone.telephone_type = mst_telephone_type.code
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_det_record_userid on
# MAGIC 			NAME_TELEPHONE.RECORD_USERID = USER_DET_RECORD_USERID.USER_NUM
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_det_userid on
# MAGIC 			NAME_TELEPHONE.USERID = USER_DET_USERID.USER_NUM
# MAGIC 		where
# MAGIC 			--name_telephone.tia_commit_date > '2021-08-01 00:49:22.0'
# MAGIC 			--and name_telephone.tia_commit_date <= '2023-03-17 15:03:14.449898'
# MAGIC 			name_telephone.telephone_type in ('01', '02', '07', '08', '10')

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table `omi-catalog`.si_dwh.entity_contact

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.si_dwh.entity_contact

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.tia_ods.name_telephone

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.si_dwh.entity_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 				val.mst_val_si_cd code,
# MAGIC 				val.mst_val_si_desc description,
# MAGIC 				mst.src_sys_mst_tbl_nm
# MAGIC 			from
# MAGIC 				`omi-catalog`.si_dwh.mst_mapping_table_list mst,
# MAGIC 				`omi-catalog`.si_dwh.mst_table_val_list val
# MAGIC 			where
# MAGIC 				mst.mst_tbl_si_id = val.mst_tbl_si_cd
# MAGIC 				and mst.src_sys_mst_tbl_nm = 'TELEPHONE_TYPE'
# MAGIC 				and mst.src_sys_nm = 'TIA'
# MAGIC 				and val.mst_val_si_language = 'en'

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.si_dwh.user_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC 			45 as AUD_BATCH_ID,
# MAGIC 			0 as AUD_IU_FLAG,
# MAGIC 			entity_details.entity_num::INT as AUD_SRC_SYS_ID,
# MAGIC 			concat('TIA', '')::VARCHAR(10) as AUD_SRC_SYS_NM,
# MAGIC 			28 as AUD_SUB_BATCH_ID,
# MAGIC 			name_telephone.record_timestamp::TIMESTAMP as AUD_TRAN_DT,
# MAGIC 			entity_details.company_id::INTEGER as COMPANY_ID,
# MAGIC 			name_telephone.telephone_type::VARCHAR(10) as CONTACT_TYPE_CD,
# MAGIC 			mst_telephone_type.description::VARCHAR(500) as CONTACT_TYPE_DESC,
# MAGIC 			concat(name_id_no::varchar(10), '|', telephone_type::varchar(10), '|', '101', '|', 'TIA')::VARCHAR(25) as entity_contact_id,
# MAGIC 			concat(name_id_no::varchar(10), '|', telephone_type::varchar(10))::VARCHAR(50) as ENTITY_CONTACT_NUM,
# MAGIC 			entity_details.entity_id::VARCHAR(50) as entity_id,
# MAGIC 			name_telephone.timestamp::TIMESTAMP as INSERT_DATE,
# MAGIC 			user_det_userid.user_id::VARCHAR(50) as INSERT_USER,
# MAGIC 			name_telephone.tia_commit_date::TIMESTAMP as last_update_date,
# MAGIC 			user_det_record_userid.user_id::VARCHAR(50) as LAST_UPDATE_USER,
# MAGIC 			name_telephone.phone_no::VARCHAR(255) as TELEPHONE_NUM,
# MAGIC             null as entity_contact_cd,
# MAGIC             null as entity_contact_start_dt,
# MAGIC             null as entity_contact_end_dt,
# MAGIC             null as entity_contact_ver_id,
# MAGIC             null as entity_contact_ver_start_dt,
# MAGIC             null as entity_contact_ver_end_dt,
# MAGIC             null as pref_contact_ind,
# MAGIC             null as pref_contact_start_time,
# MAGIC             null as pref_contact_end_time,
# MAGIC             null as telephone_country_cd,
# MAGIC             null as telephone_country_nm,
# MAGIC             null as telephone_country_num,
# MAGIC             null as telephone_area_cd,
# MAGIC             null as telephone_area_nm,
# MAGIC             null as telephone_area_num,
# MAGIC             null as telephone_ext_num,
# MAGIC             null as fax_num,
# MAGIC             null as remarks,
# MAGIC             null as aud_src_sys_cd,
# MAGIC             null as aud_src_sys_updt_ver_id,
# MAGIC             null as name_id_no,
# MAGIC             null as telephone_type
# MAGIC 		from
# MAGIC 			`omi-catalog`.tia_ods.name_telephone name_telephone
# MAGIC 		inner join `omi-catalog`.si_dwh.entity_details entity_details on
# MAGIC 			NAME_TELEPHONE.NAME_ID_NO = ENTITY_DETAILS.ENTITY_NUM
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
# MAGIC 				and mst.src_sys_mst_tbl_nm = 'TELEPHONE_TYPE'
# MAGIC 				and mst.src_sys_nm = 'TIA'
# MAGIC 				and val.mst_val_si_language = 'en') mst_telephone_type on
# MAGIC 			name_telephone.telephone_type = mst_telephone_type.code
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_det_record_userid on
# MAGIC 			NAME_TELEPHONE.RECORD_USERID = USER_DET_RECORD_USERID.USER_NUM
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_det_userid on
# MAGIC 			NAME_TELEPHONE.USERID = USER_DET_USERID.USER_NUM
# MAGIC 		where
# MAGIC 			--name_telephone.tia_commit_date > '2021-08-01 00:49:22.0'
# MAGIC 			--and name_telephone.tia_commit_date <= '2023-03-17 15:03:14.449898'
# MAGIC 			name_telephone.telephone_type in ('01', '02', '07', '08', '10') 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from
# MAGIC 			`omi-catalog`.tia_ods.name_telephone name_telephone
# MAGIC where
# MAGIC 			name_telephone.tia_commit_date > '2021-08-01 00:49:22.0'
# MAGIC 			and name_telephone.tia_commit_date <= '2023-03-17 15:03:14.449898'
# MAGIC 			and name_telephone.telephone_type in ('01', '02', '07', '08', '10')

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC ENTITY_CONTACT_NUM,
# MAGIC count(*)
# MAGIC from `omi-catalog`.si_dwh.entity_contact ec
# MAGIC group by 
# MAGIC ENTITY_CONTACT_NUM
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Null check
# MAGIC 
# MAGIC select 
# MAGIC ENTITY_CONTACT_ID,
# MAGIC ENTITY_CONTACT_NUM
# MAGIC from `omi-catalog`.si_dwh.entity_contact
# MAGIC where ENTITY_CONTACT_ID is null or ENTITY_CONTACT_NUM is null 

# COMMAND ----------

# MAGIC %sql
# MAGIC --- count check
# MAGIC 
# MAGIC select 'TGT' table_name,count (*)
# MAGIC from `omi-catalog`.si_dwh.entity_contact ec 
# MAGIC union all
# MAGIC select 'SRC' table_name,count (*)
# MAGIC from (
# MAGIC 
# MAGIC select concat(name_id_no, '|', telephone_type,'|','101','|','TIA') as entity_contact_id,
# MAGIC ed.company_id as COMPANY_ID,
# MAGIC concat(name_id_no, '|', telephone_type) as ENTITY_CONTACT_NUM,
# MAGIC ed.entity_id as entity_id,
# MAGIC nt.telephone_type as CONTACT_TYPE_CD,
# MAGIC MST1.description as CONTACT_TYPE_DESC,
# MAGIC nt.phone_no as TELEPHONE_NUM,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC ed.entity_num as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC nt.record_timestamp as AUD_TRAN_DT,
# MAGIC nt.tia_commit_date as last_update_date,
# MAGIC UD1.user_id as LAST_UPDATE_USER,
# MAGIC nt.timestamp as INSERT_DATE,
# MAGIC UD2.user_id as INSERT_USER 
# MAGIC FROM `omi-catalog`.tia_ods.name_telephone nt 
# MAGIC INNER JOIN `omi-catalog`.si_dwh.entity_details ed on nt.NAME_ID_NO = ed.ENTITY_NUM 									
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'TELEPHONE_TYPE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1
# MAGIC  on nt.telephone_type=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details UD1 ON nt.RECORD_USERID = UD1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details UD2 ON nt.USERID = UD2.USER_NUM 
# MAGIC where  nt.telephone_type in ('01','02','07','08','10'))s

# COMMAND ----------

# MAGIC %sql
# MAGIC -----Source minus target
# MAGIC 
# MAGIC select concat(name_id_no, '|', telephone_type,'|','101','|','TIA') as entity_contact_id,
# MAGIC ed.company_id as COMPANY_ID,
# MAGIC concat(name_id_no, '|', telephone_type) as ENTITY_CONTACT_NUM,
# MAGIC ed.entity_id as entity_id,
# MAGIC nt.telephone_type as CONTACT_TYPE_CD,
# MAGIC MST1.description as CONTACT_TYPE_DESC,
# MAGIC nt.phone_no as TELEPHONE_NUM,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC ed.entity_num as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC nt.record_timestamp as AUD_TRAN_DT,
# MAGIC nt.tia_commit_date as last_update_date,
# MAGIC UD1.user_id as LAST_UPDATE_USER,
# MAGIC nt.timestamp as INSERT_DATE,
# MAGIC UD2.user_id as INSERT_USER 
# MAGIC FROM `omi-catalog`.tia_ods.name_telephone nt 
# MAGIC INNER JOIN `omi-catalog`.si_dwh.entity_details ed on nt.NAME_ID_NO = ed.ENTITY_NUM 
# MAGIC and nt.telephone_type in ('01', '02', '07', '08', '10')									
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'TELEPHONE_TYPE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1
# MAGIC  on nt.telephone_type=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details UD1 ON nt.RECORD_USERID = UD1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details UD2 ON nt.USERID = UD2.USER_NUM 
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC 
# MAGIC select
# MAGIC entity_contact_id,
# MAGIC COMPANY_ID,
# MAGIC ENTITY_CONTACT_NUM,
# MAGIC entity_id,
# MAGIC CONTACT_TYPE_CD,
# MAGIC CONTACT_TYPE_DESC,
# MAGIC TELEPHONE_NUM,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC --AUD_IU_FLAG,
# MAGIC AUD_TRAN_DT,
# MAGIC last_update_date,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER
# MAGIC from `omi-catalog`.si_dwh.entity_contact 

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Target minus Source
# MAGIC 
# MAGIC select
# MAGIC entity_contact_id,
# MAGIC COMPANY_ID,
# MAGIC ENTITY_CONTACT_NUM,
# MAGIC entity_id,
# MAGIC CONTACT_TYPE_CD,
# MAGIC CONTACT_TYPE_DESC,
# MAGIC TELEPHONE_NUM,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC --AUD_IU_FLAG,
# MAGIC AUD_TRAN_DT,
# MAGIC last_update_date,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER
# MAGIC from `omi-catalog`.si_dwh.entity_contact ---)ec
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select concat(name_id_no::varchar(10), '|', telephone_type::varchar(10),'|','101','|','TIA') as entity_contact_id,
# MAGIC ed.company_id as COMPANY_ID,
# MAGIC concat(name_id_no::varchar(10), '|', telephone_type::varchar(10)) as ENTITY_CONTACT_NUM,
# MAGIC ed.entity_id as entity_id,
# MAGIC nt.telephone_type as CONTACT_TYPE_CD,
# MAGIC MST1.description as CONTACT_TYPE_DESC,
# MAGIC nt.phone_no as TELEPHONE_NUM,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC ed.entity_num as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC nt.record_timestamp as AUD_TRAN_DT,
# MAGIC nt.tia_commit_date as last_update_date,
# MAGIC UD1.user_id as LAST_UPDATE_USER,
# MAGIC nt.timestamp as INSERT_DATE,
# MAGIC UD2.user_id as INSERT_USER 
# MAGIC FROM `omi-catalog`.tia_ods.name_telephone nt 
# MAGIC INNER JOIN `omi-catalog`.si_dwh.entity_details ed on nt.NAME_ID_NO = ed.ENTITY_NUM 
# MAGIC and nt.telephone_type in ('01', '02', '07', '08', '10'1,2,7)									
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'TELEPHONE_TYPE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='TIA') MST1
# MAGIC  on nt.telephone_type=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details UD1 ON nt.RECORD_USERID = UD1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details UD2 ON nt.USERID = UD2.USER_NUM

# COMMAND ----------


