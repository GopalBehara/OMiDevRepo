# Databricks notebook source
# MAGIC %sql
# MAGIC truncate table `omi-catalog`.si_dwh.address_details

# COMMAND ----------

# MAGIC %sql
# MAGIC insert
# MAGIC 	into
# MAGIC 	`omi-catalog`.si_dwh.address_details(address_id,
# MAGIC 	ADDRESS_NUM,
# MAGIC 	address_type_cd,
# MAGIC 	ADDRESS_TYPE_DESC,
# MAGIC 	address_unknown_ind,
# MAGIC 	appartment_num,
# MAGIC 	AUD_BATCH_ID,
# MAGIC 	AUD_IU_FLAG,
# MAGIC 	AUD_SRC_SYS_ID,
# MAGIC 	AUD_SRC_SYS_NM,
# MAGIC 	AUD_SUB_BATCH_ID,
# MAGIC 	AUD_TRAN_DT,
# MAGIC 	BUILDING_NM,
# MAGIC 	BUILDING_NR,
# MAGIC 	city_nm,
# MAGIC 	country_cd,
# MAGIC 	country_nm,
# MAGIC 	district_nm,
# MAGIC 	EXT_ADDRESS_CODE,
# MAGIC 	GEO_LATITUDE,
# MAGIC 	GEO_LONGITUDE,
# MAGIC 	HOUSE_NM,
# MAGIC 	house_num,
# MAGIC 	INSERT_DATE,
# MAGIC 	INSERT_USER,
# MAGIC 	last_update_date,
# MAGIC 	LAST_UPDATE_USER,
# MAGIC 	organisation_nm,
# MAGIC 	pin_zip_cd,
# MAGIC 	post_office_box_num,
# MAGIC 	post_street,
# MAGIC 	reference_line_1,
# MAGIC 	region_nm,
# MAGIC 	street_nm,
# MAGIC     address_cd,address_start_dt,address_end_dt,address_in_use_ind,block_txt,street_cd,
# MAGIC     street_type_cd,street_type_desc,reference_line_2,reference_line_3,municipality_nm,
# MAGIC     town_nm,province_cd,province_nm,city_type_cd,city_type_desc,city_cd,district_type_cd,district_type_desc,district_cd,state_cd,
# MAGIC     state_nm,region_cd,seasonal_flag,seasonal_from_day,seasonal_from_month,seasonal_to_day,seasonal_to_month,
# MAGIC     geo_location_type_cd,geo_location_type_desc,geo_elevation,pin_zip_plus,language_cd,language_desc,aud_src_sys_cd,
# MAGIC     aud_src_sys_updt_ver_id)
# MAGIC 		select
# MAGIC 			concat(name.id_no, '|', '1', '|', '101', '|', 'TIA')::VARCHAR(50) as address_id,
# MAGIC 			concat(name.id_no, '|', '1')::VARCHAR(50) as ADDRESS_NUM,
# MAGIC 			1 as address_type_cd,
# MAGIC 			concat('Permanent Address', '')::VARCHAR(500) as ADDRESS_TYPE_DESC,
# MAGIC 			name.unknown_address::VARCHAR(3) as address_unknown_ind,
# MAGIC 			concat(name.floor, ' ', case when name.floor_ext is not null then concat(' ', name.floor_ext) end)::VARCHAR(100) as appartment_num,
# MAGIC 			45 as AUD_BATCH_ID,
# MAGIC 			0 as AUD_IU_FLAG,
# MAGIC 			concat(name.id_no, '|', '1')::VARCHAR(50) as AUD_SRC_SYS_ID,
# MAGIC 			concat('TIA', '')::VARCHAR(10) as AUD_SRC_SYS_NM,
# MAGIC 			13 as AUD_SUB_BATCH_ID,
# MAGIC 			name.record_timestamp::TIMESTAMP as AUD_TRAN_DT,
# MAGIC 			name.building_name::VARCHAR(255) as BUILDING_NM,
# MAGIC 			name.building_number::VARCHAR(20) as BUILDING_NR,
# MAGIC 			name.city::VARCHAR(500) as city_nm,
# MAGIC 			name.country_code::VARCHAR(3) as country_cd,
# MAGIC 			case
# MAGIC 				when name.country_code is null then name.country
# MAGIC 				else mst_country_code.description
# MAGIC 			end::VARCHAR(500) as country_nm,
# MAGIC 			name.country::VARCHAR(500) as district_nm,
# MAGIC 			name.address_code::VARCHAR(100) as EXT_ADDRESS_CODE,
# MAGIC 			name.latitude::numeric(20,
# MAGIC 			10) as GEO_LATITUDE,
# MAGIC 			name.longitude::numeric(20,
# MAGIC 			10) as GEO_LONGITUDE,
# MAGIC 			null as HOUSE_NM,
# MAGIC 			name.street_no::VARCHAR(100) as house_num,
# MAGIC 			name.timestamp::TIMESTAMP as INSERT_DATE,
# MAGIC 			user_det_userid.user_id::VARCHAR(50) as INSERT_USER,
# MAGIC 			name.tia_commit_date::TIMESTAMP as last_update_date,
# MAGIC 			user_det_record_userid.user_id::VARCHAR(50) as LAST_UPDATE_USER,
# MAGIC 			name.organisation::VARCHAR(60) as organisation_nm,
# MAGIC 			name.post_area::VARCHAR(255) as pin_zip_cd,
# MAGIC 			name.po_box::VARCHAR(6) as post_office_box_num,
# MAGIC 			name.post_street::VARCHAR(4) as post_street,
# MAGIC 			concat(name.street, ' ', name.street_no, case when name.floor is not null then concat(' ', name.floor) end, case when name.floor_ext is not null then concat(' ', name.floor_ext) end, case when name.city is not null then concat(', ', name.city) end)::VARCHAR(255) as reference_line_1,
# MAGIC 			name.postal_region::VARCHAR(500) as region_nm,
# MAGIC 			name.street::VARCHAR(500) as street_nm,
# MAGIC             null as address_cd,
# MAGIC             null as address_start_dt,
# MAGIC             null as address_end_dt,null as address_in_use_ind,null as block_txt,
# MAGIC             null as street_cd,
# MAGIC             null as street_type_cd,
# MAGIC             null as street_type_desc,
# MAGIC             null as reference_line_2,
# MAGIC             null as reference_line_3,
# MAGIC             null as municipality_nm,
# MAGIC             null as town_nm,
# MAGIC             null as province_cd,
# MAGIC             null as province_nm,
# MAGIC             null as city_type_cd,
# MAGIC             null as city_type_desc,
# MAGIC             null as city_cd,
# MAGIC             null as district_type_cd,
# MAGIC             null as district_type_desc,
# MAGIC             null as district_cd,
# MAGIC             null as state_cd,
# MAGIC             null as state_nm,
# MAGIC             null as region_cd,
# MAGIC             null as seasonal_flag,
# MAGIC             null as seasonal_from_day,
# MAGIC             null as seasonal_from_month,
# MAGIC             null as seasonal_to_day,
# MAGIC             null as seasonal_to_month,
# MAGIC             null as geo_location_type_cd,
# MAGIC             null as geo_location_type_desc,
# MAGIC             null as geo_elevation,
# MAGIC             null as pin_zip_plus,
# MAGIC             null as language_cd,
# MAGIC             null as language_desc,
# MAGIC             null as aud_src_sys_cd,
# MAGIC             null as aud_src_sys_updt_ver_id
# MAGIC 		from
# MAGIC 			`omi-catalog`.tia_ods.name name
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
# MAGIC 				and mst.src_sys_mst_tbl_nm = 'COUNTRY_CODE'
# MAGIC 				and mst.src_sys_nm = 'TIA'
# MAGIC 				and val.mst_val_si_language = 'en') mst_country_code on
# MAGIC 			name.country_code = mst_country_code.code
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_det_userid on
# MAGIC 			NAME.USERID = USER_DET_USERID.USER_NUM
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_det_record_userid on
# MAGIC 			NAME.RECORD_USERID = USER_DET_RECORD_USERID.USER_NUM
# MAGIC 	union
# MAGIC 		select
# MAGIC 			concat(name.id_no, '|', '2', '|', '101', '|', 'TIA')::VARCHAR(50) as address_id,
# MAGIC 			concat(name.id_no, '|', '2')::VARCHAR(50) as ADDRESS_NUM,
# MAGIC 			2 as address_type_cd,
# MAGIC 			concat('Mailing Address', '')::VARCHAR(500) as ADDRESS_TYPE_DESC,
# MAGIC 			name.mail_unknown_address::VARCHAR(3) as address_unknown_ind,
# MAGIC 			concat(name.mail_floor, ' ', case when name.mail_floor_ext is not null then concat(' ', name.mail_floor_ext) end)::VARCHAR(100) as appartment_num,
# MAGIC 			45 as AUD_BATCH_ID,
# MAGIC 			0 as AUD_IU_FLAG,
# MAGIC 			concat(name.id_no, '|', '2')::VARCHAR(50) as AUD_SRC_SYS_ID,
# MAGIC 			concat('TIA', '')::VARCHAR(10) as AUD_SRC_SYS_NM,
# MAGIC 			13 as AUD_SUB_BATCH_ID,
# MAGIC 			name.record_timestamp::TIMESTAMP as AUD_TRAN_DT,
# MAGIC 			name.mail_building_name::VARCHAR(255) as BUILDING_NM,
# MAGIC 			name.mail_building_number::VARCHAR(20) as BUILDING_NR,
# MAGIC 			name.mail_city::VARCHAR(500) as city_nm,
# MAGIC 			name.mail_country_code::VARCHAR(3) as country_cd,
# MAGIC 			case
# MAGIC 				when name.mail_country_code is null then name.mail_country
# MAGIC 				else mst_mail_country_code.description
# MAGIC 			end::VARCHAR(500) as country_nm,
# MAGIC 			name.mail_country::VARCHAR(500) as district_nm,
# MAGIC 			name.mail_address_code::VARCHAR(100) as EXT_ADDRESS_CODE,
# MAGIC 			null as GEO_LATITUDE,
# MAGIC 			null as GEO_LONGITUDE,
# MAGIC 			name.mail_house::VARCHAR(30) as HOUSE_NM,
# MAGIC 			name.mail_street_no::VARCHAR(100) as house_num,
# MAGIC 			name.timestamp::TIMESTAMP as INSERT_DATE,
# MAGIC 			user_det_userid.user_id::VARCHAR(50) as INSERT_USER,
# MAGIC 			name.tia_commit_date::TIMESTAMP as last_update_date,
# MAGIC 			user_det_record_userid.user_id::VARCHAR(50) as LAST_UPDATE_USER,
# MAGIC 			name.mail_organisation::VARCHAR(60) as organisation_nm,
# MAGIC 			name.mail_postarea::VARCHAR(255) as pin_zip_cd,
# MAGIC 			name.mail_po_box::VARCHAR(6) as post_office_box_num,
# MAGIC 			name.mail_poststreet::VARCHAR(4) as post_street,
# MAGIC 			concat(name.mail_street, ' ', name.mail_street_no, case when name.mail_floor is not null then concat(' ', name.mail_floor) end, case when name.mail_floor_ext is not null then concat(' ', name.mail_floor_ext) end, case when name.mail_city is not null then concat(', ', name.mail_city) end)::VARCHAR(255) as reference_line_1,
# MAGIC 			name.mail_postal_region::VARCHAR(500) as region_nm,
# MAGIC 			name.mail_street::VARCHAR(500) as street_nm,
# MAGIC             null as address_cd,
# MAGIC             null as address_start_dt,null as address_end_dt,null as address_in_use_ind,
# MAGIC             null as block_txt,
# MAGIC             null as street_cd,
# MAGIC             null as street_type_cd,
# MAGIC             null as street_type_desc,
# MAGIC             null as reference_line_2,
# MAGIC             null as reference_line_3,
# MAGIC             null as municipality_nm,
# MAGIC             null as town_nm,
# MAGIC             null as province_cd,
# MAGIC             null as province_nm,
# MAGIC             null as city_type_cd,
# MAGIC             null as city_type_desc,
# MAGIC             null as city_cd,
# MAGIC             null as district_type_cd,
# MAGIC             null as district_type_desc,
# MAGIC             null as district_cd,
# MAGIC             null as state_cd,
# MAGIC             null as state_nm,
# MAGIC             null as region_cd,
# MAGIC             null as seasonal_flag,
# MAGIC             null as seasonal_from_day,
# MAGIC             null as seasonal_from_month,
# MAGIC             null as seasonal_to_day,
# MAGIC             null as seasonal_to_month,
# MAGIC             null as geo_location_type_cd,
# MAGIC             null as geo_location_type_desc,
# MAGIC             null as geo_elevation,
# MAGIC             null as pin_zip_plus,
# MAGIC             null as language_cd,
# MAGIC             null as language_desc,
# MAGIC             null as aud_src_sys_cd,
# MAGIC             null as aud_src_sys_updt_ver_id
# MAGIC 		from
# MAGIC 			`omi-catalog`.tia_ods.name name
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
# MAGIC 				and mst.src_sys_mst_tbl_nm = 'COUNTRY_CODE'
# MAGIC 				and mst.src_sys_nm = 'TIA'
# MAGIC 				and val.mst_val_si_language = 'en') mst_mail_country_code on
# MAGIC 			name.mail_country_code = mst_mail_country_code.code
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_det_userid on
# MAGIC 			NAME.USERID = USER_DET_USERID.USER_NUM
# MAGIC 		left join `omi-catalog`.si_dwh.user_details user_det_record_userid on
# MAGIC 			NAME.RECORD_USERID = USER_DET_RECORD_USERID.USER_NUM

# COMMAND ----------

# MAGIC %sql
# MAGIC truncate table `omi-catalog`.si_dwh.address_details

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC ADDRESS_NUM,
# MAGIC count(*)
# MAGIC from `omi-catalog`.si_dwh.address_details ad
# MAGIC group by 
# MAGIC ADDRESS_NUM
# MAGIC having count(*)>1

# COMMAND ----------

# MAGIC %sql
# MAGIC select
# MAGIC ADDRESS_ID,
# MAGIC ADDRESS_NUM
# MAGIC from `omi-catalog`.si_dwh.address_details
# MAGIC where ADDRESS_ID is null or ADDRESS_NUM is null

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select 'TGT' Table_name ,count (*)
# MAGIC -- from `omi-catalog`.si_dwh.address_details ad 
# MAGIC -- union all 
# MAGIC -- select 'SRC' Table_name,count (*)
# MAGIC -- from (
# MAGIC SELECT concat(n.id_no,'|','1','|','101','|','TIA') AS address_id,
# MAGIC concat(n.id_no,'|','1') AS ADDRESS_NUM,
# MAGIC 1 AS address_type_cd,
# MAGIC concat('Permanent Address','') AS ADDRESS_TYPE_DESC,
# MAGIC n.unknown_address AS address_unknown_ind,
# MAGIC concat(n.floor, ' ', case when n.floor_ext is not null then concat(' ', n.floor_ext) end) AS appartment_num,
# MAGIC n.street_no AS house_num,
# MAGIC n.building_name AS BUILDING_NM,
# MAGIC n.street AS street_nm,
# MAGIC concat(n.street, ' ', n.street_no, case when n.floor is not null then concat(' ', n.floor) end, 
# MAGIC case when n.floor_ext is not null then concat(' ', n.floor_ext) end, 
# MAGIC case when n.city is not null then concat(', ', n.city) end) AS reference_line_1,
# MAGIC n.city AS city_nm,
# MAGIC n.country AS district_nm,
# MAGIC n.postal_region AS region_nm,
# MAGIC n.country_code AS country_cd,
# MAGIC case when n.country_code is null then n.country else MST1.description end AS country_nm,
# MAGIC n.latitude AS GEO_LATITUDE,
# MAGIC n.longitude AS GEO_LONGITUDE,
# MAGIC n.po_box AS post_office_box_num,
# MAGIC n.post_area AS pin_zip_cd,
# MAGIC concat('TIA','') AS AUD_SRC_SYS_NM,
# MAGIC concat(n.id_no,'|','1') AS AUD_SRC_SYS_ID,
# MAGIC --1 AS AUD_BATCH_ID,
# MAGIC --1 AS AUD_SUB_BATCH_ID,
# MAGIC --0 AS AUD_IU_FLAG,
# MAGIC n.record_timestamp AS AUD_TRAN_DT,
# MAGIC n.tia_commit_date as last_update_date,
# MAGIC ud1.user_id AS LAST_UPDATE_USER,
# MAGIC n.timestamp AS INSERT_DATE,
# MAGIC ud2.user_id AS INSERT_USER,
# MAGIC null as HOUSE_NM,
# MAGIC n.organisation AS organisation_nm,
# MAGIC n.address_code AS EXT_ADDRESS_CODE,
# MAGIC n.post_street AS post_street,
# MAGIC n.building_number AS BUILDING_NR FROM `omi-catalog`.tia_ods.name n 
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'COUNTRY_CODE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='TIA') MST1
# MAGIC  ON n.country_code=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud1 ON n.RECORD_USERID = ud1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud2 ON n.USERID = ud2.USER_NUM 
# MAGIC 
# MAGIC union all
# MAGIC  
# MAGIC SELECT concat(n.id_no,'|','2','|','101','|','TIA') as address_id,
# MAGIC concat(n.id_no,'|','2') as ADDRESS_NUM,
# MAGIC 2 as address_type_cd,
# MAGIC concat('Mailing Address','') AS ADDRESS_TYPE_DESC,
# MAGIC n.mail_unknown_address as address_unknown_ind,
# MAGIC concat(n.mail_floor, ' ', case when n.mail_floor_ext is not null then concat(' ', n.mail_floor_ext) end) as appartment_num,
# MAGIC n.mail_street_no as house_num,
# MAGIC n.mail_building_name as BUILDING_NM,
# MAGIC n.mail_street as street_nm,
# MAGIC concat(n.mail_street, ' ', n.mail_street_no, case when n.mail_floor is not null then concat(' ', n.mail_floor) end, 
# MAGIC case when n.mail_floor_ext is not null then concat(' ', n.mail_floor_ext) end, 
# MAGIC case when n.mail_city is not null then concat(', ', n.mail_city) end) as reference_line_1,
# MAGIC n.mail_city as city_nm,
# MAGIC n.mail_country as district_nm,
# MAGIC n.mail_postal_region as region_nm,
# MAGIC n.mail_country_code as country_cd,
# MAGIC case when n.mail_country_code is null then n.mail_country else MST1.description end as country_nm,
# MAGIC null as GEO_LATITUDE,
# MAGIC null as GEO_LONGITUDE,
# MAGIC n.mail_po_box as post_office_box_num,
# MAGIC n.mail_postarea as pin_zip_cd,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC concat(n.id_no,'|','2') as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC n.record_timestamp AS AUD_TRAN_DT,
# MAGIC n.tia_commit_date as last_update_date,
# MAGIC ud1.user_id as LAST_UPDATE_USER,
# MAGIC n.timestamp as INSERT_DATE,
# MAGIC ud2.user_id as INSERT_USER,
# MAGIC n.mail_house as HOUSE_NM,
# MAGIC n.mail_organisation as organisation_nm,
# MAGIC n.mail_address_code as EXT_ADDRESS_CODE,
# MAGIC n.mail_poststreet as post_street,
# MAGIC n.mail_building_number as BUILDING_NR from `omi-catalog`.tia_ods.name n 
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'COUNTRY_CODE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='TIA') MST1
# MAGIC  ON n.mail_country_code=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud1 ON n.RECORD_USERID = ud1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud2 ON n.USERID = ud2.USER_NUM 
# MAGIC --where ( mail_street is not null or mail_city is not null or mail_country_code is not null or mail_country is not null ) 
# MAGIC --)s 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from `omi-catalog`.si_dwh.address_details

# COMMAND ----------

# MAGIC %sql
# MAGIC ---------count check
# MAGIC 
# MAGIC 
# MAGIC select count(*),'SRC' from (
# MAGIC SELECT concat(n.id_no,'|','1','|','101','|','TIA') AS address_id,
# MAGIC concat(n.id_no,'|','1') AS ADDRESS_NUM,
# MAGIC 1 AS address_type_cd,
# MAGIC concat('Permanent Address','') AS ADDRESS_TYPE_DESC,
# MAGIC n.unknown_address AS address_unknown_ind,
# MAGIC concat(n.floor, ' ', case when n.floor_ext is not null then concat(' ', n.floor_ext) end) AS appartment_num,
# MAGIC n.street_no AS house_num,
# MAGIC n.building_name AS BUILDING_NM,
# MAGIC n.street AS street_nm,
# MAGIC concat(n.street, ' ', n.street_no, case when n.floor is not null then concat(' ', n.floor) end, 
# MAGIC case when n.floor_ext is not null then concat(' ', n.floor_ext) end, 
# MAGIC case when n.city is not null then concat(', ', n.city) end) AS reference_line_1,
# MAGIC n.city AS city_nm,
# MAGIC n.country AS district_nm,
# MAGIC n.postal_region AS region_nm,
# MAGIC n.country_code AS country_cd,
# MAGIC case when n.country_code is null then n.country else MST1.description end AS country_nm,
# MAGIC n.latitude AS GEO_LATITUDE,
# MAGIC n.longitude AS GEO_LONGITUDE,
# MAGIC n.po_box AS post_office_box_num,
# MAGIC n.post_area AS pin_zip_cd,
# MAGIC concat('TIA','') AS AUD_SRC_SYS_NM,
# MAGIC concat(n.id_no,'|','1') AS AUD_SRC_SYS_ID,
# MAGIC --1 AS AUD_BATCH_ID,
# MAGIC --1 AS AUD_SUB_BATCH_ID,
# MAGIC --0 AS AUD_IU_FLAG,
# MAGIC n.record_timestamp AS AUD_TRAN_DT,
# MAGIC n.tia_commit_date as last_update_date,
# MAGIC ud1.user_id AS LAST_UPDATE_USER,
# MAGIC n.timestamp AS INSERT_DATE,
# MAGIC ud2.user_id AS INSERT_USER,
# MAGIC null as HOUSE_NM,
# MAGIC n.organisation AS organisation_nm,
# MAGIC n.address_code AS EXT_ADDRESS_CODE,
# MAGIC n.post_street AS post_street,
# MAGIC n.building_number AS BUILDING_NR FROM `omi-catalog`.tia_ods.name n 
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'COUNTRY_CODE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1
# MAGIC  ON n.country_code=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud1 ON n.RECORD_USERID = ud1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud2 ON n.USERID = ud2.USER_NUM 
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC  
# MAGIC SELECT concat(n.id_no,'|','2','|','101','|','TIA') as address_id,
# MAGIC concat(n.id_no,'|','2') as ADDRESS_NUM,
# MAGIC 2 as address_type_cd,
# MAGIC concat('Mailing Address','') AS ADDRESS_TYPE_DESC,
# MAGIC n.mail_unknown_address as address_unknown_ind,
# MAGIC concat(n.mail_floor, ' ', case when n.mail_floor_ext is not null then concat(' ', n.mail_floor_ext) end) as appartment_num,
# MAGIC n.mail_street_no as house_num,
# MAGIC n.mail_building_name as BUILDING_NM,
# MAGIC n.mail_street as street_nm,
# MAGIC concat(n.mail_street, ' ', n.mail_street_no, case when n.mail_floor is not null then concat(' ', n.mail_floor) end, 
# MAGIC case when n.mail_floor_ext is not null then concat(' ', n.mail_floor_ext) end, 
# MAGIC case when n.mail_city is not null then concat(', ', n.mail_city) end) as reference_line_1,
# MAGIC n.mail_city as city_nm,
# MAGIC n.mail_country as district_nm,
# MAGIC n.mail_postal_region as region_nm,
# MAGIC n.mail_country_code as country_cd,
# MAGIC case when n.mail_country_code is null then n.mail_country else MST1.description end as country_nm,
# MAGIC null as GEO_LATITUDE,
# MAGIC null as GEO_LONGITUDE,
# MAGIC n.mail_po_box as post_office_box_num,
# MAGIC n.mail_postarea as pin_zip_cd,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC concat(n.id_no,'|','2') as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC n.record_timestamp AS AUD_TRAN_DT,
# MAGIC n.tia_commit_date as last_update_date,
# MAGIC ud1.user_id as LAST_UPDATE_USER,
# MAGIC n.timestamp as INSERT_DATE,
# MAGIC ud2.user_id as INSERT_USER,
# MAGIC n.mail_house as HOUSE_NM,
# MAGIC n.mail_organisation as organisation_nm,
# MAGIC n.mail_address_code as EXT_ADDRESS_CODE,
# MAGIC n.mail_poststreet as post_street,
# MAGIC n.mail_building_number as BUILDING_NR from `omi-catalog`.tia_ods.name n 
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'COUNTRY_CODE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1
# MAGIC  ON n.mail_country_code=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud1 ON n.RECORD_USERID = ud1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud2 ON n.USERID = ud2.USER_NUM 
# MAGIC --where ( mail_street is not null or mail_city is not null or mail_country_code is not null or mail_country is not null ) 
# MAGIC )s 
# MAGIC 
# MAGIC union all 
# MAGIC 
# MAGIC 
# MAGIC select count(*),'TGT' 
# MAGIC from `omi-catalog`.si_dwh.address_details ad 

# COMMAND ----------

# MAGIC %sql
# MAGIC ----source minus target
# MAGIC 
# MAGIC ----------------address_details 
# MAGIC 
# MAGIC 
# MAGIC select * from (
# MAGIC SELECT concat(n.id_no,'|','1','|','101','|','TIA') AS address_id,
# MAGIC concat(n.id_no,'|','1') AS ADDRESS_NUM,
# MAGIC 1 AS address_type_cd,
# MAGIC concat('Permanent Address','') AS ADDRESS_TYPE_DESC,
# MAGIC n.unknown_address AS address_unknown_ind,
# MAGIC concat(n.floor, ' ', case when n.floor_ext is not null then concat(' ', n.floor_ext) end) AS appartment_num,
# MAGIC n.street_no AS house_num,
# MAGIC n.building_name AS BUILDING_NM,
# MAGIC n.street AS street_nm,
# MAGIC concat(n.street, ' ', n.street_no, case when n.floor is not null then concat(' ', n.floor) end, 
# MAGIC case when n.floor_ext is not null then concat(' ', n.floor_ext) end, 
# MAGIC case when n.city is not null then concat(', ', n.city) end) AS reference_line_1,
# MAGIC n.city AS city_nm,
# MAGIC n.country AS district_nm,
# MAGIC n.postal_region AS region_nm,
# MAGIC n.country_code AS country_cd,
# MAGIC case when n.country_code is null then n.country else MST1.description end AS country_nm,
# MAGIC n.latitude AS GEO_LATITUDE,
# MAGIC n.longitude AS GEO_LONGITUDE,
# MAGIC n.po_box AS post_office_box_num,
# MAGIC n.post_area AS pin_zip_cd,
# MAGIC concat('TIA','') AS AUD_SRC_SYS_NM,
# MAGIC concat(n.id_no,'|','1') AS AUD_SRC_SYS_ID,
# MAGIC --1 AS AUD_BATCH_ID,
# MAGIC --1 AS AUD_SUB_BATCH_ID,
# MAGIC --0 AS AUD_IU_FLAG,
# MAGIC n.record_timestamp AS AUD_TRAN_DT,
# MAGIC n.tia_commit_date as last_update_date,
# MAGIC ud1.user_id AS LAST_UPDATE_USER,
# MAGIC n.timestamp AS INSERT_DATE,
# MAGIC ud2.user_id AS INSERT_USER,
# MAGIC null as HOUSE_NM,
# MAGIC n.organisation AS organisation_nm,
# MAGIC n.address_code AS EXT_ADDRESS_CODE,
# MAGIC n.post_street AS post_street,
# MAGIC n.building_number AS BUILDING_NR FROM `omi-catalog`.tia_ods.name n 
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'COUNTRY_CODE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1
# MAGIC  ON n.country_code=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud1 ON n.RECORD_USERID = ud1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud2 ON n.USERID = ud2.USER_NUM 
# MAGIC 
# MAGIC 
# MAGIC union all
# MAGIC 
# MAGIC  
# MAGIC SELECT concat(n.id_no,'|','2','|','101','|','TIA') as address_id,
# MAGIC concat(n.id_no,'|','2') as ADDRESS_NUM,
# MAGIC 2 as address_type_cd,
# MAGIC concat('Mailing Address','') AS ADDRESS_TYPE_DESC,
# MAGIC n.mail_unknown_address as address_unknown_ind,
# MAGIC concat(n.mail_floor, ' ', case when n.mail_floor_ext is not null then concat(' ', n.mail_floor_ext) end) as appartment_num,
# MAGIC n.mail_street_no as house_num,
# MAGIC n.mail_building_name as BUILDING_NM,
# MAGIC n.mail_street as street_nm,
# MAGIC concat(n.mail_street, ' ', n.mail_street_no, case when n.mail_floor is not null then concat(' ', n.mail_floor) end, 
# MAGIC case when n.mail_floor_ext is not null then concat(' ', n.mail_floor_ext) end, 
# MAGIC case when n.mail_city is not null then concat(', ', n.mail_city) end) as reference_line_1,
# MAGIC n.mail_city as city_nm,
# MAGIC n.mail_country as district_nm,
# MAGIC n.mail_postal_region as region_nm,
# MAGIC n.mail_country_code as country_cd,
# MAGIC case when n.mail_country_code is null then n.mail_country else MST1.description end as country_nm,
# MAGIC null as GEO_LATITUDE,
# MAGIC null as GEO_LONGITUDE,
# MAGIC n.mail_po_box as post_office_box_num,
# MAGIC n.mail_postarea as pin_zip_cd,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC concat(n.id_no,'|','2') as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC n.record_timestamp AS AUD_TRAN_DT,
# MAGIC n.tia_commit_date as last_update_date,
# MAGIC ud1.user_id as LAST_UPDATE_USER,
# MAGIC n.timestamp as INSERT_DATE,
# MAGIC ud2.user_id as INSERT_USER,
# MAGIC n.mail_house as HOUSE_NM,
# MAGIC n.mail_organisation as organisation_nm,
# MAGIC n.mail_address_code as EXT_ADDRESS_CODE,
# MAGIC n.mail_poststreet as post_street,
# MAGIC n.mail_building_number as BUILDING_NR from `omi-catalog`.tia_ods.name n 
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'COUNTRY_CODE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='en') MST1
# MAGIC  ON n.mail_country_code=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud1 ON n.RECORD_USERID = ud1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud2 ON n.USERID = ud2.USER_NUM 
# MAGIC --where ( mail_street is not null or mail_city is not null or mail_country_code is not null or mail_country is not null ) 
# MAGIC )s 
# MAGIC 
# MAGIC except 
# MAGIC 
# MAGIC 
# MAGIC select 
# MAGIC address_id,
# MAGIC ADDRESS_NUM,
# MAGIC address_type_cd,
# MAGIC ADDRESS_TYPE_DESC,
# MAGIC address_unknown_ind,
# MAGIC appartment_num,
# MAGIC house_num,
# MAGIC BUILDING_NM,
# MAGIC street_nm,
# MAGIC reference_line_1,
# MAGIC city_nm,
# MAGIC district_nm,
# MAGIC region_nm,
# MAGIC country_cd,
# MAGIC country_nm,
# MAGIC GEO_LATITUDE,
# MAGIC GEO_LONGITUDE,
# MAGIC post_office_box_num,
# MAGIC pin_zip_cd,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC --AUD_IU_FLAG,
# MAGIC AUD_TRAN_DT,
# MAGIC last_update_date,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER,
# MAGIC HOUSE_NM,
# MAGIC organisation_nm,
# MAGIC EXT_ADDRESS_CODE,
# MAGIC post_street,
# MAGIC BUILDING_NR
# MAGIC from `omi-catalog`.si_dwh.address_details ad 

# COMMAND ----------

# MAGIC %sql
# MAGIC select 
# MAGIC address_id,
# MAGIC ADDRESS_NUM,
# MAGIC address_type_cd,
# MAGIC ADDRESS_TYPE_DESC,
# MAGIC address_unknown_ind,
# MAGIC appartment_num,
# MAGIC house_num,
# MAGIC BUILDING_NM,
# MAGIC street_nm,
# MAGIC reference_line_1,
# MAGIC city_nm,
# MAGIC district_nm,
# MAGIC region_nm,
# MAGIC country_cd,
# MAGIC country_nm,
# MAGIC GEO_LATITUDE,
# MAGIC GEO_LONGITUDE,
# MAGIC post_office_box_num,
# MAGIC pin_zip_cd,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC --AUD_IU_FLAG,
# MAGIC AUD_TRAN_DT,
# MAGIC last_update_date,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER,
# MAGIC HOUSE_NM,
# MAGIC organisation_nm,
# MAGIC EXT_ADDRESS_CODE,
# MAGIC post_street,
# MAGIC BUILDING_NR
# MAGIC from `omi-catalog`.si_dwh.address_details ad 

# COMMAND ----------

# MAGIC %sql
# MAGIC ---Target minus Source
# MAGIC 
# MAGIC select 
# MAGIC address_id,
# MAGIC ADDRESS_NUM,
# MAGIC address_type_cd,
# MAGIC ADDRESS_TYPE_DESC,
# MAGIC address_unknown_ind,
# MAGIC appartment_num,
# MAGIC house_num,
# MAGIC BUILDING_NM,
# MAGIC street_nm,
# MAGIC reference_line_1,
# MAGIC city_nm,
# MAGIC district_nm,
# MAGIC region_nm,
# MAGIC country_cd,
# MAGIC country_nm,
# MAGIC GEO_LATITUDE,
# MAGIC GEO_LONGITUDE,
# MAGIC post_office_box_num,
# MAGIC pin_zip_cd,
# MAGIC AUD_SRC_SYS_NM,
# MAGIC AUD_SRC_SYS_ID,
# MAGIC --AUD_BATCH_ID,
# MAGIC --AUD_SUB_BATCH_ID,
# MAGIC --AUD_IU_FLAG,
# MAGIC AUD_TRAN_DT,
# MAGIC last_update_date,
# MAGIC LAST_UPDATE_USER,
# MAGIC INSERT_DATE,
# MAGIC INSERT_USER,
# MAGIC HOUSE_NM,
# MAGIC organisation_nm,
# MAGIC EXT_ADDRESS_CODE,
# MAGIC post_street,
# MAGIC BUILDING_NR
# MAGIC from `omi-catalog`.si_dwh.address_details 
# MAGIC 
# MAGIC except
# MAGIC 
# MAGIC select * from (
# MAGIC SELECT concat(n.id_no,'|','1','|','101','|','TIA') AS address_id,
# MAGIC concat(n.id_no,'|','1') AS ADDRESS_NUM,
# MAGIC 1 AS address_type_cd,
# MAGIC concat('Permanent Address','') AS ADDRESS_TYPE_DESC,
# MAGIC n.unknown_address AS address_unknown_ind,
# MAGIC concat(n.floor, ' ', case when n.floor_ext is not null then concat(' ', n.floor_ext) end) AS appartment_num,
# MAGIC n.street_no AS house_num,
# MAGIC n.building_name AS BUILDING_NM,
# MAGIC n.street AS street_nm,
# MAGIC concat(n.street, ' ', n.street_no, case when n.floor is not null then concat(' ', n.floor) end, 
# MAGIC case when n.floor_ext is not null then concat(' ', n.floor_ext) end, 
# MAGIC case when n.city is not null then concat(', ', n.city) end) AS reference_line_1,
# MAGIC n.city AS city_nm,
# MAGIC n.country AS district_nm,
# MAGIC n.postal_region AS region_nm,
# MAGIC n.country_code AS country_cd,
# MAGIC case when n.country_code is null then n.country else MST1.description end AS country_nm,
# MAGIC n.latitude AS GEO_LATITUDE,
# MAGIC n.longitude AS GEO_LONGITUDE,
# MAGIC n.po_box AS post_office_box_num,
# MAGIC n.post_area AS pin_zip_cd,
# MAGIC concat('TIA','') AS AUD_SRC_SYS_NM,
# MAGIC concat(n.id_no,'|','1') AS AUD_SRC_SYS_ID,
# MAGIC --1 AS AUD_BATCH_ID,
# MAGIC --1 AS AUD_SUB_BATCH_ID,
# MAGIC --0 AS AUD_IU_FLAG,
# MAGIC n.record_timestamp AS AUD_TRAN_DT,
# MAGIC n.tia_commit_date as last_update_date,
# MAGIC ud1.user_id AS LAST_UPDATE_USER,
# MAGIC n.timestamp AS INSERT_DATE,
# MAGIC ud2.user_id AS INSERT_USER,
# MAGIC null as HOUSE_NM,
# MAGIC n.organisation AS organisation_nm,
# MAGIC n.address_code AS EXT_ADDRESS_CODE,
# MAGIC n.post_street AS post_street,
# MAGIC n.building_number AS BUILDING_NR FROM `omi-catalog`.tia_ods.name n 
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'COUNTRY_CODE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='TIA') MST1
# MAGIC  ON n.country_code=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud1 ON n.RECORD_USERID = ud1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud2 ON n.USERID = ud2.USER_NUM 
# MAGIC 
# MAGIC 
# MAGIC union
# MAGIC 
# MAGIC  
# MAGIC SELECT concat(n.id_no,'|','2','|','101','|','TIA') as address_id,
# MAGIC concat(n.id_no,'|','2') as ADDRESS_NUM,
# MAGIC 2 as address_type_cd,
# MAGIC concat('Mailing Address','') AS ADDRESS_TYPE_DESC,
# MAGIC n.mail_unknown_address as address_unknown_ind,
# MAGIC concat(n.mail_floor, ' ', case when n.mail_floor_ext is not null then concat(' ', n.mail_floor_ext) end) as appartment_num,
# MAGIC n.mail_street_no as house_num,
# MAGIC n.mail_building_name as BUILDING_NM,
# MAGIC n.mail_street as street_nm,
# MAGIC concat(n.mail_street, ' ', n.mail_street_no, case when n.mail_floor is not null then concat(' ', n.mail_floor) end, 
# MAGIC case when n.mail_floor_ext is not null then concat(' ', n.mail_floor_ext) end, 
# MAGIC case when n.mail_city is not null then concat(', ', n.mail_city) end) as reference_line_1,
# MAGIC n.mail_city as city_nm,
# MAGIC n.mail_country as district_nm,
# MAGIC n.mail_postal_region as region_nm,
# MAGIC n.mail_country_code as country_cd,
# MAGIC case when n.mail_country_code is null then n.mail_country else MST1.description end as country_nm,
# MAGIC null as GEO_LATITUDE,
# MAGIC null as GEO_LONGITUDE,
# MAGIC n.mail_po_box as post_office_box_num,
# MAGIC n.mail_postarea as pin_zip_cd,
# MAGIC concat('TIA','') as AUD_SRC_SYS_NM,
# MAGIC concat(n.id_no,'|','2') as AUD_SRC_SYS_ID,
# MAGIC --1 as AUD_BATCH_ID,
# MAGIC --1 as AUD_SUB_BATCH_ID,
# MAGIC --0 as AUD_IU_FLAG,
# MAGIC n.record_timestamp AS AUD_TRAN_DT,
# MAGIC n.tia_commit_date as last_update_date,
# MAGIC ud1.user_id as LAST_UPDATE_USER,
# MAGIC n.timestamp as INSERT_DATE,
# MAGIC ud2.user_id as INSERT_USER,
# MAGIC n.mail_house as HOUSE_NM,
# MAGIC n.mail_organisation as organisation_nm,
# MAGIC n.mail_address_code as EXT_ADDRESS_CODE,
# MAGIC n.mail_poststreet as post_street,
# MAGIC n.mail_building_number as BUILDING_NR from `omi-catalog`.tia_ods.name n 
# MAGIC LEFT OUTER JOIN (select val.mst_val_si_cd code, val.mst_val_si_desc description, mst.src_sys_mst_tbl_nm
# MAGIC  from `omi-catalog`.si_dwh.mst_mapping_table_list mst, `omi-catalog`.si_dwh.mst_table_val_list val 
# MAGIC  where mst.mst_tbl_si_id = val.mst_tbl_si_cd and mst.src_sys_mst_tbl_nm = 'COUNTRY_CODE' 
# MAGIC  and mst.src_sys_nm = 'TIA'  and val.mst_val_si_language='TIA') MST1
# MAGIC  ON n.mail_country_code=MST1.code
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud1 ON n.RECORD_USERID = ud1.USER_NUM
# MAGIC LEFT OUTER JOIN `omi-catalog`.si_dwh.user_details ud2 ON n.USERID = ud2.USER_NUM 
# MAGIC --where ( mail_street is not null or mail_city is not null or mail_country_code is not null or mail_country is not null ) 
# MAGIC )s

# COMMAND ----------


