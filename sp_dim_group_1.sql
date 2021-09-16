CREATE OR REPLACE PROCEDURE edw.sp_dim_group1(in_src_last_modified "TIMESTAMP",in_run_id "int4",in_job_name "varchar") 
LANGUAGE plpgsql AS $$

DECLARE
	
v_checkpoint TEXT;
v_proc_name VARCHAR:= 'EDW.SP_DIM_GROUP - ';
v_get_sysdate TIMESTAMP;

BEGIN

SELECT GETDATE() INTO v_get_sysdate;

/* Below temporary table holds staging/landing info of various redshift tables.we apply all business rules ,joins & other transformations at this level. */
v_checkpoint = v_proc_name || ' AN ERROR OCCURRED WHILE INSERTING THE DATA INTO TEMPORARY TABLE - stg_dim_group';

DROP  TABLE IF EXISTS stg_dim_group;


CREATE TEMPORARY TABLE  stg_dim_group     
AS 
SELECT
	grp.group_id AS group_id,
	NVL(acct.account_name,'Unk') AS group_name,
	NVL(acct.csa_dim_employee_key,0) AS csa_dim_employee_key,
    NVL(acct.csa_full_name,'Unk') AS csa_full_name,
    NVL(acct.csm_dim_employee_key,0) AS csm_dim_employee_key,
    NVL(acct.csm_full_name,'Unk') AS csm_full_name,
    NVL(acct.csd_dim_employee_key,0) AS csd_dim_employee_key,
    NVL(acct.csd_full_name,'Unk') AS csd_full_name,
    NVL(acct.keyrep_dim_employee_key,0) AS keyrep_dim_employee_key,
    NVL(acct.keyrep_full_name,'Unk') AS keyrep_full_name,
    NVL(acct.dos_dim_employee_key,0) AS dos_dim_employee_key,
    NVL(acct.dos_full_name,'Unk') AS dos_full_name,
    NVL(acct.rgm_dim_employee_key,0) AS rgm_dim_employee_key,
    NVL(acct.rgm_full_name,'Unk') AS rgm_full_name,
    NVL(acct.keycam_dim_employee_key,0) AS keycam_dim_employee_key,
    NVL(acct.keycam_full_name,'Unk') AS keycam_full_name,
    NVL(acct.nrm_dim_employee_key,0) AS nrm_dim_employee_key,
    NVL(acct.nrm_full_name,'Unk') AS nrm_full_name,
    GREATEST(grp.sys_last_imported_date,acct.last_imported) AS sys_src_last_modified_date,	
	v_get_sysdate AS sys_effective_start_date,
    to_timestamp('9999-12-31 23:59:59','yyyy-mm-dd hh24:mi:ss') AS sys_effective_end_date,	
    'Yes' AS sys_current_flag,
    'INSERT' AS scd_type_action,
	v_get_sysdate AS sys_first_imported_date,
	v_get_sysdate AS sys_last_imported_date,
	in_job_name AS sys_first_imported_by,
	in_job_name AS sys_last_imported_by,
	in_run_id AS sys_created_etl_batch_key,
	in_run_id AS sys_updated_etl_batch_key,
    'Active' AS group_status				
FROM
    (SELECT group_id,MAX(sys_last_imported_date) AS sys_last_imported_date 
     FROM edw.dim_customer 
     WHERE sys_current_flag='Yes'  AND NVL(group_id,'X')<>'n/a'
     GROUP BY group_id) grp
    LEFT JOIN
	 stg_edw.sf_account_consolidated acct
    ON grp.group_id=acct.account_id 
WHERE  
     GREATEST(grp.sys_last_imported_date,acct.last_imported)>=in_src_last_modified;
    

v_checkpoint = v_proc_name || ' AN ERROR OCCURRED WHILE POPULATING EDW.DIM_GROUP DIMENSION TABLE USING SYS_EDH.SYS_AUTOMATED_QUERIES TABLE - ';

/*All Type 1 specific sql code will be executed via this SP, We need to parse table_schema and table_name as pararmeters to this */
CALL SYS_EDH.sp_execute_automated_queries('edw','dim_group',v_get_sysdate,in_job_name,in_run_id);

UPDATE edw.dim_group SET group_status='Inactive'
WHERE NVL(group_id,'XXXX') NOT IN 
(SELECT DISTINCT group_id FROM edw.dim_customer WHERE sys_current_flag='Yes')
AND NVL(group_id,'XXXX')<>'Unk';

EXCEPTION
WHEN OTHERS THEN RAISE EXCEPTION '% %',v_checkpoint,SQLERRM ;

END;

$$ ;
