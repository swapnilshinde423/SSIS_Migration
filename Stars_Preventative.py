import boto3
import base64
from botocore.exceptions import ClientError
import json
import pandas as pd
from sqlalchemy import create_engine

s3 = boto3.resource('s3')

client = boto3.client("secretsmanager", region_name="us-east-2")

get_secret_value_response = client.get_secret_value(SecretId="SecretKeysForAWSGlue")

secret = get_secret_value_response['SecretString']
secret = json.loads(secret)

pg_host=secret.get('con_db_host')
pg_port=secret.get('con_db_port')
pg_user=secret.get('con_db_user')
pg_passwd=secret.get('con_db_password')
DB_CCUSDN_SDN_DialerStore = secret.get('con_db_CCUSDN_SDN_DialerStore')
DB_P10PRDSDE001_StarsConnect = secret.get('con_db_P10PRDSDE001_StarsConnect')
DB_OPS_Stars_Call_Lists = secret.get('con_db_OPS_Stars_Call_Lists')

engine_CCUSDN_SDN_DialerStore = create_engine('postgresql://'+pg_user+':'+pg_passwd+'@'+pg_host+':'+pg_port+'/'+DB_CCUSDN_SDN_DialerStore)
engine_P10PRDSDE001_StarsConnect = create_engine('postgresql://'+pg_user+':'+pg_passwd+'@'+pg_host+':'+pg_port+'/'+DB_P10PRDSDE001_StarsConnect)
engine_OPS_Stars_Call_Lists = create_engine('postgresql://'+pg_user+':'+pg_passwd+'@'+pg_host+':'+pg_port+'/'+DB_OPS_Stars_Call_Lists)

con_CCUSDN_SDN_DialerStore=engine_CCUSDN_SDN_DialerStore.connect()
con_P10PRDSDE001_StarsConnect=engine_P10PRDSDE001_StarsConnect.connect()
con_OPS_Stars_Call_Lists=engine_OPS_Stars_Call_Lists.connect()

selectfromStarsConnect="SELECT "\
"CAST (pcd.pmd_patient_id AS VARCHAR(1000)) AS pmd_patient_id, CAST (pcd.patient_name AS VARCHAR(1000)) AS patient_name, pcd.patient_count, CAST (COALESCE(pho.phone_number, pcd.phone_number) AS VARCHAR(10)) AS phone_number, CAST (pcd.client_name AS VARCHAR(50)) AS client_name, pcd.pmd_client_id, CAST (pcd.zip_code AS VARCHAR(10)) AS zip_code, pcd.lowbound_ts, pcd.highbound_ts, CAST (pcd.state_code AS VARCHAR(4)) AS state_code, CAST (pcd.time_zone AS VARCHAR(10)) AS time_zone, now()::DATE+1 AS campaign_date, pcd.phone_type, COALESCE(cll.Filter, pcd.filter) AS filter, MAX(pcd.measureadh_status) AS measureadh_status, MIN(pcd.stockinhand) AS stockinhand, SUM(pcd.adh_calllist_order) AS adh_calllist_order, COUNT(DISTINCT pcd.measureuse) AS measure_count, pcd.planidentifier_pclm, MIN(pcd.dtp_touched_count) AS dtp_touched_count, MIN(cll.time_priority) AS time_priority "\
"FROM staging.preventativecalldetail AS pcd "\
"JOIN dbo.measures AS mea "\
"ON LOWER(pcd.measureuse) = LOWER(mea.key) "\
"JOIN dbo.dtps AS dtp "\
"ON pcd.pmd_patient_id = dtp.pmd_patient_id AND mea.id = dtp.measure_id AND dtp.dtp_year = date_part('year', clock_timestamp()) AND dtp.dtp_visibility_id = (SELECT "\
"vis.id "\
"FROM dbo.dtp_visibilities AS vis "\
"WHERE LOWER(CONCAT(date_part('year', clock_timestamp()), ' Active')) = LOWER(vis.name)) "\
"JOIN dbo.patients AS pat "\
"ON pcd.pmd_patient_id = pat.pmd_patient_id "\
"INNER JOIN LATERAL (SELECT "\
"_pho.number AS phone_number, "\
"_pho.entity_id "\
"FROM dbo.phones AS _pho "\
"WHERE LOWER(_pho.entity_type) = LOWER('Patient') AND LOWER(_pho.phone_type) = LOWER('Phone') AND _pho.primary = TRUE AND LENGTH(_pho.number) = 10 "\
"LIMIT 1) AS pho on true  "\
"LEFT JOIN LATERAL (SELECT "\
"_cll.status_detail_id, _cll.created_at, (_CLL.created_at::DATE)-(now()::date +1) AS time_priority, "\
"CASE COALESCE(_lan.language_id, 14) "\
"WHEN 14 THEN 'standard' "\
"WHEN 54 THEN 'Spanish' "\
"ELSE 'Other' "\
"END AS filter, "\
"_dtp.pmd_patient_id, "\
"_mea.key "\
"FROM dbo.calls AS _cll "\
"JOIN dbo.call_dtps AS _cdp "\
"ON _cll.id = _cdp.call_id "\
"JOIN dbo.dtps AS _dtp "\
"ON _cdp.dtp_id = _dtp.id "\
"JOIN dbo.measures AS _mea "\
"ON _dtp.measure_id = _mea.id "\
"LEFT OUTER JOIN dbo.spoken_languages AS _lan "\
"ON LOWER(_cll.entity_type) = LOWER(_lan.entity_type) AND _cll.entity_id = _lan.entity_id AND _lan.primary = TRUE "\
"WHERE _cll.created_at::DATE >= date_trunc('year', now())::DATE "\
"ORDER BY _cll.created_at DESC NULLS FIRST "\
"LIMIT 1) AS cll on TRUE  "\
"WHERE (cll.status_detail_id IS NULL OR cll.status_detail_id IN (11, 12) AND cll.time_priority >= 1 OR cll.status_detail_id IN (7, 8, 9, 14) AND cll.time_priority >= 3 OR cll.status_detail_id IN (18) AND cll.time_priority >= 7 OR cll.status_detail_id IN (15) AND cll.time_priority >= 10) "\
"AND pat.id = pho.entity_id AND pcd.pmd_patient_id = cll.pmd_patient_id AND LOWER(pcd.measureuse) = LOWER(cll.key)  "\
"GROUP BY pcd.pmd_patient_id, pcd.patient_name, pcd.patient_count, COALESCE(pho.phone_number, pcd.phone_number), pcd.client_name, pcd.pmd_client_id, pcd.zip_code, pcd.lowbound_ts, pcd.highbound_ts, pcd.state_code, pcd.time_zone, pcd.phone_type, COALESCE(cll.Filter, pcd.filter), pcd.planidentifier_pclm;"

con_CCUSDN_SDN_DialerStore.execute('TRUNCATE TABLE dbo.MstrCamp_ADH;')
df = pd.read_sql_query(selectfromStarsConnect, con = con_P10PRDSDE001_StarsConnect)
df.to_sql('mstrcamp_adh',con_CCUSDN_SDN_DialerStore,schema='dbo',if_exists='append',index=False)

selectfromStarsConnect1="Select distinct "\
"calls.id as Call_ID "\
",dtpdetail.id as DTPDetail_ID "\
",dtp.id as DTP_ID "\
",calls.dtp_cnsld_call_list_id as DTP_CNSLD_Call_List_ID "\
",patients.pmd_client_id as PMD_Client_ID "\
",patients.PMD_Patient_ID "\
",patients.member_id as Member_ID "\
",patients.plan_group as PlanIdentifier_PClm "\
",case  when m.name IN ('HRM','SUPD') then dtp.npi else cast(pc.provider_npi as varchar(25)) end AS NPI "\
",cast(m.name as varchar(50)) as MeasureUse "\
",Cast((Select * from dbo.ConvertUTCToLocal(calls.updated_at)) as date) as MeasureNoteDate "\
",(Select * from dbo.ConvertUTCToLocal(calls.created_at)) as Call_Start_At "\
",(Select * from dbo.ConvertUTCToLocal(calls.stopped_at)) as Call_End_At "\
",calls.status_id as Call_Status_ID "\
",case when (calls.status_id = 2 and calls.status_detail_id is null) then 0 else calls.status_detail_id end AS Call_Status_Detail_ID  "\
",dtpdetail.category_id category_id "\
",dtpdetail.sub_category_id sub_category_id "\
",case  when m.name IN ('HRM','SUPD')  then dtp.NDC else pc.ndc END NDC "\
",case  when m.name IN ('HRM','SUPD')   then dtp.drug_name else pc.drug_name end drug_name "\
",dtpdetail.note as FreeTextNotes "\
",users.id Caller_ID "\
",statuses.name as Call_Status "\
",callsta.description as Call_Status_Details "\
",cat.name as Category "\
",subcat.name as Subcategory "\
",CONCAT(users.last_name,' ',users.first_name) as Caller_Name "\
",calls.updated_at as LastUpdate_UTC "\
",pc.prescription_service_num "\
",pc.date_of_service "\
",pc.quantity_dispensed "\
",pc.days_supply "\
",dtp.dtp_Year "\
",dtp.dtp_visibility_id "\
",dtpvis.key "\
",calls.call_type_id "\
",calltype.name call_type_name "\
"From dbo.calls calls "\
"Inner Join dbo.call_dtps calldtps on calldtps.call_id = calls.id "\
"Inner Join dbo.call_types calltype on calltype.id= calls.call_type_id "\
"Inner Join dbo.dtps dtp on dtp.id = calldtps.dtp_id "\
"Inner Join dbo.measures m on m.id = dtp.measure_id "\
"Inner Join dbo.dtp_visibilities dtpvis on dtpvis.id = dtp.dtp_visibility_id "\
"Inner Join dbo.dtp_details dtpdetail on (calldtps.dtp_id= dtpdetail.dtp_id and calldtps.call_id= dtpdetail.call_id and dtpdetail.deleted_at is null) "\
"Inner Join dbo.users users on users.id = calls.user_id "\
"Inner Join dbo.patients patients on patients.id = dtp.patient_id "\
"Inner Join dbo.categories cat on cat.id = dtpdetail.category_id "\
"Inner Join dbo.sub_categories subcat on subcat.id = dtpdetail.sub_category_id "\
"Inner Join dbo.statuses statuses on statuses.id = calls.status_id "\
"Left Join dbo.status_details callsta on callsta.id= calls.status_detail_id "\
"Left Join dbo.dtp_pharmacy_claims dpc on dpc.dtp_id = dtp.id "\
"Left Join (Select dtp_id, max(pharmacy_claim_id) as pharmacy_claim_id from  dbo.dtp_pharmacy_claims group by dtp_id) maxdtpdrug on maxdtpdrug.dtp_id = dpc.dtp_id "\
"Left Join dbo.pharmacy_claims pc on maxdtpdrug.pharmacy_claim_id = pc.id "\
"where dtp.dtp_Year = DATE_PART('year',now()) "\
"and dtp.product_line_id = 1 "\
"UNION "\
"Select distinct "\
"calls.id as Call_ID "\
",0 as DTPDetail_ID "\
",dtp.id as DTP_ID "\
",calls.dtp_cnsld_call_list_id as DTP_CNSLD_Call_List_ID "\
",patients.pmd_client_id as PMD_Client_ID "\
",patients.PMD_Patient_ID "\
",patients.member_id as Member_ID "\
",patients.plan_group as PlanIdentifier_PClm "\
" ,case  when m.name IN('HRM','SUPD') then dtp.npi else cast(pc.provider_npi as varchar(25)) END AS NPI "\
",cast(m.name as varchar(50)) as MeasureUse "\
",Cast((Select * from dbo.ConvertUTCToLocal(calls.updated_at)) as date) as MeasureNoteDate "\
",(Select * from dbo.ConvertUTCToLocal(calls.created_at)) as Call_Start_At "\
",(Select * from dbo.ConvertUTCToLocal(calls.stopped_at)) as Call_End_At "\
",calls.status_id as Call_Status_ID "\
",case when (calls.status_id = 2 and calls.status_detail_id is null) then 0 else calls.status_detail_id end AS Call_Status_Detail_ID "\
",0 category_id "\
",0 sub_category_id "\
",case when m.name IN('HRM','SUPD') then dtp.NDC else pc.ndc end AS NDC "\
",case when m.name IN('HRM','SUPD') then dtp.drug_name else pc.drug_name end AS drug_name "\
",'' as FreeTextNotes "\
",users.id Caller_ID "\
",statuses.name as Call_Status "\
",callsta.description as Call_Status_Details "\
",'' as Category "\
",'' as Subcategory "\
",CONCAT(users.last_name,' ',users.first_name) as Caller_Name "\
",calls.updated_at as LastUpdate_UTC "\
",pc.prescription_service_num "\
",pc.date_of_service "\
",pc.quantity_dispensed "\
",pc.days_supply "\
",dtp.dtp_Year "\
",dtp.dtp_visibility_id "\
",dtpvis.key "\
",calls.call_type_id "\
",calltype.name call_type_name "\
"From dbo.calls calls  "\
"Inner Join dbo.call_dtps calldtps on calldtps.call_id = calls.id "\
"Inner Join dbo.call_types calltype on calltype.id= calls.call_type_id "\
"Inner Join dbo.dtps dtp on dtp.id = calldtps.dtp_id "\
"Inner Join dbo.measures m on m.id = dtp.measure_id "\
"Inner Join dbo.dtp_visibilities dtpvis on dtpvis.id = dtp.dtp_visibility_id "\
"Inner Join dbo.users users on users.id = calls.user_id "\
"Inner Join dbo.patients patients on patients.id = dtp.patient_id "\
"Inner Join dbo.statuses statuses on statuses.id = calls.status_id "\
"Left Join dbo.status_details callsta on callsta.id= calls.status_detail_id "\
"Left Join dbo.dtp_pharmacy_claims dpc on dpc.dtp_id = dtp.id "\
"Left Join (Select dtp_id, max(pharmacy_claim_id) as pharmacy_claim_id from  dbo.dtp_pharmacy_claims group by dtp_id) maxdtpdrug on maxdtpdrug.dtp_id = dpc.dtp_id "\
"Left Join dbo.pharmacy_claims pc on maxdtpdrug.pharmacy_claim_id = pc.id "\
"where not exists (select * from dbo.dtp_details dtpdetail where calls.id = dtpdetail.call_id and dtpdetail.deleted_at is null limit 1) "\
"and dtp.dtp_Year = DATE_PART('year',now()) "\
"and dtp.product_line_id = 1 "\
"UNION "\
"Select distinct "\
"0 as Call_ID "\
",dtpdetail.id as DTPDetail_ID "\
",dtp.id as DTP_ID "\
",0 as DTP_CNSLD_Call_List_ID "\
",patients.pmd_client_id as PMD_Client_ID "\
",patients.PMD_Patient_ID "\
",patients.member_id as Member_ID "\
",patients.plan_group as PlanIdentifier_PClm "\
",case WHEN m.name IN ('HRM','SUPD') then dtp.npi else cast(pc.provider_npi as varchar(25)) end AS NPI "\
",cast(m.name as varchar(50)) as MeasureUse "\
",Cast((Select * from dbo.ConvertUTCToLocal(dtpdetail.updated_at)) as date) as MeasureNoteDate "\
",(Select * from dbo.ConvertUTCToLocal(dtpdetail.created_at)) as Call_Start_At "\
",(Select * from dbo.ConvertUTCToLocal(dtpdetail.updated_at)) as Call_End_At "\
",0 as Call_Status_ID "\
",0 as Call_Status_Detail_ID "\
",dtpdetail.category_id category_id "\
",dtpdetail.sub_category_id sub_category_id "\
",case WHEN m.name IN ('HRM','SUPD') then dtp.NDC else pc.ndc END AS NDC "\
",case WHEN m.name IN ('HRM','SUPD') then dtp.drug_name else pc.drug_name END AS drug_name "\
",dtpdetail.note as FreeTextNotes "\
",users.id Caller_ID "\
",'Inbound Call' as Call_Status "\
",'Inbound Call' as Call_Status_Details "\
",cat.name as Category "\
",subcat.name as Subcategory "\
",CONCAT(users.last_name,' ',users.first_name) as Caller_Name "\
",dtpdetail.updated_at as LastUpdate_UTC "\
",pc.prescription_service_num "\
",pc.date_of_service "\
",pc.quantity_dispensed "\
",pc.days_supply "\
",dtp.dtp_Year "\
",dtp.dtp_visibility_id "\
",dtpvis.key "\
",CAST(Null AS INT) AS call_type_id "\
",null AS call_type_name "\
"From dbo.dtp_details dtpdetail  "\
"Inner Join dbo.users users  on users.id = dtpdetail.user_id "\
"Inner Join dbo.dtps dtp  on dtp.id = dtpdetail.dtp_id "\
"Inner Join dbo.measures m  on m.id = dtp.measure_id "\
"Inner Join dbo.dtp_visibilities dtpvis  on dtpvis.id = dtp.dtp_visibility_id "\
"Inner Join dbo.patients patients  on patients.id = dtp.patient_id "\
"Inner Join dbo.categories cat  on cat.id = dtpdetail.category_id "\
"Inner Join dbo.sub_categories subcat  on subcat.id = dtpdetail.sub_category_id "\
"Left Join dbo.dtp_pharmacy_claims dpc  on dpc.dtp_id = dtp.id "\
"Left Join (Select dtp_id, max(pharmacy_claim_id) as pharmacy_claim_id from  dbo.dtp_pharmacy_claims group by dtp_id) maxdtpdrug on maxdtpdrug.dtp_id = dpc.dtp_id "\
"Left Join dbo.pharmacy_claims pc  on maxdtpdrug.pharmacy_claim_id = pc.id "\
"where dtpdetail.call_id is null  "\
"and dtpdetail.deleted_at is null "\
"and dtp.dtp_Year = DATE_PART('year',now()) "\
"and dtp.product_line_id = 1 "

con_OPS_Stars_Call_Lists.execute('TRUNCATE TABLE staging.DTP_StarConnect_Call_List;')
df1 = pd.read_sql_query(selectfromStarsConnect1, con = con_P10PRDSDE001_StarsConnect)
df1.to_sql('DTP_StarConnect_Call_List',con_OPS_Stars_Call_Lists,schema='staging',if_exists='append',index=False)

InsertQuery="INSERT INTO dbo.dtp_starconnect_call_list (call_id, dtp_id, dtpdetail_id, dtp_cnsld_call_list_id, pmd_client_id, npi, pmd_patient_id, member_id, planidentifier_pclm, measureuse, measurenotedate, call_start_at, call_end_at, call_status, call_status_id, category_id, sub_category_id, ndc, freetextnotes, drug_name, caller_id, category, subcategory, caller_name, lastupdate_utc, call_status_detail_id, call_status_details, prescription_service_num, date_of_service, quantity_dispensed, days_supply, dtp_year, dtp_visibility_id, dtp_visibilities_key, call_type_id, call_type_name, pbp_pclm, accountid_pclm) "\
"SELECT call_id, dtp_id, dtpdetail_id, dtp_cnsld_call_list_id, pmd_client_id, npi, pmd_patient_id, member_id, s.planidentifier_pclm, measureuse, measurenotedate, call_start_at, call_end_at, call_status, call_status_id, category_id, sub_category_id, ndc, freetextnotes, drug_name, caller_id, category, subcategory, caller_name, lastupdate_utc, call_status_detail_id, call_status_details, prescription_service_num, date_of_service, quantity_dispensed, days_supply, dtp_year, dtp_visibility_id, dtp_visibilities_key, call_type_id, call_type_name, p.pbp_pclm, p.accountid_pclm "\
"FROM staging.dtp_starconnect_call_list AS s "\
"INNER JOIN public.stars_rdsm_stars_patients_detail AS p "\
"ON (s.pmd_patient_id = p.patient_unique_ident AND p.end_date IS NULL) "\
"WHERE NOT EXISTS (SELECT * FROM dbo.dtp_starconnect_call_list AS c "\
"WHERE dtp_year = date_part('year', clock_timestamp()) AND s.call_id = c.call_id AND s.dtp_id = c.dtp_id AND s.dtpdetail_id = c.dtpdetail_id LIMIT 1); "

con_OPS_Stars_Call_Lists.execute(InsertQuery)


con_CCUSDN_SDN_DialerStore.close()
con_P10PRDSDE001_StarsConnect.close()
con_OPS_Stars_Call_Lists.close()
