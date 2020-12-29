from __future__ import print_function
import psycopg2
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text 
import boto3
from boto3 import client
from botocore.exceptions import ClientError
import json
import urllib
import requests
import time
from datetime import datetime

client = boto3.client("secretsmanager", region_name="us-east-2")

get_secret_value_response = client.get_secret_value(
	SecretId="SecretKeysForAWSGlue"
)

secret = get_secret_value_response['SecretString']
secret = json.loads(secret)

glue = boto3.client('glue')

#setting child job names
stars_evaluations_intake_glue_job="Stars_Evaluations_Intake"
stars_evaluations_patients_glue_job="Stars_Evaluations_Patients"
  
con_db_host = secret.get('con_db_host')
con_db_port = secret.get('con_db_port')
con_db_user = secret.get('con_db_user')
con_db_opsdb_stars = secret.get('con_OPSDB_STARS')
con_db_opsdb_stars_warehouse = secret.get('con_db_opsdb_stars_warehouse')
con_db_opsdb_stars_rdsm = secret.get('con_db_opsdb_stars_rdsm')
con_opsdb_medispan = secret.get('con_opsdb_medispan')
con_db_opsdb_operationaldatastore = secret.get('con_db_OPSDB_OperationalDataStore')
con_db_opsdb_stars_billing = secret.get('con_db_opsdb_stars_billing')
con_opsdb_archive = secret.get('con_OPSDB_Archive')
con_db_password = secret.get('con_db_password')
flie_watch_email_recipients = secret.get('flie_watch_email_recipients')
common_email_api_url = secret.get('common_email_api_url')
file_watch_environment = secret.get("file_watch_environment")

engine1 = create_engine(
	'postgresql://' + con_db_user + ':' + con_db_password + '@' + con_db_host + ':' + con_db_port + '/' + con_db_opsdb_stars)
db_opsdb_stars = engine1.connect().execution_options(autocommit=True)

engine2 = create_engine(
	'postgresql://' + con_db_user + ':' + con_db_password + '@' + con_db_host + ':' + con_db_port + '/' + con_db_opsdb_stars_warehouse)
db_opsdb_starswarehouse = engine2.connect().execution_options(autocommit=True)

engine3 = create_engine(
	'postgresql://' + con_db_user + ':' + con_db_password + '@' + con_db_host + ':' + con_db_port + '/' + con_db_opsdb_stars_rdsm)
db_opsdb_starsrdsm = engine3.connect().execution_options(autocommit=True)

engine4 = create_engine(
	'postgresql://' + con_db_user + ':' + con_db_password + '@' + con_db_host + ':' + con_db_port + '/' + con_opsdb_medispan)
db_opsdb_medispan = engine4.connect().execution_options(autocommit=True)

engine5 = create_engine(
	'postgresql://' + con_db_user + ':' + con_db_password + '@' + con_db_host + ':' + con_db_port + '/' + con_db_opsdb_operationaldatastore)
db_opsdb_operationaldatastore = engine5.connect().execution_options(autocommit=True)

engine6 = create_engine(
	'postgresql://' + con_db_user + ':' + con_db_password + '@' + con_db_host + ':' + con_db_port + '/' + con_db_opsdb_stars_billing)
db_opsdb_stars_billing = engine6.connect().execution_options(autocommit=True)

engine7 = create_engine(
	'postgresql://' + con_db_user + ':' + con_db_password + '@' + con_db_host + ':' + con_db_port + '/' + con_opsdb_archive)
db_opsdb_archive = engine7.connect().execution_options(autocommit=True)

###################Get count###########
# SET Run Variables
request_id_query = """Select count(1) from dbo.control where Process = 1"""
request_id = db_opsdb_stars.execute(request_id_query).fetchall()[0][0]

if request_id > 0:
	print("Requestid-"+str(request_id))
	get_varible_values = """select coalesce(planidentifier_pclm,'') as planidentifier_pclm,cast(pmdclientid as varchar(10)) as pmd_client_id,runyear,1::integer evaluations,client as clientname,controlid   
		from dbo.control c where filewatcherqueuedate = (select min(filewatcherqueuedate) mindate from dbo.control where process = 1 ) and process = 1 limit 1;"""
	result = db_opsdb_stars.execute(get_varible_values).fetchall()
	var_planidentifier_pclm = result[0][0]
	var_pmd_client_id = result[0][1]
	var_runyear = result[0][2]
	var_evaluations = result[0][3]
	var_clientname = result[0][4]
	var_controlid = result[0][5]
	#print(var_planidentifier_pclm+'#'+var_pmd_client_id+'#'+str(var_runyear)+'#'+str(var_evaluations)+'#'+var_clientname+'#'+str(var_controlid))
else:
	get_varible_values = """select cast(0 as varchar(10)) planidentifier_pclm,cast(0 as varchar(10)) pmd_client_id,cast(0 as varchar(10)) clientname,1900 runyear,0::integer  evaluations,-99 as controlid;"""
	result = db_opsdb_stars.execute(get_varible_values).fetchall()
	var_planidentifier_pclm = result[0][0]
	var_pmd_client_id = result[0][1]
	var_clientname = result[0][2]
	var_runyear = result[0][3]
	var_evaluations = result[0][4]
	var_controlid = result[0][5]
	#print(var_planidentifier_pclm+'#'+var_pmd_client_id+'#'+str(var_runyear)+'#'+str(var_evaluations)+'#'+var_clientname+'#'+str(var_controlid))
print('# SET Run Variables for client-'+str(var_pmd_client_id))

#Check Evaluations value if @Evaluations==1 then proceed else Terminate
if var_evaluations == 1:
	# Set CurrentYear_Flag
	get_currentyear_value="""Select Case when """+str(var_runyear)+""" = cast(extract(Year from now()) as int) then '1' else '0' end as CurrentYear_Flag"""
	var_currentyear_flag = db_opsdb_stars.execute(get_currentyear_value).fetchall()[0][0]
	print(str(var_pmd_client_id))
	#Set Process Indicator
	update_procind_value="""Update dbo.control c Set process_ind = true   Where controlid = """+str(var_controlid)+""";"""
	run_update = db_opsdb_stars.execute(update_procind_value)
	#Stars_Evaluations_Intake
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Stars_Evaluations_Intake start:::", dt_string)
	eval_intake_runId = glue.start_job_run(JobName=stars_evaluations_intake_glue_job,Arguments={'--Client_ID': str(var_pmd_client_id)})
	while (1 == 1):
		
		status = glue.get_job_run(JobName=stars_evaluations_intake_glue_job, RunId=eval_intake_runId['JobRunId'])
		print(status['JobRun']['JobRunState'])
		if(status['JobRun']['JobRunState'] != 'RUNNING'):
			print(status)
			time.sleep(20)
			break
		#time.sleep(60)
	
	if status['JobRun']['JobRunState'] == 'FAILED':
		response = glue.get_job_runs(JobName='Stars_Evaluation',MaxResults=1)
		result=response['JobRuns'][0] #AcceSS TUPLE 1
		stop_job = glue.batch_stop_job_run(JobName='Stars_Evaluation',JobRunIds=[result['Id']])
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Stars_Evaluations_Intake done:::", dt_string)
	#print('#Stars_Evaluations_Intake done ')
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Stars_Evaluations_patient start:::", dt_string)
	eval_patient_runId = glue.start_job_run(JobName=stars_evaluations_patients_glue_job,Arguments={'--RunYear': str(var_runyear),'--PMD_CLIENT_ID': str(var_pmd_client_id)})
	while (1 == 1):
		
		status1 = glue.get_job_run(JobName=stars_evaluations_patients_glue_job, RunId=eval_patient_runId['JobRunId'])
		print(status1['JobRun']['JobRunState'])
		if(status1['JobRun']['JobRunState'] != 'RUNNING'):
			print(status1)
			time.sleep(60)
			break
		#time.sleep(60)
	
	if status1['JobRun']['JobRunState'] == 'FAILED':
		response = glue.get_job_runs(JobName='Stars_Evaluation',MaxResults=1)
		result=response['JobRuns'][0] #AcceSS TUPLE 1
		stop_job = glue.batch_stop_job_run(JobName='Stars_Evaluation',JobRunIds=[result['Id']])
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Stars_Evaluations_patient done:::", dt_string)
	# Determine Path
	if var_pmd_client_id == "30":
		now = datetime.now()
		dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
		print("Healthspring start:::", dt_string)
		# Healthspring  
		healthspring_sp = """call dbo.usp_load_pclm_star_healthspring ("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(healthspring_sp)
		now = datetime.now()
		dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
		print("Healthspring done:::", dt_string)
	if var_pmd_client_id == "63":
		now = datetime.now()
		dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
		print("ABQHP start:::", dt_string)
		# ABQHP  
		abqhp_sp = """call dbo.usp_load_pclm_star_abqhp("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(abqhp_sp)
		now = datetime.now()
		dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
		print("ABQHP done:::", dt_string)
	elif var_pmd_client_id == "52":
		print("UHC")
		uhc_truncate="""Truncate table staging.staging_UHC_Claim_Pharmacy;"""
		db_opsdb_starsrdsm.execute(uhc_truncate)
		uhc_deletepclm="""delete From dbo.PClm_Star p using (SELECT Transaction_ID, MIN(Seq_Num) as minseq 
		FROM public.opsdb_stars_rdsm_uhc_claim_pharmacy ms 
		WHERE lower(Medicare_Part_d) = lower('Y')
		AND Patient_ID IS NOT NULL AND lower(Transaction_Status) = lower('P') AND Patient_Gender IN ('1','2','F,'M')
		AND LENGTH(NDC_Code) > 1 AND extract (Year from Date_Of_Service ) = """+str(var_runyear)+"""
		GROUP BY Transaction_ID
		) a 
		where (a.Transaction_ID = p.Transaction_ID and minseq < Paired_Adjustment_Sequence_Num) and p.PMD_Client_ID = 52
		and p.RunYear = """+str(var_runyear)+""";"""
		db_opsdb_stars.execute(uhc_deletepclm)
		
		#drop_tmp_pclmstar="""drop table if exists staging.pclm_star_temp;"""
		#db_opsdb_starsrdsm.execute(drop_tmp_pclmstar)
		df_pclmstar=pd.read_sql_query("""select distinct Transaction_ID from dbo.pclm_star where pmd_client_id=52 and patient_id<>'4MU6U90QU49';""" , con=db_opsdb_stars)
		#print(df_pclmstar.head())
		#df_pclmstar.to_sql('pclm_star_temp',con=db_opsdb_starsrdsm,schema='staging',if_exists='replace',index=False)
		df_stageuhc=pd.read_sql_query("""select	distinct
			clm.transaction_id
			,clm.transaction_id::bigint as pmd_pclaim_id
			,52::bigint as pmd_client_id
			,cast(medicare_part_d as varchar(1)) as medicare_part_d_ind
			,patient_id
			,patient_id as cardholder_id
			,cast(patient_dob as date) as patient_dob
			,cast(patient_dob as date) as cardholder_dob
			,cast(date_of_service as timestamp) as date_of_service
			,cast(right('00000000000'|| ndc_code,11) as varchar(19)) as product_service_id
			,cast(product_service_name as varchar(70)) as product_service_desc
			,quantity_dispensed::float as quantity_dispensed
			,cast(days_supply as int) as days_supply
			,cast(prescriber_npi as varchar(50)) as prescriber_id
			,cast(pharmacy_npi as varchar(50)) as pharmacy_id
			,case when sqlserver_ext.isdate(adjudication_date::varchar) = 0 then null::date
			else cast(adjudication_date as date)
			end as adjudication_date
			,cast(admin_fee as money) as administrative_fee_amt
			,cast(ingredient_cost as money) as ingredient_cost
			,cast(disp_fee as money) as dispensing_fee
			,cast(sales_tax as money) as total_sales_tax
			,''::varchar(1) as drgmtch
			,cast (medstaken as varchar(50)) as compositionname
			,ndc_code as formattedpackageidentifier
			,'H0251'::varchar(255) as accountid
			,'H0251'::varchar(255) as accountid2
			,cast(prescription_num as varchar(50)) as prescription_service_reference_num
			,cast(clm.seq_num as varchar(10)) as paired_adjustment_sequence_num
			from dbo.uhc_claim_pharmacy clm 
			inner join( select transaction_id, min(seq_num) as minseq 
			from dbo.uhc_claim_pharmacy ms 
			where lower(medicare_part_d) = lower('y')
			and patient_id is not null
			and lower(transaction_status) = lower('p')
			and patient_gender in ('1','2','F','M')
			and length(ndc_code) > 1
			group by transaction_id
			) ms on (ms.transaction_id = clm.transaction_id and ms.minseq = clm.seq_num)
			left join ( select distinct ndc11,medstaken from public.opsdb_medispan_alchemy_druglist_withingrandids
			) alch on right('00000000000' || clm.ndc_code, 11) = alch.ndc11
			where lower(medicare_part_d) = lower('y')
			and patient_id is not null
			and lower(transaction_status) = lower('p')
			and patient_gender in ('1','2','F','M')
			and length(clm.ndc_code) > 1;""", con=db_opsdb_starsrdsm)
		#print(df_stageuhc.head())
		df_result=df_stageuhc[~df_stageuhc.transaction_id.isin(df_pclmstar.transaction_id)]
		df_result.to_sql('staging_uhc_claim_pharmacy',con=db_opsdb_starsrdsm,schema='staging',if_exists='append',index=False)
		df_newrecuhc=pd.read_sql_query("""select pmd_pclaim_id,pmd_client_id,medicare_part_d_ind,patient_id,cardholder_id,patient_dob,cardholder_dob,date_of_service
		product_service_id,product_service_desc,quantity_dispensed,days_supply,prescriber_id,pharmacy_id,transaction_id,adjudication_date,
		paired_adjustment_sequence_num,administrative_fee_amt,ingredient_cost,dispensing_fee,total_sales_tax,drgmtch,compositionname,
		formattedpackageidentifier,uuid_generate_v4()::varchar(40) as sourcerowid,accountid,accountid2,prescription_service_reference_num
		,now()::timestamp update_datetime,extract(year from date_of_service)::int as runyear
		from staging.staging_uhc_claim_pharmacy;""", con=db_opsdb_starsrdsm)
		df_newrecuhc.to_sql('pclm_star',con=db_opsdb_stars,schema='dbo',if_exists='append',index=False)
		db_opsdb_starsrdsm.execute("""drop table if exists staging.pclm_star_temp;""")
		print(df_newrecuhc.head())		

		#Reversals (Set to NOT NULL (333) for Claim Exception Code)
		update_pclm="""update dbo.PClm_Star p SET ClaimExcCd = 333
		from (SELECT Transaction_ID, MIN(Seq_Num) as minseq 
		FROM public.opsdb_stars_rdsm_uhc_claim_pharmacy ms 
		WHERE lower(Medicare_Part_d) = lower('Y')
		AND Patient_ID IS NOT NULL
		AND lower(Transaction_Status) = lower('X')
		AND Patient_Gender IN ('1','2','F','M')
		AND LENgth(NDC_Code) > 1
		AND extract(Year from Date_Of_Service) = """+str(var_runyear)+"""
		GROUP BY Transaction_ID
		) a  
		where  (a.Transaction_ID = p.Transaction_ID and a.minseq = p.Paired_Adjustment_Sequence_Num) and p.PMD_Client_ID = 52
		and p.RunYear = """+str(var_runyear)+""";"""
		db_opsdb_stars.execute(update_pclm)
	elif var_pmd_client_id == "15":
		print('SCAN')
		#drop_tmp_pclmstar="""drop table if exists staging.pclm_star_temp;"""
		#db_opsdb_starsrdsm.execute(drop_tmp_pclmstar)
		df_pclmstar_15=pd.read_sql_query("""select distinct runyear,transaction_id,'old' flag from dbo.pclm_star where pmd_client_id=15;""" , con=db_opsdb_stars)
		#df_pclmstar_15.to_sql('pclm_star_temp',con=db_opsdb_starsrdsm,schema='staging',if_exists='replace',index=False)
		print(df_pclmstar_15.head())

		df_scanpclm=pd.read_sql_query("""select 	distinct
		cast(transactionclaimid as bigint) as pmd_pclaim_id,15::bigint as pmd_client_id,medicareddrug::varchar(1) as medicare_part_d_ind
		,cardholderidpersoncd::varchar(50) as patient_id,cast(mbrdob as date) as patient_dob,cast(datefilled as timestamp) as date_of_service
		,cast(prescriptionorigincd as int) as prescription_origin_code,right('00000000000'|| ndc, 11)::varchar(19) as product_service_id
		,drugname::varchar(70) as product_service_desc,cast(genericcd as varchar(6)) as gcn_num,cast(gpi as char(14)) as gpi_code
		,cast(case when length(qtydispensed) = 0 then 0::int else cast(dbo.unpackdecimal(qtydispensed) as int)/1000  end as float)
		as quantity_dispensed
		,dayssupply::int as days_supply,cast(drugstrength as varchar(15)) as drug_strength,prescribernpi::varchar(50) as prescriber_id,
		pharmacynpi::varchar(50) as pharmacy_id
		,cast(transactionclaimid as varchar(100)) as transaction_id,case	when sqlserver_ext.isdate(adjudicationdate::varchar) = 0
		then null::date
		else cast(adjudicationdate as date)
		end as adjudication_date
		,cast(recordidentifier as varchar(2)) as transaction_status
		,case when length(adminfee) = 0 then 0::money else cast(dbo.unpackdecimal(adminfee) as money)/100  end as administrative_fee_amt
		,case when length(ucamt) = 0 then 0::money else cast(dbo.unpackdecimal(ucamt) as money)/100  end as usual_customary
		,case when length(calcingredientcost) = 0 then 0::money else cast(dbo.unpackdecimal(calcingredientcost) as money)/100  end as ingredient_cost
		,case when length(dispensingfee) = 0 then 0::money else cast(dbo.unpackdecimal(dispensingfee) as money)/100  end as dispensing_fee
		,case when length(taxamt) = 0 then 0::money else cast(dbo.unpackdecimal(taxamt) as money)/100  end as total_sales_tax
		,case when length(amtbilled) = 0 then 0::money else cast(dbo.unpackdecimal(amtbilled) as money)/100  end as gross_cost
		,case when length(otherpayoramt) = 0 then 0::money else cast(dbo.unpackdecimal(otherpayoramt) as money)/100  end as other_payer_amt_paid
		,''::varchar(11) as drgmtch
		,CAST(medstaken AS varchar(50)) as compositionname,ndc as formattedpackageidentifier
		,case when clm.plantype2 in ('3000','4000','4500','8000') then 'H5425'::varchar(255)
		when clm.plantype2 = '5000' then 'H5943'::varchar(255)
		end as accountid2
		,prescriptionnumber9::varchar(50) as prescription_service_reference_num
		,case when length(licssubsidyamt) = 0 then 0::money else cast(dbo.unpackdecimal(licssubsidyamt) as money)/100  end as lis_subsidy_amt
		,extract(year from datefilled::date )::int as runyear
		from dbo.scan_claims clm 
		left join (select distinct ndc11, medstaken from public.opsdb_medispan_alchemy_druglist_withingrandids ) alch on clm.ndc = alch.ndc11
		where LOWER(medicareddrug) = lower('Y')
		and cardholderidpersoncd is not null		
		and datefilled is not null
		and clm.transactiontypecd = '31'		
		and clm.plantype2 in ('3000','4000','4500','5000','8000');""", con=db_opsdb_starsrdsm)
		df_scanpclm=pd.merge(df_scanpclm,df_pclmstar_15,on=['transaction_id','runyear'],how='left')
		df_scanpclm = df_scanpclm[df_scanpclm['flag'].isnull()]
		del df_scanpclm['flag']
		df_scanpclm.to_sql('pclm_star',con=db_opsdb_stars,schema='dbo',if_exists='append',index=False)
		#print(df_scanpclm.head())
		db_opsdb_stars.execute("""drop table if exists staging.scan_claims_temp;""")
		df_scam_star=pd.read_sql_query("""select distinct xreftransactionid from dbo.scan_claims 
		where transactiontypecd = '32' and  lower(medicareddrug) = lower('Y') and cardholderidpersoncd is not null;""" , con=db_opsdb_starsrdsm)
		df_scam_star.to_sql('scan_claims_temp',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)

		update_scan_reversal = """update dbo.pclm_star s set claimexccd = 333,update_datetime = current_timestamp 
		from (select distinct xreftransactionid from staging.scan_claims_temp) sc 
		where s.transaction_id = sc.xreftransactionid and s.claimexccd is null and s.pmd_client_id = 15 and s.runyear ="""+str(var_runyear)+""";"""
		db_opsdb_stars.execute(update_scan_reversal)		

		#db_opsdb_starsrdsm.execute("""drop table if exists staging.pclm_star_temp;""")
		db_opsdb_stars.execute("""drop table if exists staging.scan_claims_temp;""")
		#print(update_scan_reversal)

	elif var_pmd_client_id == "69":
		print('ASPIRE')
		#drop_tmp_pclmstar="""drop table if exists staging.pclm_star_temp;"""
		#db_opsdb_starsrdsm.execute(drop_tmp_pclmstar)
		df_pclmstar_69=pd.read_sql_query("""select distinct runyear,transaction_id,'old' flag  from dbo.pclm_star where pmd_client_id=69;""" , con=db_opsdb_stars)
		#df_pclmstar_69.to_sql('pclm_star_temp',con=db_opsdb_starsrdsm,schema='staging',if_exists='append',index=False)
		#print(df_pclmstar_69.head())

		df_aspirepclm=pd.read_sql_query("""select distinct
		0::bigint as pmd_pclaim_id
		,cast(69 as bigint) as pmd_client_id
		,cast('D' as varchar(1)) as medicare_part_d_ind
		,cast(memberid as varchar(50))as patient_id
		,null::varchar(50) as cardholder_id
		, cast(memberdob as date)  as patient_dob
		,cast(datefilled as timestamp) as date_of_service
		,cast(right('00000000000'|| ndc, 11) as varchar(19)) as product_service_id
		,cast(productdesc as varchar(70)) as product_service_desc
		,cast(clm.metricqty as float) as quantity_dispensed
		,cast(round(dayssupply::int,0) as int) as days_supply
		,cast(prescriber_npi_nbr as varchar(50)) as prescriber_id
		,cast(pharmcacy_npi_nbr as varchar(50)) as pharmacy_id
		,cast(rtrim(ltrim(authnumber)) as varchar(100)) as transaction_id
		,null::date as adjudication_date
		,cast('' as varchar(11)) as drgmtch
		,cast(rtrim(ltrim(medstaken)) as varchar(4000)) as compositionname
		,cast(rtrim(ltrim(ndc)) as varchar(4000)) as formattedpackageidentifier
		,left(groupnumber,5) as accountid
		,left(groupnumber,5) as accountid2
		,clm.rxnumber as prescription_service_reference_num	
		,extract(year from cast(datefilled as date) )::int as runyear
		from dbo.aspire_claims clm 
		left join public.opsdb_medispan_alchemy_druglist_withingrandids alch  on clm.ndc = alch.ndc11
		where LOWER(rtrim(ltrim(coalesce(reversal,'')))) <>LOWER('R');""", con=db_opsdb_starsrdsm)
		df_aspirepclm=pd.merge(df_aspirepclm,df_pclmstar_69,on=['transaction_id','runyear'],how='left')
		df_aspirepclm = df_aspirepclm[df_aspirepclm['flag'].isnull()]
		del df_aspirepclm['flag']
		df_aspirepclm.to_sql('pclm_star',con=db_opsdb_stars,schema='dbo',if_exists='append',index=False)
		#print(df_aspirepclm.head())
		db_opsdb_stars.execute("""drop table if exists staging.aspire_claims_temp;""")
		df_aspire_star=pd.read_sql_query("""select distinct authnumber from dbo.aspire_claims 
		where LOWER(rtrim(ltrim(coalesce(reversal,'')))) =LOWER('R');""" , con=db_opsdb_starsrdsm)
		df_aspire_star.to_sql('aspire_claims_temp',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)
		update_aspire_reversal = """update dbo.pclm_star pclm set claimexccd = 333   ,update_datetime = current_timestamp
		where pmd_client_id = 69
		and pclm.runyear = """+str(var_runyear)+"""
		and exists 
		(select 1 from staging.aspire_claims_temp clm
		where pclm.transaction_id = clm.authnumber)"""
		db_opsdb_stars.execute(update_aspire_reversal)
		#db_opsdb_starsrdsm.execute("""drop table if exists staging.pclm_star_temp;""")
		db_opsdb_stars.execute("""drop table if exists staging.aspire_claims_temp;""")
		#print(update_aspire_reversal)
	elif var_pmd_client_id == "62":
		print('Meridian')
		drop_tmp_mclmstar="""drop table if exists staging.mclm_star_temp;"""
		db_opsdb_starsrdsm.execute(drop_tmp_mclmstar)
		df_mclmstar_62=pd.read_sql_query("""select distinct runyear,claim_id,claim_line_num,plan_code from dbo.mclm_star where pmd_client_id=62 and runyear="""+str(var_runyear)+""";""" , con=db_opsdb_stars)
		df_mclmstar_62.to_sql('mclm_star_temp',con=db_opsdb_starsrdsm,schema='staging',if_exists='append',index=False)
		#print(df_mclmstar_62.head())
		db_opsdb_stars.execute("Truncate TABLE dbo.mclm_star_staging")
		df_meridianmclm_staging=pd.read_sql_query("""select  distinct
		62::int pmd_client_id 
		,left("plan code",5)::varchar(5) as plan_code
		,cast("bill type" as varchar(4)) as bill_type
		,cast("claim id" as varchar(50)) as claim_id
		,cast("claim type" as varchar(2)) as claim_type
		,cast("claim line number" as bigint) as claim_line_num
		,cast("place of service" as varchar(2)) as place_of_service
		,cast("claim status" as smallint) as claim_status
		,cast("claim submit date" as date) as claim_submit_date
		,cast(cast(case when sqlserver_ext.isdate("admission date")=1 then "admission date" else '1900-01-01' end as date) as timestamp) as admission_date
		,cast("admission type" as smallint) as admission_type
		,cast(case when sqlserver_ext.isdate("discharge date")=1 then "discharge date" else '1900-01-01' end as date) as discharge_date
		,cast("discharge status" as varchar(2)) as discharge_status
		,cast("patient id" as varchar(50)) as patient_id
		,cast("patient id qual" as varchar(2)) as patient_id_qual
		,cast("servicing provider npi" as varchar(10)) as servicing_provider_npi
		,cast(coalesce("facility npi","servicing provider npi") as varchar(10)) as facility_npi
		,cast("dx code admitting" as varchar(7)) as dx_code_admitting
		,cast("dx code principal" as varchar(7)) as dx_code_principal
		,cast("dx code 2" as varchar(7)) as dx_code_2
		,cast("dx code 3" as varchar(7)) as dx_code_3
		,cast("dx code 4" as varchar(7)) as dx_code_4
		,cast("dx code 5" as varchar(7)) as dx_code_5
		,cast("dx code 6" as varchar(7)) as dx_code_6
		,cast("dx code 7" as varchar(7)) as dx_code_7
		,cast("dx code 8" as varchar(7))  as dx_code_8
		,cast("dx code 9" as varchar(7)) as dx_code_9
		,cast("dx code 10" as varchar(7)) as dx_code_10
		,cast("dx code 11" as varchar(7)) as dx_code_11
		,cast("dx code 12" as varchar(7)) as dx_code_12
		,cast("dx code 13" as varchar(7)) as dx_code_13
		,cast("dx code 14" as varchar(7)) as dx_code_14
		,cast("dx code 15" as varchar(7)) as dx_code_15
		,cast("dx code 16" as varchar(7)) as dx_code_16
		,cast("dx code 17" as varchar(7)) as dx_code_17
		,cast("dx code 18" as varchar(7)) as dx_code_18
		,cast("idc proc code prinicipal" as varchar(7)) as icd_proc_code_principal
		,cast("idc proc code 2" as varchar(7)) as icd_proc_code_2
		,cast("idc proc code 3" as varchar(7)) as icd_proc_code_3
		,cast("idc proc code 4" as varchar(7)) as icd_proc_code_4
		,cast("idc proc code 5" as varchar(7)) as icd_proc_code_5
		,cast("idc proc code 6" as varchar(7)) as icd_proc_code_6
		,cast("type of service" as varchar(3)) as type_of_service
		,cast("revenue code" as varchar(4))as revenue_code
		,cast(case when sqlserver_ext.isdate("procedure date")=1 then "procedure date" else '1900-01-01' end as date) as procedure_date
		,cast("procedure code" as varchar(5)) as procedure_code
		,cast("procedure mode code 1" as varchar(2)) as procedure_mod_code_1
		,cast("procedure mode code_2" as varchar(2)) as procedure_mod_code_2
		,cast("procedure mode code_3" as varchar(2)) as procedure_mod_code_3
		,cast("procedure mode code_4" as varchar(2)) as procedure_mod_code_4
		,cast("unit qty" as int) as unit_qty
		,cast("client def 1" as varchar(25)) as filler1 
		,cast("client def 2" as varchar(25)) as filler2 
		,cast("client def 3" as varchar(25)) as filler3 
		,clm.runyear
		,current_timestamp create_date
		from   dbo.meridian_62_medical_claims clm
		where not exists (select 1 from staging.mclm_star_temp mclm 
		where clm."claim id" = mclm.claim_id and clm."claim line number" = mclm.claim_line_num::varchar and left(clm."plan code",5)=mclm.plan_code);""", con=db_opsdb_starsrdsm)
		df_meridianmclm_staging.to_sql('mclm_star',con=db_opsdb_stars,schema='dbo',if_exists='append',index=False)

		#df_meridianmclm_staging.to_sql('mclm_star_staging',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)
		#df_meridianmclm_goodrecord=pd.read_sql_query("""select * from staging.mclm_star_staging where record_occurance::int=1;""", con=db_opsdb_stars)
		#del df_meridianmclm_goodrecord['record_occurance']
		#df_meridianmclm_goodrecord.to_sql('mclm_star',con=db_opsdb_stars,schema='dbo',if_exists='append',index=False)
		#print(df_meridianmclm.head())

		#df_meridianmclm_badrecord=pd.read_sql_query("""select *,999 as errorcode, 999 as errorcolumn, '' as errordescription from staging.mclm_star_staging where record_occurance::int<>1;""", con=db_opsdb_stars)
		#del df_meridianmclm_badrecord['record_occurance']
		#df_meridianmclm_errorrecord=pd.read_sql_query("""SELECT errorcode,pmd_client_id ,claim_id,claim_line_num ,cast(admission_date as date) admission_date, 'old' flag FROM dbo.MClm_Star_Load_Errors;""", con=db_opsdb_stars)
		#df_meridianmclm_errorrecord=pd.merge(df_meridianmclm_badrecord,df_meridianmclm_errorrecord,on=['errorcode','pmd_client_id','claim_id','claim_line_num','admission_date'],how='left')
		#df_meridianmclm_errorrecord = df_meridianmclm_errorrecord[df_meridianmclm_errorrecord['flag'].isnull()]
		#del df_meridianmclm_errorrecord['flag']
		#df_meridianmclm_errorrecord=df_meridianmclm_errorrecord[["pmd_client_id","plan_code","bill_type","claim_id","claim_type","claim_line_num","place_of_service","claim_status","claim_submit_date","admission_date","admission_type","discharge_date","discharge_status"
		#,"patient_id","patient_id_qual","servicing_provider_npi","facility_npi","dx_code_admitting","dx_code_principal","dx_code_2","dx_code_3","dx_code_4","dx_code_5","dx_code_6","dx_code_7"
		#,"dx_code_8","dx_code_9","dx_code_10","dx_code_11","dx_code_12","dx_code_13","dx_code_14","dx_code_15","dx_code_16","dx_code_17","dx_code_18","icd_proc_code_principal","icd_proc_code_2"
		#,"icd_proc_code_3","icd_proc_code_4","icd_proc_code_5","icd_proc_code_6","type_of_service","revenue_code","procedure_date","procedure_code","procedure_mod_code_1","procedure_mod_code_2"
		#,"procedure_mod_code_3","procedure_mod_code_4","unit_qty","filler1","filler2","filler3","runyear","create_date","errorcode","errorcolumn","errordescription"]]

		#df_meridianmclm_errorrecord.to_sql('mclm_star_load_errors',con=db_opsdb_stars,schema='dbo',if_exists='append',index=False)

		drop_tmp_claimstatusupdates="""drop table if exists staging.claimstatusupdates;"""
		db_opsdb_stars.execute(drop_tmp_claimstatusupdates)
		df_meridianclm_62=pd.read_sql_query("""select distinct left(c."plan code",5)::varchar(5) as plan_code,cast(c."claim id" as varchar(50)) as claim_id,c."claim line number" as claim_line_number,
		cast(c."claim status" as smallint) claim_status   
		from dbo.meridian_62_medical_claims c
		inner join (select max(cast("claim submit date" as date)) mclaim_submit_date, "claim id" as claim_id,"claim line number"as claim_line_number,
		"plan code" as plan_code  from dbo.meridian_62_medical_claims where runyear ="""+str(var_runyear)+""" group by "claim id","claim line number","plan code") m
		on cast(m.mclaim_submit_date as date) = cast(c."claim submit date" as date)
		and m.claim_id = c."claim id"
		and m.claim_line_number = c."claim line number"
		and m.plan_code = c."plan code"
		where runyear = """+str(var_runyear)+""";""", con=db_opsdb_starsrdsm)		
		df_meridianclm_62.to_sql('claimstatusupdates',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)
		#print(df_meridianclm_62.head())

		update_meridian_claim  = """		update dbo.mclm_star m set claim_status = u.claim_status,update_datetime = current_timestamp
		from staging.claimstatusupdates u
		where u.plan_code = m.plan_code and u.claim_id = m.claim_id and u.claim_line_number=m.claim_line_num::text 
		and runyear ="""+str(var_runyear)+""""""
		db_opsdb_stars.execute(update_meridian_claim)		

		db_opsdb_stars.execute("""call dbo.usp_merge_mclm_star_diagnosis("""+str(var_pmd_client_id)+""","""+str(var_runyear)+""");""")
		db_opsdb_stars.execute("""call dbo.usp_merge_mclm_star_procedure("""+str(var_pmd_client_id)+""","""+str(var_runyear)+""");""")

		drop_tmp_pclmstar="""drop table if exists staging.pclm_star_temp;"""
		db_opsdb_starsrdsm.execute(drop_tmp_pclmstar)
		df_pclmstar_62=pd.read_sql_query("""select distinct runyear,transaction_id from dbo.pclm_star where pmd_client_id=62 ;""" , con=db_opsdb_stars)
		df_pclmstar_62.to_sql('pclm_star_temp',con=db_opsdb_starsrdsm,schema='staging',if_exists='append',index=False)
		#print(df_pclmstar_62.head())

		df_meridian_pclm=pd.read_sql_query("""with cte as
		(
		select
		cast(rtrim(ltrim(transaction_id)) as bigint) as pmd_pclaim_id
		,cast(62 as bigint) as pmd_client_id
		,cast('D' as varchar(1)) as medicare_part_d_ind
		,cast(patient_id as varchar(50))as patient_id
		,cast(cardholder_id as varchar(50))as cardholder_id
		,cast(patient_dob as date) as patient_dob
		,cast(patient_dob as date) as cardholder_dob
		,cast(date_of_service as timestamp) as date_of_service
		,cast(right('00000000000'|| ndc_code, 11) as varchar(19))as product_service_id
		,cast(productnameshort as varchar(70)) as product_service_desc
		,cast(quantity_dispensed as float) as quantity_dispensed
		,cast(days_supply  as int) as days_supply
		,cast(prescriber_npi as varchar(50)) as prescriber_id
		,cast(pharmacy_npi as varchar(50)) as pharmacy_id
		,cast(rtrim(ltrim(transaction_id)) as varchar(100)) as transaction_id
		,cast(adjudication_date as date) as adjudication_date
		,cast(admin_fee as money) as administrative_fee_amt
		,cast(usual_customary as money) as usual_customary
		,cast(ingredient_cost_submitted as money) as ingredient_cost_submitted
		,cast(ingredient_cost as money) as ingredient_cost
		,cast(disp_fee as money) as dispensing_fee
		,cast(sales_tax as money) as total_sales_tax
		,cast('' as varchar(11)) as drgmtch
		,cast(rtrim(ltrim(medstaken)) as varchar(4000)) as compositionname
		,cast(rtrim(ltrim(ndc_code)) as varchar(4000)) as formattedpackageidentifier		
		,cast('' as varchar(255)) as accountid
		,cast('' as varchar(255)) as accountid2
		,prescription_num as prescription_service_reference_num
		,extract(year from cast(date_of_service as date) )::int as runyear
		,row_number() over (partition by transaction_id order by cast(adjudication_date as date) desc, file_date desc, update_datetime desc) rowordernumber
		from dbo.meridian_62_claims clm  
		left join public.opsdb_medispan_alchemy_druglist_withingrandids  alch   on clm.ndc_code = alch.ndc11
		where LOWER(rtrim(ltrim(transaction_status))) = LOWER('P')
		and LOWER(rtrim(ltrim(medicare_park_d))) = LOWER('YES' )
		and not exists (select 1 from staging.pclm_star_temp pclm  
		where pclm.runyear::int = extract(year from cast(clm.date_of_service as date) )::int and clm.transaction_id = pclm.transaction_id)
		)
		select	pmd_pclaim_id,pmd_client_id,medicare_part_d_ind,patient_id,cardholder_id,patient_dob,cardholder_dob,date_of_service
		,product_service_id,product_service_desc,quantity_dispensed,days_supply,prescriber_id,pharmacy_id,transaction_id
		,adjudication_date,administrative_fee_amt,usual_customary,ingredient_cost_submitted,ingredient_cost,dispensing_fee
		,total_sales_tax,drgmtch,compositionname,formattedpackageidentifier	,accountid,accountid2,prescription_service_reference_num
		,runyear,now() create_datetime from cte where rowordernumber = 1;""", con=db_opsdb_starsrdsm)
		df_meridian_pclm.to_sql('pclm_star',con=db_opsdb_stars,schema='dbo',if_exists='append',index=False)
		#print(df_meridian_pclm.head())

		drop_tmp_meridian62_clm="""drop table if exists staging.meridian_62_claims_temp;"""
		db_opsdb_stars.execute(drop_tmp_meridian62_clm)
		df_meridian62_clm=pd.read_sql_query("""select distinct client_def_1 from dbo.meridian_62_claims where LOWER(transaction_status) = LOWER('R') and LOWER(medicare_park_d) = LOWER('YES') ;""" , con=db_opsdb_starsrdsm)
		df_meridian62_clm.to_sql('meridian_62_claims_temp',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)
		#print(df_pclmstar_62.head())

		update_meridian62_pclaim  = """update dbo.pclm_star pclm set claimexccd = 333,update_datetime = now()
		where pmd_client_id = 62 and pclm.runyear ="""+str(var_runyear)+"""
		and exists ( select * from staging.meridian_62_claims_temp clm  
		where clm.client_def_1 = pclm.transaction_id limit 1);"""
		db_opsdb_stars.execute(update_meridian62_pclaim)
		#print(update_meridian62_pclaim)
		update_meridian62_pclaim_claimexccd  = """update dbo.pclm_star pclm set claimexccd = 333,update_datetime = now()
		where pmd_client_id = 62 and pclm.runyear ="""+str(var_runyear)+"""
		and exists ( select * from staging.meridian_62_claims_temp clm  
		where clm.client_def_1 = pclm.transaction_id limit 1);"""
		db_opsdb_stars.execute(update_meridian62_pclaim_claimexccd)

		drop_tmp_rdsm_patient="""drop table if exists staging.stars_patients_temp;"""
		db_opsdb_stars.execute(drop_tmp_rdsm_patient)
		df_rdsm_patient=pd.read_sql_query("""select distinct patient_unique_ident,patient_id from dbo.stars_patients where pmd_client_id =62;""" , con=db_opsdb_starsrdsm)
		df_rdsm_patient.to_sql('stars_patients_temp',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)
		#print(df_pclmstar_62.head())

		drop_tmp_rdsm_patient_plan="""drop table if exists staging.stars_patients_plan_temp;"""
		db_opsdb_stars.execute(drop_tmp_rdsm_patient_plan)
		df_rdsm_patient_plan=pd.read_sql_query("""select distinct patient_unique_ident,planidentifier_pclm,eligibilitystartdate,eligibilityenddate from dbo.stars_patients_plan where pmd_client_id = 62 and runyear ="""+str(var_runyear)+""";""" , con=db_opsdb_starsrdsm)
		df_rdsm_patient_plan.to_sql('stars_patients_plan_temp',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)

		update_meridian62_pclaim_accountid  = """		update dbo.pclm_star clm
		set accountid2 = left(pp.planidentifier_pclm,5),update_datetime = now()
		from staging.stars_patients_temp p  
		inner join (select patient_unique_ident::bigint,planidentifier_pclm, eligibilitystartdate::timestamp , max(eligibilityenddate::timestamp) as eligibilityenddate 
		from staging.stars_patients_plan_temp 
		group by patient_unique_ident,planidentifier_pclm, eligibilitystartdate
		) pp on (p.patient_unique_ident = pp.patient_unique_ident::bigint ) 
		where	clm.pmd_client_id = 62 and length(coalesce(clm.accountid2,'')) = 0 and (p.patient_id =clm.patient_id)
		and clm.date_of_service between pp.eligibilitystartdate::timestamp and pp.eligibilityenddate::timestamp
		and clm.runyear ="""+str(var_runyear)+""";"""
		db_opsdb_stars.execute(update_meridian62_pclaim_accountid)
		#print(update_meridian62_pclaim_accountid)
		db_opsdb_starsrdsm.execute("""drop table if exists staging.mclm_star_temp;""")
		db_opsdb_starsrdsm.execute("""drop table if exists staging.pclm_star_temp;""")
		db_opsdb_stars.execute("""drop table if exists staging.stars_patients_temp;""")
		db_opsdb_stars.execute("""drop table if exists staging.stars_patients_plan_temp;""")
		db_opsdb_stars.execute("""drop table if exists staging.claimstatusupdates;""")
		db_opsdb_stars.execute("""drop table if exists staging.meridian_62_claims_temp;""")
		#print(update_aspire_reversal)
	elif var_pmd_client_id == "67":
		print('Medicaid')
		drop_tmp_pclmstar="""drop table if exists staging.pclm_star_temp;"""
		db_opsdb_starsrdsm.execute(drop_tmp_pclmstar)
		df_pclm_67=pd.read_sql_query("""select distinct runyear,transaction_id from dbo.pclm_star where pmd_client_id=67;""" , con=db_opsdb_stars)
		df_pclm_67.to_sql('pclm_star_temp',con=db_opsdb_starsrdsm,schema='staging',if_exists='append',index=False)

		df_medicaidpclm=pd.read_sql_query("""		select distinct
		cast(rtrim(ltrim(transaction_id)) as bigint) as pmd_pclaim_id
		,cast(67 as bigint) as pmd_client_id
		,cast('D' as varchar(1)) as medicare_part_d_ind
		,cast(patient_id as varchar(50))as patient_id
		,null::varchar(50) as cardholder_id
		,null::date as patient_dob --cast(patient_dob as date) 
		,cast(date_of_service as timestamp) as date_of_service
		,cast(right('00000000000'|| ndc_code, 11) as varchar(19)) as product_service_id
		,cast(productnameshort as varchar(70)) as product_service_desc
		,cast(quantity_dispensed as float) as quantity_dispensed
		,cast(round(days_supply::INT,0) as int) as days_supply
		,cast(prescriber_npi as varchar(50)) as prescriber_id
		,cast(pharmacy_npi as varchar(50)) as pharmacy_id
		,cast(rtrim(ltrim(transaction_id)) as varchar(100)) as transaction_id
		,null::date as adjudication_date
		,cast('' as varchar(11)) as drgmtch
		,cast(rtrim(ltrim(medstaken)) as varchar(4000)) as compositionname
		,cast(rtrim(ltrim(ndc_code)) as varchar(4000)) as formattedpackageidentifier
		,plan_code as accountid
		,plan_code as accountid2
		,prescription_num as prescription_service_reference_num
		,extract(year from cast(date_of_service as date) )::int as runyear
		from dbo.centene_medicaid_claims clm 
		left join public.opsdb_medispan_alchemy_druglist_withingrandids alch on clm.ndc_code = alch.ndc11
		where LOWER(rtrim(ltrim(transaction_status)) )= LOWER('PAID')
		and not exists (select 1 from staging.pclm_star_temp pclm 
		where pclm.runyear::int = extract(year from cast(date_of_service as date) )::int
		and clm.transaction_id = pclm.transaction_id)""", con=db_opsdb_starsrdsm)
		df_medicaidpclm.to_sql('pclm_star',con=db_opsdb_stars,schema='dbo',if_exists='append',index=False)
		db_opsdb_starsrdsm.execute("""drop table if exists staging.pclm_star_temp;""")

		drop_tmp_centenmedclm="""drop table if exists staging.centene_medicaid_claims_temp;"""
		db_opsdb_stars.execute(drop_tmp_centenmedclm)
		df_medicaidrclm_67=pd.read_sql_query("""select distinct transaction_id,reversed_claim_id from dbo.centene_medicaid_claims clm where LOWER(clm.transaction_status) = LOWER('REVERSAL');""" , con=db_opsdb_starsrdsm)
		df_medicaidrclm_67.to_sql('centene_medicaid_claims_temp',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)

		#update reversal for centene medicaid as per susan we are getting two different reversal logics
		#for claims prior to 9/1/2016, the original claim id referred to on the reversal claim points to the paid claim that is being reversed
		update_pclm_medicaidrclm1="""		update dbo.pclm_star pclm
		set claimexccd = 333,update_datetime = now()
		where pmd_client_id = 67
		and pclm.runyear ="""+str(var_runyear)+"""
		and date_of_service <= '09-01-2016'::timestamp
		and exists 
		(select 1 from staging.centene_medicaid_claims_temp clm 
		where  pclm.transaction_id = clm.reversed_claim_id);"""
		db_opsdb_stars.execute(update_pclm_medicaidrclm1)

		#for dates of service from 9/1/2016 and forward have reversal transaction ids that match to the paid claim it is reversing
		update_pclm_medicaidrclm2="""		update dbo.pclm_star pclm
		set claimexccd = 333,update_datetime = now()
		where pmd_client_id = 67
		and pclm.runyear ="""+str(var_runyear)+"""
		and date_of_service > '09-01-2016'::timestamp
		and exists 
		(select 1 from staging.centene_medicaid_claims_temp clm 
		where  pclm.transaction_id = clm.transaction_id);"""
		db_opsdb_stars.execute(update_pclm_medicaidrclm2)
		db_opsdb_stars.execute("""drop table if exists staging.centene_medicaid_claims_temp;""")
		db_opsdb_starsrdsm.execute("""drop table if exists staging.pclm_star_temp;""")
	elif var_pmd_client_id == "66":
		print('QRS')
		# QRS  
		qrs_sp = """call dbo.usp_load_pclm_star_centene_qrs(2020);"""
		db_opsdb_stars.execute(qrs_sp)
		df_rdsm_qrshistory=pd.read_sql_query("""select * from dbo.centene_qrs_claims;""" , con=db_opsdb_starsrdsm)
		df_rdsm_qrshistory.to_sql('centene_qrs_claims_history',con=db_opsdb_starsrdsm,schema='dbo',if_exists='append',index=False)
		truncate_qrs_claim="""truncate table dbo.centene_qrs_claims;"""
		db_opsdb_starsrdsm.execute(truncate_qrs_claim)
	elif var_pmd_client_id == "95":
		print('UHC_PA')
		# UHC_PA  
		uhc_pa_sp = """call dbo.usp_load_pclm_star_uhc_pa("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(uhc_pa_sp)
	elif var_pmd_client_id == "76":
		print('Sunshine Health Load')
		# Sunshine Health Load  
		sunshine_sp = """call dbo.usp_load_pclm_star_sunshinehealth("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(sunshine_sp)
	elif var_pmd_client_id == "94":
		print('UHC_URS')
		# UHC_URS  
		uhc_urs_sp = """call dbo.usp_load_pclm_star_uhc_urs("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(uhc_urs_sp)
	elif var_pmd_client_id == "77":
		print('HCSC Load')
		# HCSC Load  
		hcsc_sp = """call dbo.usp_load_pclm_star_hcsc("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(hcsc_sp)
	elif var_pmd_client_id == "90":
		print('UHC_VA')
		# UHC_VA  
		uhc_va_sp = """call dbo.usp_load_pclm_star_uhc_va("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(uhc_va_sp)
	elif var_pmd_client_id == "78":
		print('IVHP Load')
		# IVHP Load  
		ivhp_sp = """call dbo.usp_load_pclm_star_ivhp("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(ivhp_sp)
	elif var_pmd_client_id == "91":
		print('HCPNV Load')
		# HCPNV Load  
		hcpnv_sp = """call dbo.usp_load_pclm_star_hcpnv("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(hcpnv_sp)
	elif var_pmd_client_id == "81":
		print('Emblem Load')
		# Emblem Load  
		emblem_sp = """call dbo.usp_load_pclm_star_emblem_current("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(emblem_sp)
	elif var_pmd_client_id == "92":
		print('BlueKC Load')
		# BlueKC Load  
		bluekc_sp = """call dbo.usp_load_pclm_star_bluekc("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(bluekc_sp)
	elif var_pmd_client_id == "82":
		print('CareNCare Load')
		# CareNCare Load  
		carencare_sp = """call dbo.usp_load_pclm_star_carencare("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(carencare_sp)
	elif var_pmd_client_id == "96":
		print('BrightHealth Load')
		# BrightHealth Load  
		brighthealth_sp = """call dbo.usp_load_pclm_star_brighthealth("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(brighthealth_sp)
	elif var_pmd_client_id == "56":
		print('Gateway Load')
		# Gateway Load  
		gateway_sp = """call dbo.usp_load_pclm_star_gateway("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(gateway_sp)
	elif var_pmd_client_id == "83":
		print('BCBSAZ Load')
		# BCBSAZ Load  
		bcbsaz_sp = """call dbo.usp_load_pclm_star_bcbsaz("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(bcbsaz_sp)
	elif var_pmd_client_id == "84":
		print('GoldenState Load')
		# GoldenState Load 
		goldenstate_sp = """call dbo.usp_load_pclm_star_goldenstate("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(goldenstate_sp)
	elif var_pmd_client_id == "85":
		print('HAP Load')
		# HAP Load 
		hap_sp = """call dbo.usp_load_pclm_star_hap("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(hap_sp)
	elif var_pmd_client_id == "87":
		print('UHC_MR Load')
		# UHC_MR Load 
		uhc_mr_sp = """call dbo.usp_load_pclm_star_uhc_mr("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(uhc_mr_sp)
	elif var_pmd_client_id == "88":
		print('UHC_NC Load')
		# UHC_NC Load 
		uhc_nc_sp = """call dbo.usp_load_pclm_star_uhc_nc("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(uhc_nc_sp)
	elif var_pmd_client_id == "89":
		print('Molina Load')
		# Molina Load 
		molina_sp = """call dbo.usp_load_pclm_star_molina("""+str(var_runyear)+""");"""
		db_opsdb_stars.execute(molina_sp)
	print('# Determine Path done ')
	##############################
	# Delete Corrected Load Errors
	delete_errors = """delete from dbo.mclm_star_load_errors e where exists (select 1 from dbo.mclm_star s where s.runyear = e.runyear and s.pmd_client_id = e.pmd_client_id and s.claim_id = e.claim_id and s.claim_line_num = e.claim_line_num and s.admission_date = e.admission_date);"""
	db_opsdb_stars.execute(delete_errors)
	print('# Delete Corrected Load Errors done ')
	
	##############################
	#Remove Failed Evals
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("Remove Failed Evals start:::", dt_string)
	remove_faileeval = """call dbo.uspdeleteevals_adh_new(""" + str(var_runyear) + """,""" + str(var_pmd_client_id) + """,'Failed');
	call dbo.uspdeleteevals_hrm_new(""" + str(var_runyear) + """,""" + str(var_pmd_client_id) + """,'Failed');
	call dbo.uspdeleteevals_dts_new(""" + str(var_runyear) + """,""" + str(var_pmd_client_id) + """,'Failed');
	call dbo.uspdeleteevals_opioids_new(""" + str(var_runyear) + """,""" + str(var_pmd_client_id) + """,'Failed');"""
	db_opsdb_starswarehouse.execute(remove_faileeval)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("Remove Failed Evals done:::", dt_string)
	##############################
	# Stars_RunData Insert--INSERT Record INTO Stars_RunData
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("Stars_RunData Insert start:::", dt_string)
	stars_rundata_insert = """do $$
	DECLARE var_evalrun_id integer;
	begin
	INSERT INTO dbo.stars_rundata (runyear, client_id,  run_date, run_type)
	SELECT  """ + str(var_runyear) + """ runyear, """ + str(var_pmd_client_id) + """ AS client_id,now() AS run_date,'Adherence' AS run_type ;
	var_evalrun_id:=(SELECT MAX(evalrun_id) FROM dbo.stars_rundata WHERE lower(run_type) = lower('Adherence') AND client_id = """ + str(var_pmd_client_id) + """ );
	call dbo.usp_getstarrundatafiletypes (var_evalrun_id); 
	end$$;"""
	db_opsdb_stars.execute(stars_rundata_insert)
	#db_opsdb_stars.execute(text(stars_rundata_insert).execution_options(autocommit=True))
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("Stars_RunData Insert done:::", dt_string)
	
	##############################
	# TRUNCATE TABLES (PRE)
	truncate_pre_table1 = """truncate table staging.pclm_star_adh_claims;
	truncate table staging.pclm_star_insulin_claims;
	truncate table staging.pclm_star_ras_exclusion;
	truncate table dbo.review_drugmismatches;
	truncate table dbo.adh_pt_meas_plan_singledrug;
	truncate table dbo.adh_pt_meas_plan_multipledrug;
	truncate table staging.processset_dayscovered;
	truncate table dbo.eligibility;"""
	db_opsdb_stars.execute(truncate_pre_table1)

	truncate_pre_table2 = """truncate table dbo.adh_eligibility_current;"""
	db_opsdb_starswarehouse.execute(truncate_pre_table2)
	print('# TRUNCATE TABLES (PRE) done ')
	
	##############################
	# Stage ADH & Exclusion Claims
	# Below set is used for all three components (Adherence,Diabetes (Insulin) Exclusions)
	
	get_rundetails = """select  evalrun_id,runyear,planidentifier_pclm,client_id
	from dbo.stars_rundata
	where evalrun_id = (select max( evalrun_id) from dbo.stars_rundata where run_type = 'Adherence')"""
	
	#get_rundetails = """select 1000,2021,'x',30""" # temp query
	get_rundetails_op=db_opsdb_stars.execute(get_rundetails).fetchall()
	adherance_evalrun_id = get_rundetails_op[0][0]
	adherance_runyear = get_rundetails_op[0][1]
	adherance_pclm = get_rundetails_op[0][2]
	adherance_pmd_client_id = get_rundetails_op[0][3]
	print('# Stage ADH & Exclusion Claims done ')
	
	#Adherence
	# TO IMPROVE PERFORMANCE OF QUERY Transfering RDSM, MEDISPAN DATA TO STARS.STAGING
	#TABLE1
	drop_table_query1="""drop table if exists staging.stars_rdsm_stars_patients"""
	db_opsdb_stars.execute(drop_table_query1)
	
	drop_table_query2="""drop table if exists staging.pqa_medispan_adh"""
	db_opsdb_stars.execute(drop_table_query2)
	
	drop_table_query3="""drop table if exists staging.pqa_medispan_insulin"""
	db_opsdb_stars.execute(drop_table_query3)

	drop_table_query4="""drop table if exists staging.pqa_druglist_ras_exclusions"""
	db_opsdb_stars.execute(drop_table_query4)

	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.stars_rdsm_stars_patients  start:::", dt_string)
	create_staging_query="""CREATE TABLE IF NOT EXISTS staging.stars_rdsm_stars_patients(patient_unique_ident bigint NOT NULL ,pmd_client_id integer NOT NULL,patient_id character varying(50))"""
	db_opsdb_stars.execute(create_staging_query)
	df_stars_patient=pd.read_sql_query("""select patient_unique_ident,pmd_client_id,patient_id from dbo.stars_patients where pmd_client_id="""+str(adherance_pmd_client_id)+""";""" , con=db_opsdb_starsrdsm)
	df_stars_patient.to_sql('stars_rdsm_stars_patients',con=db_opsdb_stars,schema='staging',if_exists='replace',index=False)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.stars_rdsm_stars_patients  done:::", dt_string)

	#TABLE2
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pqa_medispan_adh  start:::", dt_string)
	df_pqa_medispan_adh=pd.read_sql_query("""select * from dbo.pqa_medispan_adh;""" , con=db_opsdb_medispan)
	df_pqa_medispan_adh.to_sql('pqa_medispan_adh',con=db_opsdb_stars,schema='staging',if_exists='replace',index=False)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pqa_medispan_adh  done:::", dt_string)	

	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pclm_star_adh_claims  start:::", dt_string)
	df_adherance=pd.read_sql_query("""select distinct
			   """+str(adherance_evalrun_id)+""" as evalrun_id
			   ,pc.pmd_client_id 
			   ,"""+str(adherance_runyear)+""" as runyear
			   ,spts.patient_unique_ident::bigint  as pmd_patient_id 
			   ,pc.transaction_id
			   ,pc.accountid2 as planidentifier_pclm
			   ,pc.medicare_part_d_ind
			   ,pc.patient_id
			   ,pc.date_of_service
			   ,extract(doy from date_of_service)::integer as julianstart
			   ,extract(doy from pc.date_of_service + ((INTERVAL '1 day' * days_supply)- INTERVAL '1 day'))::integer as julianend
			   ,max(pc.prescriber_id)::varchar(50) as prescriber_id
			   ,max(pc.pharmacy_id)::varchar(50) as pharmacy_id
			   ,medispan.measureuse::varchar(14)
			   ,medispan."Drug Name"::varchar(60) as drug_name
			   ,pc.days_supply
			   ,pc.quantity_dispensed
			   ,max(medispan."Drug Description")::varchar(255) as "drug description"
			   ,max(medispan.strength)::varchar(15) as drug_strength 
			   ,max(medispan.strength_unit_of_measure)::varchar(10) as unit_of_measure
			   ,medispan.dosage_form::varchar(4) as dosage_form
			   ,medispan.gpi::varchar(50)gpi
			   ,medispan.ndc::varchar(50)ndc
			   ,pc.prescription_service_reference_num
				from   dbo.pclm_star pc
				inner join staging.pqa_medispan_adh medispan  on pc.product_service_id = medispan.ndc 
				inner join staging.stars_rdsm_stars_patients spts  on pc.patient_id = spts.patient_id 
				and spts.pmd_client_id::BIGINT = pc.pmd_client_id
				inner join (select distinct pmd_client_id , planidentifier_pclm  
					from dbo.client_contracts where pmd_client_id = """+str(adherance_pmd_client_id)+"""
					and contract_year= """+str(adherance_runyear)+"""
					and LOWER(measure_use) in (LOWER('Ras'), LOWER('Statin'), LOWER('Diabetes'),LOWER('Insulin'),LOWER('Asthma'),LOWER('DTS')) 
					and analytics_contract = true
					) cc on cc.pmd_client_id = pc.pmd_client_id and cc.planidentifier_pclm = pc.accountid2
				where pc.runyear = """+str(adherance_runyear)+"""
			and pc.pmd_client_id = """+str(adherance_pmd_client_id)+"""
			and pc.claimexccd is null
			and LOWER(pc.medicare_part_d_ind) in (LOWER('D'),LOWER('Y'))
			group by             
			pc.pmd_client_id,spts.patient_unique_ident,pc.transaction_id,pc.accountid2,pc.medicare_part_d_ind,pc.patient_id
			,pc.date_of_service,medispan.measureuse,medispan."Drug Name",pc.days_supply,pc.quantity_dispensed,medispan.dosage_form,medispan.gpi,medispan.ndc,pc.prescription_service_reference_num;""", con=db_opsdb_stars)
	df_adherance.to_sql('pclm_star_adh_claims',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pclm_star_adh_claims  done:::", dt_string)

	'''commenting as same table is loaded in adherance sp.	(Insuline , RAS)
	# Diabetes (Insulin) Exclusions
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pqa_medispan_insulin  start:::", dt_string)
	#TABLE3
	df_pqa_medispan_adh=pd.read_sql_query("""select * from dbo.pqa_medispan_insulin;""" , con=db_opsdb_medispan)
	df_pqa_medispan_adh.to_sql('pqa_medispan_insulin',con=db_opsdb_stars,schema='staging',if_exists='replace',index=False)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pqa_medispan_insulin  done:::", dt_string)	

	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pclm_star_insulin_claims  start:::", dt_string)
	df_diabetes=pd.read_sql_query("""select  distinct  
			"""+str(adherance_evalrun_id)+""" as evalrun_id
			,pc.pmd_client_id
			,pc.accountid2 as planidentifier_pclm
			,"""+str(adherance_runyear)+""" as runyear
			,spts.patient_unique_ident::bigint as pmd_patient_id 
			,pc.patient_id    
			,pc.transaction_id
			,pc.medicare_part_d_ind
			,pc.date_of_service
			,extract(doy from date_of_service)::integer as julianstart
			,extract(doy from pc.date_of_service + ((INTERVAL '1 day' * days_supply)- INTERVAL '1 day'))::integer as julianend
			,pc.prescriber_id
			,pc.pharmacy_id
			,'Diabetes-InsulinExclusion'::varchar(25) measureuse 
			,medispan."Drug Name"::varchar(60) as drug_name
			,pc.days_supply
			,pc.quantity_dispensed
			,medispan."Drug Description"::varchar(250) AS "drug description"
			,medispan.strength::varchar(15) as drug_strength 
			,medispan.strength_unit_of_measure::varchar(10)as unit_of_measure
			,medispan.dosage_form::varchar(4) as dosage_form
			,medispan.gpi::varchar(50) as gpi
			,medispan.ndc::varchar(50) as ndc
			from  dbo.pclm_star pc
			inner join staging.pqa_medispan_insulin medispan on pc.product_service_id = medispan.ndc    
			inner join staging.stars_rdsm_stars_patients spts on pc.patient_id = spts.patient_id 
			and pc.pmd_client_id = spts.pmd_client_id ::bigint         
			inner join (select distinct pmd_client_id, planidentifier_pclm
				from dbo.client_contracts where pmd_client_id = """+str(adherance_pmd_client_id)+"""
				and contract_year="""+str(adherance_runyear)+""" and LOWER(measure_use) in (LOWER('Ras'), LOWER('Statin'), LOWER('Diabetes'))
				and analytics_contract = true
				) cc on cc.pmd_client_id = pc.pmd_client_id and cc.planidentifier_pclm = pc.accountid2
			where  extract(year from date_of_service) = """+str(adherance_runyear)+"""
			and pc.pmd_client_id = """+str(adherance_pmd_client_id)+""";""", con=db_opsdb_stars)
		
	df_diabetes.to_sql('pclm_star_insulin_claims',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pclm_star_insulin_claims  done:::", dt_string)	

	# RAS Exclusions

	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pqa_druglist_ras_exclusions  start:::", dt_string)
	#TABLE4
	df_pqa_druglist_ras=pd.read_sql_query("""select * from ops.pqa_druglist_ras_exclusions;""" , con=db_opsdb_medispan)
	df_pqa_druglist_ras.to_sql('pqa_druglist_ras_exclusions',con=db_opsdb_stars,schema='staging',if_exists='replace',index=False)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pqa_druglist_ras_exclusions  done:::", dt_string)

	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pclm_star_ras_exclusion  start:::", dt_string)
	df_rasexclusions=pd.read_sql_query("""select distinct
		"""+str(adherance_evalrun_id)+""" as evalrun_id
		,pc.pmd_client_id
		,"""+str(adherance_runyear)+""" as runyear
		,spts.patient_unique_ident::bigint  as pmd_patient_id 
		,pc.transaction_id
		,pc.accountid2 as planidentifier_pclm
		,pc.medicare_part_d_ind
		,pc.patient_id
		,pc.date_of_service
		,extract(doy from date_of_service)::integer as julianstart
		,extract(doy from pc.date_of_service + ((INTERVAL '1 day' * days_supply)- INTERVAL '1 day'))::integer as julianend
		,pc.prescriber_id
		,pc.pharmacy_id
		,medispan.measureuse::varchar(14) as measureuse
		,medispan."Drug Name"::varchar(60) as drug_name
		,pc.days_supply
		,pc.quantity_dispensed
		,medispan."Drug Description" ::varchar(250) as "drug description"
		,medispan.strength::varchar(15) as drug_strength --medispan
		,medispan.strength_unit_of_measure::varchar(10) as unit_of_measure
		,medispan.dosage_form::varchar(4) as dosage_form
		,medispan.gpi::varchar(50) as gpi
		,medispan.ndc::varchar(50) as ndc
		,pc.prescription_service_reference_num
		from dbo.pclm_star pc
		inner join staging.pqa_medispan_adh medispan  on pc.product_service_id = medispan.ndc 
		inner join staging.stars_rdsm_stars_patients spts  on pc.patient_id = spts.patient_id 
		and spts.pmd_client_id::BIGINT = pc.pmd_client_id
		INNER join (select distinct pmd_client_id , planidentifier_pclm  
		from dbo.client_contracts where pmd_client_id ="""+str(adherance_pmd_client_id)+""" and contract_year= """+str(adherance_runyear)+"""
		and LOWER(measure_use) in (LOWER('Ras'), LOWER('Statin'), LOWER('Diabetes'),LOWER('Insulin'),LOWER('Asthma')) and analytics_contract = TRUE
		) cc on cc.pmd_client_id = pc.pmd_client_id and cc.planidentifier_pclm = pc.accountid2
		inner join staging.pqa_druglist_ras_exclusions rasexc on pc.product_service_id = rasexc.ndc
		where pc.runyear = """+str(adherance_runyear)+"""
		and pc.pmd_client_id ="""+str(adherance_pmd_client_id)+"""
		and pc.claimexccd is null
		and LOWER(pc.medicare_part_d_ind) in (LOWER('D'),LOWER('Y'));""", con=db_opsdb_stars)
	
	df_rasexclusions.to_sql('pclm_star_ras_exclusion',con=db_opsdb_stars,schema='staging',if_exists='append',index=False)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Adherence staging.pclm_star_ras_exclusion  done:::", dt_string)
	'''	
	drop_table_query1="""drop table if exists staging.stars_rdsm_stars_patients"""
	#db_opsdb_stars.execute(drop_table_query1)
	
	drop_table_query2="""drop table if exists staging.pqa_medispan_adh"""
	#db_opsdb_stars.execute(drop_table_query2)
	
	drop_table_query3="""drop table if exists staging.pqa_medispan_insulin"""
	#db_opsdb_stars.execute(drop_table_query3)

	drop_table_query4="""drop table if exists staging.pqa_druglist_ras_exclusions"""
	#db_opsdb_stars.execute(drop_table_query4)
	print('# Adherences done ')
	
	##############################
	# Update PBP & AccountID
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Update PBP & AccountID done  dbo.usp_adh_claims_update_pbp_accountid() start:::", dt_string)
	update_pbp_actid = """call dbo.usp_adh_claims_update_pbp_accountid ("""+str(var_pmd_client_id)+""","""+str(var_runyear)+""");"""
	db_opsdb_stars.execute(update_pbp_actid)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Update PBP & AccountID done done:::", dt_string)

	##############################
	# m Single VS Multi Drug
	# Staging PClm_Star_ADH_Claims (Multi)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging PClm_Star_ADH_Claims (Multi) start:::", dt_string)
	df_snglvsmulti=pd.read_sql_query("""select (select max(evalrun_id) as evalrun_id from dbo.stars_rundata where run_type = 'Adherence') as evalrun_id
		,pmd_patient_id::varchar(50) as pmd_patient_id
		,measureuse
		,cast(planidentifier_pclm as varchar(5)) as planidentifier_pclm
		,count(*)::integer pt_meas_plan_drugcounter
		from    (select pmd_patient_id,measureuse,planidentifier_pclm,count(*) pt_composition_meas_plan_claimcounter
				from   staging.pclm_star_adh_claims
				where  evalrun_id = (select max(evalrun_id) as evalrun_id from dbo.stars_rundata where run_type = 'Adherence') 
				group by pmd_patient_id,measureuse,planidentifier_pclm) bb
		group by pmd_patient_id,measureuse,planidentifier_pclm
		having  count(*) > 1""", con=db_opsdb_stars)
	
	df_snglvsmulti.to_sql('adh_pt_meas_plan_multipledrug',con=db_opsdb_stars,schema='dbo',if_exists='append',index=False)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging PClm_Star_ADH_Claims (Multi) done:::", dt_string)
	
	# Staging PClm_Star_ADH_Claims (Single)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging PClm_Star_ADH_Claims (Single) start:::", dt_string)
	df_staging_pclm_adh_clm=pd.read_sql_query("""select  (select max(evalrun_id) as evalrun_id from dbo.stars_rundata where run_type = 'Adherence') as evalrun_id
		,pmd_patient_id::varchar(50) as pmd_patient_id
		,measureuse
		,planidentifier_pclm
		,count(*)::integer pt_meas_plan_drugcounter
		from    (select pmd_patient_id,measureuse,cast(planidentifier_pclm as varchar(5)) as planidentifier_pclm
				,count(*) pt_composition_meas_plan_claimcounter
				from   staging.pclm_star_adh_claims
				where  evalrun_id = (select max(evalrun_id) as evalrun_id from dbo.stars_rundata where run_type = 'Adherence') 
				group by pmd_patient_id,measureuse,planidentifier_pclm) bb
		group by pmd_patient_id,measureuse,planidentifier_pclm
		having  count(*) = 1""", con=db_opsdb_stars)
	
	df_staging_pclm_adh_clm.to_sql('adh_pt_meas_plan_singledrug',con=db_opsdb_stars,schema='dbo',if_exists='append',index=False)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging PClm_Star_ADH_Claims (Single) done:::", dt_string)
	
	##############################
	#New ADH_StatusData
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#New ADH_StatusData start:::", dt_string)
	new_adh_statusdata = """call adherence.usp_adherence ("""+str(var_pmd_client_id)+""","""+str(var_runyear)+""");"""
	db_opsdb_stars.execute(new_adh_statusdata)
	##db_opsdb_stars.execute(text(new_adh_statusdata).execution_options(autocommit=True))
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#New ADH_StatusData done:::", dt_string)
	
	##############################
	#Archive to Stars_Warehouse
	# Eligibility 1
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Eligibility 1  start:::", dt_string)
	df_eligibility1=pd.read_sql_query("""select evalrun_id,patient_eligibility_id,pmd_patient_id,planidentifier_pclm,eligibilitystartdate,eligibilityenddate from dbo.eligibility;""" , con=db_opsdb_stars)
	df_eligibility1.to_sql('adh_eligibility_current',con=db_opsdb_starswarehouse,schema='dbo',if_exists='append',index=False)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Eligibility 1  done:::", dt_string)

	#Eligibility
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Eligibility start:::", dt_string)
	df_eligibility=pd.read_sql_query("""select evalrun_id,patient_eligibility_id,pmd_patient_id,planidentifier_pclm,eligibilitystartdate,eligibilityenddate from dbo.eligibility;""" , con=db_opsdb_stars)
	df_eligibility.to_sql('adh_eligibility',con=db_opsdb_starswarehouse,schema='dbo',if_exists='append',index=False)    
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Eligibility done:::", dt_string)

	# Multiple Drug Review 1
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Multiple Drug Review 1 start:::", dt_string)
	df_multipledrug=pd.read_sql_query("""select evalrun_id::int as evalrun_id,pmd_patient_id::bigint as pmd_patient_id,planidentifier_pclm::varchar(5) as planidentifier_pclm,measureuse,pt_meas_plan_drugcounter from dbo.adh_pt_meas_plan_multipledrug;""" , con=db_opsdb_stars)
	df_multipledrug.to_sql('adh_pt_meas_plan_multipledrug_currreview',con=db_opsdb_starswarehouse,schema='dbo',if_exists='append',index=False) 
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Multiple Drug Review 1 done:::", dt_string)

	# Single Drug Review 1
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Single Drug Review 1 start:::", dt_string)
	df_singledrug=pd.read_sql_query("""select distinct evalrun_id::int as evalrun_id,pmd_patient_id::bigint as pmd_patient_id,planidentifier_pclm::varchar(5) as planidentifier_pclm,measureuse,pt_meas_plan_drugcounter from dbo.adh_pt_meas_plan_singledrug;""" , con=db_opsdb_stars)
	df_singledrug.to_sql('adh_pt_meas_plan_singledrug_currreview',con=db_opsdb_starswarehouse,schema='dbo',if_exists='append',index=False) 
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Single Drug Review 1 done:::", dt_string)

	# Staging PClm_Star_ADH_Claims
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging PClm_Star_ADH_Claims start:::", dt_string)
	df_staging_pclmstar_adhclm=pd.read_sql_query("""select evalrun_id,pmd_client_id,measureuse,planidentifier_pclm,pmd_patient_id,transaction_id,runyear,
		patient_id,medicare_part_d_ind,cast(date_of_service as date) as date_of_service,julianstart,julianend,prescriber_id,pharmacy_id,drug_name,
		days_supply,quantity_dispensed,drug_strength ,"drug description" ,unit_of_measure ,dosage_form,gpi,ndc
		,prescription_service_reference_num,cast(now() as date) as evaluation_date,pbp_pclm,accountid_pclm             
		from staging.pclm_star_adh_claims;""" , con=db_opsdb_stars)
	df_staging_pclmstar_adhclm.to_sql('pclm_star_adh_claims_current',con=db_opsdb_starswarehouse,schema='dbo',if_exists='append',index=False) 
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging PClm_Star_ADH_Claims done:::", dt_string)

	# Staging PClm_Star_ADH_Claims1
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging PClm_Star_ADH_Claims1 start:::", dt_string)
	df_staging_pclmstar_adhclm1=pd.read_sql_query("""select evalrun_id,pmd_client_id,measureuse,planidentifier_pclm,pmd_patient_id,transaction_id,runyear,
		patient_id,medicare_part_d_ind,cast(date_of_service as date) as date_of_service,julianstart,julianend,prescriber_id,pharmacy_id,drug_name,
		days_supply,quantity_dispensed,drug_strength,"drug description",unit_of_measure,dosage_form,gpi,ndc
		,prescription_service_reference_num,cast(now() as date) as evaluation_date,pbp_pclm,accountid_pclm             
		from staging.pclm_star_adh_claims;""" , con=db_opsdb_stars)
	df_staging_pclmstar_adhclm1.to_sql('pclm_star_adh_claims',con=db_opsdb_starswarehouse,schema='dbo',if_exists='append',index=False) 
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging PClm_Star_ADH_Claims1 done:::", dt_string)
	
	# Staging PClm_Star_Insulin_Claims
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging PClm_Star_Insulin_Claims1 start:::", dt_string)
	df_staging_pclmstar_insulineclm=pd.read_sql_query("""select evalrun_id,pmd_client_id,planidentifier_pclm,runyear,pmd_patient_id,patient_id,transaction_id,medicare_part_d_ind,
		date_of_service,julianstart,julianend,prescriber_id,pharmacy_id,measureuse,drug_name,days_supply,quantity_dispensed,
		"drug description",drug_strength,unit_of_measure,dosage_form,gpi,ndc
		from staging.pclm_star_insulin_claims;""" , con=db_opsdb_stars)
	df_staging_pclmstar_insulineclm.to_sql('pclm_star_insulin_claims',con=db_opsdb_starswarehouse,schema='dbo',if_exists='append',index=False) 
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging PClm_Star_Insulin_Claims1 done:::", dt_string)
	
	# Staging ProcessSet_Days_Covered
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging ProcessSet_Days_Covered start:::", dt_string)
	df_process_days_covered=pd.read_sql_query("""select cast(now() as date) as archive_date,evalrun,pmd_patient_id,cast(planidentifier_pclm as varchar(5)) as planidentifier_pclm,
		measureuse,julianstart,julianend,modifiedjulianstart,modifiedjulianend,days_supply,slack,processed,
		patient_eligibility_id,drug_name,runyear,
		pmd_client_id,transaction_id,drugbypatientid,julianstartbydrugid
		from staging.processset_dayscovered;""" , con=db_opsdb_stars)
	df_process_days_covered.to_sql('processset_dayscovered',con=db_opsdb_starswarehouse,schema='dbo',if_exists='append',index=False)    
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("# Staging ProcessSet_Days_Covered done:::", dt_string)

	#print('#Archive to Stars_Warehouse done ')
	 
	##############################
	#Get MaxEvalRun_ID
	get_maxevalrun_id_query = """select coalesce(max(evalrun_id),0) as maxeval from dbo.adh_statusdata 
	where pmd_client_id = """+str(var_pmd_client_id)+"""
	and evalrun_id not in (select evalrun_id from dbo.clientoutput_membermeasure where pmd_client_id = """+str(var_pmd_client_id)+""");"""
	maxevalrun_id = db_opsdb_starswarehouse.execute(get_maxevalrun_id_query).fetchall()[0][0]
	print('#Get MaxEvalRun_ID done ')
	
	##############################
	#if request_id >= 0:
	#Archive to ClientOutputMemberMeasure
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Archive to ClientOutputMemberMeasure start:::", dt_string)
	df_archive_to_clientoptmeambrmsure=pd.read_sql_query("""select  a.evalrun_id,a.pmd_client_id,a.planidentifier_pclm,a.pmd_patient_id,a.measureuse,a.evaluationmonth,a.qualifyingmonthmedcount,a.pmdstatusdescr,a.measurequal_status_eoy,a.mininsulinmonth,a.acumenpreviousyear_status,a.acumenpreviousyear_pdc_unadjusted,a.measureadh_status,a.measureadh_statusdescription,a.stockstatus,a.stockstatusdescription,a.supplyend_julian,a.measurequalified,a.memberinnumerator,a.distinctmedications,a.pdc_for_month,a.pdc_numerator_month,a.pdc_denominator_month,a.daysneeded,a.daysremaining,a.stockinhand,a.dayscalc,a.evaluationdate
	,a.eligibilitystartdate,a.eligibilityenddate,sr.runyear,a.pbp_pclm,a.accountid_pclm
	from dbo.adh_statusdata  a
	inner join public.stars_stars_rundata sr
	on a.evalrun_id = sr.evalrun_id
	where a.evalrun_id = """+str(maxevalrun_id)+""";""" , con=db_opsdb_starswarehouse)
	df_archive_to_clientoptmeambrmsure.to_sql('clientoutput_membermeasure',con=db_opsdb_starswarehouse,schema='dbo',if_exists='append',index=False) 
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Archive to ClientOutputMemberMeasure done:::", dt_string)

	##############################
	#UPDATE Stars_RunData ADH
	update_stars_rundata = """update dbo.stars_rundata set run_completed = now() where evalrun_id = (select max(evalrun_id) as evalrun_id from dbo.stars_rundata where run_type = 'Adherence')"""
	db_opsdb_stars.execute(update_stars_rundata)
	print('#UPDATE Stars_RunData ADH done ')
	
	##############################
	#STARS HRM Evaluation
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#STARS HRM Evaluation start:::", dt_string)
	stars_hrm_eval = """call dbo.stars_hrm_evaluation("""+str(var_pmd_client_id)+""",'"""+str(var_planidentifier_pclm)+"""',"""+str(var_runyear)+""")"""
	db_opsdb_stars.execute(stars_hrm_eval)
	#print(stars_hrm_eval)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#STARS HRM Evaluation done:::", dt_string)
	##############################
	#Stars QIM Evaluation---Need to write dynamic sp calling part call Intake.usp_Archive();-

	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Stars QIM Evaluation child sp start:::", dt_string)
	fileloop_counts = """SELECT COUNT(*)c FROM intake.vw_intake;"""
	fileloop_counts = db_opsdb_operationaldatastore.execute(fileloop_counts).fetchall()[0][0]
	print(str(fileloop_counts))
	iterator=1
	truncate_patient_table = """TRUNCATE TABLE intake.patientleveldetail;"""
	db_opsdb_operationaldatastore.execute(truncate_patient_table)
	
	while iterator <= fileloop_counts:
		#print('hi')
		alias_value = """SELECT LOWER(filetype)filetype,LOWER(alias)alias FROM intake.vw_intake where vw_intakeid="""+str(iterator)+""";"""
		alias = db_opsdb_operationaldatastore.execute(alias_value).fetchall()
		filetype = alias[0][0]
		alias = alias[0][1]
		strquery='call '+str(filetype)+'."usp_'+str(alias)+'"();'
		db_opsdb_archive.execute(strquery)
		print(strquery)
		iterator=iterator+1

	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#Stars QIM Evaluation child sp done:::", dt_string)

	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#intake.usp_archive() sp start:::", dt_string)
	stars_qim_eval1 = """call intake.usp_archive();"""
	db_opsdb_operationaldatastore.execute(stars_qim_eval1)
	#db_opsdb_operationaldatastore.execute(text(stars_qim_eval1).execution_options(autocommit=True))
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#intake.usp_archive() sp done:::", dt_string)

	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#evaluation.usp_patientleveldetail() sp start:::", dt_string)
	stars_qim_eval2 = """call evaluation.usp_patientleveldetail();"""
	db_opsdb_operationaldatastore.execute(stars_qim_eval2)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#evaluation.usp_patientleveldetail() sp done:::", dt_string)
	print('#Stars QIM Evaluation done ')
	##############################
	#STARS SUPD Evaluation 
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#STARS SUPD Evaluation start:::", dt_string)
	stars_supd_eval = """call dbo.usp_dts_statusdata("""+str(var_pmd_client_id)+""",'"""+str(var_planidentifier_pclm)+"""',"""+str(var_runyear)+""")"""
	db_opsdb_stars.execute(stars_supd_eval)
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#STARS SUPD Evaluation done:::", dt_string)

	##############################
	#Update Statistics
	update_statistics1 = """ANALYSE dbo.pclm_star;"""
	#db_opsdb_stars.execute(update_statistics1)


	##############################
	update_statistics2 = """ANALYSE dbo.adh_statusdata;
		ANALYSE dbo.clientoutput_membermeasure;
		ANALYSE dbo.hrm_statusdata;
		ANALYSE dbo.pclm_star_adh_claims_current;
		ANALYSE dbo.processset_dayscovered_current;"""
	#db_opsdb_starswarehouse.execute(update_statistics2)
	print('#Update Statistics done ')	
	##############################
	#TRUNCATE TABLE(s)
	truncate_tables = """truncate table staging.pclm_star_adh_claims;
		truncate table staging.pclm_star_insulin_claims;
		truncate table staging.processset_dayscovered;
		truncate table dbo.review_drugmismatches;
		truncate table dbo.adh_pt_meas_plan_singledrug;
		truncate table dbo.adh_pt_meas_plan_multipledrug;
		truncate table dbo.processset_dayscovered;
		truncate table dbo.eligibility;"""
	#db_opsdb_stars.execute(truncate_tables)
	print('#TRUNCATE TABLE(s) done ')
	##############################
	#Send Billing Request
	send_billing_request = """do $$
		declare par_pmd_client_id integer="""+str(var_pmd_client_id)+""";
		declare par_runyear integer="""+str(var_runyear)+""";
		declare par_pendingbillingrequests integer;
		begin
		par_pendingbillingrequests := 
		(select count(*) from dbo.billing_request where pmd_client_id = par_pmd_client_id and runyear = par_runyear 
		 and completed is null);
		if par_pendingbillingrequests = 0 then
			insert into dbo.billing_request (requester,pmd_client_id,runyear,billing_mode)      
			select 'EVAL', par_pmd_client_id, par_runyear,'PMD';
		end if;
		if par_pendingbillingrequests > 0 then
			update dbo.billing_request br 
			set create_datetime = now()
			where pmd_client_id = par_pmd_client_id and runyear = par_runyear and completed is null;
		end if;
		end$$;select 1;"""
	db_opsdb_stars_billing.execute(send_billing_request)
	#db_opsdb_stars_billing.execute(text(send_billing_request).execution_options(autocommit=True))
	##############################
	#Remove Successful Evals from _current tables
						
	remove_success_evals = """call dbo.uspdeleteevals_adh_new("""+str(var_runyear)+""", """+str(var_pmd_client_id)+""", 'Success');
		call dbo.uspdeleteevals_hrm_new("""+str(var_runyear)+""", """+str(var_pmd_client_id)+""", 'Success');
		call dbo.uspdeleteevals_dts_new("""+str(var_runyear)+""", """+str(var_pmd_client_id)+""", 'Success');
		call dbo.uspdeleteevals_adh_clientoutput_membermeasure();"""
	db_opsdb_stars.execute(remove_success_evals)
	print('#Remove Successful Evals done ')

	##############################
	#ExecSQL Stars_Warehouse dbo usp_Load_ADH_StatusData_Today_Comb
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#ExecSQL Stars_Warehouse dbo usp_Load_ADH_StatusData_Today_Comb start:::", dt_string)
	call_adh_comb = """call dbo.usp_load_adh_statusdata_today_comb();"""
	db_opsdb_starswarehouse.execute(call_adh_comb) 
	now = datetime.now()
	dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
	print("#ExecSQL Stars_Warehouse dbo usp_Load_ADH_StatusData_Today_Comb done:::", dt_string)
	#Confirm Evaluation Success
	task = {
			"title": None,
			"from_address": flie_watch_email_recipients, #flie_watch_email_recipients,
			"to_address": [flie_watch_email_recipients],
			"cc_address": None,
			"bcc_address": None,
			"message": None,
			"html_message": file_watch_environment+' Stars Evaluation Completed for '+var_clientname+' ('+var_planidentifier_pclm+')',
			"attachments": None,
			"importance": None
	}
	headers = {'Content-Type': 'application/json', 'Authorization': 'Token token=portal_token'}
	resp = requests.post(common_email_api_url, json=task, headers=headers)
	if resp.text == "Sent":
		print("********MAIL SENT********")
	else:
		print("********No***********")
		
	
	#Reset Control Table
	reset_control_table = """update dbo.control c set process_ind = false,process = 0 where process_ind=true ;"""
	db_opsdb_stars.execute(reset_control_table)
	
	reset_filewatch_table = """update dbo.filewatch_config f set unprocessed_filecount = 0 where pmd_client_id = """+str(var_pmd_client_id)+""";"""
	db_opsdb_starsrdsm.execute(reset_filewatch_table)

	db_opsdb_stars.close()
	db_opsdb_starswarehouse.close()
	db_opsdb_starsrdsm.close()
	db_opsdb_medispan.close()
	db_opsdb_operationaldatastore.close()
	db_opsdb_stars_billing.close()
	db_opsdb_archive.close()
	print('Finish')
else:
	#Terminate Package (No Evals)
	print('Terminate Package (No Evals)')
	db_opsdb_stars.close()
	db_opsdb_starswarehouse.close()
	db_opsdb_starsrdsm.close()
	db_opsdb_medispan.close()
	db_opsdb_operationaldatastore.close()
	db_opsdb_stars_billing.close()
	db_opsdb_archive.close()