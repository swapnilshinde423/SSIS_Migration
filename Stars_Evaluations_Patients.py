import psycopg2
import sys
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import boto3
from boto3 import client
from botocore.exceptions import ClientError
import json
from awsglue.utils import getResolvedOptions
from sqlalchemy import text 
args = getResolvedOptions(sys.argv,['PMD_CLIENT_ID'])
pmd_client_id=args['PMD_CLIENT_ID']
print(pmd_client_id)

flag52='false'
flag62='false'
flag76='false'
flag83='false'
flag69='false'
flag78='false'
flag77='false'
flag67='false'
flag_dnc69='false'
flag_dnc73='false'
flag_dnc30='false'
flag87='false'
flag96='false'
flag94='false'
Last_Import_Date='2010-06-10 08:34:33.603677+00'
args1 = getResolvedOptions(sys.argv,['RUN_YEAR'])
run_year=args1['RUN_YEAR']
print(run_year)

client = boto3.client("secretsmanager", region_name="us-east-2")

get_secret_value_response = client.get_secret_value(
				SecretId="SecretKeysForAWSGlue"
)

secret = get_secret_value_response['SecretString']
secret = json.loads(secret)

con_db_host = secret.get('con_db_host')
con_db_port = secret.get('con_db_port')
con_db_user = secret.get('con_db_user')
con_db_password=secret.get('con_db_password')
con_db_opsdb_stars_rdsm=secret.get('con_db_OPSDB_STARS_rdsm')
engine = create_engine('postgresql://'+con_db_user+':'+con_db_password+'@'+con_db_host+':'+con_db_port+'/'+con_db_opsdb_stars_rdsm)
conn_stars_rdsm=engine.connect()

con_db_opsdb_stars=secret.get('con_OPSDB_STARS')
engine2 = create_engine('postgresql://'+con_db_user+':'+con_db_password+'@'+con_db_host+':'+con_db_port+'/'+con_db_opsdb_stars)
con_db_opsdb_stars=engine2.connect()
conn1 = psycopg2.connect(database=con_db_opsdb_stars_rdsm, user=con_db_user,password=con_db_password, host=con_db_host, port= con_db_port)

####################Populate run data #########################################
populate_run_data=conn_stars_rdsm.execute("""insert into dbo.stars_patient_rundata(run_datetime,stars_patients,stars_patients_detail,pmd_client_id)select
now() , 'Not Complete','Not Complete',cast("""+str(pmd_client_id)+""" as int) as pmd_client_id""")
conn_stars_rdsm.execute("""truncate table staging.mtm_patients""")
######################Get Max Run ID########################################
max_run_id=conn_stars_rdsm.execute(""" select max(run_id) from dbo.stars_patient_rundata where pmd_client_id = cast("""+str(pmd_client_id)+""" as int) """).fetchall()
max_run_id=max_run_id[0][0]
print('@@@@@@@@@@@@@@@',str(max_run_id))
##############################Get Start_Date########################################
start_date=conn_stars_rdsm.execute(""" select max(run_datetime) as start_date from dbo.stars_patient_rundata """)
#start_date='2020-11-11 05:29:08.032976'
print(start_date)


#Container START- Stars_Patients

##################################stars patient######################################################################
########################Truncate Staging###################################
conn_stars_rdsm.execute("""truncate table staging.staging_stars_patients;
														truncate table staging.staging_stars_patients_updates;""")
														
###########################Get Stars Patient LastRunDate########################################
patient_lastrundate=conn_stars_rdsm.execute("""select coalesce(max(run_datetime), cast('1900-01-01' as timestamp)) as patient_lastrundatetime from dbo.stars_patient_rundata
where stars_patients = 'Success' and pmd_client_id = cast("""+str(pmd_client_id)+""" as int) """).fetchall()
patient_lastrundate=patient_lastrundate[0][0]
print(patient_lastrundate)
print('Get Stars Patient LastRunDate#')

#######################Stage Patients##############################
if pmd_client_id=='30':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_stars_patients_healthspring('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
		print("usp_stars_patients_healthspring")
if pmd_client_id=='15':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Scan('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
		print("usp_Stars_Patients_Scan#")
if pmd_client_id=='52':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_UHC('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='54':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Highmark('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()		
if pmd_client_id=='56':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Gateway('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()		
if pmd_client_id=='63':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_ABQHP('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()		
if pmd_client_id=='62':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Meridian('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='66':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Centene_QRS('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='67':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Centene_Medicaid('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='69':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Aspire('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='75':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_UHC_Nevada('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='76':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_SunshineHealth('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='77':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_HCSC('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='78':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_IVHP('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='81':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Emblem_Current('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='82':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_CareNCare('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='83':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_BCBSAZ('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='84':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_GoldenState('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='85':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_HAP('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='87':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_UHC_MR('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='88':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_UHC_NC('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='89':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Molina('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='90':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_UHC_VA('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='91':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_HCPNV('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='92':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_BlueKC('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='94':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_UHC_URS('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='95':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_UHC_PA('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()		
if pmd_client_id=='96':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_BrightHealth('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()

###########################################Reset ACTIVE flag for Emblem#######################################

if pmd_client_id=='81':
		cur1 = conn1.cursor()
		cur1.execute("""UPDATE dbo.Stars_Patients SET ACTIVE=0 WHERE PMD_Client_ID="""+pmd_client_id+""" """)
		conn1.commit()
		cur1.close()
#################################stars Patient merge###################################
cur2=conn1.cursor()
cur2.execute('call dbo.usp_starsmerge_patients(%s)',[max_run_id])
conn1.commit()
cur2.close()
#conn1.close()
print('stars Patient merge')

###############################job working on update patient load success flag ###########################################
conn_stars_rdsm.execute("""update dbo.stars_patient_rundata
				set stars_patients ='Success',stars_patients_complete_datetime =now() from dbo.stars_patient_rundata r where r.run_id = """+str(max_run_id)+""" """)
conn_stars_rdsm.execute("""update dbo.stars_patient_rundata set patients_imported = (select count(*) from dbo.stars_patients where run_id = """+str(max_run_id)+""") from dbo.stars_patient_rundata p  where p.run_id = """+str(max_run_id)+""" """)

#Container End- Stars_Patients


#Container stART- Stars_Patients_Detail

###############################now working on stars patients details ######################################
################################Truncate Staging##################################
conn_stars_rdsm.execute(""" truncate table staging.staging_stars_patients_detail;
truncate table staging.staging_stars_patients_phone_override; """)
##############################Get Stars Details LastRunDate#####################################
detail_lastrundatetime=conn_stars_rdsm.execute("""select coalesce(max(run_datetime), cast('1900-01-01' as timestamp))  as detail_lastrundatetime from dbo.stars_patient_rundata
where stars_patients_detail = 'Success' and pmd_client_id = cast("""+str(pmd_client_id)+""" as int) """).fetchall()
detail_lastrundatetime=detail_lastrundatetime[0][0]
print('##########',detail_lastrundatetime)
##############################Stage Patient Details#########################
if pmd_client_id=='30':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_Healthspring('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
		print("usp_Stars_Patients_Detail_Healthspring")
if pmd_client_id=='15':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_Scan('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
		print("usp_Stars_Patients_Detail_Scan#")
if pmd_client_id=='52':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_UHC('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='54':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_Highmark('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()		
if pmd_client_id=='56':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_Gateway('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()		
if pmd_client_id=='62':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_Meridian('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='63':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_ABQHP('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='66':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_Centene_QRS('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='67':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_Centene_Medicaid('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='69':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_Aspire('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='75':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_UHC_Nevada('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='76':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_SunshineHealth('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='77':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_HCSC('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='78':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_IVHP('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='81':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_Emblem_Current('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='82':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_CareNCare('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='83':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_BCBSAZ('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='84':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_GoldenState('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='85':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_HAP('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='87':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_UHC_MR('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='88':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_UHC_NC('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='89':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_Molina('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='90':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_UHC_VA('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='91':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_HCPNV('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='92':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_BlueKC('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='94':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_UHC_URS('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
if pmd_client_id=='95':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_UHC_PA('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()		
if pmd_client_id=='96':
		cur1 = conn1.cursor()
		sp_call="CALL dbo.usp_Stars_Patients_Detail_BrightHealth('"+str(patient_lastrundate)+"'::TIMESTAMP WITHOUT TIME ZONE)"
		cur1.execute(sp_call)
		conn1.commit()
		cur1.close()
		
####################Apply override to staging #####################################
conn_stars_rdsm.execute(""" --update phone numbers in staging with verified phone numbers in starsconnect for each patient in starsconnect that 
--has a verified phone number that is the primary number. by default, phone numbers
--from the eligibility file that do not have a corresponding patient that has a verified phone number in starsconnect
--will be allowed to pass through to dbo.stars_patients_detail, where they will be used by the call list for that patient.

update staging.staging_stars_patients_detail
set phone_number = ov.phone_number
from  staging.staging_stars_patients_phone_override ov
where  staging_stars_patients_detail.patient_unique_ident = ov.pmd_patient_id""")

###################################### new and updated block ##############################################
conn_stars_rdsm.execute("""drop table if exists staging.staging_stars_patients_detail_new_or_updated;
select 
				distinct spd.patient_unique_ident
				,spd.patient_unique_key
				,spd.pbp_pclm
				,spd.address
				,spd.address_2
				,spd.city
				,spd.state
				,spd.zip_code
				,spd.phone_number        
				,cast(case when date_part('year', now()::date)-date_part('year', p.dob::date) <= 10 then '0 - tens' 
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 11 and 19 then 'teens' 
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 20 and 29 then 'twenties' 
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 30 and 39 then 'thirties' 
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 40 and 49 then 'forties' 
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 50 and 59 then 'fifties' 
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 60 and 69 then 'sixties' 
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 70 and 79 then 'seventies' 
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 80 and 89 then 'eighties' 
				when date_part('year', now()::date)-date_part('year', p.dob::date)  >= 90 
				then 'ninety_andabove' else 'nodob' end as varchar(20)) as patient_ageband
				,case when date_part('year', now()::date)-date_part('year', p.dob::date) <= 10 then 0 
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 11 and 19 then 1
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 20 and 29 then 2
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 30 and 39 then 3
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 40 and 49 then 4
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 50 and 59 then 5
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 60 and 69 then 6
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 70 and 79 then 7
				when date_part('year', now()::date)-date_part('year', p.dob::date)  between 80 and 89 then 8
				when date_part('year', now()::date)-date_part('year', p.dob::date)  >= 90 
				then 9 else '999' end as patient_agebandsort
				,spd.lis_flag
				,spd.esrd
				,spd.supc_flag
into staging.staging_stars_patients_detail_new_or_updated
from staging.staging_stars_patients_detail spd 
inner join dbo.stars_patients p on spd.patient_unique_ident = p.patient_unique_ident
	
where not exists (select 1 from dbo.stars_patients_detail pd 
												where spd.patient_unique_ident = pd.patient_unique_ident
												and spd.address = pd.address
												and coalesce(spd.address_2,'x') = coalesce(pd.address_2,'x')
												and coalesce(spd.city,'x') = coalesce(pd.city,'x')
												and coalesce(spd.state,'x') = coalesce(pd.state,'x')
												and coalesce(spd.zip_code,'x') = coalesce(pd.zip_code,'x')
												and coalesce(spd.phone_number,'x') = coalesce(pd.phone_number,'x')                
												and coalesce(spd.pbp_pclm,'x') = coalesce(pd.pbp_pclm,'x')
												and
												case when date_part('year', now()::date)-date_part('year', p.dob::date) <=10 then 0 
														 when date_part('year', now()::date)-date_part('year', p.dob::date)  between 11 and 19 then 1
														 when date_part('year', now()::date)-date_part('year', p.dob::date)  between 20 and 29 then 2
														 when date_part('year', now()::date)-date_part('year', p.dob::date)  between 30 and 39 then 3
														 when date_part('year', now()::date)-date_part('year', p.dob::date)  between 40 and 49 then 4
														 when date_part('year', now()::date)-date_part('year', p.dob::date)  between 50 and 59 then 5
														 when date_part('year', now()::date)-date_part('year', p.dob::date)  between 60 and 69 then 6
														 when date_part('year', now()::date)-date_part('year', p.dob::date)  between 70 and 79 then 7
														 when date_part('year', now()::date)-date_part('year', p.dob::date)  between 80 and 89 then 8
														 when date_part('year', now()::date)-date_part('year', p.dob::date)  >= 90 
														then 9 else '999' end = pd.patient_agebandsort                        
												and coalesce(spd.lis_flag,false) = coalesce(pd.lis_flag,false)
												and coalesce(spd.esrd,false) = coalesce(pd.esrd,false)
												and spd.supc_flag = pd.supc_flag
												and pd.end_date is null    
										)
order by patient_unique_ident; """)



#########################Merge SCD2 Changes##############################################################

var_sql = """ do $$ 
declare par_currentdate timestamp:=cast(cast(now() as date) as timestamp);
declare par_firstrecordstartdate timestamp = cast(cast('2013-01-01' as date) as timestamp);
begin

 
insert into dbo.stars_patients_detail(patient_unique_ident ,patient_unique_key ,address ,address_2 ,city ,state ,zip_code ,phone_number ,start_date ,end_date ,patient_ageband ,patient_agebandsort 
,pbp_pclm,run_id,lis_flag,esrd,supc_flag,   create_datetime,update_datetime)
select st.patient_unique_ident ,st.patient_unique_key ,st.address ,st.address_2 ,st.city ,st.state ,st.zip_code ,st.phone_number ,par_firstrecordstartdate ,null ,st.patient_ageband,st.patient_agebandsort,st.pbp_pclm,"""+str(max_run_id)+""",st.lis_flag,st.esrd,st.supc_flag,par_currentdate,par_currentdate 
from (select patient_unique_ident
			,patient_unique_key
			,pbp_pclm
			,address
			,address_2
			,city
			,state
			,zip_code
			,max(phone_number) as phone_number
			,patient_ageband
			,patient_agebandsort
			,lis_flag
			,esrd
			,supc_flag
	from  staging.staging_stars_patients_detail_new_or_updated
	group by 
	patient_unique_ident
			,patient_unique_key
			,pbp_pclm
			,address
			,address_2
			,city
			,state
			,zip_code
			,patient_ageband
			,patient_agebandsort
			,lis_flag
			,esrd
			,supc_flag) st
			left join dbo.stars_patients_detail tt on (tt.patient_unique_ident = st.patient_unique_ident) where tt.patient_unique_ident  is null;

update dbo.stars_patients_detail tt 
set end_date = par_currentdate
from (select patient_unique_ident
			,patient_unique_key
			,pbp_pclm
			,address
			,address_2
			,city
			,state
			,zip_code
			,max(phone_number) as phone_number
			,patient_ageband
			,patient_agebandsort
			,lis_flag
			,esrd
			,supc_flag
	from  staging.staging_stars_patients_detail_new_or_updated
	group by 
	patient_unique_ident
			,patient_unique_key
			,pbp_pclm
			,address
			,address_2
			,city
			,state
			,zip_code
			,patient_ageband
			,patient_agebandsort
			,lis_flag
			,esrd
			,supc_flag) st
where (tt.patient_unique_ident = st.patient_unique_ident) and tt.end_date  is null;


 
-- this inserts another record to the dimension for scd type 2 changes
insert into dbo.stars_patients_detail
				( 
				patient_unique_ident ,
				patient_unique_key ,
				address ,
				address_2 ,
				city ,
				state ,
				zip_code ,
				phone_number ,
				start_date ,
				end_date ,
				patient_ageband ,
				patient_agebandsort ,
				pbp_pclm,
				run_id,
				lis_flag,
				esrd,
				supc_flag,
				create_datetime,
				update_datetime
				)
select 
			st.patient_unique_ident ,
			st.patient_unique_key ,
			st.address ,
			st.address_2 ,
			st.city ,
			st.state ,
			st.zip_code ,
			case when  (st.phone_number like '999%') or (st.phone_number like '111%')  or (st.phone_number like '222%')  or (st.phone_number like '333%')  or 
				(st.phone_number like '444%')  or (st.phone_number like '555%')  or (st.phone_number like '666%')  or (st.phone_number like '777%')  or (st.phone_number like '888%')  or (st.phone_number like '123%')  or
				(st.phone_number like '000%') or (st.phone_number is null) or (length(trim(st.phone_number)) < 10) then 
				(select b.phone_number from dbo.stars_patients_detail b where b.patient_unique_ident=st.patient_unique_ident and b.end_date=par_currentdate limit 1)
				else st.phone_number 
				end as phone_number,
				par_currentdate as start_date ,
				null as end_date ,
				st.patient_ageband ,
				st.patient_agebandsort , 
				st.pbp_pclm,
				"""+str(max_run_id)+""" as run_id,
				st.lis_flag,
				st.esrd,
				st.supc_flag,
				par_currentdate as create_datetime,
				par_currentdate as update_datetime
from staging.staging_stars_patients_detail_new_or_updated st
where exists (select * from dbo.stars_patients_detail pd where pd.patient_unique_ident=st.patient_unique_ident and pd.end_date is not null);

 

end $$; """
conn_stars_rdsm.execute(text(var_sql).execution_options(autocommit=True))
 
print("Merge SCD2 Change#")
######################################################Update MTMP All clients#############################################################

Last_Membership_Date="""select COALESCE(max(cast(Update_Datetime as date)),'01/01/1900') from dbo.UHC_MTMP_Current"""
Last_Membership_Date=conn_stars_rdsm.execute(Last_Membership_Date).fetchall()
Last_Membership_Date=Last_Membership_Date[0][0]
Last_Membership_Date=str(Last_Membership_Date)
print(pmd_client_id)
#print(Last_Membership_Date)
# reset and Set a mtmp_flag UHC(52)
if pmd_client_id=='52' and Last_Membership_Date >=Last_Import_Date:
    #print("start")
    conn_stars_rdsm.execute("""update dbo.Stars_Patients_Detail pd
	Set mtmp_flg = false
	from dbo.Stars_Patients p
	where p.Patient_Unique_Ident = pd.Patient_Unique_Ident and pd.End_Date is null
	AND p.PMD_Client_ID ="""+pmd_client_id+""" """)
    #print("demo")
    conn_stars_rdsm.execute("""Update dbo.Stars_Patients_Detail pd
    Set mtmp_flg = true
    from dbo.Stars_Patients p
    where  p.Patient_Unique_Ident = pd.Patient_Unique_Ident and pd.End_Date is null
    AND p.PMD_Client_ID ="""+pmd_client_id+"""
    and p.Patient_ID in(select m.MemberId from dbo.UHC_MTMP_Current m where cast(m.Update_Datetime as date) ='"""+Last_Membership_Date+"""')""")
    #print("second")
    
Last_Membership_Date_Meridian_62="""select COALESCE(max(cast(Update_Datetime as date)),'01/01/1900') from dbo.Meridian_62_Membership"""
Last_Membership_Date_Meridian_62=conn_stars_rdsm.execute(Last_Membership_Date_Meridian_62).fetchall()
Last_Membership_Date_Meridian_62=Last_Membership_Date_Meridian_62[0][0]
Last_Membership_Date_Meridian_62=str(Last_Membership_Date_Meridian_62)
#print(Last_Membership_Date_Meridian_62)
# reset and Set a mtmp_flag Meridian_62
if pmd_client_id=='62' and Last_Membership_Date_Meridian_62 >=Last_Import_Date:
    conn_stars_rdsm.execute("""update dbo.Stars_Patients_Detail pd
	Set mtmp_flg = false
	from dbo.Stars_Patients p 
	where p.Patient_Unique_Ident = pd.Patient_Unique_Ident and pd.End_Date is null
	AND p.PMD_Client_ID ="""+pmd_client_id+""" """)
    #print("Meridian Start")
    conn_stars_rdsm.execute("""Update dbo.Stars_Patients_Detail pd
    Set mtmp_flg =true
    from dbo.Stars_Patients p
    WHERE p.Patient_Unique_Ident = pd.Patient_Unique_Ident and pd.End_Date is null
    AND p.PMD_Client_ID ="""+pmd_client_id+"""
    and p.Patient_ID in(select patientID from dbo.Meridian_62_Membership m
    where cast(m.Update_Datetime as date) ='"""+Last_Membership_Date_Meridian_62+"""'
    and client_def_1 = 'Y')""")
    #print("meredian end")

Last_Membership_Date_UHCMRCTX_MTM="""select COALESCE(max(cast(File_Date as date)),'01/01/1900') from dbo.UHCMRCTX_MTM"""
Last_Membership_Date_UHCMRCTX_MTM=conn_stars_rdsm.execute(Last_Membership_Date_UHCMRCTX_MTM).fetchall()
Last_Membership_Date_UHCMRCTX_MTM=Last_Membership_Date_UHCMRCTX_MTM[0][0]
Last_Membership_Date_UHCMRCTX_MTM=str(Last_Membership_Date_UHCMRCTX_MTM)
#print(Last_Membership_Date_UHCMRCTX_MTM)
# reset and Set a mtmp_flag UHCMRCTX(73)
if pmd_client_id=='73' and Last_Membership_Date_UHCMRCTX_MTM >=Last_Import_Date:
    conn_stars_rdsm.execute("""update dbo.Stars_Patients_Detail pd
    Set mtmp_flg =false
    from dbo.Stars_Patients p
    where p.Patient_Unique_Ident = pd.Patient_Unique_Ident and pd.End_Date is null
    AND p.PMD_Client_ID ="""+pmd_client_id+""" """)
    #print("start MTM")
    conn_stars_rdsm.execute("""Update dbo.Stars_Patients_Detail pd
    Set mtmp_flg =true
    from dbo.Stars_Patients p
    where p.Patient_Unique_Ident = pd.Patient_Unique_Ident and pd.End_Date is null
    AND p.PMD_Client_ID ="""+pmd_client_id+"""
    and p.Patient_ID in(select m.HICNumber from dbo.UHCMRCTX_MTM m
    where cast(m.File_Date as date) ='"""+Last_Membership_Date_UHCMRCTX_MTM+"""')""")
    #print("end MTM")
    
Last_Membership_Date_IVHP="""select COALESCE(max(cast(Update_Datetime as date)),'01/01/1900') as Update_Datetime  from dbo.IVHP_Membership"""
Last_Membership_Date_IVHP=conn_stars_rdsm.execute(Last_Membership_Date_IVHP).fetchall()
Last_Membership_Date_IVHP=Last_Membership_Date_IVHP[0][0]
Last_Membership_Date_IVHP=str(Last_Membership_Date_IVHP)
print(Last_Membership_Date_IVHP)
# reset and Set a mtmp_flag IVHP(78)
if pmd_client_id=='78' and Last_Membership_Date_IVHP >=Last_Import_Date:
    conn_stars_rdsm.execute("""update dbo.Stars_Patients_Detail pd
    Set mtmp_flg = false
    from dbo.Stars_Patients p
    WHERE p.Patient_Unique_Ident = pd.Patient_Unique_Ident and pd.End_Date is null
    AND p.PMD_Client_ID ="""+pmd_client_id+""" """)
    #print("start IVHP")
    conn_stars_rdsm.execute("""Update dbo.Stars_Patients_Detail pd
    Set mtmp_flg = true
    from dbo.Stars_Patients p
    WHERE p.Patient_Unique_Ident = pd.Patient_Unique_Ident and pd.End_Date is null
    AND p.PMD_Client_ID ="""+pmd_client_id+"""
    and p.Patient_ID in
    (select patientID from dbo.IVHP_Membership m
    where cast(m.Update_Datetime as date) = '"""+Last_Membership_Date_IVHP+"""'
    and MTMEligible = '1')""")
    #print("end IVHP")

################################# to update latest detail record ###########################################
#Reset flag as NULL for LIS_Flag and LIS_discount

conn_stars_rdsm.execute("""Update dbo.Stars_Patients_Detail pd
	set LIS_Flag = NULL
		,LIS_discount=NULL
	From dbo.Stars_Patients p
	where  p.patient_unique_ident = pd.patient_unique_ident
	and pd.end_date is null 
	and  p.PMD_Client_ID =15""")
	
conn_stars_rdsm.execute("""Update dbo.Stars_Patients_Detail pd
	set   LIS_Flag =sb.LIS_Flag
		, LIS_discount=sb.Discount_Status
	From dbo.Stars_Patients p,dbo.Scan_Benefits sb
    where  p.patient_unique_ident = pd.patient_unique_ident
	and pd.end_date is null  --Latest detail record
	AND sb.pmd_patient_Id = p.patient_unique_ident
    AND p.PMD_Client_ID =15 and sb.RunYear ="""+run_year+""" """)

conn_stars_rdsm.execute(""" 
--update the planidentifier_pclm for the client that is being loaded

	update dbo.stars_patients_detail
set planidentifier_pclm = pp.planidentifier_pclm

	from dbo.stars_patients_plan pp
 --get the record associated with the last known plan period
	inner join (select max(eligibilitystartdate) as eligibilitystartdate, patient_unique_ident from dbo.stars_patients_plan where pmd_client_id = """+str(pmd_client_id)+""" and runyear = """+str(run_year)+""" group by patient_unique_ident) pm
	on pp.patient_unique_ident = pm.patient_unique_ident
	and pp.eligibilitystartdate = pm.eligibilitystartdate
	
where pp.patient_unique_ident=stars_patients_detail.patient_unique_ident
	and stars_patients_detail.end_date is null	
	and pp.pmd_client_id="""+str(pmd_client_id)+""" and pp.runyear="""+str(run_year)+"""; """)
	
##########     HAP_ESRD   #######
conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd
			SET ESRD=true
			FROM dbo.Stars_Patients p,dbo.HAP_ESRD_Exclusions sb
			where p.patient_unique_ident = pd.patient_unique_ident
			AND sb.Hap_id = p.patient_id
			AND p.PMD_Client_ID =85 
			AND pd.End_Date IS NULL""")
			
			
conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd 
			SET Hospice =true
			FROM dbo.Stars_Patients p,dbo.HAP_Hospice_Exclusions sb
			where p.patient_unique_ident = pd.patient_unique_ident
			AND sb.Hap_id = p.patient_id
			AND p.PMD_Client_ID =85 
			AND pd.End_Date IS NULL""")

###reset the MTMP_Flag
conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd
	SET mtmp_flg = false
	FROM dbo.Stars_Patients p
	where  p.Patient_Unique_Ident= pd.Patient_Unique_Ident and pd.End_Date is null
	AND p.PMD_Client_ID = 85""")

### Set the MTMP_Flag as per the current file
conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd
	SET  MTMP_Flg ='true'
	FROM dbo.Stars_Patients p,dbo.HAP_MTM m
	where p.patient_unique_ident = pd.patient_unique_ident
	AND cast (m.PMD_Patient_ID as character varying) = cast (p.Patient_Unique_Ident as character varying)
	AND p.PMD_Client_ID =85 and pd.End_Date is null""")	

########################################################## Update SSH_PhoneNumber ################
conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd
SET Phone_Number=s.UpdatedNumber
FROM dbo.SunShineHealth_PhoneNumbers s ,dbo.Stars_Patients p
where  cast (s.PatientID as character varying) = cast (pd.Patient_Unique_Ident as character varying)
AND pd.patient_unique_ident = p.patient_unique_ident 
AND PMD_Client_ID=76 and pd.End_Date is NULL""")	

##########################################################Update Asthma_statin_SUPD Flag########################

if pmd_client_id=='95':
    conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd SET  Statin_Flag = false FROM dbo.Stars_Patients p,staging.UHC_PA_Membership_StatinSPD a where  p.patient_unique_ident = pd.patient_unique_ident AND  a.Patient_ID = p.Patient_ID AND p.PMD_Client_ID =95 and pd.End_Date is NULL""")
    print("one")
    conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd
		                            SET Statin_Flag = true
		                            FROM dbo.Stars_Patients p,staging.UHC_PA_Membership_Statin a
		                            where  p.patient_unique_ident = pd.patient_unique_ident
		                            AND a.Patient_ID = p.Patient_ID
		                            AND p.PMD_Client_ID =95 and pd.End_Date is NULL""")
    print("two")
    conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd
		                            SET  Asthma_Flag = true
		                            FROM dbo.Stars_Patients p,staging.UHC_PA_Membership_Asthma a
	                             	where  p.patient_unique_ident = pd.patient_unique_ident
		                            AND a.Patient_ID = p.Patient_ID
	                             	AND p.PMD_Client_ID =95 and pd.End_Date is NULL""")
if pmd_client_id=='94':									
	conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd
                                    SET  SUPD_Flag = CASE WHEN M.Client_Def_3='1'  THEN 1
                                    WHEN M.Client_Def_3='2' THEN 2
                                    ELSE 0
                                    END 
                                    FROM dbo.Stars_Patients p,staging.UHC_URS_Membership M
                                    where  p.patient_unique_ident = pd.patient_unique_ident
                                    and  M.Patient_ID = p.Patient_ID
                                    and  p.PMD_Client_ID =94 and pd.End_Date is null""")
									
if pmd_client_id=='87':									
	conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd
                                    SET  SUPD_Flag= CASE WHEN M.Client_Def_3='1'  THEN 1
                                    WHEN M.Client_Def_3='2'  THEN 2
                                    ELSE 0
                                    END 
                                    FROM dbo.Stars_Patients p,staging.UHC_MR_Membership_History M
                                    where  p.patient_unique_ident = pd.patient_unique_ident
                                    and  M.Patient_ID = p.Patient_ID
                                    and  p.PMD_Client_ID =87 and pd.End_Date is null""")
                                    
if pmd_client_id=='78':									
	conn_stars_rdsm.execute("""UPDATE dbo.Stars_Patients_Detail pd
    SET  SUPD_Flag=1
    FROM dbo.Stars_Patients p,dbo.IVHP_SUPD_Membership M
    where  p.patient_unique_ident = pd.patient_unique_ident
    AND  M."Mem Nbr" = p.Patient_ID
    AND p.PMD_Client_ID =78 and pd.End_Date is null""")

######  Truncate Staging_MR ########################

conn_stars_rdsm.execute("""truncate table staging.UHC_MR_Membership_History""")
                                    
####################################Update Patient Detail Load Success Flag#################

conn_stars_rdsm.execute("""update dbo.stars_patient_rundata
set stars_patients_detail ='Success'
,stars_patients_detail_complete_datetime=now()
--from dbo.stars_patient_rundata r
where stars_patient_rundata.run_id = """+str(max_run_id)+""";

update dbo.stars_patient_rundata
set patients_detail_imported = (select count(*) from dbo.stars_patients_detail where run_id = """+str(max_run_id)+""")
--from dbo.stars_patient_rundata p 
where stars_patient_rundata.run_id = """+str(max_run_id)+"""; """)

print("Container end Stars_Patients_Detail")

########################################stars patient plan block###################################
#########################################Truncate staging table#######################
conn_stars_rdsm.execute(""" truncate table staging.staging_stars_patients_plan""")
#########################Stage Patients Plan###################################
if pmd_client_id=='30':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_stars_patients_plan_healthspring(%s)',[run_year])
    conn1.commit()
    cur4.close()
    print("Hello")
if pmd_client_id=='15':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_Scan(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='52':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_UHC(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='54':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_Highmark(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='56':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_Gateway(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='62':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_Meridian(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='63':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_ABQHP(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='66':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_Centene_QRS(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='67':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_Centene_Medicaid(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='69':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_Aspire(%s)',[run_year])
    conn1.commit()
    cur4.close()
    print("Hello")
if pmd_client_id=='75':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_UHC_Nevada(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='76':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_SunshineHealth(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='77':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_HCSC(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='78':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_IVHP(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='81':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_Emblem_Current(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='82':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_CareNCare(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='83':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_BCBSAZ(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='84':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_GoldenState(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='85':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_HAP(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='87':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_UHC_MR(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='88':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_UHC_NC(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='89':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_Molina(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='90':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_UHC_VA(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='91':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_HCPNV(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='92':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_BlueKC(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='94':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_UHC_URS(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='95':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_UHC_PA(%s)',[run_year])
    conn1.commit()
    cur4.close()
if pmd_client_id=='96':
    cur4=conn1.cursor()
    cur4.execute('call dbo.usp_Stars_Patients_Plan_BrightHealth(%s)',[run_year])
    conn1.commit()
    cur4.close()

    
#########################Update Stars_Patients_Plan#################################

cur5=conn1.cursor()
cur5.execute('call dbo.uspstarspatientsplan_merge(%s)',[run_year])
conn1.commit()
cur5.close()
print("HelloAAA")

#################################EXEC Patient Active & Inactive###############################################
patient_active_inactive=("""do $$
declare par_run_id integer=(select max(run_id) from dbo.stars_patient_rundata where pmd_client_id ="""+str(pmd_client_id)+""");
declare par_patients_imported integer;

begin 
select patients_imported into par_patients_imported  from dbo.stars_patient_rundata rd where run_id = par_run_id;
if par_patients_imported>0
then
call dbo.usp_repair_stars_patient_plan_periods("""+str(pmd_client_id)+""","""+str(run_year)+""");
call dbo.usp_stars_patients_plan_active("""+str(pmd_client_id)+""","""+str(run_year)+""");
call  dbo.usp_stars_patients_plan_inactive("""+str(pmd_client_id)+""","""+str(run_year)+""");
end if;
end $$; """)
#cur6=conn1.cursor()
#cur6.execute(patient_active_inactive)
conn_stars_rdsm.execute(text(patient_active_inactive).execution_options(autocommit=True))
#conn1.commit()
#cur6.close()
print("HelloBB")

#############################Set active in active patients ####################

set_active_inactive_patients=("""do $$
begin
                if ("""+str(run_year)+""" = extract(year from now())) then 

                
                --inactivate patients all patients
                update dbo.stars_patients
                set active = false
                --from dbo.stars_patients p 
                where stars_patients.pmd_client_id = """+str(pmd_client_id)+""";


                --activate patients that have a current eligibility period in stars_patients_plan
                update dbo.stars_patients set 
                                active = True
                                ,eligibilityyear = extract (year from now())
                --from dbo.stars_patients p 
                where exists (select 1 from dbo. stars_patients_plan pp 
                                                                                where pp.runyear="""+str(run_year)+"""
                                                                                and stars_patients.patient_unique_ident = pp.patient_unique_ident 
                                                                                and cast(now() as date) between pp.eligibilitystartdate and pp.eligibilityenddate 
                                                                                and extract (year from eligibilityenddate ) = extract (year from now()) )
                and stars_patients.pmd_client_id = """+str(pmd_client_id)+""";




                --set eligibility year of patients that have an eligibility period in stars_patients_plan in the current year.
                update dbo.stars_patients
                set eligibilityyear = """+str(run_year)+"""
                --from dbo.stars_patients p 
                 where exists (select 1 from dbo. stars_patients_plan pp 
                                                                                                where pp.runyear = """+str(run_year)+""" 
                                                                                                and stars_patients.patient_unique_ident = pp.patient_unique_ident  
                                                                                                and extract (year from eligibilityenddate) = """+str(run_year)+""")
                  and stars_patients.pmd_client_id = """+str(pmd_client_id)+""" ;

end if;
end $$; """)
#conn_stars_rdsm.execute(set_active_inactive_patients)
conn_stars_rdsm.execute(text(set_active_inactive_patients).execution_options(autocommit=True))
####################################set_is_deleted_patient################################
is_deleted_patient="""--set is_deleted stars_patients if they have an eligibility period less than prior year or current year
update dbo.stars_patients
set is_deleted = true

--from dbo.stars_patients p
where  stars_patients.pmd_client_id = """+str(pmd_client_id)+"""
and stars_patients.eligibilityyear < extract(year from now())-1 or stars_patients.eligibilityyear is null;


--set is_deleted stars_patients_detail
update dbo.stars_patients_detail
set is_deleted = true

from dbo.stars_patients p
--inner join dbo.stars_patients_detail pd
where  p.pmd_client_id = """+str(pmd_client_id)+"""
and p.eligibilityyear < extract(year from now())-1 or p.eligibilityyear is null and p.patient_unique_ident = stars_patients_detail.patient_unique_ident;


--set is_deleted stars_patients to an undeleted state if they have an eligibility period in the prior year or current year
update dbo.stars_patients
set is_deleted = false 
--from dbo.stars_patients p
where  stars_patients.pmd_client_id = """+str(pmd_client_id)+"""
and stars_patients.eligibilityyear >= extract(year from now())-1 ; """
conn_stars_rdsm.execute(is_deleted_patient)

#############################Special eligibility Centene_QRS#############################################
special_eligibility_centene_qrs= (""" ----special eligibility centene_qrs


do $$

begin
-- centene check if they have any gaps in their eligibilty then they will be out of call list
if ("""+str(run_year)+""" = date_part('year',now()) and """+str(pmd_client_id)+""" = 66)



then

  -- check if they have any gaps in eligibilty periods
    create temp table eligibitygappatients as 
    select toteli.patient_unique_ident
    from
    (
        select patient_unique_ident, (max(eligibilityenddate)::timestamp + interval '1 day') - min(eligibilitystartdate::timestamp)  as totalelidays
        from dbo.stars_patients_plan
        where runyear = """+str(run_year)+""" and pmd_client_id = """+str(pmd_client_id)+"""    and date_part('year',eligibilityenddate) = date_part('year',now())
        group by patient_unique_ident
    ) toteli
    inner join 
    (
        select patient_unique_ident, sum((eligibilityenddate::timestamp + interval '1 day')::timestamp - eligibilitystartdate::timestamp) as sumofeliperiod
        from dbo.stars_patients_plan 
        where runyear = """+str(run_year)+""" and pmd_client_id = """+str(pmd_client_id)+"""    and date_part('year',eligibilityenddate) = date_part('year',now())
        group by patient_unique_ident
    ) sumeli on (toteli.patient_unique_ident = sumeli.patient_unique_ident and toteli.totalelidays > sumeli.sumofeliperiod);



    --in activate patients who has gaps in eligibility period in stars_patients_plan
    update dbo.stars_patients p  set 
    active = false
    where p.pmd_client_id = """+str(pmd_client_id)+"""
    and p.patient_unique_ident in (select patient_unique_ident from eligibitygappatients);



    --in activate patients who age to < 18, is not between 18 and 64
    update dbo.stars_patients p set 
    active = false
    where p.pmd_client_id = """+str(pmd_client_id)+"""
    -- anyone 18+ can qualify in the measure 
     and (date_part('year', age(now(), dob)) -    case when now()::date::timestamp < dob + interval '1 year' * date_part('year', age(now(), dob)) then 1 else 0 end) < 18;



end if;
end$$; """)

#conn_stars_rdsm.execute(special_eligibility_centene_qrs)
conn_stars_rdsm.execute(text(special_eligibility_centene_qrs).execution_options(autocommit=True))
#############################Special eligibility UHC_PA ##############################



special_eligibility_uhc_pa=("""do $$
begin
-- centene check if they have any gaps in their eligibilty then they will be out of call list
if ("""+str(run_year)+""" = date_part('year',now()) and """+str(pmd_client_id)+""" = 95)



then
--in activate patients who age to < 5, is not between 5 and 64
   update     dbo.stars_patients p  set 
        active = false
    where p.pmd_client_id = """+str(pmd_client_id)+"""
    -- anyone 5 to 64  can qualify in the measure 
     and ((date_part('year', now()::date)-date_part('year', dob::date)) -    case when (now()::date::timestamp) < (dob + interval '1 year' * (date_part('year', now()::date)-date_part('year', dob::date))) then 1 else 0 end) not between  5 and 64
     ;
end if;
end$$; """)

#conn_stars_rdsm.execute(special_eligibility_uhc_pa)
conn_stars_rdsm.execute(text(special_eligibility_uhc_pa).execution_options(autocommit=True))

###############################starpatient plan PBP###########################################
###################################truncate staging table PBP#################################

conn_stars_rdsm.execute("""truncate table staging.staging_stars_patients_plan_pbp""")

####################################UHC to PBP Staging #########################################
if pmd_client_id=='52':
    flag52='true'
    print("yoyo")
    uhc_membership_query=("""do $$
    begin
    drop table if exists t1;
    create temp table t1 as
    select 
                pmd_client_id
                ,patient_unique_ident
                ,planidentifier_pclm
                ,pbp_pclm
                --eligibilitystartdates that have a year prior to the runyear where the tranformed eligibilityenddate has a year equal to the runyear 
                --must be reset to the beginning of the runyear or else this will have an adverse effect on running evals
                --for prior years if the elig end date is accidentally reset to the end of current year (interfering with the 91 day rule)
                ,case when date_part('year',eligibilitystartdate) < """+str(run_year)+""" and date_part('year',(case when date_part('year',eligibilityenddate) > """+str(run_year)+""" 
                                  then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else eligibilityenddate end)) = """+str(run_year)+"""     
                                  then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-01-01') as date) else eligibilitystartdate end as eligibilitystartdate
                --eligibilityenddates that go beyond end of year must be reset to the end of the plan year of the current year.
                ,case when date_part('year',eligibilityenddate) > """+str(run_year)+""" then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else eligibilityenddate end  eligibilityenddate
                ,runyear
    from 
    (
    select 
                52 pmd_client_id
                ,'h0251' planidentifier_pclm                                       
                ,pat.patient_unique_ident
                ,account_code pbp_pclm
                ,uhc.mbr_plan_eff_dt as eligibilitystartdate                                                                                            
                ,max(uhc.dsenrl_dt) as eligibilityenddate
                ,runyear
    from dbo.uhc_membership_current uhc
    inner join dbo.stars_patients pat
    on uhc.unique_patient_id=pat.patient_id
    and pat.pmd_client_id =52
    where runyear = """+str(run_year)+""" and length(uhc.account_code) >0
    group by patient_unique_ident
                ,mbr_plan_eff_dt
                ,runyear
                ,account_code
    ) e;
    end$$; """)
    conn_stars_rdsm.execute(text(uhc_membership_query).execution_options(autocommit=True))
    df_uhc_membership_query=pd.read_sql_query("""select * from public.t1;""",con=conn_stars_rdsm)
    df_uhc_membership_query.to_sql('staging_stars_patients_plan_pbp',con=conn_stars_rdsm,schema='staging',if_exists='append',index=False)
####################meridian_62_staging_query#############################
if pmd_client_id=='62':
    flag62='true'
    meridian_62_staging_query=("""do $$
    begin
    drop table if exists t1;
    create table t1 as
    select                distinct 
                 pmd_client_id
                ,patient_unique_ident
                ,left(planidentifier_pclm,5) as planidentifier_pclm
                ,right(planidentifier_pclm,3) as pbp_pclm
                --eligibilitystartdates that have a year prior to the runyear where the tranformed eligibilityenddate has a year equal to the runyear must be reset to the beginning of 
                --the runyear or else this will have an adverse effect on running evals
                --for prior years if the elig end date is accidentally reset to the end of current year (interfering with the 91 day rule)
                ,case when date_part('year',eligibilitystartdate) < """+str(run_year)+""" 
                                  and date_part('year',(case when date_part('year',eligibilityenddate) > """+str(run_year)+""" then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else eligibilityenddate end)) = """+str(run_year)+"""    
                                  then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-01-01') as date) else eligibilitystartdate end as eligibilitystartdate
                --eligibilityenddates that go beyond end of year must be reset to the end of the plan year of the current year.
                ,case when date_part('year',eligibilityenddate) > """+str(run_year)+""" then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else eligibilityenddate end  eligibilityenddate
                ,runyear
    from 
    (select 
                                 62 as pmd_client_id
                                ,plan_code as planidentifier_pclm
                                ,patient_unique_ident
                                ,case when sqlserver_ext.isdate(mem.patient_effdate)= 0 then cast('1900-01-01' as date) else cast(mem.patient_effdate as date) end as eligibilitystartdate                                                                                          
                                ,max(case when sqlserver_ext.isdate(mem.prgm_term_date)=0 then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else cast(mem.prgm_term_date as date) end) as eligibilityenddate
                                ,runyear
    from dbo.meridian_62_membership mem
    inner join dbo.stars_patients pat          on pat.pmd_client_id=62             and mem.patientid=pat.patient_id                                                                          
    where runyear = """+str(run_year)+""" and mem.plan_code is not null
    group by 
                plan_code
                ,patient_unique_ident
                ,patient_effdate
                ,runyear

    ) e;
    end $$;""")
    conn_stars_rdsm.execute(text(meridian_62_staging_query).execution_options(autocommit=True))
    df_meridian_62_staging_query=pd.read_sql_query("""select * from t1;""",con=conn_stars_rdsm)
    df_meridian_62_staging_query.to_sql('staging_stars_patients_plan_pbp',con=conn_stars_rdsm,schema='staging',if_exists='append',index=False)
##########################Sunshine to Staging############################
if pmd_client_id=='76':
    flag76='true'
    cur7=conn1.cursor()
    cur7.execute('call dbo.usp_stars_patients_plan_pbp_sunshinehealth(%s)',[run_year])
    conn1.commit()
    cur7.close()
############################BCBSAZ PBP to Staging################################
if pmd_client_id=='83':
    flag83='true'
    cur8=conn1.cursor()
    cur8.execute('call dbo.usp_stars_patients_plan_pbp_bcbsaz(%s)',[run_year])
    conn1.commit()
    cur8.close()
####################Aspire to Staging########################
if pmd_client_id=='69':
    flag69='true'
    aspire_to_staging_query=("""do $$
    begin
    drop table if exists t1;
    create table t1 as
    select 
                pmd_client_id
                ,patient_unique_ident
                ,left(planidentifier_pclm,5) as planidentifier_pclm
                ,case when length(pbp_pclm) = 0 then 'none' else pbp_pclm end pbp_pclm
                --eligibilitystartdates that have a year prior to the runyear where the tranformed eligibilityenddate has a year equal to the runyear 
                --must be reset to the beginning of the runyear or else this will have an adverse effect on running evals
                --for prior years if the elig end date is accidentally reset to the end of current year (interfering with the 91 day rule)
                ,case when date_part('year',eligibilitystartdate) < """+str(run_year)+""" and date_part('year',(case when date_part('year',eligibilityenddate) > """+str(run_year)+""" 
                                  then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else eligibilityenddate end)) = """+str(run_year)+"""     
                                  then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-01-01') as date) else eligibilitystartdate end as eligibilitystartdate
                --eligibilityenddates that go beyond end of year must be reset to the end of the plan year of the current year.
                ,case when date_part('year',eligibilityenddate) > """+str(run_year)+""" then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else eligibilityenddate end  eligibilityenddate
                ,runyear
   from 
    (
    select 
                69 pmd_client_id
                ,asp.group_code planidentifier_pclm                                      
                ,pat.patient_unique_ident
                ,account_code pbp_pclm
                ,case when sqlserver_ext.isdate(asp.patient_effective_date) = 0 then cast('1900-01-01' as date) else cast(asp.patient_effective_date as date) end as eligibilitystartdate                                                                               
                ,max(case when sqlserver_ext.isdate(asp.patient_term_date) = 0 then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else cast(asp.patient_term_date as date) end) as eligibilityenddate
                ,runyear
    from dbo.aspire_membership_current asp
    inner join dbo.stars_patients pat
    on asp.patient_id=pat.patient_id
    and pat.pmd_client_id =69
    where runyear = """+str(run_year)+""" and length(asp.account_code) >0
    group by patient_unique_ident
                ,asp.patient_effective_date
                ,runyear
                ,account_code
                ,asp.group_code
    ) e;
    end $$;""")
    conn_stars_rdsm.execute(text(aspire_to_staging_query).execution_options(autocommit=True))
    df_aspire_to_staging_query=pd.read_sql_query("""select * from t1;""",con=conn_stars_rdsm)
    df_aspire_to_staging_query.to_sql('staging_stars_patients_plan_pbp',con=conn_stars_rdsm,schema='staging',if_exists='append',index=False)
    
###################################IVHP to Staging##########################################
if pmd_client_id=='78':
    flag78='true'
    cur9=conn1.cursor()
    cur9.execute('call dbo.usp_stars_patients_plan_pbp_ivhp(%s)',[run_year])
    conn1.commit()
    cur9.close()
########################HCSC to  Staging#######################################
if pmd_client_id=='77':
   flag77='true'
   cur10=conn1.cursor()
   cur10.execute('call dbo.usp_stars_patients_plan_pbp_hcsc(%s)',[run_year])
   conn1.commit()
   cur10.close()
###################UHCMR PBP to Staging  #################
if pmd_client_id=='87':
    flag87='true'
    cur8=conn1.cursor()
    cur8.execute('call dbo.usp_Stars_Patients_Plan_PBP_UHC_MR(%s)',[run_year])
    conn1.commit()
    cur8.close()
	
#####BrightHealth to Staging######################
if pmd_client_id=='96':
    flag96='true'
    cur8=conn1.cursor()
    cur8.execute('call dbo.usp_Stars_Patients_Plan_PBP_BrightHealth(%s)',[run_year])
    conn1.commit()
    cur8.close()
#############UHCURS PBP to Staging############
if pmd_client_id=='94':
    flag94='true'
    cur8=conn1.cursor()
    cur8.execute('call dbo.usp_Stars_Patients_Plan_PBP_UHC_URS(%s)',[run_year])
    conn1.commit()
    cur8.close()
###################################Update Stars_Patients_Plan_PBP#################################
if flag52=='true' or flag62=='true' or flag76=='true' or flag83=='true' or flag69=='true' or flag78=='true' or flag77=='true' or flag87=='true' or flag96=='true' or flag94=='true':
    print("yupieee")
    cur10=conn1.cursor()
    cur10.execute('call dbo.uspstarspatientsplanpbp_merge(%s)',[run_year])
    conn1.commit()
    cur10.close()
##################Stars_Patients_Guardian##########################
#########################Truncate Table#################################

conn_stars_rdsm.execute("""truncate table staging.staging_stars_patients_guardians""")

####################Centene_Medicaid to Staging#######################

if pmd_client_id=='67':
    flag67='true'
    centene_medicaid_membership=("""do $$ 
    begin
    drop table if exists t1;
    create temp table t1 as
    select distinct 
                pmd_client_id
                ,patient_unique_ident
                ,planidentifier_pclm
                ,guardian_id
                ,guardian_firstname 
                ,guardian_lastname
                ,max(guardian_phone) as guardian_phone
                --eligibilitystartdates that have a year prior to the runyear where the tranformed eligibilityenddate has a year equal to the runyear must be reset to the beginning of the runyear or else this will have an adverse effect on running evals
                --for prior years if the elig end date is accidentally reset to the end of current year (interfering with the 91 day rule)
                ,case when date_part('year',eligibilitystartdate) < """+str(run_year)+""" and date_part('year',(case when date_part('year',eligibilityenddate) > """+str(run_year)+""" then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else eligibilityenddate end)) = """+str(run_year)+"""    
                then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-01-01') as date) else eligibilitystartdate end as eligibilitystartdate
                --eligibilityenddates that go beyond end of year must be reset to the end of the plan year of the current year.
                ,max(case when date_part('year',eligibilityenddate) > """+str(run_year)+""" then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else eligibilityenddate end) as   eligibilityenddate
                ,runyear

    from 
                (select 
                                67 as pmd_client_id
                                ,group_code as planidentifier_pclm
                                ,patient_unique_ident
                                ,md5(concat(cardholder_fname,cardholder_lname)) as guardian_id
                                ,cardholder_fname as guardian_firstname 
                                ,cardholder_lname as guardian_lastname
                                ,mem.patient_phone_1 as guardian_phone                         
                                ,case when sqlserver_ext.isdate(mem.patient_effective_date) = 0 then cast('1900-01-01' as date) else cast(mem.patient_effective_date as date) end as eligibilitystartdate                                                                                            
                                ,max(case when sqlserver_ext.isdate(mem.patient_term_date) = 0 then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else cast(mem.patient_term_date as date) end) as eligibilityenddate
                                ,runyear               
                from dbo.centene_medicaid_membership mem
                inner join dbo.stars_patients pat 
                on pat.pmd_client_id = 67 and mem.patient_id = pat.patient_id                                                                
                inner join (select patient_id, max(update_datetime) as maxupdatedate 
                                                from dbo.centene_medicaid_membership group by patient_id
                                                ) maxdate on mem.patient_id = maxdate.patient_id and mem.update_datetime = maxdate.maxupdatedate
                
                where runyear = """+str(run_year)+""" and length(cardholder_fname) >0   and  length(cardholder_lname)>0  
                --and pat.patient_unique_ident= 12570623

                group by 
                                group_code
                                ,patient_unique_ident
        ,cardholder_fname 
                                ,cardholder_lname
                                ,mem.patient_phone_1
                                ,patient_effective_date
                                ,runyear
                ) e 

                group by
    
                pmd_client_id
                ,patient_unique_ident
                ,planidentifier_pclm
                ,guardian_id
                ,guardian_firstname 
                ,guardian_lastname
                ,case when date_part('year',eligibilitystartdate) < """+str(run_year)+""" and date_part('year',(case when date_part('year',eligibilityenddate) > """+str(run_year)+""" then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-12-31') as date) else eligibilityenddate end)) = """+str(run_year)+"""    
                then cast(concat(cast("""+str(run_year)+""" as varchar(4)) ,'-01-01') as date) else eligibilitystartdate end
                
                ,e.runyear;
                
    end $$;             """)
    conn_stars_rdsm.execute(text(centene_medicaid_membership).execution_options(autocommit=True))
    df_centene_medicaid_membership_query=pd.read_sql_query("""select * from t1;""",con=conn_stars_rdsm)
    df_centene_medicaid_membership_query.to_sql('staging_stars_patients_guardians',con=conn_stars_rdsm,schema='staging',if_exists='append',index=False)
if pmd_client_id=='78':
    flag_gardian_78='true'
    conn_stars_rdsm.execute("""insert into staging.staging_stars_patients_guardians(pmd_client_id, patient_unique_ident, planidentifier_pclm, guardian_id, guardian_firstname, guardian_lastname, guardian_phone, eligibilitystartdate, eligibilityenddate, runyear) select pmd_client__id,patient_unique__ident,planidentifier__pclm,guardian__id,guardian__firstname,guardian__lastnamee,guardian__phone,eligibilityend_date,eligibilitystart_date,run_year 
    from dbo.usp_stars_patients_guardian_ivhp("""+run_year+""")""")
    if flag67=='true'or flag_gardian_78=='true':
       conn_stars_rdsm.execute("""update dbo.stars_patients_guardians as t
        set eligibilityenddate = s.eligibilityenddate, guardian_phone = s.guardian_phone
        from (select distinct 
                                 patient_unique_ident
                                ,planidentifier_pclm
                                ,pmd_client_id
                                ,guardian_id
                                ,guardian_firstname
                                ,guardian_lastname
                                ,eligibilitystartdate
                                ,eligibilityenddate
                                ,max(guardian_phone) as guardian_phone
                                ,runyear
        from staging.staging_stars_patients_guardians
        group by       
                                 patient_unique_ident
                                ,planidentifier_pclm
                                ,pmd_client_id
                                ,guardian_id
                                ,guardian_firstname
                                ,guardian_lastname
                                ,guardian_phone
                                ,eligibilitystartdate
                                ,eligibilityenddate
                                ,runyear) as s
        where (t.patient_unique_ident = s.patient_unique_ident
                    and t.planidentifier_pclm=s.planidentifier_pclm
                    and t.guardian_id = s.guardian_id
                    and t.eligibilitystartdate = s.eligibilitystartdate               ) ;

        insert into dbo.stars_patients_guardians(patient_unique_ident,planidentifier_pclm,pmd_client_id,guardian_id,guardian_firstname,guardian_lastname,eligibilitystartdate,eligibilityenddate,guardian_phone,runyear)
        select s.patient_unique_ident,s.planidentifier_pclm,s.pmd_client_id,s.guardian_id,s.guardian_firstname,s.guardian_lastname,s.eligibilitystartdate,s.eligibilityenddate,s.guardian_phone,s.runyear from (select distinct 
                                 patient_unique_ident
                                ,planidentifier_pclm
                                ,pmd_client_id
                                ,guardian_id
                                ,guardian_firstname
                                ,guardian_lastname
                                ,eligibilitystartdate
                                ,eligibilityenddate
                                ,max(guardian_phone) as guardian_phone
                                ,runyear
        from staging.staging_stars_patients_guardians

        group by       
                                 patient_unique_ident
                                ,planidentifier_pclm
                                ,pmd_client_id
                                ,guardian_id
                                ,guardian_firstname
                                ,guardian_lastname
                                ,guardian_phone
                                ,eligibilitystartdate
                                ,eligibilityenddate
                                ,runyear) as s
        left join dbo.stars_patients_guardians as t                   
        on (t.patient_unique_ident = s.patient_unique_ident
                    and t.planidentifier_pclm=s.planidentifier_pclm
                    and t.guardian_id = s.guardian_id
                    and t.eligibilitystartdate = s.eligibilitystartdate               ) 
        where t.patient_unique_ident is null; """)
        
##################update DNC FLAG ##############################
##################truncate staging table ####################
conn_stars_rdsm.execute("""truncate table staging.staging_stars_patients_dnc_membership""")

####################Aspire staging #################################
if pmd_client_id=='69':
    flag_dnc69='true'
    df_aspire_membership_query=pd.read_sql_query("""select 
       p.patient_unique_ident
     
    from dbo.aspire_membership_current m
    inner join (select max(update_datetime) maxupdate, patient_id from dbo.aspire_membership_current group by patient_id) maxrec
    on maxrec.patient_id = m.patient_id
    and maxrec.maxupdate = m.update_datetime

    inner join dbo.stars_patients p
    on p.patient_id = m.patient_id
    and p.pmd_client_id = '69' where carrier_code = 1::varchar(2);""",con=conn_stars_rdsm)
    df_aspire_membership_query.to_sql('staging_stars_patients_dnc_membership',con=conn_stars_rdsm,schema='staging',if_exists='append',index=False)
    
#######################UHCMRCTX Staging ################################################
if pmd_client_id=='73':
    flag_dnc73='true'
    df_uhcmrctx_dnc_query=pd.read_sql_query("""select  distinct p.patient_unique_ident from dbo.uhcmrtx_dnc dnc
    inner join dbo.stars_patients p on dnc.hicn=p.patient_id where "ok to call" = 'n' and dnc.runyear = """+str(run_year)+""" """,con=conn_stars_rdsm)
    df_uhcmrctx_dnc_query.to_sql('staging_stars_patients_dnc_membership',con=conn_stars_rdsm,schema='staging',if_exists='append',index=False)

########################Healthspring Staging##################################################

if pmd_client_id=='30':
    flag_dnc30='true'
    df_healthspring_dnc_query=pd.read_sql_query("""select p.patient_unique_ident from dbo.healthspring_membership m inner join dbo.stars_patients p
    on m.patientid = p.patient_id where file_date = (select max(file_date) from dbo.healthspring_membership) and clientdef1 = 'dnc'; """,con=conn_stars_rdsm)
    df_healthspring_dnc_query.to_sql('staging_stars_patients_dnc_membership',con=conn_stars_rdsm,schema='staging',if_exists='append',index=False)
    
###########################Update DNC Flag and Source ############################################

if flag_dnc69=='true' or flag_dnc73=='true' or flag_dnc30=='true':
    conn_stars_rdsm.execute("""update dbo.stars_patients p  set dnc_flag = true, dnc_flag_source='dncfile' from staging.staging_stars_patients_dnc_membership s
    where s.patient_unique_ident::bigint = p.patient_unique_ident; """)
    
##########################Update Misc and Exceptions########################################
#########################ActiveInactivePatients For Centene Medicaid######################
active_inactivepatients_centene_medicaid=("""do $$ 
--centene medicaid
begin
if ("""+str(run_year)+""" = date_part('year',now()) and """+str(pmd_client_id)+""" = 67)
then
update dbo.stars_patients p
set active = false
where pmd_client_id = 67
and exists
--active
(select 1
  from dbo.centene_medicaid_membership a 
      where a.runyear = """+str(run_year)+"""
                  and nurtur_member='y'
      and p.patient_id = a.patient_id
);
end if;
end $$; """)
#conn_stars_rdsm.execute(active_inactivepatients_centene_medicaid)
conn_stars_rdsm.execute(text(active_inactivepatients_centene_medicaid).execution_options(autocommit=True))
#################################Stage Eligibility Exceptions##############################
cur11=conn1.cursor()
cur11.execute('call dbo.usp_eligibilityexception(%s)',[pmd_client_id])
conn1.commit()
cur11.close()

##############################Merge Eligibility Exceptions ######################################
conn_stars_rdsm.execute("""update dbo.stars_patients_eligibility_exceptions as target
set    exceptionenddate = source.exceptionenddate
from   (select distinct ee.excepttype,
                        ee.pmd_client_id,
                        ee.patient_unique_ident,
                        ee.planidentifier_pclm,
                        ee.exceptionstartdate,
                        ee.exceptionenddate
        from   staging.staging_stars_patients_eligibility_exceptions ee
               --since clients can send overlapping periods with same exceptionstartdate, take the record with the latest exceptionenddate
               inner join
               (select patient_unique_ident,
                       planidentifier_pclm,
                       exceptionstartdate,
                       max(exceptionenddate) as maxend
                from   staging.staging_stars_patients_eligibility_exceptions
                group  by patient_unique_ident,
                          planidentifier_pclm,
                          exceptionstartdate) maxexceptend
                       on maxexceptend.exceptionstartdate =
                          ee.exceptionstartdate
                          and maxexceptend.patient_unique_ident =
                              ee.patient_unique_ident
                          and maxexceptend.planidentifier_pclm =
                              ee.planidentifier_pclm
                          and maxexceptend.maxend = ee.exceptionenddate) as
       source
where  ( target.excepttype = source.excepttype
         and target.patient_unique_ident = source.patient_unique_ident
         and target.planidentifier_pclm = source.planidentifier_pclm
         and target.exceptionstartdate = source.exceptionstartdate ); 

insert into dbo.stars_patients_eligibility_exceptions (excepttype,pmd_client_id,patient_unique_ident,planidentifier_pclm,exceptionstartdate,exceptionenddate,updatedate)
select source.excepttype,source.pmd_client_id,source.patient_unique_ident,source.planidentifier_pclm,source.exceptionstartdate, source.exceptionenddate
                                                                                                ,now() from (select distinct ee.excepttype
                                                                  ,ee.pmd_client_id
                                                                  ,ee.patient_unique_ident
                                                                  ,ee.planidentifier_pclm
                                                                  ,ee.exceptionstartdate
                                                                  ,ee.exceptionenddate
                                                  from staging.staging_stars_patients_eligibility_exceptions ee

                                                  --since clients can send overlapping periods with same exceptionstartdate, take the record with the latest exceptionenddate
                                                  inner join (select patient_unique_ident, planidentifier_pclm, exceptionstartdate, max(exceptionenddate) as maxend from staging.staging_stars_patients_eligibility_exceptions group by patient_unique_ident, planidentifier_pclm, exceptionstartdate) maxexceptend
                                                  on maxexceptend.exceptionstartdate = ee.exceptionstartdate
                                                  and maxexceptend.patient_unique_ident = ee.patient_unique_ident
                                                  and maxexceptend.planidentifier_pclm = ee.planidentifier_pclm
                                                  and maxexceptend.maxend = ee.exceptionenddate

                                                                                ) as source
left join dbo.stars_patients_eligibility_exceptions as target
on (
                                                target.excepttype=source.excepttype    
                                                and target.patient_unique_ident=source.patient_unique_ident
                                                and target.planidentifier_pclm=source.planidentifier_pclm
                                                and target.exceptionstartdate=source.exceptionstartdate
                                                )
where target.excepttype is null; """)

#########################################Calc Orphaned Claims#######################################
calc_orphaned_claims_query=(""" do $$ 


begin
update dbo.stars_patient_rundata r
set           orphaned_measureclaims_to_date =
              (
                     select count(*)
                     from   public.opsdb_stars_pclm_star c
                     where  not exists (
                                         select 1
                                         from   dbo.stars_patients p
                                         where  p.patient_id = c.patient_id 
                                         and           p.pmd_client_id = """+str(pmd_client_id)+""")
                     and           c.pmd_client_id = """+str(pmd_client_id)+"""
                     and           c.product_service_id in (select ndc::varchar(50) from public.opsdb_stars_staging_ndcmaster)
              )
where  run_id = """+str(max_run_id)+""";
end $$; """)

#conn_stars_rdsm.execute(calc_orphaned_claims_query)
conn_stars_rdsm.execute(text(calc_orphaned_claims_query).execution_options(autocommit=True))

print("Finally")
con_db_opsdb_stars.close()
conn_stars_rdsm.close()
conn1.close()