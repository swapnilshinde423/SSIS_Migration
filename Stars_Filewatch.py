#Star File watcher job code
import psycopg2
import base64
import fnmatch
import boto3
from boto3 import client
import sys
from awsglue.utils import getResolvedOptions
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
import requests
import json

###### Connect Secret Manager and collects required input parameters #####
str_client = boto3.client("secretsmanager", region_name="us-east-2")
get_secret_value_response = str_client.get_secret_value(
        SecretId="SecretKeysForAWSGlue"
)

secret = get_secret_value_response['SecretString']
secret = json.loads(secret)

con_db_host = secret.get('con_db_host')
con_db_port = secret.get('con_db_port')
con_db_user = secret.get('con_db_user')
con_db_password = secret.get('con_db_password')
con_db_opsdb_mrm = secret.get('con_db_opsdb_mrm')
con_db_odsdb_starsconnect = secret.get('con_db_odsdb_starsconnect')
con_db_opsdb_stars_rdsm = secret.get('con_db_opsdb_stars_rdsm')
con_db_opsdb_stars = secret.get("con_db_opsdb_stars")
file_watch_environment = secret.get("file_watch_environment")
file_watch_s3_bucket = secret.get("file_watch_s3_bucket")
flie_watch_email_recipients = secret.get("flie_watch_email_recipients")
common_email_api_url = secret.get("common_email_api_url")

engine = create_engine('postgresql://'+con_db_user+':'+con_db_password+'@'+con_db_host+':'+con_db_port+'/'+con_db_opsdb_stars_rdsm)
conn=engine.connect()
#### Below query to collect the list of active Client's and Client folder
file_config_query = """SELECT pmd_client_id, filename,
                        CASE WHEN UPPER('"""+file_watch_environment+"""') = 'DEV' THEN filelocationdev
                            WHEN UPPER('"""+file_watch_environment+"""') = 'QA' THEN filelocationqa
                            WHEN UPPER('"""+file_watch_environment+"""') = 'PROD' THEN filelocation
                            ELSE 'Dev'
                            END AS filelocation
                    FROM dbo.filewatch_config WHERE active = true and trigger_eval_engine=true and pmd_client_id = 78
                    ORDER BY pmd_client_id DESC;"""
df_active_client = pd.read_sql_query(file_config_query, con = conn)
#print(file_config_query)
### Below query collects the Clients for which Evaluation engine process is in-progress
engine2 = create_engine('postgresql://'+con_db_user+':'+con_db_password+'@'+con_db_host+':'+con_db_port+'/'+con_db_opsdb_stars)
conn2=engine2.connect()
file_control_query = """SELECT DISTINCT pmdclientid AS pmd_client_id, controlid FROM dbo.control WHERE process_ind = true;"""
df_file_control = pd.read_sql_query(file_control_query, con = conn2)

df_active_clnt_rdy_temp=pd.merge(df_active_client,df_file_control,on='pmd_client_id', how='left')
df_active_clnt_rdy_process = df_active_clnt_rdy_temp[df_active_clnt_rdy_temp['controlid'].isnull()]
is_s3_file_available = False
#print(df_active_clnt_rdy_temp.head())
### Loop through each Client folder in S3 Bucket and check whether any Source files are arrived.
conn3 = psycopg2.connect(database=con_db_opsdb_stars_rdsm, user=con_db_user, password=con_db_password, host=con_db_host, port= con_db_port)   
conn3.autocommit = True
cursor = conn3.cursor()
for index, row in df_active_clnt_rdy_process.iterrows(): 
    var_filelocation = row["filelocation"]
    var_pmd_client_id = row["pmd_client_id"]
    var_filename = row["filename"]
    #print(var_filename)
    #print("var_filelocation:"+var_filelocation+str(var_pmd_client_id)+var_filename)
    conn = client('s3')
    unprocessed_filecount_new = 0
    file_size = 0
    file_data_count = 0
    s3 = boto3.resource('s3')
    bucket=s3.Bucket(file_watch_s3_bucket);
    obj = s3.Object(file_watch_s3_bucket,var_filelocation)
    counter=0
    #print(file_watch_s3_bucket+var_filelocation)
    for key in bucket.objects.filter(Prefix=var_filelocation+'/'+var_filename.rsplit("*",1)[0]):
        counter=counter+1
    #print("Folder Check:"+str(counter))
    if counter!= 0:
        for key in conn.list_objects(Bucket=file_watch_s3_bucket, Prefix=var_filelocation+'/'+var_filename.rsplit("*",1)[0])['Contents']:
            s3_file = key['Key'].rsplit("/",1)[1]
            #print("S3_FILE"+s3_file)
            file_size = boto3.resource('s3').Bucket(file_watch_s3_bucket).Object(key['Key']).content_length
            #print("file_size::"+str(file_size))
            s3obj = boto3.resource('s3').Object( file_watch_s3_bucket, key['Key'])
            filedata= s3obj.get()["Body"].read()
            file_data_count = filedata.decode('utf8').count('\n')-1
            #print (str(file_data_count))
            if file_data_count == -1:
                file_data_count = 0
            if fnmatch.fnmatch(s3_file, var_filename):
                #print("s3_file:::"+s3_file+":::var_filename:::"+var_filename)
                unprocessed_filecount_new = unprocessed_filecount_new+1
                is_s3_file_available = True
                file_info_query = """call dbo.usp_stars_insertfileinfo("""+str(var_pmd_client_id)+""",'"""+var_filename+"""','"""+s3_file+"""','"""+str(file_size)+"""',"""+str(file_data_count)+""",'"""+file_watch_environment+"""')"""
                #print(var_filename)
                #print(file_info_query)
                cursor.execute(file_info_query)
    if unprocessed_filecount_new != 0 :
        update_query = """UPDATE dbo.filewatch_config
                    SET lastdatereceived = now()
                    ,unprocessed_filecount ="""+str(unprocessed_filecount_new)+"""
                    WHERE pmd_client_id = """+str(var_pmd_client_id)+""" AND filename = '"""+var_filename+"""'"""
        #print(update_query)
        cursor.execute(update_query)
    if (is_s3_file_available):
        conn2.execute("""UPDATE dbo.control
                        SET process=1
                        ,FileWatcherQueueDate = now()
                        WHERE pmdclientid ="""+str(var_pmd_client_id)+"""
                        and Runyear = DATE_PART('year',NOW())
                        and process = 0;""")
if (is_s3_file_available):
    history_query = """
            UPDATE dbo.filewatch_history
            SET lastdatereceived = fc.lastdatereceived
            FROM dbo.filewatch_config AS fc
            WHERE filewatch_history.filetype = fc.filetype
                AND filewatch_history.pmd_client_id = fc.pmd_client_id
                AND filewatch_history.unprocessed_filecount = fc.unprocessed_filecount
                AND TO_CHAR(filewatch_history.lastdatereceived,'YYYY-MM-DD') = TO_CHAR(fc.lastdatereceived,'YYYY-MM-DD')
                AND fc.unprocessed_filecount <> 0;
                
            INSERT INTO dbo.filewatch_history (pmd_client_id,filetype,filename,filelocation,active,lastdatereceived,unprocessed_filecount)
            SELECT fc.pmd_client_id,fc.filetype,fc.filename
                ,CASE WHEN UPPER('"""+file_watch_environment+"""') = 'DEV' THEN fc.filelocationdev
                    WHEN UPPER('"""+file_watch_environment+"""') = 'QA' THEN fc.filelocationdev
                    ELSE fc.filelocation
                    END AS filelocation,fc.active,fc.lastdatereceived,fc.unprocessed_filecount
            FROM dbo.filewatch_config AS fc
            LEFT JOIN dbo.filewatch_history AS his
                ON his.filetype = fc.filetype
                AND his.pmd_client_id = fc.pmd_client_id
                AND his.unprocessed_filecount = fc.unprocessed_filecount
                AND TO_CHAR(his.lastdatereceived,'YYYY-MM-DD') = TO_CHAR(fc.lastdatereceived,'YYYY-MM-DD')
            WHERE fc.unprocessed_filecount <> 0 AND his.pmd_client_id IS NULL;"""
    #print(history_query)
    cursor.execute(history_query)
    cursor.execute("""SELECT '<html><head><style>table cellpadding=10 cellspacing=10, th, td { border: 1px solid black; border-collapse: collapse;}</style></head><body><H3>File Watcher notification</H3>'
                  ||'<table border = 1><tr><th> PMD Client ID </th> <th> File Type </th> <th>File Pattern</th><th>File Location</th><th>Last Date Received</th><th>Unprocessed Filecount</th></tr><tr>'
                  ||STRING_AGG('<td>'||pmd_client_id::VARCHAR(10)||'</td><td>'||filetype||'</td><td>'||filename||'</td><td>'||filelocation||'</td><td>'||lastdatereceived||'</td><td>'||unprocessed_filecount::VARCHAR(10)||'</td>','</tr><tr>')
                  ||'</tr></table><br/><br/>'
                  AS email_query
                FROM dbo.filewatch_config
                WHERE active = true AND unprocessed_filecount > 0""")
    
    email_body = cursor.fetchall()[0][0]
    print(email_body)
    task = {
            "title": file_watch_environment+" Stars Filewatcher Notification",
            "from_address": flie_watch_email_recipients,
            "to_address": [flie_watch_email_recipients],
            "cc_address": None,
            "bcc_address": None,
            "message": None,
            "html_message": email_body,
            "attachments": None,
            "importance": None
    }
    headers = {'Content-Type': 'application/json', 'Authorization': 'Token token=portal_token'}
    resp = requests.post(common_email_api_url, json=task, headers=headers)
    if resp.text == "Sent":
        print("********File Watcher Job has identified new files from S3 Client folder and notified the same to Support team********")
    else:
        print("********No file present in given S3 bucket***********")
conn3.commit()
conn3.close()