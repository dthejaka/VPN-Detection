
import datetime
from datetime import datetime,timedelta,date
import pyspark
from pyspark.sql.functions import unix_timestamp,when,lit
from pyspark.sql.functions import from_unixtime, substring
#import pandas as pd
#import numpy as np

#import findspark
#findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,ArrayType ,DateType, ShortType, StringType, IntegerType, TimestampType, LongType, BooleanType, DoubleType
from pyspark.sql import functions as f

from pyspark.sql.functions import dense_rank, row_number,concat, regexp_replace
from pyspark.sql.window import Window

import os
import configparser
import logging

#read conf file
myconfig = configparser.ConfigParser()
myconfig.read('/home/vizuamatix/vx_freedom/analytics_scripts/cfi_summary.conf')

#log file    
logging.basicConfig(filename=myconfig.get('log_files','metadata_read'),format='%(asctime)s - %(message)s',level=logging.INFO)

logging.info('Process is started')
if __name__ == "__main__":
    spark = SparkSession         .builder         .appName("meta data reading process")         .master("local[*]")         .getOrCreate()
    sc = spark.sparkContext


    headers = {
        'Content-Type': 'application/json',
    }
    
params = (
        ('pretty', ''),
    )

    #db_properties = {
     #   'username':'vxanalytics',
      #  'url' : 'jdbc:postgresql://localhost:5432/vx_freedom',
       # 'password' : 'vxanalytics@123',
        #'host' : '127.0.0.1',
        #'port' : 5432,
        #'database': 'vx_freedom',
        #'driver' : 'org.postgresql.Driver'
    #}

db_properties={}
db_properties['username']=myconfig.get('db_conf','user')
db_properties['password']=myconfig.get('db_conf','password')
db_properties['url']= myconfig.get('db_conf','url')
db_properties['driver']= myconfig.get('db_conf','driver')


logging.info('metadata read process is start')

base_path = '/data/raw_ipdr'
all_paths = set(os.listdir(base_path))
print(all_paths)

no_of_days=1
yesterday = datetime.now() - timedelta(days=no_of_days)

DAY_START = yesterday
DAY_END = datetime.now()
today= datetime.now().strftime('%Y-%m-%d').replace('-','')
yest= yesterday.strftime('%Y-%m-%d').replace('-','')

#print(DAY_START)

#print(DAY_END)

sql = "SELECT * FROM public.ipdr_base WHERE file_date_time BETWEEN '{}' AND '{}'".format(DAY_START, DAY_END)
#sql = "SELECT * FROM public.ipdr_base"

df_load= spark.read.format('jdbc').options(url=db_properties['url'], driver=db_properties['driver'], user=db_properties['username'], password=db_properties['password'], query=sql).load()

df_load=df_load.withColumn('file_name',regexp_replace('file_name', '/data/raw_ipdr/', ''))
exist_paths= (df_load.select('file_name').collect())

exist_paths= set([str(i.file_name) for i in exist_paths])
new_paths= all_paths- exist_paths

correct_new_paths=[]

for i in new_paths:
    #print('00000in for loop 000000000')
    if '.csv.gz' in i:
        i_part= str(i.split('__')[1])[0:8]
        print(i_part)
        if (i_part==today or i_part==yest):
            #print('......,,,,,,,,,...........if true.............')
            rec_count=spark.read.csv(path=('/data/raw_ipdr/'+i), sep= ';', header= False).count()
            correct_new_paths.append((i,rec_count,0,None,None,None,'',0,0))

print('need paths..........................')
#print(new_paths)
#print(correct_new_paths)

schema = StructType([
    StructField('file_name', StringType(),True),
    StructField('record_count', LongType(),True),
    StructField('processed_count', LongType(),True),
    StructField('status', IntegerType(),True),
    StructField('file_date_time', TimestampType(),True),
    StructField('processed_time', TimestampType(),True),
    StructField('process_file_path', StringType(),True),
    StructField('api_call_status', ShortType(),True),
    StructField('api_selected_status', ShortType(),True)])

df= spark.createDataFrame(correct_new_paths, schema)

#df= df.withColumn('file_name', str('/data/raw_ipdr/')+df.file_name)
df= df.withColumn('file_date_time', substring(df.file_name,20,13))
df= df.withColumn('file_date_time',from_unixtime(unix_timestamp( df.file_date_time, "yyyyMMdd'_'HHmm" ), "yyyy-MM-dd' 'HH:mm:00" ))
df= df.withColumn('file_date_time', df.file_date_time.cast(TimestampType()))
df= df.withColumn('status', lit(1))
df= df.withColumn('file_name', concat(lit('/data/raw_ipdr/'),df.file_name))
df= df.withColumn('api_call_status', lit(0))
df= df.withColumn('api_selected_status', lit(0))

#df.show()

df.write.format("jdbc").mode('append').option("url", db_properties['url']).option("driver", db_properties['driver']).option("dbtable", "ipdr_base").option("user", db_properties['username']).option("password", db_properties['password']).save()



#df.show()
print('..............done............')
sc.stop()
exit()

