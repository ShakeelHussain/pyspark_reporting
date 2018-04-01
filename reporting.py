from sqlalchemy import create_engine
import pandas as pd
from pandas.io.json import json_normalize
from datetime import datetime,timedelta
from pyspark.sql import SparkSession
import json
def checkstopped(df):
    return (df.speed==0) & (df.lead_speed==0)

now=datetime.now()
five_mins_back = now-timedelta(minutes=5)
engine = create_engine('')
spark = SparkSession.builder.appName("SimpleApp").master('local[3]').getOrCreate()

current_positions = pd.read_sql_query('SELECT * FROM positions where servertime <= "'+str(now)+'" and servertime>="'+str(five_mins_back)+'"', engine)
# value=current_positions['attributes'].to_json()
# json_data=json.loads(value)
# dfs=pd.DataFrame()
# dfs.from_dict(json_data['0'])
#
# for o in json_data:
#     result = json_normalize(json_data[o])
#     result['rowID'] = o
#     dfs = pd.concat([dfs, result])
#
# dfs.head()
# print(dfs.head())
positions_spark = spark.createDataFrame(current_positions[['speed','deviceid','id','servertime','attributes']])

positions_spark.createOrReplaceTempView("current_positions")

lead_speed_df=spark.sql("select id,deviceid,speed , lead(speed,1) Over(Partition by deviceid Order by id) - speed as"
                        " lead_speed ,servertime,attributes from current_positions order by id asc")

lead_speed_df.createOrReplaceTempView("lead_speed_df")

stops=lead_speed_df[checkstopped(lead_speed_df)]
stops.toPandas().to_csv('mycsv.csv')
