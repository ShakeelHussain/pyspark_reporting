from sqlalchemy import create_engine
import pandas as pd

from datetime import datetime
from pyspark.sql import SparkSession

def checkstopped(df):
    return (df.speed==0) & (df.lead_speed>=2)

now=datetime.now()
engine = create_engine('mysql+pymysql://root:@localhost:3306/traccar')
spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

current_positions = pd.read_sql_query('SELECT * FROM positions where servertime <= "'+str(now)+'"', engine)
print(current_positions.head())

positions_spark = spark.createDataFrame(current_positions[['speed','deviceid','id']])

positions_spark.createOrReplaceTempView("current_positions")

lead_speed_df=spark.sql("select id,deviceid,speed , lead(speed,1) Over(Partition by deviceid Order by id) - speed as"
                        " lead_speed from current_positions order by deviceid asc ,id asc ")

lead_speed_df.createOrReplaceTempView("lead_speed_df")

lead_speed_df[checkstopped(lead_speed_df)].show(400)