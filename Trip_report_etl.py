from pyspark.sql import SparkSession
from pyspark.sql.functions import lag
from pyspark.sql.window import Window

spark = SparkSession.builder.appName("SimpleApp").getOrCreate()

jdbcDF = spark.read.format("jdbc").options(
            url ="jdbc:mysql://localhost:3306/traccar",
            driver="com.mysql.jdbc.Driver",
            dbtable="traccar.positions",
            user="root",
            password=""
            ).load()


w =  jdbcDF.groupby("id")
jdbcDF=jdbcDF.withColumn("new_col", lag(jdbcDF["speed"],1,).over((Window.partitionBy(jdbcDF["deviceid"])).orderBy('deviceid')))
jdbcDF.createOrReplaceTempView("positions")

sqlDF = spark.sql("select deviceid,speed,new_col from positions order by deviceid desc ")
sqlDF.show(49)