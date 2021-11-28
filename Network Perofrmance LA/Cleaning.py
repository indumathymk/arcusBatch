import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import *
import re
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
spark = SparkSession(SparkContext.getOrCreate())
import datetime
from datetime import date

year, week_no, day_of_week = datetime.date.today().isocalendar()

YEAR_OF_WEEK = year
WEEK_NO = week_no - 1

print(WEEK_NO)

# [Table] NP LA Channel

glueContext = GlueContext(SparkContext.getOrCreate())
channel = glueContext.create_dynamic_frame.from_catalog(database="barc", table_name="network_la_analysis_input_la")
channelDF = channel.toDF()
channelDF = channelDF.where( (F.col("year_of_week") == str(YEAR_OF_WEEK)) & (F.col("week_no") == str(WEEK_NO)) )

def removeSpecialCharacters(value):
    return float(re.sub(",","", str(value)))
    
removeSpecialCharactersFun = F.udf(removeSpecialCharacters, DoubleType())

channelDF = channelDF.withColumn("grp", removeSpecialCharactersFun("rat%"))
channelDF = channelDF.withColumn("share_pct", removeSpecialCharactersFun("shr%"))
channelDF = channelDF.withColumn("year_of_week", channelDF["year_of_week"].cast(LongType()))
channelDF = channelDF.withColumn("week_no", channelDF["week"].cast(LongType()))

channelDF = channelDF.selectExpr("channel" , "week_no", "targets", "regions", "grp" , "share_pct" , "year_of_week").where(~ F.col("channel").like("%(na)%")) #.where(~ F.col("GRP").like("0.00"))


# [Table] Genre Master

genre_master = glueContext.create_dynamic_frame.from_catalog(database="barc_master", table_name="tbl_channel_genre_mst")
genre_masterDF = genre_master.toDF()
#genre_masterDF . show(3)


genre_masterDF = genre_masterDF.selectExpr("channel as channel_temp" , "network", "network1", "genre", "genre_fta_pay","genre_lang" , "genre_consl","genre_movie_others","language")
#genre_masterDF1.show(3)

channelDF = channelDF.join(genre_masterDF, F.upper(channelDF.channel) == F.upper(genre_masterDF.channel_temp),"left_outer")

columns_to_drop = ["channel_temp"]
channelDF = channelDF.drop(*columns_to_drop)
channelDF.createOrReplaceTempView("LA_CHANNEL_TBL")

channelDF.printSchema()

LA_CHANNEL_TBL_FILE='s3://spni-datalake-dev-raw-barc/network_la_analysis/output/LA_CHANNEL_TBL/P_YEAR_OF_WEEK='+ str(YEAR_OF_WEEK) +'/P_WEEK_NO='+ str(WEEK_NO) +'/'
                                                                                                        
channelDF.repartition(1).write.mode("overwrite").parquet(LA_CHANNEL_TBL_FILE)

# [Table] NP LA Channel Genre

genre = glueContext.create_dynamic_frame.from_catalog(database="barc", table_name="network_la_analysis_input_la_genre")
genreDF = genre.toDF()

genreDF = genreDF.withColumn("share_pct", removeSpecialCharactersFun("shr%"))
genreDF = genreDF.withColumn("year_of_week", genreDF["year_of_week"].cast(LongType()))

genreDF = genreDF.selectExpr("channel" , "week as week_no", "regions", "share_pct" , "year_of_week").where(~ F.col("channel").like("%(na)%")).where(~ F.col("share_pct").like("0.00"))


genre_master = glueContext.create_dynamic_frame.from_catalog(database="barc_master", table_name="tbl_channel_genre_mst")
genre_masterDF = genre_master.toDF()
genre_masterDF = genre_masterDF.selectExpr("channel as channel_temp" , "network", "network1", "genre", "genre_fta_pay","genre_lang" , "genre_consl","genre_movie_others","language")


genreDF = genreDF.join(genre_masterDF, genreDF.channel == genre_masterDF.channel_temp,"left_outer")

columns_to_drop = ["channel_temp"]
genreDF = genreDF.drop(*columns_to_drop)

genreDF.createOrReplaceTempView("LA_CHANNEL_TBL_GENRE")


#genreDF.show()

genreDF.printSchema()

LA_CHANNEL_TBL_GENRE_FILE='s3://spni-datalake-dev-raw-barc/network_la_analysis/output/LA_CHANNEL_TBL_GENRE/P_YEAR_OF_WEEK='+ str(YEAR_OF_WEEK) +'/P_WEEK_NO='+ str(WEEK_NO) +'/'
                                                                                                        
genreDF.repartition(1).write.mode("overwrite").parquet(LA_CHANNEL_TBL_GENRE_FILE)
