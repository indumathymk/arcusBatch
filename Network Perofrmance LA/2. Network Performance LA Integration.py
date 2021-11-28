import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import pyspark.sql.functions as F
from pyspark.sql.types import *
import re
import math
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
spark = SparkSession(SparkContext.getOrCreate())
import datetime
from datetime import date

year, week_no, day_of_week = datetime.date.today().isocalendar()

YEAR_OF_WEEK = year
WEEK_NO = week_no - 1

print(WEEK_NO)

glueContext = GlueContext(SparkContext.getOrCreate())

# to verify with historic table
channelH = glueContext.create_dynamic_frame.from_catalog(database="barc", table_name="la_channel_tbl")
channelHDF = channelH.toDF()
#channelHDF.count()

channelHDF = channelHDF.withColumn("network", F.trim(F.col("network")))
channelHDF = channelHDF.withColumn("channel", F.trim(F.col("channel")))
channelHDF = channelHDF.withColumn("genre", F.trim(F.col("genre")))
channelHDF = channelHDF.withColumn("regions", F.trim(F.col("regions")))
channelHDF = channelHDF.withColumn("targets", F.trim(F.col("targets")))
# channelHDF = channelHDF.withColumn("year_of_week", channelHDF["year_of_week"].cast(StringType()))
# channelHDF = channelHDF.withColumn("week_no", channelHDF["week_no"].cast(StringType()))
channelHDF.printSchema()

channelHDF.createOrReplaceTempView("LA_CHANNEL_TBL")

wk = 52 * int(YEAR_OF_WEEK) + int(WEEK_NO)
print(wk)

# NP LA Channel - Network share

tableA = spark.sql("select (case when network1='Star' then network1 else network end ) as network,\
week_no,year_of_week,sum(share_pct) as share_pct \
from LA_CHANNEL_TBL where targets='Universe' and regions='India' \
group by (case when network1='Star' then network1 else network end ),week_no,year_of_week")
tableA.createOrReplaceTempView("tableA")

tableA.show(4)

tableB = spark.sql("select (case when upper(a.network) like 'STAR%' then 'Star' when upper(a.network) like 'SONY%' \
then 'Sony' when upper(a.network) like 'VIACOM%' Then 'Viacom' when upper(a.network) like 'SUN%' then 'Sun' \
when upper(a.network) like 'ZEE%ENTERTAINMENT%' then 'Zee' else a.network end) as network, \
CAST( 52*a.year_of_week+a.week_no as int) as week_no, sum(share_pct) as share_pct from tableA a group by \
(case when upper(a.network) like 'STAR%' then 'Star' when upper(a.network) like 'SONY%' then 'Sony' when upper(a.network) \
like 'VIACOM%' Then 'Viacom' when upper(a.network) like 'SUN%' then 'Sun' when upper(a.network) like 'ZEE%ENTERTAINMENT%' \
then 'Zee' else a.network end),(52*a.year_of_week+a.week_no)")
tableB.createOrReplaceTempView("tableB")

tableD = spark.sql("select network," + str(WEEK_NO) + " as week_no,avg(case when week_no=" + str(wk) + " then CAST(share_pct as DECIMAL(18,7)) end) as mkt_share_cw1,avg(case when week_no between CAST(" + str(wk) + "-4 as int) and CAST(" + str(wk) + "-1 as int) then CAST(share_pct as DECIMAL(18,7)) end) as share_l4w1,avg(case when week_no >=52*(" + str(YEAR_OF_WEEK) + ")+14 then CAST(share_pct as DECIMAL(18,7)) end) as share_ytd1 from tableB as b group by network")
tableD.createOrReplaceTempView("tableD")

networkShareDF1 = spark.sql("select network," + str(WEEK_NO) + " as week_no,mkt_share_cw1,IF(ISNULL(share_l4w1), 0.0, share_l4w1) as share_l4w1,share_ytd1,rank () over (order by d.mkt_share_cw1 desc) as rank from tableD d where (upper(network) ='ZEE' or upper(network) ='STAR' or upper(network) ='VIACOM' or upper(network) ='SUN' or upper(network) ='SONY')")

networkShareDF1.show(4)


def ComputeDateTime(number):
    return math.floor(number * 10 ** 6) / 10 ** 6

ComputeDateTimeFun = F.udf(ComputeDateTime, DoubleType())

networkShareDF1 = networkShareDF1.withColumn("mkt_share_cw", ComputeDateTimeFun("mkt_share_cw1"))
networkShareDF1 = networkShareDF1.withColumn("share_l4w", ComputeDateTimeFun("share_l4w1"))
networkShareDF1 = networkShareDF1.withColumn("share_ytd", ComputeDateTimeFun("share_ytd1"))

columns_to_drop = ['mkt_share_cw1','share_l4w1','share_ytd1']
networkShareDF1 = networkShareDF1.drop(*columns_to_drop)

networkShareDF1.printSchema()

networkShareDF1.show()

LA_NETWORK_SHARE_File='s3://spni-datalake-dev-raw-barc/network_la_analysis/output/LA_NETWORK_SHARE/P_YEAR_OF_WEEK=0000/P_WEEK_NO=00/'
                                                                                                        
networkShareDF1.repartition(1).write.mode("overwrite").parquet(LA_NETWORK_SHARE_File)

# to verify with historic table
channel_shareH = glueContext.create_dynamic_frame.from_catalog(database="barc", table_name="la_channel_tbl_genre")
channel_shareHDF = channel_shareH.toDF()

channel_shareHDF = channel_shareHDF.withColumn("network", F.trim(F.col("network")))
channel_shareHDF = channel_shareHDF.withColumn("channel", F.trim(F.col("channel")))
channel_shareHDF = channel_shareHDF.withColumn("genre", F.trim(F.col("genre")))
channel_shareHDF = channel_shareHDF.withColumn("regions", F.trim(F.col("regions")))
channel_shareHDF = channel_shareHDF.withColumn("targets", F.trim(F.col("targets")))

channel_shareHDF.createOrReplaceTempView("LA_CHANNEL_TBL_GENRE")


# NP LA Channel Genre - Genre share

genreTableA = spark.sql("select t3.regions,(case when t3.network1='Star' then t3.network1 else t3.network end ) as network, (52*t3.year_of_week+t3.week_no) as week_no,sum(t3.share_pct) as share_pct from LA_CHANNEL_TBL_GENRE t3 group by regions,(case when network1='Star' then network1 else network end ),(52*t3.year_of_week+t3.week_no)")
genreTableA.createOrReplaceTempView("genreTableA")

genreTableB = spark.sql("select a.*,sum(a.share_pct) over (partition by a.regions,a.week_no) as total_pct from genreTableA a")
genreTableB = genreTableB.na.drop()
genreTableB.createOrReplaceTempView("genreTableB")

genreTableC = spark.sql("select (case when upper(b.network) like 'STAR%' then 'Star' when upper(b.network) like 'SONY%' then 'Sony' when upper(b.network) like 'VIACOM%' Then 'Viacom' when upper(b.network) like 'SUN%' then 'Sun' when upper(b.network) like 'ZEE%' then 'Zee' else network end) as network, \
avg(case when b.week_no= " + str(wk) + " then CAST(b.share_pct*100/b.total_pct as DECIMAL(18,7)) end) as mkt_share_cw1, \
avg(case when b.week_no between CAST(" + str(wk) + "-4 as int) and CAST(" + str(wk) + "-1 as int) then CAST(b.share_pct as DECIMAL(18,7)) end) as share_l4w1, \
avg(case when b.week_no>=52*( " + str(YEAR_OF_WEEK) + ")+14 then CAST(b.share_pct as DECIMAL(18,7)) end) as share_ytd1 \
from genreTableB b group by (case when upper(b.network) like 'STAR%' then 'Star' when upper(b.network) like 'SONY%' then 'Sony' when upper(b.network) like 'VIACOM%' Then 'Viacom' when upper(b.network) like 'SUN%' then 'Sun' when upper(b.network) like 'ZEE%' then 'Zee'else network end)")

genreTableC.createOrReplaceTempView("genreTableC")

genreTableD = spark.sql("select network,mkt_share_cw1,cast( share_l4w1*100/sum(share_l4w1) over () as DECIMAL(18,7)) as share_l4w1, CAST( share_ytd1*100/sum(share_ytd1) over () as DECIMAL(18,7)) as share_ytd1 from genreTableC c")
genreTableD.createOrReplaceTempView("genreTableD")

genreShare = spark.sql("select d.*, rank () over (order by d.mkt_share_cw1 desc) as rank from genreTableD d where (upper(network) like 'ZEE%' or upper(network) like 'STAR%' or upper(network) like 'VIACOM%' or upper(network) like 'SUN%' or upper(network) like 'SONY%')")
#genreShare.show()

genreShare = genreShare.withColumn("mkt_share_cw", ComputeDateTimeFun("mkt_share_cw1"))
genreShare = genreShare.withColumn("share_l4w", ComputeDateTimeFun("share_l4w1"))
genreShare = genreShare.withColumn("share_ytd", ComputeDateTimeFun("share_ytd1"))

columns_to_drop = ['mkt_share_cw1','share_l4w1','share_ytd1']
genreShare = genreShare.drop(*columns_to_drop)

genreShare.printSchema()

genreShare.show()

genreShare.createOrReplaceTempView("genreShareq")
spark.sql("select sum(share_ytd) from genreShareq").show()

LA_NETWORK_GENRE_SHARE_File='s3://spni-datalake-dev-raw-barc/network_la_analysis/output/LA_NETWORK_GENRE_SHARE/P_YEAR_OF_WEEK=0000/P_WEEK_NO=00/'
                                                                                                        
genreShare.repartition(1).write.mode("overwrite").parquet(LA_NETWORK_GENRE_SHARE_File)

# # NP LA Channel - channel details
channelDetails = spark.sql("select network,(case when upper(genre_fta_pay) = 'SPORTS' then (case \
when upper(channel) in ('SONY SIX(V)','SONY TEN 1','SONY TEN 2','SONY TEN 3','SONY TEN 4(V)') then 'Sony Sports' \
when upper(channel) in ('1SPORTS','EUROSPORT') then 'Other Sports' \
else 'Star Sports Channels' end) else channel end) as channel,genre_fta_pay as genre,\
regions as market,targets as demography,CAST(52*year_of_week+week_no as int) as week_no,sum(grp) grp \
from LA_CHANNEL_TBL  \
where upper(channel) in ('SONY TEN 4(V)','&TV','COLORS','JEET PRIME(V)','LIFE OK(NA)','SONY SAB', \
'SONY ENTERTAINMENT TELEVISION','STAR BHARAT','STAR PLUS', \
'ZEE TV','BIG MAGIC','COLORS RISHTEY','DANGAL','SONY PAL', \
'STAR UTSAV','ZEE ANMOL','ZINDAGI(NA)','&PICTURES','COLORS CINEPLEX', \
'SONY MAX(V)','SONY MAX 2','STAR GOLD 2','STAR GOLD','STAR GOLD SELECT SD', \
'UTV ACTION','UTV MOVIES','ZEE ACTION','ZEE BOLLYWOOD','ZEE CINEMA', \
'ZEE CLASSIC','ABZY MOVIES','B4U KADAK','B4U MOVIES','CINEMA TV INDIA', \
'DABANGG','ENTERR 10','MAHA MOVIE','RISHTEY CINEPLEX','SONY WAH', \
'STAR UTSAV MOVIES','WOW CINEMA ONE','ZEE ANMOL CINEMA','&FLIX','HBO(NA)', \
'MNX','MOVIES NOW','SONY PIX','ROMEDY NOW','STAR MOVIES ACTION(NA)', \
'STAR MOVIES','WB(NA)','ZEE STUDIO(NA)','ANIMAL PLANET','DISCOVERY CHANNEL(V)', \
'DISCOVERY SCIENCE','DTAMIL','HISTORY TV 18(V)','NAT GEO WILD(V)', \
'NATIONAL GEOGRAPHIC(V)','SONY BBC EARTH(V)','DD SPORTS','EUROSPORT','NEO PRIME(NA)', \
'NEO SPORTS(NA)','SONY ESPN(V)','SONY SIX(V)','STAR SPORTS 1','STAR SPORTS 1 HINDI', \
'STAR SPORTS 2(V)','STAR SPORTS 3(V)','STAR SPORTS FIRST','STAR SPORTS SELECT 1 SD', \
'STAR SPORTS SELECT 2 SD','SONY TEN 1','SONY TEN 2','SONY TEN 3','AAKASH AATH','COLORS BANGLA', \
'DD BANGLA','RUPOSHI BANGLA','SONY AATH','STAR JALSHA','SUN BANGLA','ZEE BANGLA','COLORS MARATHI', \
'DD SAHYADRI','SONY MARATHI','STAR PRAVAH','ZEE MARATHI','ZEE YUVA','CARTOON NETWORK(V)', \
'DISNEY CHANNEL(V)','DISNEY JUNIOR(V)','DISNEY XD','DISCOVERY KIDS','HUNGAMA(V)','MAHA CARTOON TV(NA)', \
'NICK JUNIOR(V)','NICK(V)','POGO TV(V)','SONIC NICKELODEON(V)','SONY YAY(V)','TOONAMI(NA)') \
group by network,(case when upper(genre_fta_pay) = 'SPORTS' then (case  \
when UPPER(channel) in ('SONY SIX(V)','SONY TEN 1','SONY TEN 2','SONY TEN 3','SONY TEN 4(V)') then 'Sony Sports' \
when upper(channel) in ('1SPORTS','EUROSPORT') then 'Other Sports' \
else 'Star Sports Channels' end) else channel end), genre_fta_pay,regions,targets,CAST(52*year_of_week+week_no as int)")

channelDetails.createOrReplaceTempView("channelDetails")

channelTableA = spark.sql("select network,channel,genre,market,demography, avg(case when week_no=" + str(wk) +" then grp end) as grp\
,avg(case when week_no between " + str(wk) +"-4 and " + str(wk) +"-1 then grp end) as grp_l4w,\
avg(case when week_no>=52*(" + str(YEAR_OF_WEEK) +")+14 then grp end) as grp_ytd from channelDetails \
group by network,channel,genre,market,demography")

channelTableA.createOrReplaceTempView("channelTableA")

channelTableB = spark.sql("select a.*, (case when upper(channel) in ('&TV','COLORS','JEET PRIME(V)','LIFE OK(NA)','SONY SAB','SONY ENTERTAINMENT TELEVISION','STAR BHARAT','STAR PLUS','ZEE TV') then 1 \
            when upper(channel) in ('BIG MAGIC','COLORS RISHTEY','DANGAL','SONY PAL','STAR UTSAV','ZEE ANMOL','ZINDAGI(NA)') then 2 \
            when upper(channel) in ('&PICTURES','COLORS CINEPLEX','SONY MAX(V)','SONY MAX 2','STAR GOLD 2','STAR GOLD','STAR GOLD SELECT SD','UTV ACTION','UTV MOVIES','ZEE ACTION','ZEE BOLLYWOOD','ZEE CINEMA','ZEE CLASSIC') then 3 \
            when upper(channel) in ('ABZY MOVIES','B4U KADAK','B4U MOVIES','CINEMA TV INDIA','DABANGG','ENTERR 10','MAHA MOVIE','RISHTEY CINEPLEX','SONY WAH','STAR UTSAV MOVIES','WOW CINEMA ONE','ZEE ANMOL CINEMA') then 4 \
            when upper(channel) in ('&FLIX','HBO(NA)','MNX','MOVIES NOW','SONY PIX','ROMEDY NOW','STAR MOVIES ACTION(NA)','STAR MOVIES','WB(NA)','ZEE STUDIO(NA)') then 5 \
            when upper(channel) in ('ANIMAL PLANET','DISCOVERY CHANNEL(V)','DISCOVERY SCIENCE','DTAMIL','HISTORY TV 18(V)','NAT GEO WILD(V)','NATIONAL GEOGRAPHIC(V)','SONY BBC EARTH(V)') then 6 \
            when upper(channel) in ('SONY SPORTS','STAR SPORTS CHANNELS','OTHER SPORTS') then 7 \
            when upper(channel) in ('AAKASH AATH','COLORS BANGLA','DD BANGLA','RUPOSHI BANGLA','SONY AATH','STAR JALSHA','SUN BANGLA','ZEE BANGLA') then 8 \
            when upper(channel) in ('COLORS MARATHI','DD SAHYADRI','SONY MARATHI','STAR PRAVAH','ZEE MARATHI','ZEE YUVA') then 9 \
            when upper(channel) in ('CARTOON NETWORK(V)','DISNEY CHANNEL(V)','DISNEY JUNIOR(V)','DISNEY XD','DISCOVERY KIDS','HUNGAMA(V)','MAHA CARTOON TV(NA)','NICK JUNIOR(V)','NICK(V)','POGO TV(V)', \
            'SONIC NICKELODEON(V)','SONY YAY(V)','TOONAMI(NA)') then 10 end) as Group_ch from channelTableA a")

channelTableB.createOrReplaceTempView("channelTableB")

channalTableC = spark.sql("select b.*,RANK() OVER (PARTITION BY b.group_ch,b.MARKET, b.demography ORDER BY b.GRP DESC) as RANK from channelTableB b")
channalTableC.createOrReplaceTempView("channalTableC")
channelDetails = spark.sql("select c.network,c.channel,c.genre,c.market,c.demography,IF(ISNULL(c.grp), 0, CAST(c.grp as DECIMAL(18,7))) as grp1,IF(ISNULL(c.grp_l4w), 0, CAST(c.grp_l4w as DECIMAL(18,7))) as grp_l4w1,IF(ISNULL(c.grp_ytd), 0, CAST(c.grp_ytd as DECIMAL(18,7))) as grp_ytd1,c.rank," + str(WEEK_NO) + " as week_no from channalTableC c")
channelDetails = channelDetails.withColumn("update_date",F.current_timestamp())

channelDetails = channelDetails.withColumn("grp_ytd", ComputeDateTimeFun("grp_ytd1"))
channelDetails = channelDetails.withColumn("grp", ComputeDateTimeFun("grp1"))
channelDetails = channelDetails.withColumn("grp_l4w", ComputeDateTimeFun("grp_l4w1"))

columns_to_drop = ['grp_ytd1','grp1','grp_l4w1']
channelDetails = channelDetails.drop(*columns_to_drop)

channelDetails.printSchema()

channelDetails.count()

LA_CHANNEL_DETAILS_FILE='s3://spni-datalake-dev-raw-barc/network_la_analysis/output/LA_CHANNEL_DETAILS/P_YEAR_OF_WEEK=0000/P_WEEK_NO=00/'
                                                                                                        
channelDetails.repartition(1).write.mode("overwrite").parquet(LA_CHANNEL_DETAILS_FILE)
