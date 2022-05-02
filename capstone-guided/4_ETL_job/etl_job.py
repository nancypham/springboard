from datetime import datetime, timedelta

from pyspark.sql import SparkSession

spark = SparkSession.builder.master('local').appName('app').getOrCreate()

spark.conf.set(
    "fs.azure.account.key.<storage-account-name>.blob.core.windows.net",
    "<your-storage-account-access-key>"
) # same as line 118-120 from data_ingestion

df = spark.read.parquet("cloud-storage-path/trade/date={}".format("2020-07-29"))

df = spark.sql("select symbol, event_tm, event_seq_nb, trade_pr from trades where trade_dt = '2020-07-29'")

df.createOrReplaceTempView("tmp_trade_moving_avg")

# Calculate 30-min moving average
mov_avg_df = spark.sql("""
select symbol, exchange, event_tm, event_seq_nb, trade_pr, 
AVG(trade_pr) OVER (PARTITION BY (symbol) ORDER BY CAST(event_tm AS timestamp) 
RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr   
from tmp_trade_moving_avg
""")

mov_avg_df.write.saveAsTable("temp_trade_moving_avg")

# Get previous date value
date = datetime.strptime('2020-08-04', '%Y-%m-%d')
prev_date_str = str(date - timedelta(days=1))

df = spark.sql("select symbol, event_tm, event_seq_nb, trade_pr from trades where trade_dt = '{}'".format(prev_date_str))

df.createOrReplaceTempView("tmp_last_trade")

# Calculate last trade price
last_pr_df = spark.sql("""select symbol, exchange, last_pr from (select
symbol, exchange, event_tm, event_seq_nb, trade_pr,
AVG(trade_pr) OVER (PARTITION BY (symbol) ORDER BY CAST(event_tm AS timestamp) 
RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) AS last_pr
FROM tmp_trade_moving_avg) a
""")

mov_avg_df.write.saveAsTable("temp_last_trade")