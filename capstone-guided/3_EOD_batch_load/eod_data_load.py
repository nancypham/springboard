from pyspark.sql import SparkSession

def applyLatest(df):
    most_recent = df.groupBy('trade_dt').agg(max('file_tm'))
    return most_recent

spark = SparkSession.builder.master('local').appName('app').getOrCreate()

spark.conf.set(
    "fs.azure.account.key.<storage-account-name>.blob.core.windows.net",
    "<your-storage-account-access-key>"
) # same as line 118-120 from data_ingestion

# Populate trade dataset
trade_common = spark.read.parquet('output_dir/partition=T')

trade = trade_common.select('trade_dt', 'symbol', 'exchange', 'event_tm',
'event_seq_nb', 'file_tm', 'trade_pr')

trade_corrected = applyLatest(trade)

trade_date = '2020-07-29'
trade_corrected.write.parquet('cloud-storage-path/trade/trade_dt={}'.format(trade_date))

# Populate quote dataset
quote_common = spark.read.parquet('output_dir/partition=Q')

quote = quote_common.select('trade_dt', 'symbol', 'exchange', 'event_tm',
'event_seq_nb', 'file_tm', 'bid_price', 'bid_size', 'ask_price', 'ask_size')

quote_corrected = applyLatest(quote)

quote_date = '2020-07-29'
quote_corrected.write.parquet('cloud-storage-path/trade/trade_dt={}'.format(quote_date))