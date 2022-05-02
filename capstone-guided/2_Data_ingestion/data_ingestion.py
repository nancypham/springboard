from datetime import datetime
from typing import List
import json
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


def parse_csv(line: str):
    record_type_pos = 2
    record = line.split(",")
    try:
        if record[record_type_pos] == "T":
            partition = "T"
            trade_dt = record[0]
            arrival_tm = record[1]
            rec_type = record[2]
            symbol = record[3]
            event_tm = record[4]
            event_seq_nb = int(record[5])
            exchange = record[6]
            trade_pr = float(record[7])
            trade_size = int(record[8])
            bid_pr = None
            bid_size = None
            ask_pr = None
            ask_size = None

            event = (partition, trade_dt, arrival_tm, rec_type, symbol, event_tm, event_seq_nb, exchange, trade_pr, trade_size, bid_pr, bid_size, ask_pr, ask_size)
            return event
        elif record[record_type_pos] == "Q":
            partition = "Q"
            trade_dt = record[0]
            arrival_tm = record[1]
            rec_type = record[2]
            symbol = record[3]
            event_tm = record[4]
            event_seq_nb = int(record[5])
            exchange = record[6]
            trade_pr = None
            trade_size = None
            bid_pr = float(record[7])
            bid_size = int(record[8])
            ask_pr = float(record[9])
            ask_size = int(record[10])

            event = (partition, trade_dt, arrival_tm, rec_type, symbol, event_tm, event_seq_nb, exchange, trade_pr, trade_size, bid_pr, bid_size, ask_pr, ask_size)
            return event
    except Exception as e:
        # Save record to dummy event in bad partition
        event = ("B", None, None, None, None, None, None, None, None, None, None, None, None, None,)
        return event

def parse_json(line: str):
    record = json.loads(line)
    record_type = record['event_type']
    try:
        if record_type == "T":
            partition = "T"
            trade_dt = record['trade_dt']
            arrival_tm = record['file_tm']
            rec_type = record['event_type']
            symbol = record['symbol']
            event_tm = record['event_tm']
            event_seq_nb = int(record['event_seq_nb'])
            exchange = record['exchange']
            trade_pr = float(record['price'])
            trade_size = int(record['size'])
            bid_pr = None
            bid_size = None
            ask_pr = None
            ask_size = None

            event = (partition, trade_dt, arrival_tm, rec_type, symbol, event_tm, event_seq_nb, exchange, trade_pr, trade_size, bid_pr, bid_size, ask_pr, ask_size)
            return event
        elif record_type == "Q":
            partition = "Q"
            trade_dt = record['trade_dt']
            arrival_tm = record['file_tm']
            rec_type = record['event_type']
            symbol = record['symbol']
            event_tm = record['event_tm']
            event_seq_nb = int(record['event_seq_nb'])
            exchange = record['exchange']
            trade_pr = None
            trade_size = None
            bid_pr = float(record['bid_pr'])
            bid_size = int(record['bid_size'])
            ask_pr = float(record['ask_pr'])
            ask_size = int(record['ask_size'])

            event = (partition, trade_dt, arrival_tm, rec_type, symbol, event_tm, event_seq_nb, exchange, trade_pr, trade_size, bid_pr, bid_size, ask_pr, ask_size)
            return event
    except Exception as e:
        # Save record to dummy event in bad partition
        event = ("B", None, None, None, None, None, None, None, None, None, None, None, None, None,)
        return event

schema = StructType([
    StructField("partition", StringType(), True),
    StructField("trade_dt", StringType(), True),
    StructField("event_tm", StringType(), True),
    StructField("rec_type", StringType(), True),
    StructField("symbol", StringType(), True),
    StructField("arrival_tm", StringType(), True),
    StructField("event_seq_nb", IntegerType(), True),
    StructField("exchange", StringType()),
    StructField("trade_pr", StringType(), True),
    StructField("trade_size", StringType(), True),
    StructField("bid_pr", FloatType(), True),
    StructField("bid_size", IntegerType(), True),
    StructField("ask_pr", FloatType(), True),
    StructField("ask_size", IntegerType(), True)
    
])

spark = SparkSession.builder.master('local').appName('app').getOrCreate()

spark.conf.set(
    "fs.azure.account.key.<storage-account-name>.blob.core.windows.net",
    "<your-storage-account-access-key>"
)

raw_json = spark.textFile("wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/<path_in_container>")
parsed_json = raw_json.map(lambda line: parse_json(line))
json_data = spark.createDataFrame(parsed_json)

raw_csv = spark.textFile("wasbs://<container-name>@<storage-account-name>.blob.core.windows.net/<path_in_container>")
parsed_csv = raw_csv.map(lambda line: parse_csv(line))
csv_data = spark.createDataFrame(parsed_csv)

json_data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")
csv_data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")