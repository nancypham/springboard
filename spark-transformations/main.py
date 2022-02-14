from pyspark import SparkContext
from operator import add

def extract_vin_key_value(line: str):
    """
    Input: A record line
    Output: key: vin_number, value: make, year, incident type
    """
    record = line.strip().split(',')
    key = record[2].strip()
    value = record[3].strip(), record[5].strip(), record[1].strip()
    return [key, value]

def populate_make(record: list):
    """
    Input: List of values for each record
    Output: Accident records with make and year
    """
    accident_records = []

    for item in record:
        if item[0] != '':
            make, year = item[0], item[1]
        if item[2] == 'A':
            accident_records.append((make, year, item[2]))    
    
    return accident_records

def extract_make_key_value(record: list):
    """
    Input: List of values for each record
    Output: key: make-year, value: 1
    """
    key = str(record[0].strip() + record[1].strip())
    value = 1

    return [key, value]

def create_accident_report(file):
    """
    Input: File of all vehicle records
    Output: Count of vehicles that have been in an accident by make/year
    """
    # Step 1: Filter out accident incidents with make and year
    vin_kv = file.map(lambda x: extract_vin_key_value(x))
    enhance_make = vin_kv.groupByKey().flatMap(lambda kv: populate_make(kv[1]))

    # Step 2: Count number of occurrence for accidents for the vehicle make and year
    make_kv = enhance_make.map(lambda x: extract_make_key_value(x))

    # Aggregate the key and count the number of records in total per key
    count_kv = make_kv.reduceByKey(add)

    return count_kv

if __name__ == '__main__':
    sc = SparkContext("local", "My Application")
    raw_rdd = sc.textFile("data.csv")
    report = create_accident_report(raw_rdd)
    report.saveAsTextFile("accident_report")