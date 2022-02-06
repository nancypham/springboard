# Vehicle History MapReduce Hadoop Project

## Overview
Utilize data from an automobile tracking platform that tracks the history of important incidents after the initial sale of a new vehicle. Such incidents include subsequent private sales, repairs, and accident reports. The platform provides a good reference for second-hand buyers to understand the vehicles they are interested in.

## Input
A .csv dataset with a history report of various vehicles.

## Output
A MapReduce program to produce a report of the total number of accidents per make and
year of the car.

## Shell scripts for execution
### Bash
```
cat data.csv | python src/autoinc_mapper1.py | sort | python src/autoinc_reducer1.py | python src/autoinc_mapper2.py | sort | python src/autoinc_reducer2.py
```

### Hadoop
```
hadoop jar /usr/local/hadoop/contrib/streaming/hadoop-*streaming*.jar \
-file autoinc_mapper1.py -mapper autoinc_mapper1.py \
-file autoinc_reducer1.py -reducer autoinc_reducer1.py \
-input input/data.csv -output output/all_accidents

hadoop jar /usr/local/hadoop/contrib/streaming/hadoop-*streaming*.jar \
-file autoinc_mapper2.py -mapper autoinc_mapper2.py \
-file autoinc_reducer2.py -reducer autoinc_reducer2.py \
-input output/all_accidents -output output/make_year_count
```