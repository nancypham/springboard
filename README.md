# Spark Optimization Mini Project

This exercise gives hands-on experience optimizing PySpark code from looking at the physical plan for the query execution and then modifying the query to improve performance.

There are several ways to improve performance of a Spark job:
1. By picking the right operators
2. Reduce the number of shuffles and the amount of data shuffled
3. Tuning Resource Allocation
4. Tuning the Number of Partitions
5. Reducing the Size of Data Structures
6. Choosing Data Formats