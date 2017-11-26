# Climate Analysis in MapReduce using Secondary Sort

### Requirements
1. Create time series of temperature data. Using 10 years of input data
(1880.csv to 1889.csv), calculate mean minimum temperature and mean maximum
temperature, by station, by year. 

1. Use the secondary sort design pattern to minimize the
amount of memory utilized during Reduce function execution. (Do not tinker with data types,
e.g., short versus long integers, but focus on exploiting sort order in the Reduce function input
list to reduce the need for many local variables.) Memory-inefficient solutions will lose points.

    Reducer Output Format (lines do not have to be sorted by StationID):
    ```
    StationIda, [(1880, MeanMina0, MeanMaxa0), (1881, MeanMina1, MeanMaxa1) … (1889 …)]
    StationIdb, [(1880, MeanMinb0, MeanMaxb0), (1881, MeanMinb1, MeanMaxb1) … (1889 …)]
    ```
