## Climate Analysis in MapReduce

#### Requirements
1. Task 1: Write three MapReduce programs that calculate the mean minimum
temperature and the mean maximum temperature, by station, for a single year of data.

    a. NoCombiner: This program should have a Mapper and a Reducer class with no custom
    setup or cleanup methods, and no Combiner or custom Partitioner.

    b. Combiner: This version of the program should use a Combiner. Define the Combiner in
    the best possible way you can think of.

    c. InMapperComb: This version of the program should use in-mapper combining to reduce
    data traffic from Mappers to Reducers. Think of the best possible way to achieve this
    and make sure the Combiner is disabled.

    Reducer Output Format (lines do not have to be sorted by StationID):
    ```
    StationId0, MeanMinTemp0, MeanMaxTemp0
    StationId1, MeanMinTemp1, MeanMaxTemp1
    …
    StationIdn, MeanMinTempn, MeanMinTempn
    ```

2. Task 2: Create time series of temperature data. Using 10 years of input data
(1880.csv to 1889.csv), calculate mean minimum temperature and mean maximum
temperature, by station, by year. Use the secondary sort design pattern to minimize the
amount of memory utilized during Reduce function execution. (Do not tinker with data types,
e.g., short versus long integers, but focus on exploiting sort order in the Reduce function input
list to reduce the need for many local variables.) Memory-inefficient solutions will lose points.
Reducer Output Format (lines do not have to be sorted by StationID):
    ```
    StationIda, [(1880, MeanMina0, MeanMaxa0), (1881, MeanMina1, MeanMaxa1) … (1889 …)]
    StationIdb, [(1880, MeanMinb0, MeanMaxb0), (1881, MeanMinb1, MeanMaxb1) … (1889 …)]
    ```