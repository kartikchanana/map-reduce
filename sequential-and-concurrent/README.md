##Analyze Data with Sequential and Concurrent Java Programs

####Requirements
A. To eliminate the effect of I/O, create a 1-to-1 in-memory copy of the input file as follows: Write
a loader routine that takes an input filename, reads the file, and returns a String[] or List<String>
containing the lines of the file. Do not change the content or order of the lines in any way. This
array or list is the starting point for all of the versions of the program described in B and C.
B. Write five versions of the program. All of them will parse the lines of the file and then calculate
the average TMAX temperature by station. A record will usually have format (StationId, Date,
Type, Reading,…), but, as in any real data set, there could be errors or missing values. (We
recommend you try to find documentation for the data and consult it.) Note that the
computation should ignore all records (e.g., TMIN, PRCP) that are not TMAX records.
1. SEQUENTIAL: Sequential version that calculates the average of the TMAX temperatures by
station Id. Pick a data structure that makes sense for grouping the accumulated
temperatures and count of records by station. We will refer to this as the accumulation
data structure.
For the threaded versions, spawn exactly the maximum number of worker threads that your
processor can schedule concurrently and assign about the same amount of work to each.
2. NO-LOCK: Multi-threaded version that assigns subsets of the input String[] (or
List<String>) for concurrent processing by separate threads. This version should use a
single shared accumulation data structure and should use no locks or synchronization
on it, i.e., it completely ignores any possible data inconsistency due to parallel
execution.
3. COARSE-LOCK: Multi-threaded version that assigns subsets of the input String[] (or
List<String>) for processing by separate threads. This version should also use a single
shared accumulation data structure and can only use the single lock on the entire data
structure. Design your program to ensure (1) correct multithreaded execution and (2)
minimal delays by holding the lock only when absolutely necessary.
4. FINE-LOCK: Multi-threaded version that assigns subsets of the input String[] (or
List<String>) for processing by separate threads. This version should also use a single
shared accumulation data structure, but should lock only the accumulation value
objects and not the whole data structure. Design your program to ensure (1) correct
multithreaded execution and (2) minimal delays by holding the locks only when
absolutely necessary. Try to accomplish this using a data structure which will avoid data
races.
5. NO-SHARING: Per-thread data structure multi-threaded version that assigns subsets of
the input String[] (or List<String>) for processing by separate threads. Each thread
should work on its own separate instance of the accumulation data structure. Hence no
locks are needed. However, you need a barrier to determine when the separate threads
have terminated and then reduce the separate data structures into a single one using
the main thread.

For each of the above versions, use System.currentTimeMillis() within your code to time its
execution. Note that you should not time the file loading routine (i.e., step A above), nor should
you time the printing of results to stdout. Carefully use barriers for multi-threaded code to time
only the record parsing and per station average calculations. Time the execution of your
calculation code 10 times in a loop within the same execution of the program, after loading the
data. Output the average, minimum, and maximum running time observed.
C. Now we want to see what happens when computing a more expensive function. For simplicity,
we use the following trick to simulate this with minimal programming effort: Modify the value
accumulation structure to slow down the updates by executing Fibonacci(17) whenever a
temperature is added to a station's running sum. If this update occurs in a synchronized method
or while holding a lock, this Fibonacci evaluation should also occur in that method / with that
lock. Time all five versions exactly as in B above.
