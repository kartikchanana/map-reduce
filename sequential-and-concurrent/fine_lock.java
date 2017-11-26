package multithreading;

import java.util.*;

//thread class for fine-lock execution
class fine_lock_thread extends Thread{
    HashMap<String, float[]> tmaxresults;
    List<String> records;
    //initialize data passed to thread in constructor
    public fine_lock_thread(HashMap<String, float[]> tmaxresults, List<String> records) {
        this.records = records;
        this.tmaxresults = tmaxresults;
    }

    //function to check valid temperature value for record
    private boolean checkValidTemp(String s){
        try{
            Float.parseFloat(s);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    @Override
    public void run() {
        for(String record : records) {
            String[] tokens = record.split(",");
            if(tokens[2].equals("TMAX") && checkValidTemp(tokens[3])) {
                String station = tokens[0];
                if(tmaxresults.containsKey(station)) {
                    float[] entry = tmaxresults.get(station);
                    //obtain lock only over the particular record
                    synchronized(entry){
                        float temperature = Float.parseFloat(tokens[3]);
                        entry[0] = entry[0] + temperature;
                        entry[1]++;
                    }
                    //release lock
                }
                else {
                    //lock entire data structure to avoid inconsistent data, because multiple thread writes for a new but the same station Id
                    // will result in one or more records missing
                    synchronized(tmaxresults){
                        float[] entry = {Float.parseFloat(tokens[3]), 1};
                        tmaxresults.put(tokens[0], entry);
                    }
                }
            }
        }
    }
}

//main class for fine-lock execution
public class fine_lock {
    public static void main(String[] args){
    fileReader fr = new fileReader();
    List<String> records = fr.readFile(args[0]);
        int loopcount = 1;
        Long maxTime=0L;//Max time taken for execution
        Long minTime = 0L;//Min time taken for execution
        Long totalTime = 0L;//total execution time for 10 loops
        //Run the sequential execution 10 times
        while(loopcount<=10) {
            //create shared data-structure for all threads to store results
            HashMap<String, float[]> tmaxResults = new HashMap<>();
            //final hashmap
            HashMap<String, Float> finalResults = new HashMap<>();
            int count = records.size();
            int recordCap = count / 4;
            //provide all threads with subset of data and shared data structure
            Thread thread1 = new fine_lock_thread(tmaxResults, records.subList(0, recordCap));
            Thread thread2 = new fine_lock_thread(tmaxResults, records.subList(recordCap, recordCap * 2));
            Thread thread3 = new fine_lock_thread(tmaxResults, records.subList((recordCap * 2), (recordCap * 3)));
            Thread thread4 = new fine_lock_thread(tmaxResults, records.subList((recordCap * 3), count));
            Long startTime = System.currentTimeMillis();
            thread1.start();
            thread2.start();
            thread3.start();
            thread4.start();
            try {
                //wait for all threads to join
                thread1.join();
                thread2.join();
                thread3.join();
                thread4.join();
                Set<String> stations = tmaxResults.keySet();
                //iterate over all stations and calculate average
                for (String stationID : stations) {
                    float[] entry = tmaxResults.get(stationID);
                    float average = entry[0] / entry[1];
                    finalResults.put(stationID, average);
                }
                Long endTime = System.currentTimeMillis();
                Long timeTaken = endTime - startTime;
                totalTime+=timeTaken;
                //calculate min and max time taken
                if(loopcount==1){
                    minTime=timeTaken;
                    maxTime=timeTaken;
                }else{
                    if(timeTaken<=minTime){
                        minTime=timeTaken;
                    }else if(timeTaken>maxTime){
                        maxTime=timeTaken;
                    }
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            loopcount++;
        }
        System.out.println("minTime: " + minTime);
        System.out.println("maxTime: " + maxTime);
        double avgTime = totalTime/10.0;
        System.out.println("avgTime: " + avgTime);
    }
}
