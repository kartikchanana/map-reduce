package multithreading;

import java.util.*;

//thread class for no-share execution
class no_sharing_thread extends Thread{
    HashMap<String, float[]> results;
    List<String> records;
    //initialize data provided to thread using constructor
    public no_sharing_thread(HashMap<String, float[]> results, List<String> records) {
        this.records = records;
        this.results = results;
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
                if(results.containsKey(station)) {
                    // no lock required as no shared data structure
                    float[] entry = results.get(station);
                    float temperature = Float.parseFloat(tokens[3]);
                    entry[0] = entry[0] + temperature;
                    entry[1]++;
                    results.put(station, entry);
                }
                else {
                    float[] entry = {Float.parseFloat(tokens[3]), 1};
                    results.put(tokens[0], entry);
                }
            }
        }
    }
}
//main class for no-share execution
public class no_sharing {
    public static void main(String[] args) {
        fileReader fr = new fileReader();
        List<String> records = fr.readFile(args[0]);
        int count = records.size();
        int recordCap = count/4;
        int loopcount = 1;
        Long maxTime=0L;//Max time taken for execution
        Long minTime = 0L;//Min time taken for execution
        Long totalTime = 0L;//total execution time for 10 loops
        //Run the sequential execution 10 times
        while(loopcount<=10){
            //create seperate hashmaps for each thread
            HashMap<String, float[]> t1results = new HashMap<>();
            HashMap<String, float[]> t2results = new HashMap<>();
            HashMap<String, float[]> t3results = new HashMap<>();
            HashMap<String, float[]> t4results = new HashMap<>();
            //provide threads with own data structure and subset of input data
            Thread thread1 = new no_sharing_thread(t1results, records.subList(0,recordCap));
            Thread thread2 = new no_sharing_thread(t2results, records.subList(recordCap,recordCap*2));
            Thread thread3 = new no_sharing_thread(t3results, records.subList((recordCap*2),(recordCap*3)));
            Thread thread4 = new no_sharing_thread(t4results, records.subList((recordCap*3),count));
            Long startTime = System.currentTimeMillis();
            thread1.start();
            thread2.start();
            thread3.start();
            thread4.start();
            try {
                //wait for all threads to finish
                thread1.join();
                thread2.join();
                thread3.join();
                thread4.join();
                //final hashmap for results
                HashMap<String, Float> finalresults = new HashMap<>();
                //gather all unique station IDs
                HashSet<String> stations = new HashSet<>(t1results.keySet());
                stations.addAll(t2results.keySet());
                stations.addAll(t3results.keySet());
                stations.addAll(t4results.keySet());
                //iterate over station IDs and calculate average
                for(String stationId : stations){
                    //if exists, get values from all 3 hashmaps for a stationID
                    float[] val1 = t1results.get(stationId);
                    float[] val2 = t2results.get(stationId);
                    float[] val3 = t3results.get(stationId);
                    float[] val4 = t4results.get(stationId);
                    float sum=0;
                    float recordsCount=0;
                    if(val1 != null){ sum += val1[0];recordsCount += val1[1]; }
                    if(val2 != null){ sum += val2[0];recordsCount += val2[1]; }
                    if(val3 != null){ sum += val3[0];recordsCount += val3[1]; }
                    if(val4 != null){ sum += val4[0];recordsCount += val4[1]; }
                    float average = sum/recordsCount;
                    finalresults.put(stationId, average); //final hashmap with station and average TMAX
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
