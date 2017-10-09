package multithreading;

import java.util.*;

//Main class for sequential execution
public class sequential {

	private boolean checkValidTemp(String s){
		try{
			Float.parseFloat(s);
			return true;
		}catch (Exception e){
			return false;
		}
	}
	private Long execute_sequential(List<String> records){
		//hashmap where key=stationID, value = array[2] containing a[0]=sum of TMAX, a[1]=count of values
		HashMap<String, float[]> tmaxResults = new HashMap<>();
		//final hashmap having key=stationID, value=average TMAX
		HashMap<String, Float> finalResults = new HashMap<>();
		long startTime = System.currentTimeMillis();
		//iterate over records
		for (String record : records) {
			String[] tokens = record.split(",");
			//Get TMAX records and check for invalid or null temperature values
			if (tokens[2].equals("TMAX") && checkValidTemp(tokens[3])) {
				String station = tokens[0];
				//if stationID exists
				if (tmaxResults.containsKey(station)) {
					float[] entry = tmaxResults.get(station);
					float temperature = Float.parseFloat(tokens[3]);
					entry[0] = entry[0] + temperature;
					entry[1]++;
					tmaxResults.put(station, entry);
				} else {
					//create new entry
					float[] entry = {Float.parseFloat(tokens[3]), 1};
					tmaxResults.put(tokens[0], entry);
				}
			}
		}
		//List of unique station IDS
		Set<String> stations = tmaxResults.keySet();
		//iterate over uniques station IDs and create final hashmap
		for (String stationID : stations) {
			float[] entry = tmaxResults.get(stationID);
			float average = entry[0] / entry[1];
			finalResults.put(stationID, average);
		}
		Long endTime = System.currentTimeMillis();
		return (endTime-startTime);
	}

	public static void main(String[] args) {
		fileReader fr = new fileReader();
		List<String> records = fr.readFile(args[0]);
		int loopcount = 1;
		Long maxTime = 0L;//Max time taken for execution
		Long minTime = 0L;//Min time taken for execution
		Long totalTime = 0L;//total execution time for 10 loops
		//Run the sequential execution 10 times
		while (loopcount <= 10) {
			sequential sb = new sequential();
			Long timeTaken = sb.execute_sequential(records);
			totalTime+=timeTaken;
			//calculating min and maxtime taken
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
			loopcount++;
			System.out.println("timeTaken: " + timeTaken);
		}
		System.out.println("minTime: " + minTime);
		System.out.println("maxTime: " + maxTime);
		double avgTime = totalTime/10.0;
		System.out.println("avgTime: " + avgTime);
	}
}
