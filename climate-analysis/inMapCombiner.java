
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class inMapCombiner {

    //mapper function receives record and outputs stationId and mean temperature string
    public static class TemperatureMapper
            extends Mapper<Object, Text, Text, Text>{

        private HashMap<String, int[]> TempMap;
        private Text record = new Text();

        //setup the global hashmap
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            TempMap = new HashMap<String, int[]>();
        }

        //check valid temperature
        private boolean checkValidTemp(String s){
            try{
                Integer.parseInt(s);
                return true;
            }catch(Exception e){
                return false;
            }
        }

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                record.set(itr.nextToken());
                //split the input record
                String[] tokens = record.toString().split(",");
                String stationID = tokens[0];
                if((tokens[2].equals("TMAX") || tokens[2].equals("TMIN")) && checkValidTemp(tokens[3])){
                    if(tokens[2].equals("TMAX")) {
                        if (TempMap.containsKey(stationID)) { //stationID already present
                            int[] data = TempMap.get(stationID);
                            data[2] += Integer.parseInt(tokens[3]);
                            data[3]++;
                            TempMap.put(stationID, data);
                        } else {//new stationID, set count to 1
                            int[] data = {0, 0, Integer.parseInt(tokens[3]), 1};
                            TempMap.put(stationID, data);
                        }
                    }else if(tokens[2].equals("TMIN")) {
                        if (TempMap.containsKey(stationID)) {
                            int[] data = TempMap.get(stationID);
                            data[0] += Integer.parseInt(tokens[3]);
                            data[1]++;
                            TempMap.put(stationID, data);
                        } else {
                            int[] data = {Integer.parseInt(tokens[3]), 1, 0, 0};
                            TempMap.put(stationID, data);
                        }
                    }
                }
            }
        }

        //cleanup iterates over the global map and outputs stationID and temperature sum string
        protected void cleanup(Context context)
        throws IOException,InterruptedException{
            Iterator<HashMap.Entry<String, int[]>> it = TempMap.entrySet().iterator();
            while(it.hasNext()){
                HashMap.Entry<String, int[]> entry = it.next();
                int[] data = entry.getValue();
                String finalData = String.join(",", Integer.toString(data[0]),Integer.toString(data[1]),Integer.toString(data[2]),Integer.toString(data[3]));
                context.write(new Text(entry.getKey()), new Text(finalData));
            }
        }
    }

    //reducer class receives station ID and temperature sum string,
    //outputs stationId anf temperature mean string
    public static class TemperatureReducer
            extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {

            int minSum = 0;
            int minCount = 0;
            int maxCount = 0;
            int maxSum = 0;

            //iterate over values
            for (Text value : values) {
                String[] data = value.toString().split(",");
                minSum += Integer.parseInt(data[0]);
                minCount += Integer.parseInt(data[1]);
                maxSum += Integer.parseInt(data[2]);
                maxCount += Integer.parseInt(data[3]);
            }
            if(minCount !=0 && maxCount !=0) {
                float meanMax = maxSum / maxCount;
                float meanMin = minSum / minCount;
                String meanMaxStr = Float.toString(meanMax);
                String meanMinStr = Float.toString(meanMin);
                Text result = new Text(meanMaxStr + ", " + meanMinStr);
                context.write(key, result);
            }else{//no record for TMIN/TMAX received
                context.write(key,null);
            }

        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        BasicConfigurator.configure();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: inMapCombiner <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "mean temperature");
        job.setJarByClass(inMapCombiner.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job,
                new Path(otherArgs[otherArgs.length - 1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
