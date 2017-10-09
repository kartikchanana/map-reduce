import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class noCombiner {

    //custom WritableComparable object that stores type of temperature reading(TMAX/TMIN) and the value
    public static class tempData implements WritableComparable<tempData>{
        private Text type;
        private IntWritable temperature;

        //Default constructor
        public tempData(){
            this.type = new Text();
            this.temperature = new IntWritable();
        }

        //Standard getter functions
        public Text getType(){
            return this.type;
        }

        public IntWritable getTemp(){
            return this.temperature;
        }

        //Setter function
        public void set(Text t, IntWritable i){
            this.type = t;
            this.temperature = i;
        }

        //compulsory implementations of Writable Interface
        public void readFields(DataInput in) throws IOException{
            type.readFields(in);
            temperature.readFields(in);
        }

        public void write(DataOutput out) throws IOException{
            type.write(out);
            temperature.write(out);
        }

        public int compareTo(tempData t) {
            return 0;
        }
    }


    //Mapper class that takes in input text, and outputs stationID string as key,
    // and custom temperature object as value
    public static class TemperatureMapper
            extends Mapper<Object, Text, Text, tempData>{

        //checking if temperature is integer
        private boolean checkValidTemp(String s){
            try{
                Integer.parseInt(s);
                return true;
            }catch(Exception e){
                return false;
            }
        }

        private tempData td = new tempData();
        private IntWritable temperature = new IntWritable();
        private Text type = new Text();
        private Text record = new Text();

        //mapper function that outputs the key,value pair
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                record.set(itr.nextToken());
                String[] tokens = record.toString().split(",");
                Text stationID = new Text(tokens[0]);

                //only accept if it has TMAX or TMIN value
                if((tokens[2].equals("TMAX") || tokens[2].equals("TMIN")) && checkValidTemp(tokens[3])){
                    type.set(tokens[2]);
                    temperature.set(Integer.parseInt(tokens[3]));
                    td.set(type, temperature);
                    //write the key, value where key=stationID, value = custom temperature object
                    context.write(stationID, td);
                }
            }
        }
    }

    //reducer class receives stationID and temperature object a key
    //Output stationID and mean values as joint string
    public static class TemperatureReducer
            extends Reducer<Text,tempData,Text,Text> {

        public void reduce(Text key, Iterable<tempData> values,
                           Context context
        ) throws IOException, InterruptedException {

            int minSum = 0;
            int minCount = 0;
            int maxCount = 0;
            int maxSum = 0;

            //iterate over all values for a key i.e. station
            for (tempData data : values) {
                if(data.getType().toString().equals("TMAX")){
                    maxSum += data.getTemp().get();
                    maxCount++;
                }else if(data.getType().toString().equals("TMIN")){
                    minSum += data.getTemp().get();
                    minCount++;
                }
            }
            //calculate mean after all values are considered
            if(minCount !=0 && maxCount !=0) {
                float meanMax = maxSum / maxCount;
                float meanMin = minSum / minCount;
                String meanMaxStr = Float.toString(meanMax);
                String meanMinStr = Float.toString(meanMin);
                Text result = new Text(meanMaxStr + ", " + meanMinStr);
                //write final output
                context.write(key, result);
            }
            else{
                // TMIN or TMAX records found, hence null
                context.write(key, null);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        BasicConfigurator.configure();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: noCombiner <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "noCombiner");
        //main class
        job.setJarByClass(noCombiner.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setReducerClass(TemperatureReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(tempData.class);
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
