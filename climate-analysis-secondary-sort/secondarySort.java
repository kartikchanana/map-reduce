import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.StringTokenizer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;


public class secondarySort {

    //custom object stores stationID and year
    //also implements compareTo where smaller stationID is returned first
    //if same the smaller year is returned
    public static class stationData implements WritableComparable<stationData>{
        private Text stationID;
        private IntWritable year;

        //Default constructor
        public stationData(){
            this.stationID = new Text();
            this.year = new IntWritable();
        }

        //standard getter setter functions
        public Text getStationID(){
            return this.stationID;
        }
        
        public IntWritable getYear(){
            return this.year;
        }

        public void set(Text id, IntWritable year){
            this.stationID = id;
            this.year = year;
        }

        public void readFields(DataInput in) throws IOException{
            stationID.readFields(in);
            year.readFields(in);
        }

        public void write(DataOutput out) throws IOException{
            stationID.write(out);
            year.write(out);
        }

        public int compareTo(stationData t) {
            int compareValue = this.stationID.compareTo(t.getStationID());
            if (compareValue == 0) {
                compareValue = year.compareTo(t.getYear());
            }
             return compareValue;    // sort ascending
        }
    }


    //mapper class receives records, outputs stationData object and temperature string
    public static class TemperatureMapper
            extends Mapper<Object, Text, stationData, Text>{

        //check if temperature is integer
        private boolean checkValidTemp(String s){
            try{
                Integer.parseInt(s);
                return true;
            }catch(Exception e){
                return false;
            }
        }

        private stationData sd = new stationData();
        private Text record = new Text();

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {

            int minT = 0;
            int maxT = 0;
            int minCt = 0;
            int maxCt = 0;
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                record.set(itr.nextToken());
                String[] tokens = record.toString().split(",");
                Text stationID = new Text(tokens[0]);
                int year = Integer.parseInt(tokens[1].substring(0,4));
                sd.set(stationID, new IntWritable(year));
                if((tokens[2].equals("TMAX") || tokens[2].equals("TMIN")) && checkValidTemp(tokens[3])){
                    if(tokens[2].equals("TMAX")){
                        maxT = Integer.parseInt(tokens[3]);
                        maxCt = 1;
                    }else if(tokens[2].equals("TMIN")){
                        minT = Integer.parseInt(tokens[3]);
                        minCt = 1;
                    }
                    Text tempData = new Text(String.join(",", Integer.toString(minT), Integer.toString(minCt), Integer.toString(maxT), Integer.toString(maxCt)));
                    context.write(sd, tempData);
                }
            }
        }
    }

    //hash partitioner that partitions based on stationId
    public static class StationPartitioner
            extends Partitioner<stationData, Text> {

        @Override
        public int getPartition(stationData key, Text value, int numPartitions) {
            // multiply by 127 to perform some mixing
            int hash= key.getStationID().hashCode();
            return Math.abs(hash * 127) % numPartitions;
        }
    }

    //Key comparator the compares the stationData as key, smaller stationID is returned first
    //if same the smaller year is returned
    public static class KeyComparator extends WritableComparator {
        public KeyComparator() {
            super(stationData.class, true);
        }
        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            stationData sd1 = (stationData) w1;
            stationData sd2 = (stationData) w2;
            int cmp =sd1.getStationID().compareTo(sd2.getStationID());
            if (cmp == 0) {
                return sd1.getYear().compareTo(sd2.getYear());
            }
            return cmp;
        }
    }

    //grouping comparator groups based on natural key i.e. stationID
    public static class GroupingComparator extends WritableComparator {
        public GroupingComparator() {
            super(stationData.class, true);
        }


        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            stationData key1 = (stationData) w1;
            stationData key2 = (stationData) w2;
            return key1.getStationID().compareTo(key2.getStationID());
        }
    }

    //reducer receives all data for a particular station grouped by grouping comparator
    //the year is also sorted in ascending order
    public static class TemperatureReducer
            extends Reducer<stationData,Text,Text,NullWritable> {

        public void reduce(stationData key, Iterable<Text> values,
                           Context context
        ) throws IOException, InterruptedException {
            System.err.println("in reducer");
            int minSum = 0;
            int minCount = 0;
            int maxCount = 0;
            int maxSum = 0;

            //get the year value
            int prevYear = key.getYear().get();
            //start building output string
            String stationOutput = key.getStationID().toString() + ", [";
            //iterate over values
            for(Text value: values) {
                String data = value.toString();
                if(prevYear == key.getYear().get()) { //another value for same year
                    String[] tokens = data.toString().split(",");
                    minSum += Integer.parseInt(tokens[0]);
                    minCount += Integer.parseInt(tokens[1]);
                    maxSum += Integer.parseInt(tokens[2]);
                    maxCount += Integer.parseInt(tokens[3]);
                }else{//new year encountered, calculate average and appen to the string
                    if(minCount !=0 && maxCount !=0){
                        float meanMax = maxSum/maxCount;
                        float meanMin = minSum/minCount;
                        String meanMaxStr = Float.toString(meanMax);
                        String meanMinStr = Float.toString(meanMin);
                        stationOutput += "(" + prevYear + ", " + meanMinStr + ", " + meanMaxStr + "), ";
                        prevYear = key.getYear().get(); //set preYear to the new year
                        String[] tokens = data.toString().split(",");
                        //reset values for the new year encountered
                        minSum = Integer.parseInt(tokens[0]);
                        minCount = Integer.parseInt(tokens[1]);
                        maxSum = Integer.parseInt(tokens[2]);
                        maxCount = Integer.parseInt(tokens[3]);
                    }
                }
            }
            //emit output after iterating over all values
            if(prevYear == key.getYear().get()){
                if(minCount !=0 && maxCount !=0) {
                    float meanMax = maxSum / maxCount;
                    float meanMin = minSum / minCount;
                    String meanMaxStr = Float.toString(meanMax);
                    String meanMinStr = Float.toString(meanMin);
                    stationOutput += "(" + prevYear + " ," + meanMinStr + ", " + meanMaxStr + ")]";
                }
            }
            context.write(new Text(stationOutput), NullWritable.get());
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        BasicConfigurator.configure();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: secondarySort <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "mean temperature");
        job.setJarByClass(secondarySort.class);
        job.setMapperClass(TemperatureMapper.class);
//        job.setPartitionerClass(StationPartitioner.class);
//        job.setSortComparatorClass(KeyComparator.class);
        job.setGroupingComparatorClass(GroupingComparator.class);
        job.setReducerClass(TemperatureReducer.class);
        job.setMapOutputKeyClass(stationData.class);
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
