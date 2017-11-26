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


public class withCombiner {

    //custom data object which stores 4 integer values TMINsum, TMINcount, TMAXsum, TMAXcount
    public static class tempData implements WritableComparable<tempData>{
        private IntWritable minTemp;
        private IntWritable maxTemp;
        private IntWritable minCount;
        private IntWritable maxCount;

        //Default constructor
        public tempData(){
            this.minTemp = new IntWritable();
            this.maxTemp = new IntWritable();
            this.minCount = new IntWritable();
            this.maxCount = new IntWritable();
        }

        public IntWritable getMinCount(){
            return this.minCount;
        }

        public IntWritable getMaxCount(){
            return this.maxCount;
        }

        public IntWritable getMinTemp(){
            return this.minTemp;
        }

        public IntWritable getMaxTemp(){
            return this.maxTemp;
        }

        public void set(IntWritable min, IntWritable max, IntWritable minCt, IntWritable maxCt){
            this.minTemp = min;
            this.maxTemp = max;
            this.minCount = minCt;
            this.maxCount = maxCt;
        }

        public void readFields(DataInput in) throws IOException{
            minTemp.readFields(in);
            maxTemp.readFields(in);
            minCount.readFields(in);
            maxCount.readFields(in);
        }

        public void write(DataOutput out) throws IOException{
            minTemp.write(out);
            maxTemp.write(out);
            minCount.write(out);
            maxCount.write(out);
        }

        public int compareTo(tempData t) {
            return 1;
        }
    }


    //Mapper class that receives input record and outputs stationID, tempData object
    public static class TemperatureMapper
            extends Mapper<Object, Text, Text, tempData>{

        //check if temperature received is valid
        private boolean checkValidTemp(String s){
            try{
                Integer.parseInt(s);
                return true;
            }catch(Exception e){
                return false;
            }
        }
        private tempData td = new tempData();
        private Text record = new Text();

        //map function
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            IntWritable minT = new IntWritable(0);
            IntWritable maxT = new IntWritable(0);
            IntWritable minCt = new IntWritable(0);
            IntWritable maxCt = new IntWritable(0);
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                record.set(itr.nextToken());
                //split input record
                String[] tokens = record.toString().split(",");
                Text stationID = new Text(tokens[0]);
                if((tokens[2].equals("TMAX") || tokens[2].equals("TMIN")) && checkValidTemp(tokens[3])){
                    if(tokens[2].equals("TMAX")){
                        maxT.set(Integer.parseInt(tokens[3]));
                        maxCt.set(1);
                    }else if(tokens[2].equals("TMIN")){
                        minT.set(Integer.parseInt(tokens[3]));
                        minCt.set(1);
                    }
                    //set input values into output custom object
                    td.set(minT, maxT, minCt, maxCt);
                    context.write(stationID, td);
                }
            }
        }
    }

    //combiner class receives stationID and list of tempData
    //outputs stationID and single tempData entry after combining
    public static class TemperatureCombiner
            extends Reducer<Text,tempData,Text,tempData> {

        public void reduce(Text key, Iterable<tempData> values,
                           Context context
        ) throws IOException, InterruptedException {

            int minSum = 0;
            int minCount = 0;
            int maxCount = 0;
            int maxSum = 0;

            //iterate over all values
            for (tempData data : values) {
                minSum += data.getMinTemp().get();
                maxSum += data.getMaxTemp().get();
                minCount += data.getMinCount().get();
                maxCount += data.getMaxCount().get();
            }
            tempData td = new tempData();
            td.set(new IntWritable(minSum), new IntWritable(maxSum), new IntWritable(minCount), new IntWritable(maxCount));
            context.write(key, td);
        }
    }


    //reducer class receives stationID and list of tempData,
    // outputs stationID and mean temperature string
    public static class TemperatureReducer
            extends Reducer<Text,tempData,Text,Text> {

        public void reduce(Text key, Iterable<tempData> values,
                           Context context
        ) throws IOException, InterruptedException {

            int minSum = 0;
            int minCount = 0;
            int maxCount = 0;
            int maxSum = 0;

            //iterate over values
            for (tempData data : values) {
                minSum += data.getMinTemp().get();
                maxSum += data.getMaxTemp().get();
                minCount += data.getMinCount().get();
                maxCount += data.getMaxCount().get();
            }
            //if no entry for staionID received
            if(minCount !=0 && maxCount !=0) {
                float meanMax = maxSum / maxCount;
                float meanMin = minSum / minCount;
                String meanMaxStr = Float.toString(meanMax);
                String meanMinStr = Float.toString(meanMin);
                Text result = new Text(meanMaxStr + ", " + meanMinStr);
                context.write(key, result);
            }else{
                context.write(key, null);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        BasicConfigurator.configure();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: withCombiner <in> [<in>...] <out>");
            System.exit(2);
        }
        Job job = Job.getInstance(conf, "mean temperature");
        //set combiner class
        job.setJarByClass(withCombiner.class);
        job.setMapperClass(TemperatureMapper.class);
        job.setCombinerClass(TemperatureCombiner.class);
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
