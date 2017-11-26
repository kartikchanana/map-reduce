
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

public class sortNodeRanks {

    //mapper class that takes input and outputs the local top 100 nodes
    public static class mapper extends
            Mapper<Object, Text, NullWritable, Text> {
        // Our output key and value Writables
        private TreeMap<Double, Text> localTop = new TreeMap<Double, Text>();

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String[] tokens = value.toString().split("\t");
            String node = tokens[0];
            double rank = Double.parseDouble(tokens[1]);

            // Get will return null if the key is not there
            if (node == null) {
                // skip this record
                return;
            }
            //output string containing page name and page rank to globally sort in reducer
            StringBuilder sb = new StringBuilder();
            sb.append(node);
            sb.append(" " ).append(String.valueOf(rank));
            localTop.put(rank, new Text(sb.toString()));

            if (localTop.size() > 100) {
                localTop.remove(localTop.firstKey());
            }
        }

        //emit local top 100
        @Override
        protected void cleanup(Context context) throws IOException,
                InterruptedException {
            for (Text t : localTop.values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }

    //reducer class that takes all local pages and outputs final top 100 and their page ranks
    public static class reducer extends
            Reducer<NullWritable, Text, NullWritable, Text> {

        private TreeMap<Double, Text> finalTop = new TreeMap<Double, Text>();

        @Override
        public void reduce(NullWritable key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException {
            for (Text value : values) {
                String[] tokens = value.toString().split(" ");
                String node = tokens[0];
                double rank = Double.parseDouble(tokens[1]);

                finalTop.put(rank, new Text(node+ " " + String.valueOf(rank)));

                if (finalTop.size() > 100) {
                    finalTop.remove(finalTop.firstKey());
                }
            }

            for (Text t : finalTop.descendingMap().values()) {
                context.write(NullWritable.get(), t);
            }
        }
    }
}
