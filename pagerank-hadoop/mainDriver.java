import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.BasicConfigurator;

import javax.naming.ConfigurationException;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;

//Main driver class to run all three jobs
public class mainDriver {
    //Local variable for total page count
    public static Long pageCount;


    public static void main(String[] args) throws Exception {
        //Configuration object for all jobs
        Configuration conf = new Configuration();
        BasicConfigurator.configure();

        mainDriver md = new mainDriver();
        //run input pasrser job number one
        Path parsedInput = md.parseInput(conf, args);

        //run pagerank job number 2
        Path pageRankOutput = md.pageRankDriver(conf,parsedInput, args[1]);

        md.sortPages(conf, pageRankOutput, args[1]);
    }




    //wiki parser job number one
    public Path parseInput(Configuration conf, String[] args) throws InterruptedException, IOException, ClassNotFoundException {

        File inputFile = new File(args[0]);

        Job job = Job.getInstance(conf, "parser");
        job.setJarByClass(mainDriver.class);
        job.setMapperClass(parser.Map.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        //create sub directory for output
        Path outputPath = new Path(args[1], "adjacencyList");
        FileOutputFormat.setOutputPath(job, outputPath);
        job.waitForCompletion(true);
        //update the total page count after parser job completion
        pageCount = job.getCounters().findCounter(nodesCounter.nodesCount).getValue();
        //return path to use as input for next job
        return outputPath;
    }


    //sub driver that calls driver program for page rank
    public Path pageRankDriver(Configuration conf, Path ip, String op) throws Exception {

        int iter = 1;
        Path outputPath = new Path(op, "PageRankOutput");
        Path inputPath = new Path(outputPath, "initialPageRank");
        //create the input file with initial page ranks set for all nodes
        createInputFile(conf, ip, inputPath);

        while(iter<=10){
            Path jobOutputPath = new Path(outputPath, String.valueOf(iter));
            //page rank job function called 10 times
            calcPageRank(conf, inputPath, jobOutputPath);
            //reset input path for next iteration
            inputPath = jobOutputPath;
            iter++;
        }
        return inputPath;
    }

    //driver program that is called 10 times
    public static void calcPageRank(Configuration conf, Path inputPath, Path outputPath)
            throws Exception {
        //add total nodes count to Configuration object to be accessed later
        conf.setLong("totalPages", pageCount);
        Job job = Job.getInstance(conf, "PageRankJob");
        job.setJarByClass(mainDriver.class);
        job.setMapperClass(pagerank.Map.class);
        job.setReducerClass(pagerank.Reduce.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        if (!job.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }
    }

    //job to create input file with initial page ranks initialised
    public static void createInputFile(Configuration conf, Path file, Path targetFile)
            throws Exception {
        conf.setLong("totalPages", pageCount);
        Job job = Job.getInstance(conf, "InitialRankJob");
        job.setJarByClass(initialPagerank.class);
        job.setMapperClass(initialPagerank.Map.class);
        job.setInputFormatClass(KeyValueTextInputFormat.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        FileInputFormat.setInputPaths(job, file);
        FileOutputFormat.setOutputPath(job, targetFile);

        if (!job.waitForCompletion(true)) {
            throw new Exception("Job failed");
        }
    }

    //job to get the top 100 pages by page rank
    public void sortPages(Configuration conf, Path file, String targetFile) throws Exception {
        Job job = Job.getInstance(conf, "Top 10 by page rank");
        job.setJarByClass(sortNodeRanks.class);
        job.setMapperClass(sortNodeRanks.mapper.class);
        job.setReducerClass(sortNodeRanks.reducer.class);
        job.setNumReduceTasks(1);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, file);
        FileOutputFormat.setOutputPath(job, new Path(targetFile, "sortedHundred"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
