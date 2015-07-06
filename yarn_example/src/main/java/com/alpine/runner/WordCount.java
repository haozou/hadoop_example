package com.alpine.runner;

import com.alpine.hadoop.wordcount.WCIntSumReducer;
import com.alpine.hadoop.wordcount.WCTokenizerMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.security.PrivilegedExceptionAction;


public class WordCount extends Configured implements Tool {
    public static void main(final String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("mapred");
        ugi.doAs(new PrivilegedExceptionAction<WordCount>() {
            public WordCount run() throws Exception {
                WordCount mr = new WordCount();
                int res = ToolRunner.run(new Configuration(), mr, args);
                return mr;
            }
        });
    }

    public int run(String[] args) throws Exception {
        //creating a JobConf object and assigning a job name for identification purposes
        JobConf conf = new JobConf(getConf(), WordCount.class);
        conf.setJobName("WordCount");


        //Setting configuration object with the Data Type of output Key and Value
        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        //mapred.jar -> see core-site.xml
        //  conf.set("mapred.jar", "/Users/zhaoyong/git/codeboyyong/hadoop-sample/wordcount_cli/target/wordcount_cli-1.0.jar");

        Job job = Job.getInstance(conf);

        //Providing the mapper and reducer class names
        job.setMapperClass(WCTokenizerMapper.class);
        job.setReducerClass(WCIntSumReducer.class);

        //the hdfs input and output directory to be fetched from the command line
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        job.waitForCompletion(true);
        return 0;
    }
}
