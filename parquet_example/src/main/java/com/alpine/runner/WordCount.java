package com.alpine.runner;

import com.alpine.parquet.WCTokenizerMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import parquet.column.ColumnDescriptor;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.example.ExampleInputFormat;
import parquet.hadoop.metadata.ParquetMetadata;

import java.io.IOException;
import java.security.PrivilegedExceptionAction;
import java.util.List;


public class WordCount extends Configured implements Tool {
    public static void main(final String[] args) throws Exception {

//        UserGroupInformation ugi = UserGroupInformation
//                .createRemoteUser("mapred");
//        ugi.doAs(new PrivilegedExceptionAction<WordCount>() {
//            public WordCount run() throws Exception {
//                WordCount mr = new WordCount();
//                int res = ToolRunner.run(new Configuration(), mr, args);
//                return mr;
//            }
//        });
        printOutParquetSchema(args[0]);
    }

    public int run(String[] args) throws Exception {
        //creating a JobConf object and assigning a job name for identification purposes
        Configuration conf = getConf();
        conf.set("mapred.child.java.opts", "-Xmx8000m");
        conf.set("mapreduce.child.java.opts", "-Xmx8000m");
        conf.set("mapreduce.reduce.memory.mb", "6000");
        conf.set("mapreduce.reduce.java.opts", "-Djava.net.preferIPv4Stack=true -Xmx4294967296");


        Job job = Job.getInstance(conf);
        job.setJobName("WordCount");
        //Setting configuration object with the Data Type of output Key and Value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //Providing the mapper and reducer class names
        job.setMapperClass(WCTokenizerMapper.class);

        //the hdfs input and output directory to be fetched from the command line
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setInputFormatClass(ExampleInputFormat.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void printOutParquetSchema(String fileName) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "file:///");
        //conf.set("yarn.resourcemanager.scheduler.address", "local");
        ParquetMetadata m = ParquetFileReader.readFooter(conf, new Path(fileName));
        parquet.schema.MessageType schema = m.getFileMetaData().getSchema();
        List<ColumnDescriptor> columns = schema.getColumns();
        System.out.println(columns);

    }
}
