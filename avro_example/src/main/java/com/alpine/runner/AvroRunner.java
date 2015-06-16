package com.alpine.runner;

import com.alpine.avro.AvroMapper;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
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

import java.security.PrivilegedExceptionAction;

/**
 * Created by Hao on 6/4/15.
 */
public class AvroRunner extends Configured implements Tool {
    public int run(String[] args) throws Exception {

        Configuration conf = getConf();

        FileSystem fs = FileSystem.get(conf);
        Job job = Job.getInstance(conf);

        job.setJarByClass(AvroRunner.class);

        job.setJobName("Color Count");

        job.setInputFormatClass(AvroKeyInputFormat.class);
        job.setMapperClass(AvroMapper.class);
        //File file = new File("users.avro");

        //DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
        //DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(file, datumReader);
        //AvroJob.setInputKeySchema(job, dataFileReader.getSchema());
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        if (fs.exists(new Path(args[1]))) {
            fs.delete(new Path(args[1]), true);
        }
        //System.out.println(file.getAbsolutePath());
        job.waitForCompletion(true);
        return 0;
    }

    public static void main(final String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("mapred");
        ugi.doAs(new PrivilegedExceptionAction<AvroRunner>() {
            public AvroRunner run() throws Exception {
                AvroRunner sample = new AvroRunner();
                ToolRunner.run(new Configuration(), sample, args);
                return sample;
            }
        });
    }
}
