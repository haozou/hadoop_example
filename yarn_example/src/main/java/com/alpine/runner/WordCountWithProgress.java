package com.alpine.runner;

import java.util.Timer;
import java.util.TimerTask;

import com.alpine.hadoop.wordcount.progressable.SlowWCIntSumReducer;
import com.alpine.hadoop.wordcount.progressable.SlowWCTokenizerMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

 

public class WordCountWithProgress extends Configured implements Tool {

	private static final int interval = 500;// 0.5 seconds

	public int run(String[] args) throws Exception {
		// creating a JobConf object and assigning a job name for identification
		// purposes
		JobConf conf = new JobConf(getConf(), WordCountWithProgress.class);
		conf.setJobName("WordCount");

		// Setting configuration object with the Data Type of output Key and
		// Value
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);

		// mapred.jar -> see core-site.xml
		// conf.set("mapred.jar",
		// "/Users/zhaoyong/git/codeboyyong/hadoop-sample/wordcount_cli/target/wordcount_cli-1.0.jar");

		Job job = new Job(conf);

		job.setJarByClass(WordCountWithProgress.class);
		// Providing the mapper and reducer class names
		job.setMapperClass(SlowWCTokenizerMapper.class);
		job.setReducerClass(SlowWCIntSumReducer.class);

		// the hdfs input and output directory to be fetched from the command
		// line

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//startProgressTimer(job);
		job.waitForCompletion(true);
		return 0;
	}

	private void startProgressTimer(final Job job) {

		Timer timer = new Timer();
		TimerTask task = new  TimerTask(){

			@Override
			public void run() {
				try {
//					if(job.getStatus().equals(JobStatus.RUNNING)){
						System.out.println("[" + job.getStatus().getState() 
								+ "] map progress ="+job.mapProgress()+ " reduce progress = " +job.reduceProgress());	
//					}
					
				} catch (Exception e) {
 					//e.printStackTrace();
				}
			
				
				
			}
			
		};
		timer.schedule(task, interval, interval);
 
	}

	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(),
				new WordCountWithProgress(), args);
		System.exit(res);
	}
}
