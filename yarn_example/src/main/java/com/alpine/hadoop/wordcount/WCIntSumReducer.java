package com.alpine.hadoop.wordcount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class WCIntSumReducer extends
		Reducer<Text, IntWritable, Text, IntWritable> {
 
	protected IntWritable result = new IntWritable();

	public WCIntSumReducer(){
		
	}
	
	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		int sum = 0;
		System.out.println("reduce [" + this + "]: key =" + key.toString()+", values = ");
		for (IntWritable val : values) {
			int value = val.get(); 
			sum += value;
		 
 			System.out.print(value+",");
 		}
		result.set(sum);
		context.write(key, result);
	}
}