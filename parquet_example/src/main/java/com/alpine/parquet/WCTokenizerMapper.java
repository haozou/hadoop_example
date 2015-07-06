package com.alpine.parquet;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import parquet.example.data.Group;
import parquet.schema.GroupType;

import java.io.IOException;

/**
 * take a simple test and split by token
 *
 * @author hao
 */
public class WCTokenizerMapper extends Mapper<LongWritable, Group, Text, IntWritable> {

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    public void map(LongWritable key, Group value, Context context)
            throws IOException, InterruptedException {
        GroupType schema = value.getType();
        int numOfCols = schema.getFieldCount();
        for (int i = 0; i < numOfCols; i++) {
            if (value.getFieldRepetitionCount(i) == 0) {
                continue;
            } else if (value.getFieldRepetitionCount(i) != 1 && !schema.getType(i).isPrimitive()) {
                continue;
            } else {
                word.set((value).getValueToString(i, 0));
                context.write(word, one);
            }
        }
    }
}