package com.alpine.hadoop.hcatalog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;

public class HCatMapper extends
        Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {

    public int temperature;

    @Override
    protected void map(
            WritableComparable key,
            HCatRecord value,
            org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord,
                    IntWritable, IntWritable>.Context context)
            throws IOException, InterruptedException {
        int numOfCols = value.size();
        System.out.println(numOfCols);
        temperature = (Integer) value.get(2);
        context.write(new IntWritable(temperature), new IntWritable(1));
    }
}