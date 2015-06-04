package com.alpine.hadoop.hcatalog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;

public class HCatMapper extends
        Mapper<WritableComparable, HCatRecord, IntWritable, IntWritable> {

    public int age;

    @Override
    protected void map(
            WritableComparable key,
            HCatRecord value,
            org.apache.hadoop.mapreduce.Mapper<WritableComparable, HCatRecord,
                    IntWritable, IntWritable>.Context context)
            throws IOException, InterruptedException {
        int numOfCols = value.size();
        System.out.println(numOfCols);
        age = (Integer) value.get(1);
        context.write(new IntWritable(age), new IntWritable(1));
    }
}