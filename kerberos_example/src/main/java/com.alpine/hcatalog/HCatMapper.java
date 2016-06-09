package com.alpine.hcatalog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hive.hcatalog.data.HCatRecord;

import java.io.IOException;

public class HCatMapper extends
        Mapper<WritableComparable, HCatRecord, Text, IntWritable> {

    public String temperature;

    @Override
    protected void map(
            WritableComparable key,
            HCatRecord value,
            Context context)
            throws IOException, InterruptedException {
        int numOfCols = value.size();
        System.out.println(numOfCols);
        temperature = value.get(4).toString();
        context.write(new Text(temperature), new IntWritable(1));
    }
}