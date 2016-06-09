package com.alpine.hcatalog;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by Hao on 5/19/15.
 */
public class HCatReducer extends Reducer<Text, IntWritable,
        WritableComparable, IntWritable> {


    @Override
    protected void reduce(
            Text key,
            Iterable<IntWritable> values,
            Context context)
            throws IOException, InterruptedException {
        int sum = 0;
        Iterator<IntWritable> iter = values.iterator();
        while (iter.hasNext()) {
            sum++;
            iter.next();
        }
        /*HCatRecord record = new DefaultHCatRecord(2);
        record.set(0, key.get());
        record.set(1, sum);
        context.write(null, record);*/
        context.write(new Text(key.toString()), new IntWritable(sum));
    }
}