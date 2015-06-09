package com.alpine.avro;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by Hao on 6/4/15.
 */
public class AvroMapper extends
        Mapper<AvroKey<GenericRecord>, NullWritable, Text, IntWritable> {
    @Override
    public void map(AvroKey<GenericRecord> key, NullWritable value, Context context) throws IOException, InterruptedException {
        int size = key.datum().getSchema().getFields().size();
        String str = "";
        for (int i = 0; i < size; i++) {
            str += key.datum().get(i) + ",";
        }
        context.write(new Text(str), new IntWritable(1));
    }
}
