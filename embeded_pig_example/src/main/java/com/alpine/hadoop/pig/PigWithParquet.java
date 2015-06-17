package com.alpine.hadoop.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;

import java.security.PrivilegedExceptionAction;

/**
 * Created by Hao on 6/16/15.
 */
public class PigWithParquet {
    public static void main(final String[] args) throws Exception {
        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("mapred");
        ugi.doAs(new PrivilegedExceptionAction<PigWithParquet>() {
            public PigWithParquet run() throws Exception {
                PigWithParquet sample = new PigWithParquet();
                sample.runPig(args);
                return sample;
            }
        });
    }
    public void runPig(String[] args) throws Exception {
        Configuration conf = new Configuration();

        PigServer server = new PigServer(ExecType.MAPREDUCE, conf);
        server.registerJar("/Users/Hao/workspace/hadoop_example/embeded_pig_example/libjars/*.jar");
        //server.registerJar("/Users/Hao/workspace/hadoop_example/embeded_pig_example/target/embeded_pig_example-1.0.jar");
        //server.registerQuery("register 'hdfs://awshdp2regression.alpinenow.local:8020/tmp/*.jar';");
        //server.registerQuery("register '/tmp/*.jar';");
        server.registerQuery("A = load '/automation_test_data/parquet/golfnew_snappy_parquet/part-m-00000.snappy.parquet' USING parquet.pig.ParquetLoader();");
        //server.registerQuery("A = load '/automation_test_data/csv/apple_customers.csv' using PigStorage();");
        // This store will generate a job without reducer task, it will work
        server.store("A", "he/tmp/result_aaaxaaa");
    }
}
