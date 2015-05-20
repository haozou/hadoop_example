package com.alpine.runner;

import com.alpine.hadoop.hcatalog.HCatMapper;
import com.alpine.hadoop.hcatalog.HCatReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.data.DefaultHCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.data.schema.HCatSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.hive.hcatalog.mapreduce.HCatOutputFormat;
import org.apache.hive.hcatalog.mapreduce.OutputJobInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;

/**
 * @author hao
 */
public class HCatalogColumnFilter extends Configured implements Tool {

    public static int exitCode = 0;

    public static void main(final String[] args) throws Exception {


        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("hdfs");
        ugi.doAs(new PrivilegedExceptionAction<HCatalogColumnFilter>() {
            public HCatalogColumnFilter run() throws Exception {
                HCatalogColumnFilter mr = new HCatalogColumnFilter();
                exitCode = ToolRunner.run(new Configuration(), mr, args);
                return mr;
            }
        });
//		int exitCode = ToolRunner.run(conf, new HCatalogColumnFilter(), args);
		System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();
        conf.set("fs.default.name", "hdfs://awshdp2regression.alpinenow.local:8020");

        conf.set("mapreduce.framework.name", "yarn");
        conf.set("yarn.application.classpath", "/etc/hadoop/conf,/usr/lib/hadoop/*,/usr/lib/hadoop/lib/*,/usr/lib/hadoop-hdfs/*,/usr/lib/hadoop-hdfs/lib/*,/usr/lib/hadoop-yarn/*,/usr/lib/hadoop-yarn/lib/*,/usr/lib/hadoop-mapreduce/*,/usr/lib/hadoop-mapreduce/lib/*");
        conf.set("yarn.resourcemanager.address", "awshdp2regression.alpinenow.local:8050");
        conf.set("yarn.resourcemanager.scheduler.address", "awshdp2regression.alpinenow.local:8030");
        conf.set("hive.metastore.client.connect.retry.delay", "1");
        conf.set("hive.metastore.client.socket.timeout", "600");
        conf.set("hive.metastore.uris", "thrift://awshdp2regression.alpinenow.local:9083");

        conf.set("javax.jdo.option.ConnectionURL", "jdbc:mysql://awshdp2regression.alpinenow.local/hivemetastoredb?createDatabaseIfNotExist=true");

        conf.set("javax.jdo.option.ConnectionDriverName", "org.mysql.jdbc.Driver");
        conf.set("javax.jdo.option.ConnectionUserName", "hive");
        conf.set("javax.jdo.option.ConnectionPassword", "hive");
        args = new GenericOptionsParser(conf, args).getRemainingArgs();

        // Get the input and output table names as arguments
        String inputTableName = args[0];
        String outputTableName = args[1];

        /*HCatClient example code start*/
        HCatClient client = HCatClient.create(conf);
        PrimitiveTypeInfo typeInfo = new PrimitiveTypeInfo();

        ArrayList<HCatFieldSchema> cols = new ArrayList<HCatFieldSchema>();
        typeInfo.setTypeName("int");
        cols.add(new HCatFieldSchema("id", typeInfo, "id columns"));
        cols.add(new HCatFieldSchema("value", typeInfo, "value columns"));
        HCatCreateTableDesc tableDesc = HCatCreateTableDesc.create(null, args[1], cols).build();
        System.err.println("dbname: " + client.getDatabase("default").getName());
        if (client.listTableNamesByPattern(null, args[1]).isEmpty()) {
            System.err.println(args[1] + " is not exists, create it");
            client.createTable(tableDesc);
        }
        for (String name: client.listTableNamesByPattern("default", "*")) {
            System.err.println(name);
        }
        /*HCatClient example code end*/


        // Assume the default database
        String dbName = null;


        Job job = Job.getInstance(conf, "HCatalogColumnFilter");
        /*Set the input table*/
        HCatInputFormat.setInput(job, dbName, inputTableName);
        //job.setJarByClass(HCatalogColumnFilter.class);
        job.setJar("/Users/Hao/workspace/hadoop_example/hcatalog_example/target/hcatalog_example-1.0.jar");

        HCatSchema inputSchema = HCatInputFormat.getTableSchema(job.getConfiguration());


        System.err.println("INFO: input schema is :" + inputSchema);
        System.err.println("INFO: input field is:" + inputSchema.getFieldNames());
        job.setInputFormatClass(HCatInputFormat.class);
        job.setMapperClass(HCatMapper.class);
        job.setReducerClass(HCatReducer.class);
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(DefaultHCatRecord.class);

		/*Set the output table*/
        HCatOutputFormat.setOutput(job,
                OutputJobInfo.create(dbName, outputTableName, null));
        /**
         * will auto connect to HCatalog to get the table column information as
         * the output schema information this will work because the output table
         * is alreasy exists!!!
         */

        HCatSchema outputSchema = HCatOutputFormat.getTableSchema(job
                .getConfiguration());
        System.err.println("INFO: output schema explicitly set for writing:"
                + outputSchema);
        HCatOutputFormat.setSchema(job, outputSchema);
        job.setOutputFormatClass(HCatOutputFormat.class);

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}