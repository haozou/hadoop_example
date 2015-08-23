package com.alpine.runner;

import com.alpine.hadoop.hcatalog.HCatMapper;
import com.alpine.hadoop.hcatalog.HCatReducer;
import com.alpine.utility.Utilities;
import com.facebook.fb303.FacebookBase;
import org.apache.commons.lang.ArrayUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatCreateTableDesc;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;
import org.apache.hive.hcatalog.mapreduce.HCatInputFormat;
import org.apache.thrift.meta_data.EnumMetaData;

import java.security.PrivilegedExceptionAction;
import java.util.ArrayList;
import java.util.List;

/**
 * @author hao
 */
public class HCatalogWordCount extends Configured implements Tool {

    public static final List<Class> libjars = new ArrayList<Class>() {{
        add(HCatRecord.class); //hcatalog-core
        //add(HCatClient.class); //hcatalog-webclient
        //add(HiveEndPoint.class);
        //add(HCatEventMessage.class);
        add(HiveMetaStore.class);
        add(EnumMetaData.class);
        //add(AbstractMapJoinOperator.class);
        add(FacebookBase.class);
    }};
    public static int exitCode = 0;

    public static void main(final String[] args) throws Exception {

        final String[] hadoopArgs = {"-libjars", Utilities.libjars(libjars)};

        UserGroupInformation ugi = UserGroupInformation
                .createRemoteUser("mapred");
        ugi.doAs(new PrivilegedExceptionAction<HCatalogWordCount>() {
            public HCatalogWordCount run() throws Exception {
                HCatalogWordCount mr = new HCatalogWordCount();
                exitCode = ToolRunner.run(new Configuration(), mr, (String[]) ArrayUtils.addAll(hadoopArgs, args));
                return mr;
            }
        });
        System.exit(exitCode);
    }

    public int run(String[] args) throws Exception {
        Configuration conf = getConf();

        // have to do these two set to overwrite the default hive-site.xml
        HiveConf hiveConf = new HiveConf();
        conf.set("fs.defaultFS", "hdfs://10.10.2.36:8020");

        conf.set("hive.metastore.client.connect.retry.delay", "1");
        conf.set("hive.metastore.client.socket.timeout", "600");
        conf.set("hive.metastore.uris", "thrift://10.10.2.36:9083");

        /*conf.set("javax.jdo.option.ConnectionURL", "jdbc:mysql://awshdp2regression.alpinenow.local/hivemetastoredb?createDatabaseIfNotExist=true");
        conf.set("javax.jdo.option.ConnectionDriverName", "org.mysql.jdbc.Driver");
        conf.set("javax.jdo.option.ConnectionUserName", "hive");
        conf.set("javax.jdo.option.ConnectionPassword", "hive");*/

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

        for (String name : client.listTableNamesByPattern("default", "*")) {
            System.err.println(name);
        }
        /*HCatClient example code end*/


        // Assume the default database
        String dbName = null;
        Job job = Job.getInstance(conf, "HCatalogWordCount");

        job.setJar("/Users/Hao/workspace/hadoop_example/hcatalog_example/target/hcatalog_example-1.0.jar");

        job.setMapperClass(HCatMapper.class);
        job.setReducerClass(HCatReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(WritableComparable.class);
        job.setOutputValueClass(IntWritable.class);
        hiveConf.set("fs.defaultFS", "hdfs://10.10.2.36:8020");
        Hive.get(hiveConf);
        /*Set the input table*/
        HCatInputFormat.setInput(job.getConfiguration(), dbName, inputTableName);
        job.setInputFormatClass(HCatInputFormat.class);
        /*HCatSchema inputSchema = HCatInputFormat.getTableSchema(job.getConfiguration());
        System.err.println("INFO: input schema is :" + inputSchema);
        System.err.println("INFO: input field is:" + inputSchema.getFieldNames());*/

		/*Set the output table*/
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(new Path(outputTableName))) {
            fs.delete(new Path(outputTableName), true);
        }
        FileOutputFormat.setOutputPath(job, new Path(outputTableName));
        /*HCatOutputFormat.setOutput(job, OutputJobInfo.create(dbName, outputTableName, null));
        job.setOutputFormatClass(HCatOutputFormat.class);
        HCatSchema outputSchema = HCatOutputFormat.getTableSchema(job.getConfiguration());
        System.err.println("INFO: output schema explicitly set for writing:" + outputSchema);
        HCatOutputFormat.setSchema(job, outputSchema);*/

        return (job.waitForCompletion(true) ? 0 : 1);
    }
}